"""Snowflake's numeric format model for TO_CHAR / TO_VARCHAR.

Every expectation here is one of the examples in Snowflake's `Number Formats`
documentation, or follows directly from the rules it states: the result is
right-justified in a field as wide as the model plus a column for the sign, and
a value too wide for the model's integer positions renders as `#`.

The failure this guards against is silent: sqlglot drops a format it cannot
render, so `TO_CHAR(1234.5, '9999.99')` used to come back as the bare number.
"""

import pytest

# (id, sql, expected) - the SQL brackets the result so trailing blanks are visible.
CASES: list[tuple[str, str, str | None]] = [
    # -- Snowflake's own documented examples --------------------------------
    ("negative_rounds", "TO_CHAR(-12.391, '99.9')", "-12.4"),
    ("leading_zero_suppressed", "TO_CHAR(0.5, '99.9')", "   .5"),
    ("leading_sign_and_zero_fill", "TO_CHAR(-1234.56, 'S000000.00')", "-001234.56"),
    ("blank_when_zero", "TO_CHAR(0, 'B9999.9')", ""),
    ("trailing_minus", "TO_CHAR(-1234.56, '9999.99MI')", " 1234.56-"),
    ("angle_brackets", "TO_CHAR(-1234.56, '9999.99PR')", "<1234.56>"),
    ("currency_and_grouping", "TO_CHAR(1234.56, '$9,999.99')", " $1,234.56"),
    ("overflow_hashes", "TO_CHAR(12345.67, '9999.99')", "########"),
    ("text_minimum", "TO_CHAR(1234.56, 'TM9')", "1234.56"),
    # -- The rules those examples follow from -------------------------------
    ("too_few_integer_digits", "TO_CHAR(1234.5, '999.99')", "#######"),
    ("pads_the_scale", "TO_CHAR(1234.5, '9999.99')", " 1234.50"),
    ("to_varchar_is_the_same", "TO_VARCHAR(1234.5, '9,999.99')", " 1,234.50"),
    ("zero_fill_keeps_grouping", "TO_CHAR(1234.56, '0,000,000.00')", " 0,001,234.56"),
    ("grouping_at_the_boundary", "TO_CHAR(1000000, '9,999,999')", " 1,000,000"),
    ("negative_below_one", "TO_CHAR(-0.5, '99.9')", "  -.5"),
    ("leading_sign_on_positive", "TO_CHAR(12.3, 'S99.9')", "+12.3"),
    ("trailing_minus_on_positive", "TO_CHAR(1234.56, '9999.99MI')", " 1234.56 "),
    ("angle_brackets_on_positive", "TO_CHAR(1234.56, '9999.99PR')", " 1234.56 "),
    ("integer_only_model", "TO_CHAR(42, '999')", "  42"),
    ("rounds_into_overflow", "TO_CHAR(999.6, '999')", "####"),
    # `D` and `G` are the locale-independent spellings of `.` and `,`.
    ("locale_separators", "TO_CHAR(1234.5, '9G999D99')", " 1,234.50"),
]

NULL_CASES = [
    ("null_input", "TO_CHAR(NULL, '999.99')"),
]

# A date format in the same argument position must keep working: the two
# families are told apart by their alphabet, not by the argument's type.
DATE_CASES: list[tuple[str, str, str]] = [
    ("date_format", "TO_CHAR('2024-01-15'::DATE, 'MM/DD/YYYY')", "01/15/2024"),
    (
        "timestamp_format",
        "TO_CHAR('2024-01-15'::TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS')",
        "2024-01-15 00:00:00",
    ),
    ("no_format_at_all", "TO_CHAR(1234.5)", "1234.5"),
]


@pytest.mark.parametrize(
    "expression,expected",
    [(expression, expected) for _, expression, expected in CASES + DATE_CASES],
    ids=[case_id for case_id, _, _ in CASES + DATE_CASES],
)
def test_number_format(conn, expression, expected):
    with conn.cursor() as cur:
        cur.execute(f"SELECT {expression}")
        assert cur.fetchone()[0] == expected


@pytest.mark.parametrize(
    "expression",
    [expression for _, expression in NULL_CASES],
    ids=[case_id for case_id, _ in NULL_CASES],
)
def test_number_format_null(conn, expression):
    with conn.cursor() as cur:
        cur.execute(f"SELECT {expression}")
        assert cur.fetchone()[0] is None


def test_number_format_applies_to_a_column(conn):
    """The model has to survive a non-literal argument, which carries no type."""
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE amounts (amount NUMBER(10, 2))")
        cur.execute("INSERT INTO amounts VALUES (1234.5), (-7.25), (0)")
        cur.execute("SELECT TO_CHAR(amount, '9,999.99') FROM amounts ORDER BY amount")
        assert [row[0] for row in cur.fetchall()] == [
            "    -7.25",
            "      .00",
            " 1,234.50",
        ]
