"""Type, NULL and ordering semantics.

These are the quiet ones: expressions that run either way and differ only in
the value or the column type that comes back.
"""

import pytest

# (id, sql, expected)
CASES: list[tuple[str, str, object]] = [
    # NULLs sort as the largest value, so ASC puts them last.
    (
        "null_sorts_last_asc",
        "SELECT id FROM (SELECT 1 id UNION ALL SELECT NULL) ORDER BY id",
        [1, None],
    ),
    (
        "null_sorts_first_desc",
        "SELECT id FROM (SELECT 1 id UNION ALL SELECT NULL) ORDER BY id DESC",
        [None, 1],
    ),
    # NULL propagation
    ("concat_null", "SELECT 'a' || NULL", None),
    ("replace_null", "SELECT REPLACE('abc', 'b', NULL)", None),
    ("length_null", "SELECT LENGTH(NULL)", None),
    ("greatest_null", "SELECT GREATEST(1, NULL)", None),
    ("sum_of_nulls", "SELECT SUM(x) FROM (SELECT NULL::INT x)", None),
    ("case_without_else", "SELECT CASE WHEN FALSE THEN 1 END", None),
    ("iff_null_condition", "SELECT IFF(NULL, 'y', 'n')", "n"),
    # Numbers
    ("division_is_exact", "SELECT 5/2", 2.5),
    ("round_half_away_from_zero", "SELECT ROUND(-2.5)", -3),
    ("truncate_towards_zero", "SELECT TRUNCATE(-2.7)", -2),
    ("ceil_negative", "SELECT CEIL(-1.5)", -1),
    ("floor_negative", "SELECT FLOOR(-1.5)", -2),
    ("mod_negative", "SELECT MOD(-5, 3)", -2),
    # Strings - 1-based, and position 0 behaves like 1
    ("substr_from_zero", "SELECT SUBSTR('abcdef', 0, 2)", "ab"),
    ("substr_negative_start", "SELECT SUBSTR('abcdef', -2)", "ef"),
    ("split_part_zero_is_first", "SELECT SPLIT_PART('a,b,c', ',', 0)", "a"),
    ("split_part_negative", "SELECT SPLIT_PART('a,b,c', ',', -1)", "c"),
    ("position_not_found_is_zero", "SELECT POSITION('z', 'abc')", 0),
    ("charindex_not_found_is_zero", "SELECT CHARINDEX('z', 'abc')", 0),
    ("trim_with_characters", "SELECT TRIM('xxaxx', 'x')", "a"),
    ("lpad_pads", "SELECT LPAD('ab', 5, '*')", "***ab"),
    ("lpad_truncates", "SELECT LPAD('abcdef', 3, 'x')", "abc"),
    # Booleans
    ("to_boolean_yes", "SELECT TO_BOOLEAN('yes')", True),
    ("try_to_boolean_bad", "SELECT TRY_TO_BOOLEAN('maybe')", None),
    # Dates
    ("last_day_leap_year", "SELECT LAST_DAY('2024-02-05'::DATE)", "2024-02-29"),
    (
        "datediff_months",
        "SELECT DATEDIFF(month, '2024-01-31'::DATE, '2024-02-29'::DATE)",
        1,
    ),
    ("to_date_with_format", "SELECT TO_DATE('01/15/2024', 'MM/DD/YYYY')", "2024-01-15"),
    (
        "to_char_with_format",
        "SELECT TO_CHAR('2024-01-15'::DATE, 'YYYY/MM/DD')",
        "2024/01/15",
    ),
    # VARIANT access
    ("colon_path", "SELECT PARSE_JSON('{\"a\":1}'):a::INT", 1),
    ("nested_colon_path", 'SELECT PARSE_JSON(\'{"a":{"b":2}}\'):a:b::INT', 2),
    ("bracket_path", "SELECT PARSE_JSON('{\"a\":1}')['a']::INT", 1),
    ("array_index_is_zero_based", "SELECT PARSE_JSON('[1,2,3]')[1]::INT", 2),
    ("missing_key_is_null", "SELECT PARSE_JSON('{\"a\":1}'):zz", None),
    # Aggregates
    (
        "listagg_within_group_orders",
        "SELECT LISTAGG(x, ',') WITHIN GROUP (ORDER BY x) FROM (SELECT 'b' x UNION ALL SELECT 'a')",
        "a,b",
    ),
    (
        "listagg_distinct",
        "SELECT LISTAGG(DISTINCT x, ',') FROM (SELECT 'a' x UNION ALL SELECT 'a')",
        "a",
    ),
    (
        "count_distinct",
        "SELECT COUNT(DISTINCT x) FROM (SELECT 1 x UNION ALL SELECT 1)",
        1,
    ),
    (
        "avg_of_integers_is_fractional",
        "SELECT AVG(x) FROM (SELECT 1 x UNION ALL SELECT 2)",
        1.5,
    ),
]


@pytest.mark.parametrize(
    "sql,expected",
    [(sql, expected) for _, sql, expected in CASES],
    ids=[case_id for case_id, _, _ in CASES],
)
def test_type_and_null_semantics(conn, sql, expected):
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    if isinstance(expected, list):
        assert [row[0] for row in rows] == expected
        return

    actual = rows[0][0] if rows else None
    if expected is None:
        assert actual is None
        return
    if isinstance(expected, bool):
        assert actual is expected
        return
    assert actual == expected or str(actual) == str(expected)
