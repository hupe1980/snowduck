"""Array representation and date-arithmetic result types.

SnowDuck stores a Snowflake ARRAY as DuckDB JSON, while functions like SPLIT
return a native DuckDB list. Both have to work as arguments to the array
functions - casting straight to JSON[] re-parsed each element and blew up on
plain strings.
"""

import datetime

import pytest

# Expressions producing a native DuckDB list rather than JSON.
NATIVE_LIST_SOURCES = [
    "SPLIT('a,b,c', ',')",
    "STRTOK_TO_ARRAY('a.b', '.')",
    'OBJECT_KEYS(PARSE_JSON(\'{"a":1,"b":2}\'))',
    "ARRAY_CONSTRUCT('a', 'b')",
]


@pytest.mark.parametrize("source", NATIVE_LIST_SOURCES)
def test_array_size_over_native_list(conn, source):
    with conn.cursor() as cur:
        cur.execute(f"SELECT ARRAY_SIZE({source})")
        assert cur.fetchone()[0] >= 2


def test_array_to_string_does_not_quote(conn):
    """Joining must use the element values, not their JSON spelling."""
    with conn.cursor() as cur:
        cur.execute("SELECT ARRAY_TO_STRING(SPLIT('a,b', ','), '-')")
        assert cur.fetchone()[0] == "a-b"

        cur.execute("SELECT ARRAY_TO_STRING(ARRAY_CONSTRUCT('a', 'b'), '-')")
        assert cur.fetchone()[0] == "a-b"


def test_array_contains_over_native_list(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ARRAY_CONTAINS('a', SPLIT('a,b', ',')), "
            "ARRAY_CONTAINS('z', SPLIT('a,b', ','))"
        )
        assert cur.fetchone() == (True, False)


def test_array_functions_over_json(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT ARRAY_SIZE(PARSE_JSON('[1,2,3]'))")
        assert cur.fetchone()[0] == 3


# ---------------------------------------------------------------------------
# DATEADD / ADD_MONTHS result types
# ---------------------------------------------------------------------------

# Snowflake: "If date_or_time_part is day or larger ... the function returns a
# DATE value"; anything smaller returns TIMESTAMP_NTZ.
DATE_PRESERVING = ["day", "week", "month", "quarter", "year"]


@pytest.mark.parametrize("unit", DATE_PRESERVING)
def test_dateadd_on_a_date_returns_a_date(conn, unit):
    with conn.cursor() as cur:
        cur.execute(f"SELECT DATEADD({unit}, 1, '2024-01-31'::DATE)")
        value = cur.fetchone()[0]

    assert isinstance(value, datetime.date)
    assert not isinstance(value, datetime.datetime)


@pytest.mark.parametrize("unit", ["hour", "minute", "second"])
def test_dateadd_below_a_day_returns_a_timestamp(conn, unit):
    with conn.cursor() as cur:
        cur.execute(f"SELECT DATEADD({unit}, 1, '2024-01-31'::DATE)")
        assert isinstance(cur.fetchone()[0], datetime.datetime)


def test_add_months_is_a_date_and_clamps_to_month_end(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT ADD_MONTHS('2024-01-31'::DATE, 1)")
        value = cur.fetchone()[0]

    assert isinstance(value, datetime.date)
    assert not isinstance(value, datetime.datetime)
    assert str(value) == "2024-02-29"


def test_add_months_backwards(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT ADD_MONTHS('2024-03-31'::DATE, -1)")
        assert str(cur.fetchone()[0]) == "2024-02-29"


def test_add_months_mid_month_is_not_clamped(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT ADD_MONTHS('2024-01-15'::DATE, 1)")
        assert str(cur.fetchone()[0]) == "2024-02-15"
