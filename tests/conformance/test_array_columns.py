"""Array functions must work on stored ARRAY columns, not just literals.

SnowDuck models Snowflake's ARRAY as DuckDB JSON (Snowflake arrays are
heterogeneous, DuckDB lists are not). DuckDB's list functions only accept a
real T[], so before the coercion pass only 3 of these 16 bound at all - every
array function applied to a table column failed.
"""

import pytest

ARRAY_EXPRESSIONS = [
    "ARRAY_SIZE(tags)",
    "ARRAY_CONTAINS(2, tags)",
    "ARRAY_SLICE(tags, 0, 2)",
    "ARRAY_TO_STRING(tags, ',')",
    "ARRAY_MIN(tags)",
    "ARRAY_MAX(tags)",
    "ARRAY_SUM(tags)",
    "ARRAY_DISTINCT(tags)",
    "ARRAY_REVERSE(tags)",
    "ARRAY_SORT(tags)",
    "ARRAY_APPEND(tags, 9)",
    "ARRAY_PREPEND(tags, 0)",
    "ARRAY_CAT(tags, tags)",
    "ARRAY_COMPACT(tags)",
    "ARRAY_POSITION(2, tags)",
    "ARRAYS_OVERLAP(tags, tags)",
    "GET(tags, 0)",
]


@pytest.fixture
def array_table(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE array_col_t (tags ARRAY)")
        cur.execute("INSERT INTO array_col_t VALUES ([1, 2, 3])")
    return conn


@pytest.mark.parametrize("expression", ARRAY_EXPRESSIONS)
def test_array_function_on_column(array_table, expression):
    with array_table.cursor() as cur:
        cur.execute(f"SELECT {expression} FROM array_col_t")
        cur.fetchone()


def test_array_values_on_column(array_table):
    with array_table.cursor() as cur:
        cur.execute("""
            SELECT ARRAY_SIZE(tags),
                   ARRAY_CONTAINS(2, tags),
                   ARRAY_CONTAINS(9, tags),
                   ARRAY_MIN(tags),
                   ARRAY_MAX(tags),
                   ARRAY_SUM(tags)
            FROM array_col_t
        """)
        size, has_2, has_9, low, high, total = cur.fetchone()

    assert size == 3
    assert has_2 is True
    assert has_9 is False
    assert (float(low), float(high), float(total)) == (1.0, 3.0, 6.0)


def test_array_min_max_are_numeric_not_lexicographic(conn):
    """JSON text comparison would rank '10' below '2'."""
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE array_num_t (tags ARRAY)")
        cur.execute("INSERT INTO array_num_t VALUES ([2, 10])")
        cur.execute("SELECT ARRAY_MIN(tags), ARRAY_MAX(tags) FROM array_num_t")
        low, high = cur.fetchone()

    assert (float(low), float(high)) == (2.0, 10.0)
