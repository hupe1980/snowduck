"""Tests for extended date functions."""

import duckdb
from sqlglot import parse_one


def test_add_months(dialect_context):
    """Test ADD_MONTHS adds months to a date."""
    sql = "SELECT ADD_MONTHS(DATE '2024-01-15', 2)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Result is a timestamp, check the date part
    assert "2024-03-15" in str(res[0])


def test_add_months_negative(dialect_context):
    """Test ADD_MONTHS with negative months."""
    sql = "SELECT ADD_MONTHS(DATE '2024-03-15', -2)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert "2024-01-15" in str(res[0])


def test_add_months_end_of_month(dialect_context):
    """Test ADD_MONTHS handles end-of-month correctly."""
    sql = "SELECT ADD_MONTHS(DATE '2024-01-31', 1)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Feb 2024 has 29 days (leap year)
    assert "2024-02-29" in str(res[0])


def test_strtok(dialect_context):
    """Test STRTOK extracts tokens from a string."""
    sql = "SELECT STRTOK('a,b,c', ',', 1)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "a"


def test_strtok_second_token(dialect_context):
    """Test STRTOK extracts second token."""
    sql = "SELECT STRTOK('hello-world-test', '-', 2)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "world"


def test_strtok_third_token(dialect_context):
    """Test STRTOK extracts third token."""
    sql = "SELECT STRTOK('a|b|c|d', '|', 3)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "c"
