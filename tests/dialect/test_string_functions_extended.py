"""Tests for extended string functions."""

import duckdb
from sqlglot import parse_one


def test_space(dialect_context):
    """Test SPACE function generates repeated spaces."""
    sql = "SELECT SPACE(5)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "     "
    assert len(res[0]) == 5


def test_space_zero(dialect_context):
    """Test SPACE(0) returns empty string."""
    sql = "SELECT SPACE(0)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == ""


def test_truncate(dialect_context):
    """Test TRUNCATE function truncates to specified decimal places."""
    sql = "SELECT TRUNCATE(3.567, 1)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 3.5


def test_truncate_zero_decimals(dialect_context):
    """Test TRUNCATE with zero decimal places."""
    sql = "SELECT TRUNCATE(123.999, 0)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 123


def test_truncate_negative(dialect_context):
    """Test TRUNCATE with negative number."""
    sql = "SELECT TRUNCATE(-3.567, 1)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == -3.5
