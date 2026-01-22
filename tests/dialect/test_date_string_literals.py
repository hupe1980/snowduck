"""Tests for date functions with string literal arguments.

These tests verify that date functions properly handle string literals
without explicit CAST, matching Snowflake's implicit conversion behavior.
"""

import duckdb
from sqlglot import parse_one


def test_add_months_string_literal(dialect_context):
    """Test ADD_MONTHS with string literal date (no explicit CAST)."""
    sql = "SELECT ADD_MONTHS('2024-01-15', 3)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert "2024-04-15" in str(res[0])


def test_add_months_end_of_month_string_literal(dialect_context):
    """Test ADD_MONTHS handles end-of-month with string literal."""
    sql = "SELECT ADD_MONTHS('2024-01-31', 1)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Feb 2024 has 29 days (leap year)
    assert "2024-02-29" in str(res[0])


def test_date_trunc_string_literal(dialect_context):
    """Test DATE_TRUNC with string literal date."""
    sql = "SELECT DATE_TRUNC('month', '2024-03-15')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert "2024-03-01" in str(res[0])


def test_last_day_string_literal(dialect_context):
    """Test LAST_DAY with string literal date."""
    sql = "SELECT LAST_DAY('2024-02-15')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Feb 2024 has 29 days (leap year)
    assert "2024-02-29" in str(res[0])


def test_year_string_literal(dialect_context):
    """Test YEAR with string literal date."""
    sql = "SELECT YEAR('2024-06-15')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 2024


def test_month_string_literal(dialect_context):
    """Test MONTH with string literal date."""
    sql = "SELECT MONTH('2024-06-15')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 6


def test_day_string_literal(dialect_context):
    """Test DAY with string literal date."""
    sql = "SELECT DAY('2024-06-15')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 15


def test_dayofweek_string_literal(dialect_context):
    """Test DAYOFWEEK with string literal date."""
    sql = "SELECT DAYOFWEEK('2024-01-15')"  # Monday
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Monday is 1 in DuckDB
    assert res[0] == 1


def test_extract_year_string_literal(dialect_context):
    """Test EXTRACT(YEAR FROM ...) with string literal date."""
    sql = "SELECT EXTRACT(YEAR FROM '2024-06-15')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 2024


def test_extract_month_string_literal(dialect_context):
    """Test EXTRACT(MONTH FROM ...) with string literal date."""
    sql = "SELECT EXTRACT(MONTH FROM '2024-06-15')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 6


def test_quarter_string_literal(dialect_context):
    """Test QUARTER with string literal date."""
    sql = "SELECT QUARTER('2024-06-15')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 2


def test_week_string_literal(dialect_context):
    """Test WEEK with string literal date."""
    sql = "SELECT WEEK('2024-01-15')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 3  # Jan 15, 2024 is in week 3


def test_dayofyear_string_literal(dialect_context):
    """Test DAYOFYEAR with string literal date."""
    sql = "SELECT DAYOFYEAR('2024-02-01')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 32  # Jan has 31 days, so Feb 1 is day 32


def test_date_functions_with_column_reference(dialect_context):
    """Test that date functions work with column references (not just literals)."""
    # This should still work - we don't cast column references
    sql = "SELECT YEAR(date_col) FROM (SELECT DATE '2024-06-15' as date_col)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 2024


def test_date_functions_with_explicit_cast(dialect_context):
    """Test that date functions with explicit CAST still work."""
    sql = "SELECT YEAR(DATE '2024-06-15')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 2024
