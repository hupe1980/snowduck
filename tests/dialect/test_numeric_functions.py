"""Tests for Snowflake numeric/math functions.

These functions are commonly used in production Snowflake queries
and SnowDuck should support them with correct Snowflake semantics.
"""

import duckdb
import pytest
from sqlglot import parse_one

from snowduck.dialect.context import DialectContext
from snowduck.info_schema.manager import InfoSchemaManager


@pytest.fixture
def dialect_context():
    conn = duckdb.connect()
    manager = InfoSchemaManager(conn)
    return DialectContext(info_schema_manager=manager)


def test_abs(dialect_context):
    """Test ABS function - absolute value."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT ABS(-42), ABS(42)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 42
    assert res[1] == 42


def test_ceil_floor(dialect_context):
    """Test CEIL and FLOOR functions."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT CEIL(3.2), FLOOR(3.8)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 4
    assert res[1] == 3


def test_round(dialect_context):
    """Test ROUND function with precision."""

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    # Round to 2 decimal places
    sql = "SELECT ROUND(3.14159, 2)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    # DuckDB returns Decimal
    assert float(res[0]) == pytest.approx(3.14, abs=0.001)


def test_trunc(dialect_context):
    """Test TRUNC/TRUNCATE function."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT TRUNC(3.789, 1)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    # DuckDB returns Decimal
    assert float(res[0]) == pytest.approx(3.7, abs=0.001)


def test_mod(dialect_context):
    """Test MOD function - modulo operation."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT MOD(10, 3)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 1


def test_sign(dialect_context):
    """Test SIGN function - returns -1, 0, or 1."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT SIGN(-5), SIGN(0), SIGN(5)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == -1
    assert res[1] == 0
    assert res[2] == 1


def test_sqrt(dialect_context):
    """Test SQRT function - square root."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT SQRT(16)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 4.0


def test_power(dialect_context):
    """Test POWER/POW function - exponentiation."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT POWER(2, 10)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 1024


def test_log_ln(dialect_context):
    """Test LOG and LN functions."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    # LN (natural log)
    sql = "SELECT LN(2.718281828)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert abs(res[0] - 1.0) < 0.001


def test_exp(dialect_context):
    """Test EXP function - e raised to power."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT EXP(1)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert abs(res[0] - 2.718281828) < 0.001


def test_greatest_least(dialect_context):
    """Test GREATEST and LEAST functions."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT GREATEST(1, 5, 3), LEAST(1, 5, 3)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 5
    assert res[1] == 1


def test_div0(dialect_context):
    """Test DIV0 function - division that returns 0 instead of error on divide by zero."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT DIV0(10, 2), DIV0(10, 0)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 5
    assert res[1] == 0


def test_div0null(dialect_context):
    """Test DIV0NULL function - returns NULL instead of error on divide by zero."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT DIV0NULL(10, 2), DIV0NULL(10, 0)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 5
    assert res[1] is None


def test_width_bucket(dialect_context):
    """Test WIDTH_BUCKET function - histogram binning."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    # WIDTH_BUCKET(expr, min, max, num_buckets)
    # Buckets: 0 (below min), 1-num_buckets (within range), num_buckets+1 (above max)

    # Value 5 in range 0-10 with 5 buckets -> bucket 3
    sql = "SELECT WIDTH_BUCKET(5, 0, 10, 5)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert int(res[0]) == 3

    # Value below min -> bucket 0
    sql = "SELECT WIDTH_BUCKET(-1, 0, 10, 5)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert int(res[0]) == 0

    # Value at/above max -> bucket num_buckets + 1
    sql = "SELECT WIDTH_BUCKET(10, 0, 10, 5)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert int(res[0]) == 6  # 5 + 1


def test_random(dialect_context):
    """Test RANDOM function."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT RANDOM()"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    # Random returns a value
    assert res[0] is not None


def test_bitwise_operations(dialect_context):
    """Test bitwise operations - AND, OR, XOR, NOT."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT BITAND(12, 10), BITOR(12, 10), BITXOR(12, 10)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    # 12 = 1100, 10 = 1010
    # AND = 1000 = 8, OR = 1110 = 14, XOR = 0110 = 6
    assert res[0] == 8
    assert res[1] == 14
    assert res[2] == 6
