"""Tests for Snowflake UUID and utility function compatibility."""

import re

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


def test_uuid_string(dialect_context):
    """Test UUID_STRING function generates valid UUIDs."""
    sql = "SELECT UUID_STRING()"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()

    # DuckDB returns UUID object, convert to string for validation
    uuid_str = str(res[0])
    # Verify it's a valid UUID format (8-4-4-4-12 hex characters)
    uuid_pattern = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )
    assert uuid_pattern.match(uuid_str)


def test_uuid_uniqueness(dialect_context):
    """Test UUID_STRING generates unique values."""
    sql = "SELECT UUID_STRING(), UUID_STRING()"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()

    # Two UUIDs should be different
    assert res[0] != res[1]


def test_typeof(dialect_context):
    """Test TYPEOF function returns type information."""
    sql = "SELECT TYPEOF(123), TYPEOF('hello'), TYPEOF(3.14)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()

    # DuckDB returns type names
    assert "INT" in res[0].upper()
    assert "VARCHAR" in res[1].upper()
    # DOUBLE or DECIMAL for floating point
    assert "DOUBLE" in res[2].upper() or "DECIMAL" in res[2].upper()


def test_current_timestamp(dialect_context):
    """Test CURRENT_TIMESTAMP function."""
    from datetime import datetime

    sql = "SELECT CURRENT_TIMESTAMP()"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()

    # Should return a timestamp
    assert res[0] is not None
    assert res[0].year == datetime.now().year


def test_current_user(dialect_context):
    """Test CURRENT_USER function."""
    sql = "SELECT CURRENT_USER()"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()

    # DuckDB should return something (usually empty string or user)
    assert res[0] is not None


def test_coalesce(dialect_context):
    """Test COALESCE function returns first non-NULL."""
    sql = "SELECT COALESCE(NULL, NULL, 'first_non_null', 'second')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "first_non_null"


def test_nullif(dialect_context):
    """Test NULLIF function returns NULL when equal."""
    sql = "SELECT NULLIF(5, 5), NULLIF(5, 3)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is None
    assert res[1] == 5
