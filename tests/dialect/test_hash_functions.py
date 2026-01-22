"""Tests for Snowflake hash function compatibility."""

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


def test_md5(dialect_context):
    """Test MD5 hash function."""
    sql = "SELECT MD5('hello')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # MD5 of 'hello' is a well-known value
    assert res[0] == "5d41402abc4b2a76b9719d911017c592"


def test_sha1(dialect_context):
    """Test SHA1 hash function."""
    sql = "SELECT SHA1('hello')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # SHA1 of 'hello'
    assert res[0] == "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"


def test_hash_function(dialect_context):
    """Test HASH function (returns integer hash)."""
    sql = "SELECT HASH('hello')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # HASH returns a bigint
    assert isinstance(res[0], int)


def test_hash_multiple_values(dialect_context):
    """Test HASH with multiple values."""
    sql = "SELECT HASH('hello', 'world')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert isinstance(res[0], int)


def test_sha256(dialect_context):
    """Test SHA256 hash function."""
    sql = "SELECT SHA2('hello', 256)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # SHA256 of 'hello'
    assert res[0] == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
