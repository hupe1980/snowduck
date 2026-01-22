"""Tests for Snowflake regex function compatibility."""

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


def test_regexp_like(dialect_context):
    """Test REGEXP_LIKE function for pattern matching."""
    sql = "SELECT REGEXP_LIKE('hello123', '[a-z]+[0-9]+')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is True


def test_regexp_like_no_match(dialect_context):
    """Test REGEXP_LIKE when pattern doesn't match."""
    sql = "SELECT REGEXP_LIKE('hello', '[0-9]+')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is False


def test_regexp_substr(dialect_context):
    """Test REGEXP_SUBSTR function for extracting substrings."""
    sql = "SELECT REGEXP_SUBSTR('abc123def456', '[0-9]+')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Should extract first numeric sequence
    assert res[0] == "123"


def test_regexp_replace(dialect_context):
    """Test REGEXP_REPLACE function for pattern substitution."""
    sql = "SELECT REGEXP_REPLACE('hello123world', '[0-9]+', 'X')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "helloXworld"


def test_regexp_replace_all(dialect_context):
    """Test REGEXP_REPLACE with 'g' flag replaces all occurrences."""
    sql = "SELECT REGEXP_REPLACE('a1b2c3', '[0-9]', 'X', 'g')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "aXbXcX"


def test_regexp_count(dialect_context):
    """Test REGEXP_COUNT function for counting matches."""
    sql = "SELECT REGEXP_COUNT('abc123def456', '[0-9]+')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Two numeric sequences: 123 and 456
    assert res[0] == 2


def test_regexp_count_no_match(dialect_context):
    """Test REGEXP_COUNT with no matches."""
    sql = "SELECT REGEXP_COUNT('no numbers here', '[0-9]+')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 0


def test_rlike_alias(dialect_context):
    """Test RLIKE (alias for REGEXP_LIKE)."""
    sql = "SELECT 'hello' RLIKE 'ell'"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is True
