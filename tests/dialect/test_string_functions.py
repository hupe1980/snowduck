"""Tests for Snowflake string function compatibility."""

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


def test_string_concat(dialect_context):
    """Test CONCAT function with multiple arguments."""
    sql = "SELECT CONCAT('Hello', ' ', 'World')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    # Verify it executes correctly
    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "Hello World"


def test_split_part(dialect_context):
    """Test SPLIT_PART function for string tokenization."""
    sql = "SELECT SPLIT_PART('a,b,c', ',', 2)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "b"


def test_startswith(dialect_context):
    """Test STARTSWITH function."""
    sql = "SELECT STARTSWITH('snowflake', 'snow')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is True


def test_endswith(dialect_context):
    """Test ENDSWITH function."""
    sql = "SELECT ENDSWITH('snowflake', 'flake')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is True


def test_contains(dialect_context):
    """Test CONTAINS function for substring matching."""
    sql = "SELECT CONTAINS('snowflake', 'flake')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # 'snowflake' contains 'flake'
    assert res[0] is True


def test_regexp_replace(dialect_context):
    """Test REGEXP_REPLACE for pattern-based substitution."""
    sql = "SELECT REGEXP_REPLACE('abc123def', '[0-9]+', 'X')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "abcXdef"


def test_left_right_functions(dialect_context):
    """Test LEFT and RIGHT string extraction functions."""
    sql = "SELECT LEFT('snowflake', 4), RIGHT('snowflake', 5)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "snow"
    assert res[1] == "flake"


def test_reverse(dialect_context):
    """Test REVERSE function."""
    sql = "SELECT REVERSE('abc')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "cba"
