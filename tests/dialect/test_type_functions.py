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

def test_try_cast(dialect_context):
    """Test TRY_CAST function."""
    sql = "SELECT TRY_CAST('123' AS INTEGER)"
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    # DuckDB has TRY_CAST natively
    assert "try_cast" in transpiled.lower()

def test_try_to_number(dialect_context):
    """Test TRY_TO_NUMBER function (Snowflake-specific)."""
    sql = "SELECT TRY_TO_NUMBER('123.45')"
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    # Should transpile to TRY_CAST or similar
    assert "try_cast" in transpiled.lower() or "try_to_number" in transpiled.lower()

def test_try_to_decimal(dialect_context):
    """Test TRY_TO_DECIMAL function."""
    sql = "SELECT TRY_TO_DECIMAL('123.45', 10, 2)"
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    # Should map to TRY_CAST(...  AS DECIMAL(10,2))
    assert "decimal" in transpiled.lower()

def test_iff_function(dialect_context):
    """Test IFF function (Snowflake's inline IF)."""
    sql = "SELECT IFF(col1 > 10, 'high', 'low') FROM my_table"
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    # DuckDB uses IF or CASE WHEN
    assert "if" in transpiled.lower() or "case" in transpiled.lower()
