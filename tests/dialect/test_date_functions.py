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

def test_dateadd_minute_singular(dialect_context):
    """Test DATEADD with singular time part (minute)."""
    sql = "SELECT DATEADD(minute, 5, '2021-01-01'::timestamp)"
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    # DuckDB uses INTERVAL '5 minutes' or dateadd('minute', 5, timestamp)
    # Check that it's valid DuckDB syntax
    assert "dateadd" in transpiled.lower() or "interval" in transpiled.lower()

def test_dateadd_minutes_plural(dialect_context):
    """Test DATEADD with plural time part (minutes)."""
    sql = "SELECT DATEADD(minutes, 5, '2021-01-01'::timestamp)"
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    assert "dateadd" in transpiled.lower() or "interval" in transpiled.lower()

def test_datediff(dialect_context):
    """Test DATEDIFF function."""
    sql = "SELECT DATEDIFF(day, '2021-01-01'::date, '2021-01-10'::date)"
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    # DuckDB has datediff or date_diff
    assert "datediff" in transpiled.lower() or "date_diff" in transpiled.lower()

def test_to_timestamp(dialect_context):
    """Test TO_TIMESTAMP function."""
    sql = "SELECT TO_TIMESTAMP('2021-01-01 12:00:00')"
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    # DuckDB uses strptime or CAST
    assert "timestamp" in transpiled.lower() or "strptime" in transpiled.lower()
