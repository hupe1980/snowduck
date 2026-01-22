"""Tests for Snowflake aggregate function compatibility."""

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


def test_listagg(dialect_context):
    """Test LISTAGG aggregate function."""
    sql = """
    SELECT LISTAGG(col1, ',') WITHIN GROUP (ORDER BY col1)
    FROM (VALUES ('a'), ('b'), ('c')) AS t(col1)
    """
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "a,b,c"


def test_median(dialect_context):
    """Test MEDIAN aggregate function."""
    sql = """
    SELECT MEDIAN(col1)
    FROM (VALUES (1), (2), (3), (4), (5)) AS t(col1)
    """
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 3


def test_approx_count_distinct(dialect_context):
    """Test APPROX_COUNT_DISTINCT aggregate function."""
    sql = """
    SELECT APPROX_COUNT_DISTINCT(col1)
    FROM (VALUES (1), (2), (2), (3), (3), (3)) AS t(col1)
    """
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Should be approximately 3
    assert res[0] in [3, 4]  # Allow for approximation


def test_mode(dialect_context):
    """Test MODE aggregate function."""
    sql = """
    SELECT MODE(col1)
    FROM (VALUES (1), (2), (2), (3)) AS t(col1)
    """
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 2  # Most frequent value


def test_array_agg(dialect_context):
    """Test ARRAY_AGG aggregate function."""
    sql = """
    SELECT ARRAY_AGG(col1)
    FROM (VALUES (1), (2), (3)) AS t(col1)
    """
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Result should be an array/list
    assert len(res[0]) == 3
    assert set(res[0]) == {1, 2, 3}


def test_percentile_cont(dialect_context):
    """Test PERCENTILE_CONT for continuous percentiles."""
    sql = """
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col1)
    FROM (VALUES (1), (2), (3), (4), (5)) AS t(col1)
    """
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # 50th percentile of [1,2,3,4,5] should be 3
    assert res[0] == 3.0


def test_stddev_pop(dialect_context):
    """Test STDDEV_POP for population standard deviation."""
    sql = """
    SELECT STDDEV_POP(col1)
    FROM (VALUES (1), (2), (3), (4), (5)) AS t(col1)
    """
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Population stddev of [1,2,3,4,5]
    assert res[0] > 0  # Should be non-zero


def test_variance(dialect_context):
    """Test VARIANCE aggregate function."""
    sql = """
    SELECT VARIANCE(col1)
    FROM (VALUES (1), (2), (3), (4), (5)) AS t(col1)
    """
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    
    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Variance should be positive
    assert res[0] > 0
