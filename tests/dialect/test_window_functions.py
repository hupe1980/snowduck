"""Tests for Snowflake window function compatibility."""

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


def test_row_number(dialect_context):
    """Test ROW_NUMBER window function."""
    sql = """
    SELECT col1, ROW_NUMBER() OVER (ORDER BY col1) as rn
    FROM (VALUES (1), (2), (3)) AS t(col1)
    ORDER BY col1
    """
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchall()
    assert res[0][1] == 1
    assert res[1][1] == 2
    assert res[2][1] == 3


def test_rank(dialect_context):
    """Test RANK window function with ties."""
    sql = """
    SELECT col1, RANK() OVER (ORDER BY col1) as rnk
    FROM (VALUES (1), (2), (2), (3)) AS t(col1)
    ORDER BY col1
    """
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchall()
    assert res[0][1] == 1  # First value
    assert res[1][1] == 2  # Tied values
    assert res[2][1] == 2  # Tied values
    assert res[3][1] == 4  # Skips 3


def test_dense_rank(dialect_context):
    """Test DENSE_RANK window function."""
    sql = """
    SELECT col1, DENSE_RANK() OVER (ORDER BY col1) as drnk
    FROM (VALUES (1), (2), (2), (3)) AS t(col1)
    ORDER BY col1
    """
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchall()
    assert res[0][1] == 1  # First value
    assert res[1][1] == 2  # Tied values
    assert res[2][1] == 2  # Tied values
    assert res[3][1] == 3  # No skip


def test_lead_lag(dialect_context):
    """Test LEAD and LAG window functions."""
    sql = """
    SELECT col1, 
           LAG(col1, 1) OVER (ORDER BY col1) as prev_val,
           LEAD(col1, 1) OVER (ORDER BY col1) as next_val
    FROM (VALUES (1), (2), (3)) AS t(col1)
    ORDER BY col1
    """
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchall()
    # First row: prev=NULL, next=2
    assert res[0][1] is None
    assert res[0][2] == 2
    # Middle row: prev=1, next=3
    assert res[1][1] == 1
    assert res[1][2] == 3
    # Last row: prev=2, next=NULL
    assert res[2][1] == 2
    assert res[2][2] is None


def test_first_value_last_value(dialect_context):
    """Test FIRST_VALUE and LAST_VALUE window functions."""
    sql = """
    SELECT col1,
           FIRST_VALUE(col1) OVER (ORDER BY col1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first,
           LAST_VALUE(col1) OVER (ORDER BY col1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last
    FROM (VALUES (1), (2), (3)) AS t(col1)
    ORDER BY col1
    """
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchall()
    # All rows should have first=1 and last=3
    for row in res:
        assert row[1] == 1
        assert row[2] == 3


def test_ntile(dialect_context):
    """Test NTILE window function for quantile buckets."""
    sql = """
    SELECT col1, NTILE(4) OVER (ORDER BY col1) as quartile
    FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8)) AS t(col1)
    ORDER BY col1
    """
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchall()
    # Should divide into 4 buckets
    quartiles = [row[1] for row in res]
    assert 1 in quartiles
    assert 2 in quartiles
    assert 3 in quartiles
    assert 4 in quartiles


def test_partition_by(dialect_context):
    """Test window functions with PARTITION BY."""
    sql = """
    SELECT grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn
    FROM (VALUES ('A', 1), ('A', 2), ('B', 1), ('B', 2)) AS t(grp, val)
    ORDER BY grp, val
    """
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchall()
    # Each partition should restart numbering
    assert res[0] == ("A", 1, 1)
    assert res[1] == ("A", 2, 2)
    assert res[2] == ("B", 1, 1)
    assert res[3] == ("B", 2, 2)


def test_sum_over_window(dialect_context):
    """Test aggregate SUM with window frame."""
    sql = """
    SELECT col1, 
           SUM(col1) OVER (ORDER BY col1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum
    FROM (VALUES (1), (2), (3), (4)) AS t(col1)
    ORDER BY col1
    """
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchall()
    # Running sum: 1, 3, 6, 10
    assert res[0][1] == 1
    assert res[1][1] == 3
    assert res[2][1] == 6
    assert res[3][1] == 10
