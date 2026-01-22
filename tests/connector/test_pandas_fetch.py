"""Tests for pandas fetch methods."""

import pytest
import pandas as pd
import snowflake.connector

from snowduck import mock_snowflake


@mock_snowflake
def test_fetch_pandas_all_basic():
    """Test fetch_pandas_all returns all rows as DataFrame."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE test_pandas (id INTEGER, name VARCHAR)")
    cur.execute("INSERT INTO test_pandas VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
    
    cur.execute("SELECT * FROM test_pandas ORDER BY id")
    df = cur.fetch_pandas_all()
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert list(df.columns) == ['id', 'name']
    assert df['name'].tolist() == ['Alice', 'Bob', 'Carol']


@mock_snowflake
def test_fetch_pandas_all_empty():
    """Test fetch_pandas_all with empty result set."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE empty_table (id INTEGER)")
    cur.execute("SELECT * FROM empty_table")
    
    df = cur.fetch_pandas_all()
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


@mock_snowflake
def test_fetch_pandas_all_before_execute():
    """Test fetch_pandas_all raises error before execute."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    with pytest.raises(TypeError, match="No open result set"):
        cur.fetch_pandas_all()


@mock_snowflake
def test_fetch_pandas_batches_basic():
    """Test fetch_pandas_batches yields DataFrames."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    # Create test data
    cur.execute("CREATE TABLE batch_test (id INTEGER, value VARCHAR)")
    values = ", ".join([f"({i}, 'val_{i}')" for i in range(100)])
    cur.execute(f"INSERT INTO batch_test VALUES {values}")
    
    cur.execute("SELECT * FROM batch_test ORDER BY id")
    
    batches = list(cur.fetch_pandas_batches(batch_size=30))
    
    # Should have multiple batches
    assert len(batches) >= 1
    
    # All batches should be DataFrames
    for batch in batches:
        assert isinstance(batch, pd.DataFrame)
    
    # Total rows should be 100
    total_rows = sum(len(batch) for batch in batches)
    assert total_rows == 100


@mock_snowflake
def test_fetch_pandas_batches_before_execute():
    """Test fetch_pandas_batches raises error before execute."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    with pytest.raises(TypeError, match="No open result set"):
        list(cur.fetch_pandas_batches())


@mock_snowflake
def test_get_result_batches():
    """Test get_result_batches returns Arrow batches."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT 1 as a, 'test' as b UNION ALL SELECT 2, 'test2'")
    
    batches = cur.get_result_batches()
    
    assert isinstance(batches, list)
    assert len(batches) > 0
    
    # Each batch should be a pyarrow RecordBatch
    import pyarrow as pa
    for batch in batches:
        assert isinstance(batch, pa.RecordBatch)


@mock_snowflake
def test_get_result_batches_before_execute():
    """Test get_result_batches raises error before execute."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    with pytest.raises(TypeError, match="No open result set"):
        cur.get_result_batches()
