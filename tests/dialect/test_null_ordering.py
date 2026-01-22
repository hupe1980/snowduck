"""Tests for Snowflake NULL ordering compatibility.

Snowflake's default NULL ordering (DEFAULT_NULL_ORDERING = 'LAST'):
- ASC:  NULLs are returned LAST (matches DuckDB default)
- DESC: NULLs are returned FIRST (differs from DuckDB which uses LAST)

SnowDuck should automatically add NULLS FIRST to DESC clauses without
explicit NULL ordering to match Snowflake's behavior.
"""

import snowflake.connector

from snowduck.decorators import mock_snowflake


@mock_snowflake
def test_null_ordering_desc_default():
    """Test that DESC ordering puts NULLs first (Snowflake default)."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    # Create test data with NULLs
    cur.execute("CREATE TABLE null_test (id INT, name VARCHAR(50))")
    cur.execute("INSERT INTO null_test VALUES (1, 'Alice'), (NULL, 'Bob'), (3, 'Charlie'), (NULL, NULL)")
    
    # Snowflake: ORDER BY id DESC puts NULLs FIRST by default
    cur.execute("SELECT id, name FROM null_test ORDER BY id DESC")
    results = cur.fetchall()
    
    # Expected order: NULLs first, then 3, then 1
    assert results[0][0] is None or results[1][0] is None, "NULLs should come first in DESC ordering"
    # The two NULLs should be at the beginning
    null_count = sum(1 for r in results[:2] if r[0] is None)
    assert null_count == 2, f"Expected 2 NULLs at the start, got {null_count}"
    # Then 3, then 1
    assert results[2][0] == 3, f"Expected 3, got {results[2][0]}"
    assert results[3][0] == 1, f"Expected 1, got {results[3][0]}"


@mock_snowflake
def test_null_ordering_asc_default():
    """Test that ASC ordering puts NULLs last (same as DuckDB default)."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE null_test2 (id INT)")
    cur.execute("INSERT INTO null_test2 VALUES (1), (NULL), (3), (2)")
    
    # Snowflake: ORDER BY ASC puts NULLs LAST by default
    cur.execute("SELECT id FROM null_test2 ORDER BY id ASC")
    results = cur.fetchall()
    
    # Expected order: 1, 2, 3, NULL
    assert results[0][0] == 1
    assert results[1][0] == 2
    assert results[2][0] == 3
    assert results[3][0] is None, "NULL should come last in ASC ordering"


@mock_snowflake
def test_null_ordering_implicit_asc():
    """Test that implicit ASC (no keyword) also puts NULLs last."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE null_test3 (id INT)")
    cur.execute("INSERT INTO null_test3 VALUES (1), (NULL), (3)")
    
    # No ASC/DESC keyword = ASC by default, NULLs should be LAST
    cur.execute("SELECT id FROM null_test3 ORDER BY id")
    results = cur.fetchall()
    
    assert results[0][0] == 1
    assert results[1][0] == 3
    assert results[2][0] is None, "NULL should come last with implicit ASC"


@mock_snowflake
def test_null_ordering_explicit_nulls_first_preserved():
    """Test that explicit NULLS FIRST is preserved."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE null_test4 (id INT)")
    cur.execute("INSERT INTO null_test4 VALUES (1), (NULL), (3)")
    
    # Explicit NULLS FIRST with ASC
    cur.execute("SELECT id FROM null_test4 ORDER BY id ASC NULLS FIRST")
    results = cur.fetchall()
    
    assert results[0][0] is None, "NULL should come first with explicit NULLS FIRST"
    assert results[1][0] == 1
    assert results[2][0] == 3


@mock_snowflake
def test_null_ordering_explicit_nulls_last_preserved():
    """Test that explicit NULLS LAST is preserved."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE null_test5 (id INT)")
    cur.execute("INSERT INTO null_test5 VALUES (1), (NULL), (3)")
    
    # Explicit NULLS LAST with DESC
    cur.execute("SELECT id FROM null_test5 ORDER BY id DESC NULLS LAST")
    results = cur.fetchall()
    
    assert results[0][0] == 3
    assert results[1][0] == 1
    assert results[2][0] is None, "NULL should come last with explicit NULLS LAST"


@mock_snowflake
def test_null_ordering_multiple_columns():
    """Test NULL ordering with multiple ORDER BY columns."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE null_test6 (category VARCHAR(10), value INT)")
    cur.execute("""
        INSERT INTO null_test6 VALUES 
        ('A', 1), ('A', NULL), ('B', 2), (NULL, 3), ('A', 3)
    """)
    
    # Multiple columns: category ASC (NULLs last), value DESC (NULLs first)
    cur.execute("SELECT category, value FROM null_test6 ORDER BY category ASC, value DESC")
    results = cur.fetchall()
    
    # Category 'A' first (3 rows), sorted by value DESC with NULLs first
    # Expected A rows: (A, NULL), (A, 3), (A, 1)
    a_rows = [r for r in results if r[0] == 'A']
    assert a_rows[0] == ('A', None), f"Expected ('A', None) first in A group, got {a_rows[0]}"
    assert a_rows[1] == ('A', 3), f"Expected ('A', 3) second in A group, got {a_rows[1]}"
    assert a_rows[2] == ('A', 1), f"Expected ('A', 1) third in A group, got {a_rows[2]}"
    
    # Category NULL should be last (NULLs last for ASC)
    assert results[-1][0] is None, "NULL category should come last"


@mock_snowflake
def test_null_ordering_in_window_function():
    """Test NULL ordering in window function ORDER BY clause."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE null_test7 (id INT, value INT)")
    cur.execute("INSERT INTO null_test7 VALUES (1, 10), (2, NULL), (3, 30)")
    
    # Window function with ORDER BY DESC should put NULLs first
    cur.execute("""
        SELECT id, value, ROW_NUMBER() OVER (ORDER BY value DESC) as rn
        FROM null_test7
        ORDER BY rn
    """)
    results = cur.fetchall()
    
    # Row with NULL value should have rn=1 (first in DESC with NULLs first)
    assert results[0][1] is None, "NULL value should be ranked first in DESC window ordering"
    assert results[0][2] == 1, "NULL should have row_number 1"


@mock_snowflake
def test_null_ordering_with_limit():
    """Test NULL ordering works correctly with LIMIT."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE null_test8 (id INT)")
    cur.execute("INSERT INTO null_test8 VALUES (1), (NULL), (3), (NULL), (5)")
    
    # DESC with LIMIT - NULLs should come first
    cur.execute("SELECT id FROM null_test8 ORDER BY id DESC LIMIT 3")
    results = cur.fetchall()
    
    # Top 3 DESC with NULLs first: NULL, NULL, 5
    assert results[0][0] is None
    assert results[1][0] is None
    assert results[2][0] == 5
