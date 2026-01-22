"""Tests for Snowflake variable substitution (SET/SELECT $var)."""

import pytest
import snowflake.connector

from snowduck import mock_snowflake


@mock_snowflake
def test_set_variable_basic():
    """Test SET variable = value syntax."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    # Set a variable
    cur.execute("SET my_var = 'hello'")
    
    # Use the variable
    cur.execute("SELECT $my_var")
    result = cur.fetchone()
    assert result[0] == "hello"


@mock_snowflake
def test_set_variable_numeric():
    """Test SET with numeric values."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SET my_num = 42")
    cur.execute("SELECT $my_num")
    result = cur.fetchone()
    assert result[0] == 42


@mock_snowflake
def test_set_variable_in_where_clause():
    """Test variable substitution in WHERE clause."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE test_vars (id INT, name VARCHAR)")
    cur.execute("INSERT INTO test_vars VALUES (1, 'alice'), (2, 'bob')")
    
    cur.execute("SET filter_id = 1")
    cur.execute("SELECT name FROM test_vars WHERE id = $filter_id")
    result = cur.fetchone()
    assert result[0] == "alice"


@mock_snowflake
def test_set_multiple_variables():
    """Test multiple variables in same session."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SET var1 = 'foo'")
    cur.execute("SET var2 = 'bar'")
    
    cur.execute("SELECT $var1, $var2")
    result = cur.fetchone()
    assert result[0] == "foo"
    assert result[1] == "bar"


@mock_snowflake
def test_set_variable_overwrite():
    """Test overwriting existing variable."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SET my_var = 'first'")
    cur.execute("SET my_var = 'second'")
    
    cur.execute("SELECT $my_var")
    result = cur.fetchone()
    assert result[0] == "second"


@mock_snowflake
def test_variable_in_expression():
    """Test variable used in arithmetic expression."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SET base_value = 10")
    cur.execute("SELECT $base_value * 2 + 5")
    result = cur.fetchone()
    assert result[0] == 25


@mock_snowflake
def test_undefined_variable_error():
    """Test that using undefined variable raises error."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    with pytest.raises(Exception) as exc_info:
        cur.execute("SELECT $undefined_var")
    
    # Should contain error about undefined variable
    assert "undefined" in str(exc_info.value).lower() or "not found" in str(exc_info.value).lower()


@mock_snowflake
def test_variable_isolation_between_sessions():
    """Test that variables are session-isolated."""
    conn1 = snowflake.connector.connect()
    conn2 = snowflake.connector.connect()
    
    cur1 = conn1.cursor()
    cur2 = conn2.cursor()
    
    # Set variable in session 1
    cur1.execute("SET session_var = 'session1'")
    
    # Should not be visible in session 2
    with pytest.raises((ValueError, Exception), match=".*variable.*"):
        cur2.execute("SELECT $session_var")
