"""Test Snowflake JSON function emulation."""

import json

import pytest


def test_parse_json_basic(conn):
    """Test PARSE_JSON with simple JSON string."""
    cur = conn.cursor()
    cur.execute("SELECT PARSE_JSON('{\"name\": \"Alice\", \"age\": 30}')")
    result = cur.fetchone()[0]
    
    # DuckDB returns JSON as string, parse it
    parsed = json.loads(result) if isinstance(result, str) else result
    assert parsed["name"] == "Alice"
    assert parsed["age"] == 30


def test_parse_json_with_column(conn):
    """Test PARSE_JSON on column data."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE json_data (id INT, data VARCHAR);
    """)
    cur.execute("""
        INSERT INTO json_data VALUES 
            (1, '{"user": "alice", "score": 95}'),
            (2, '{"user": "bob", "score": 87}');
    """)
    
    cur.execute("SELECT id, PARSE_JSON(data) FROM json_data ORDER BY id")
    results = cur.fetchall()
    
    assert len(results) == 2
    data1 = json.loads(results[0][1]) if isinstance(results[0][1], str) else results[0][1]
    assert data1["user"] == "alice"


def test_get_path_simple(conn):
    """Test GET_PATH to extract JSON values."""
    cur = conn.cursor()
    cur.execute("""
        SELECT GET_PATH(
            PARSE_JSON('{"user": {"name": "Alice", "id": 123}}'),
            'user.name'
        )
    """)
    result = cur.fetchone()[0]
    assert result == "Alice"


def test_get_path_array_index(conn):
    """Test GET_PATH with array indexing."""
    cur = conn.cursor()
    cur.execute("""
        SELECT GET_PATH(
            PARSE_JSON('{"items": [{"name": "A"}, {"name": "B"}]}'),
            'items[1].name'
        )
    """)
    result = cur.fetchone()[0]
    assert result == "B"


def test_json_extract_path_text(conn):
    """Test JSON_EXTRACT_PATH_TEXT for text extraction."""
    cur = conn.cursor()
    cur.execute("""
        SELECT JSON_EXTRACT_PATH_TEXT('{"a": {"b": "value"}}', 'a', 'b')
    """)
    result = cur.fetchone()[0]
    assert result == "value"


def test_object_construct(conn):
    """Test OBJECT_CONSTRUCT to build JSON objects."""
    cur = conn.cursor()
    cur.execute("""
        SELECT OBJECT_CONSTRUCT('name', 'Alice', 'age', 30)
    """)
    result = cur.fetchone()[0]
    parsed = json.loads(result) if isinstance(result, str) else result
    assert parsed["name"] == "Alice"
    assert parsed["age"] == 30


def test_object_construct_from_columns(conn):
    """Test OBJECT_CONSTRUCT with table columns."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE users (id INT, name VARCHAR, age INT);
    """)
    cur.execute("""
        INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25);
    """)
    
    cur.execute("""
        SELECT OBJECT_CONSTRUCT('id', id, 'name', name, 'age', age)
        FROM users
        ORDER BY id
    """)
    results = cur.fetchall()
    
    obj1 = json.loads(results[0][0]) if isinstance(results[0][0], str) else results[0][0]
    assert obj1["name"] == "Alice"
    assert obj1["age"] == 30


def test_json_null_handling(conn):
    """Test JSON functions with NULL values."""
    cur = conn.cursor()
    cur.execute("SELECT PARSE_JSON(NULL)")
    result = cur.fetchone()[0]
    assert result is None
    
    cur.execute("SELECT GET_PATH(NULL, 'path')")
    result = cur.fetchone()[0]
    assert result is None


def test_json_invalid_json(conn):
    """Test PARSE_JSON with invalid JSON."""
    cur = conn.cursor()
    # Should handle gracefully or raise appropriate error
    # DuckDB's InvalidInputException for invalid JSON conversion
    with pytest.raises((Exception,), match="Malformed|JSON|Invalid|Error"):
        cur.execute("SELECT PARSE_JSON('not valid json')")
        cur.fetchone()


def test_nested_json_operations(conn):
    """Test complex nested JSON operations."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE events (
            id INT,
            payload VARCHAR
        );
    """)
    cur.execute("""
        INSERT INTO events VALUES 
            (1, '{"event": "login", "user": {"id": 123, "name": "Alice"}}'),
            (2, '{"event": "logout", "user": {"id": 456, "name": "Bob"}}');
    """)
    
    cur.execute("""
        SELECT 
            id,
            GET_PATH(PARSE_JSON(payload), 'event') as event_type,
            GET_PATH(PARSE_JSON(payload), 'user.name') as user_name
        FROM events
        ORDER BY id
    """)
    results = cur.fetchall()
    
    assert len(results) == 2
    assert results[0][1] == "login"
    assert results[0][2] == "Alice"
    assert results[1][1] == "logout"
    assert results[1][2] == "Bob"
