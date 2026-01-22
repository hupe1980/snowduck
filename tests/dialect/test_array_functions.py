"""Test Snowflake ARRAY function emulation."""



def test_array_construct_basic(conn):
    """Test ARRAY_CONSTRUCT with literal values."""
    cur = conn.cursor()
    cur.execute("SELECT ARRAY_CONSTRUCT(1, 2, 3, 4, 5)")
    result = cur.fetchone()[0]
    assert result == [1, 2, 3, 4, 5]


def test_array_construct_mixed_types(conn):
    """Test ARRAY_CONSTRUCT with mixed types."""
    cur = conn.cursor()
    cur.execute("SELECT ARRAY_CONSTRUCT('a', 'b', 'c')")
    result = cur.fetchone()[0]
    assert result == ['a', 'b', 'c']


def test_array_construct_from_columns(conn):
    """Test ARRAY_CONSTRUCT with column values."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE data (a INT, b INT, c INT);
    """)
    cur.execute("INSERT INTO data VALUES (1, 2, 3), (4, 5, 6)")
    
    cur.execute("SELECT ARRAY_CONSTRUCT(a, b, c) FROM data ORDER BY a")
    results = cur.fetchall()
    
    assert results[0][0] == [1, 2, 3]
    assert results[1][0] == [4, 5, 6]


def test_array_size(conn):
    """Test ARRAY_SIZE to get array length."""
    cur = conn.cursor()
    cur.execute("SELECT ARRAY_SIZE(ARRAY_CONSTRUCT(1, 2, 3, 4))")
    result = cur.fetchone()[0]
    assert result == 4
    
    cur.execute("SELECT ARRAY_SIZE(ARRAY_CONSTRUCT())")
    result = cur.fetchone()[0]
    assert result == 0


def test_array_contains(conn):
    """Test ARRAY_CONTAINS to check element existence."""
    cur = conn.cursor()
    cur.execute("SELECT ARRAY_CONTAINS(3, ARRAY_CONSTRUCT(1, 2, 3, 4))")
    result = cur.fetchone()[0]
    assert result is True
    
    cur.execute("SELECT ARRAY_CONTAINS(5, ARRAY_CONSTRUCT(1, 2, 3, 4))")
    result = cur.fetchone()[0]
    assert result is False


def test_array_slice(conn):
    """Test ARRAY_SLICE to extract subarray."""
    cur = conn.cursor()
    cur.execute("SELECT ARRAY_SLICE(ARRAY_CONSTRUCT(1, 2, 3, 4, 5), 1, 3)")
    result = cur.fetchone()[0]
    assert result == [2, 3, 4]  # DuckDB uses 0-based indexing internally


def test_array_to_string(conn):
    """Test ARRAY_TO_STRING to join array elements."""
    cur = conn.cursor()
    cur.execute("SELECT ARRAY_TO_STRING(ARRAY_CONSTRUCT('a', 'b', 'c'), ',')")
    result = cur.fetchone()[0]
    assert result == "a,b,c"


def test_array_agg(conn):
    """Test ARRAY_AGG to aggregate values into array."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE items (category VARCHAR, item VARCHAR);
    """)
    cur.execute("""
        INSERT INTO items VALUES 
            ('fruit', 'apple'),
            ('fruit', 'banana'),
            ('veg', 'carrot');
    """)
    
    cur.execute("""
        SELECT category, ARRAY_AGG(item) as items
        FROM items
        GROUP BY category
        ORDER BY category
    """)
    results = cur.fetchall()
    
    assert len(results) == 2
    fruit_items = results[0][1]
    assert 'apple' in fruit_items
    assert 'banana' in fruit_items


def test_array_null_handling(conn):
    """Test ARRAY functions with NULL values."""
    cur = conn.cursor()
    cur.execute("SELECT ARRAY_CONSTRUCT(1, NULL, 3)")
    result = cur.fetchone()[0]
    assert result == [1, None, 3]
    
    cur.execute("SELECT ARRAY_SIZE(NULL)")
    result = cur.fetchone()[0]
    assert result is None


def test_array_position(conn):
    """Test ARRAY_POSITION to find element index."""
    cur = conn.cursor()
    cur.execute("SELECT ARRAY_POSITION(3, ARRAY_CONSTRUCT(1, 2, 3, 4))")
    result = cur.fetchone()[0]
    # Snowflake uses 0-based indexing for ARRAY_POSITION
    assert result == 2  # Index of value 3


def test_get_array_element(conn):
    """Test GET to access array elements by index."""
    cur = conn.cursor()
    cur.execute("SELECT GET(ARRAY_CONSTRUCT('a', 'b', 'c'), 1)")
    result = cur.fetchone()[0]
    assert result == 'b'  # 0-based indexing


def test_array_compact(conn):
    """Test ARRAY_COMPACT to remove NULLs."""
    cur = conn.cursor()
    cur.execute("SELECT ARRAY_COMPACT(ARRAY_CONSTRUCT(1, NULL, 2, NULL, 3))")
    result = cur.fetchone()[0]
    assert result == [1, 2, 3]


def test_array_distinct(conn):
    """Test ARRAY_DISTINCT to remove duplicates."""
    cur = conn.cursor()
    cur.execute("SELECT ARRAY_DISTINCT(ARRAY_CONSTRUCT(1, 2, 2, 3, 3, 3))")
    result = cur.fetchone()[0]
    assert sorted(result) == [1, 2, 3]


def test_array_intersection(conn):
    """Test ARRAY_INTERSECTION to find common elements."""
    cur = conn.cursor()
    cur.execute("""
        SELECT ARRAY_INTERSECTION(
            ARRAY_CONSTRUCT(1, 2, 3, 4),
            ARRAY_CONSTRUCT(3, 4, 5, 6)
        )
    """)
    result = cur.fetchone()[0]
    assert sorted(result) == [3, 4]


def test_array_flatten_simple(conn):
    """Test FLATTEN to unnest array elements."""
    cur = conn.cursor()
    cur.execute("""
        SELECT value
        FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT(1, 2, 3)))
        ORDER BY value
    """)
    results = cur.fetchall()
    assert [r[0] for r in results] == [1, 2, 3]


def test_array_in_where_clause(conn):
    """Test using arrays in WHERE conditions."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE products (id INT, tags ARRAY);
    """)
    # DuckDB syntax for array literals
    cur.execute("""
        INSERT INTO products VALUES 
            (1, [1, 2, 3]),
            (2, [2, 3, 4]),
            (3, [5, 6, 7]);
    """)
    
    cur.execute("""
        SELECT id
        FROM products
        WHERE ARRAY_CONTAINS(2, tags)
        ORDER BY id
    """)
    results = cur.fetchall()
    assert [r[0] for r in results] == [1, 2]
