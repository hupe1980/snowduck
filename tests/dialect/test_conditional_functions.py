"""Tests for Snowflake conditional and NULL handling functions.

These functions are commonly used in production Snowflake queries
and SnowDuck should support them with correct Snowflake semantics.
"""

import snowflake.connector

from snowduck.decorators import mock_snowflake


@mock_snowflake
def test_nvl_basic():
    """Test NVL(expr, default) returns expr if not null, else default."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # NVL with non-null value returns the value
    cur.execute("SELECT NVL(10, 0)")
    assert cur.fetchone()[0] == 10

    # NVL with null value returns default
    cur.execute("SELECT NVL(NULL, 0)")
    assert cur.fetchone()[0] == 0

    # NVL with string
    cur.execute("SELECT NVL(NULL, 'default')")
    assert cur.fetchone()[0] == "default"


@mock_snowflake
def test_nvl_in_table_query():
    """Test NVL with table data."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    cur.execute("CREATE TABLE nvl_test (id INT, name VARCHAR(50))")
    cur.execute("INSERT INTO nvl_test VALUES (1, 'Alice'), (2, NULL), (3, 'Charlie')")

    cur.execute("SELECT id, NVL(name, 'Unknown') FROM nvl_test ORDER BY id")
    results = cur.fetchall()

    assert results[0] == (1, "Alice")
    assert results[1] == (2, "Unknown")  # NULL replaced
    assert results[2] == (3, "Charlie")


@mock_snowflake
def test_nvl2_basic():
    """Test NVL2(expr, if_not_null, if_null) returns appropriate value."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # NVL2 with non-null: returns second arg
    cur.execute("SELECT NVL2(10, 'has value', 'no value')")
    assert cur.fetchone()[0] == "has value"

    # NVL2 with null: returns third arg
    cur.execute("SELECT NVL2(NULL, 'has value', 'no value')")
    assert cur.fetchone()[0] == "no value"


@mock_snowflake
def test_nvl2_in_table_query():
    """Test NVL2 with table data."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    cur.execute("CREATE TABLE nvl2_test (id INT, bonus DECIMAL(10,2))")
    cur.execute("INSERT INTO nvl2_test VALUES (1, 100.00), (2, NULL), (3, 50.00)")

    # Show 'Eligible' if bonus exists, 'Not Eligible' if null
    cur.execute(
        "SELECT id, NVL2(bonus, 'Eligible', 'Not Eligible') FROM nvl2_test ORDER BY id"
    )
    results = cur.fetchall()

    assert results[0] == (1, "Eligible")
    assert results[1] == (2, "Not Eligible")  # NULL bonus
    assert results[2] == (3, "Eligible")


@mock_snowflake
def test_ifnull_basic():
    """Test IFNULL(expr, default) - alias for NVL."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # IFNULL is just an alias for NVL
    cur.execute("SELECT IFNULL(NULL, 42)")
    assert cur.fetchone()[0] == 42

    cur.execute("SELECT IFNULL(100, 42)")
    assert cur.fetchone()[0] == 100


@mock_snowflake
def test_zeroifnull_basic():
    """Test ZEROIFNULL(expr) returns 0 if null, else the value."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # ZEROIFNULL with null returns 0
    cur.execute("SELECT ZEROIFNULL(NULL)")
    assert cur.fetchone()[0] == 0

    # ZEROIFNULL with value returns the value
    cur.execute("SELECT ZEROIFNULL(42)")
    assert cur.fetchone()[0] == 42


@mock_snowflake
def test_zeroifnull_in_aggregation():
    """Test ZEROIFNULL in aggregation context."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    cur.execute("CREATE TABLE sales (id INT, amount DECIMAL(10,2))")
    cur.execute("INSERT INTO sales VALUES (1, 100.00), (2, NULL), (3, 50.00)")

    # Sum with ZEROIFNULL to treat NULLs as 0
    cur.execute("SELECT SUM(ZEROIFNULL(amount)) FROM sales")
    result = cur.fetchone()[0]
    assert float(result) == 150.0  # 100 + 0 + 50


@mock_snowflake
def test_nullifzero_basic():
    """Test NULLIFZERO(expr) returns NULL if 0, else the value."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # NULLIFZERO with 0 returns NULL
    cur.execute("SELECT NULLIFZERO(0)")
    assert cur.fetchone()[0] is None

    # NULLIFZERO with non-zero returns the value
    cur.execute("SELECT NULLIFZERO(42)")
    assert cur.fetchone()[0] == 42

    # NULLIFZERO with NULL returns NULL
    cur.execute("SELECT NULLIFZERO(NULL)")
    assert cur.fetchone()[0] is None


@mock_snowflake
def test_decode_basic():
    """Test DECODE(expr, search1, result1, search2, result2, ..., default)."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # Simple DECODE - match first case
    cur.execute("SELECT DECODE(1, 1, 'one', 2, 'two', 'other')")
    assert cur.fetchone()[0] == "one"

    # DECODE - match second case
    cur.execute("SELECT DECODE(2, 1, 'one', 2, 'two', 'other')")
    assert cur.fetchone()[0] == "two"

    # DECODE - no match, returns default
    cur.execute("SELECT DECODE(3, 1, 'one', 2, 'two', 'other')")
    assert cur.fetchone()[0] == "other"


@mock_snowflake
def test_decode_without_default():
    """Test DECODE without default value returns NULL on no match."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # No match and no default -> NULL
    cur.execute("SELECT DECODE(3, 1, 'one', 2, 'two')")
    assert cur.fetchone()[0] is None


@mock_snowflake
def test_decode_with_null():
    """Test DECODE handles NULL values correctly."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # DECODE can match NULL (unlike CASE)
    cur.execute("SELECT DECODE(NULL, NULL, 'is null', 'not null')")
    assert cur.fetchone()[0] == "is null"


@mock_snowflake
def test_decode_in_table_query():
    """Test DECODE with table data - common status code mapping."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    cur.execute("CREATE TABLE orders (id INT, status INT)")
    cur.execute("INSERT INTO orders VALUES (1, 1), (2, 2), (3, 3), (4, 99)")

    cur.execute("""
        SELECT id, DECODE(status, 
            1, 'Pending', 
            2, 'Processing', 
            3, 'Shipped', 
            'Unknown'
        ) as status_name
        FROM orders 
        ORDER BY id
    """)
    results = cur.fetchall()

    assert results[0] == (1, "Pending")
    assert results[1] == (2, "Processing")
    assert results[2] == (3, "Shipped")
    assert results[3] == (4, "Unknown")


@mock_snowflake
def test_iff_basic():
    """Test IFF(condition, true_val, false_val) - Snowflake's inline IF."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # IFF with true condition
    cur.execute("SELECT IFF(1 > 0, 'yes', 'no')")
    assert cur.fetchone()[0] == "yes"

    # IFF with false condition
    cur.execute("SELECT IFF(1 < 0, 'yes', 'no')")
    assert cur.fetchone()[0] == "no"


@mock_snowflake
def test_iff_with_null_condition():
    """Test IFF handles NULL condition as false."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # NULL condition is treated as false
    cur.execute("SELECT IFF(NULL, 'yes', 'no')")
    assert cur.fetchone()[0] == "no"


@mock_snowflake
def test_iff_in_table_query():
    """Test IFF with table data."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    cur.execute("CREATE TABLE scores (name VARCHAR(50), score INT)")
    cur.execute("INSERT INTO scores VALUES ('Alice', 85), ('Bob', 55), ('Charlie', 72)")

    cur.execute("""
        SELECT name, IFF(score >= 60, 'Pass', 'Fail') as result
        FROM scores
        ORDER BY name
    """)
    results = cur.fetchall()

    assert results[0] == ("Alice", "Pass")
    assert results[1] == ("Bob", "Fail")
    assert results[2] == ("Charlie", "Pass")


@mock_snowflake
def test_equal_null():
    """Test EQUAL_NULL(expr1, expr2) - NULL-safe equality comparison."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # Both NULL -> true
    cur.execute("SELECT EQUAL_NULL(NULL, NULL)")
    assert cur.fetchone()[0] is True

    # One NULL -> false
    cur.execute("SELECT EQUAL_NULL(1, NULL)")
    assert cur.fetchone()[0] is False

    # Both equal non-null -> true
    cur.execute("SELECT EQUAL_NULL(1, 1)")
    assert cur.fetchone()[0] is True

    # Both different non-null -> false
    cur.execute("SELECT EQUAL_NULL(1, 2)")
    assert cur.fetchone()[0] is False
