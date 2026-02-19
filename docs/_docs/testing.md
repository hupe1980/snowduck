---
layout: default
title: Testing
parent: Documentation
nav_order: 3
---

# Testing with SnowDuck

SnowDuck is designed for testing Snowflake applications. This guide covers best practices for using SnowDuck in your test suite.

## pytest Integration

### Using the Decorator

The simplest way to use SnowDuck in tests is with the `@mock_snowflake` decorator:

```python
import pytest
from snowduck import mock_snowflake
import snowflake.connector

@mock_snowflake
def test_simple_query():
    conn = snowflake.connector.connect(user="x", password="x", account="x")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 + 1 AS result")
    assert cursor.fetchone()[0] == 2

@mock_snowflake
def test_string_functions():
    conn = snowflake.connector.connect(user="x", password="x", account="x")
    cursor = conn.cursor()
    cursor.execute("SELECT UPPER('hello'), LOWER('WORLD')")
    row = cursor.fetchone()
    assert row[0] == "HELLO"
    assert row[1] == "world"
```

### Using a Fixture

For more control, create a pytest fixture:

```python
import pytest
from snowduck import start_patch_snowflake, stop_patch_snowflake
import snowflake.connector

@pytest.fixture
def snowflake_conn():
    """Provide a mocked Snowflake connection."""
    start_patch_snowflake()
    conn = snowflake.connector.connect(user="test", password="test", account="test")
    yield conn
    conn.close()
    stop_patch_snowflake()

def test_with_fixture(snowflake_conn):
    cursor = snowflake_conn.cursor()
    cursor.execute("SELECT CURRENT_DATE()")
    assert cursor.fetchone()[0] is not None
```

### Shared Fixture for Performance

To share database state across tests in a module:

```python
import pytest
from snowduck import start_patch_snowflake, stop_patch_snowflake, seed_table
import snowflake.connector

@pytest.fixture(scope="module")
def db_connection():
    """Module-scoped connection with seeded data."""
    start_patch_snowflake()
    conn = snowflake.connector.connect(user="test", password="test", account="test")
    
    # Seed test data once
    seed_table(conn, "products", {
        "id": [1, 2, 3],
        "name": ["Widget", "Gadget", "Gizmo"],
        "price": [9.99, 19.99, 29.99]
    })
    
    yield conn
    conn.close()
    stop_patch_snowflake()

def test_product_count(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM products")
    assert cursor.fetchone()[0] == 3

def test_product_total(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT SUM(price) FROM products")
    assert cursor.fetchone()[0] == pytest.approx(59.97)
```

## Testing Data Transformations

### Testing SQL Logic

```python
from snowduck import mock_snowflake, seed_table
import snowflake.connector

@mock_snowflake
def test_aggregation_logic():
    conn = snowflake.connector.connect(user="x", password="x", account="x")
    
    # Seed input data
    seed_table(conn, "sales", {
        "region": ["East", "East", "West", "West"],
        "amount": [100, 200, 150, 250]
    })
    
    # Test your aggregation query
    cursor = conn.cursor()
    cursor.execute("""
        SELECT region, SUM(amount) as total
        FROM sales
        GROUP BY region
        ORDER BY region
    """)
    
    results = cursor.fetchall()
    assert results[0] == ("East", 300)
    assert results[1] == ("West", 400)
```

### Testing CTEs and Window Functions

```python
@mock_snowflake
def test_window_function():
    conn = snowflake.connector.connect(user="x", password="x", account="x")
    
    seed_table(conn, "employees", {
        "dept": ["Sales", "Sales", "Engineering"],
        "name": ["Alice", "Bob", "Charlie"],
        "salary": [50000, 60000, 70000]
    })
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            name,
            salary,
            RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rank
        FROM employees
        QUALIFY rank = 1
    """)
    
    # Get top earner per department
    results = {row[0]: row[1] for row in cursor.fetchall()}
    assert results["Bob"] == 60000      # Top in Sales
    assert results["Charlie"] == 70000  # Top in Engineering
```

## Testing Edge Cases

### NULL Handling

```python
@mock_snowflake
def test_null_handling():
    conn = snowflake.connector.connect(user="x", password="x", account="x")
    cursor = conn.cursor()
    
    # Test COALESCE
    cursor.execute("SELECT COALESCE(NULL, NULL, 'default')")
    assert cursor.fetchone()[0] == "default"
    
    # Test NVL
    cursor.execute("SELECT NVL(NULL, 'fallback')")
    assert cursor.fetchone()[0] == "fallback"
    
    # Test NULLIF
    cursor.execute("SELECT NULLIF('same', 'same')")
    assert cursor.fetchone()[0] is None
```

### Type Coercion

```python
@mock_snowflake
def test_type_coercion():
    conn = snowflake.connector.connect(user="x", password="x", account="x")
    cursor = conn.cursor()
    
    # String to number
    cursor.execute("SELECT '123'::INTEGER")
    assert cursor.fetchone()[0] == 123
    
    # Number to string  
    cursor.execute("SELECT 456::VARCHAR")
    assert cursor.fetchone()[0] == "456"
```

## Best Practices

{: .note }
> **Isolation**: Each test should be independent. Use fresh connections or clean up data between tests.

{: .tip }
> **Performance**: Use `seed_table` instead of multiple INSERT statements for faster test setup.

{: .warning }
> **Not for Production**: SnowDuck is for testing only. Always validate critical queries against real Snowflake before production deployment.

### Recommended Test Structure

```python
import pytest
from snowduck import mock_snowflake, seed_table
import snowflake.connector

class TestMyFeature:
    """Group related tests together."""
    
    @mock_snowflake
    def test_happy_path(self):
        """Test the expected behavior."""
        pass
    
    @mock_snowflake
    def test_edge_case(self):
        """Test boundary conditions."""
        pass
    
    @mock_snowflake
    def test_error_handling(self):
        """Test error conditions."""
        pass
```
