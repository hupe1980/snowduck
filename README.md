# ❄️🦆 SnowDuck

[![CI/CD Pipeline](https://github.com/hupe1980/snowduck/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/snowduck/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

> **Run Snowflake SQL locally, powered by DuckDB**

SnowDuck is a lightweight, in-memory SQL engine that emulates Snowflake's behavior for development and testing. Write and test Snowflake SQL locally without cloud access or costs.

**Recent Improvements:**
- ✅ **Zero linting errors** - Professional, clean codebase
- ✅ **Modern CI/CD** - PyPI Trusted Publishers with package attestations
- ✅ **128 tests passing** in <2s - Fast, comprehensive test suite
- ✅ **Production-ready** for development & testing

## Why SnowDuck?

- 🚀 **Fast Development** - Test SQL queries instantly without waiting for cloud connections
- 💰 **Zero Cloud Costs** - Develop and test locally without Snowflake compute charges
- 🧪 **Easy Testing** - Mock Snowflake databases for unit tests and CI/CD pipelines
- ⚡ **Lightning Fast** - Powered by DuckDB's in-memory execution engine
- 🔌 **Drop-in Compatible** - Uses Snowflake's connector interface - just patch and go

## Features

| Feature | Support |
|---------|---------|
| DDL Operations | ✅ CREATE/DROP DATABASE, SCHEMA, TABLE |
| String Functions | ✅ UPPER, LOWER, CONCAT, LENGTH, SUBSTRING |
| Aggregate Functions | ✅ COUNT, SUM, AVG, MIN, MAX, GROUP BY |
| Window Functions | ✅ ROW_NUMBER, RANK, DENSE_RANK, PARTITION BY |
| Session Variables | ✅ SET/SELECT $variable syntax |
| Information Schema | ✅ Query metadata (databases, tables, columns) |
| Advanced SQL | ✅ CTEs, JOINs, subqueries, CASE statements || Arrow/REST Server | ✅ HTTP API with Arrow IPC format || Snowflake Functions | 🚧 90%+ compatibility, growing |

> **Note**: SnowDuck is experimental. Use for development/testing only, not production workloads.

## Quick Start

### Installation

```bash
# Using uv (recommended - 10-100x faster)
uv pip install snowduck

# Or using pip
pip install snowduck
```

### Usage

```python
import snowflake.connector
from snowduck import start_patch_snowflake

# Patch the Snowflake connector to use DuckDB
start_patch_snowflake()

# Use Snowflake connector as normal - it's now backed by DuckDB!
with snowflake.connector.connect() as conn:
    cursor = conn.cursor()
    
    # Create a database
    cursor.execute("CREATE DATABASE my_database")
    cursor.execute("USE DATABASE my_database")
    
    # Create a table
    cursor.execute("""
        CREATE TABLE employees (
            id INTEGER,
            name VARCHAR,
            salary INTEGER
        )
    """)
    
    # Insert data
    cursor.execute("""
        INSERT INTO employees VALUES
        (1, 'Alice', 95000),
        (2, 'Bob', 75000),
        (3, 'Carol', 105000)
    """)
    
    # Query with Snowflake SQL
    cursor.execute("""
        SELECT 
            name,
            salary,
            RANK() OVER (ORDER BY salary DESC) as rank
        FROM employees
    """)
    
    for row in cursor.fetchall():
        print(f"{row[0]}: ${row[1]:,} (Rank: {row[2]})")
```

**Output:**
```
Carol: $105,000 (Rank: 1)
Alice: $95,000 (Rank: 2)
Bob: $75,000 (Rank: 3)
```

### Data Persistence Options

SnowDuck supports two storage modes:

**In-Memory (Default)**: Fast, isolated, data lost on exit
```python
start_patch_snowflake()  # or start_patch_snowflake(db_file=':memory:')
```

**File-Based**: Persistent, data survives restarts, perfect for testing
```python
# Data persists to file and survives program restarts
start_patch_snowflake(db_file='test_data.duckdb')

with snowflake.connector.connect() as conn:
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE employees (...)")
    # Data saved to test_data.duckdb

# Later - data still exists!
with snowflake.connector.connect() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM employees")  # ✅ Works!
```
**3. Fresh Start**: Reset clears existing data (great for notebooks!)
```python
# Deletes existing database file for a clean slate
start_patch_snowflake(db_file='demo.duckdb', reset=True)
```
Use file-based storage for:
- 🧪 **Test fixtures** that persist across test runs
- 📊 **Shared test data** across multiple test files
- 🔄 **CI/CD pipelines** with reproducible datasets
- 🚀 **Development** with sample data you want to keep

### Data Seeding Made Easy

SnowDuck includes `seed_table()` for effortless test data creation:

```python
from snowduck import seed_table
import pandas as pd

with snowflake.connector.connect() as conn:
    # From dict of lists (easiest!)
    seed_table(conn, 'customers', {
        'id': [1, 2, 3],
        'name': ['Acme Corp', 'TechStart', 'DataCo'],
        'revenue': [1000000, 50000, 800000]
    })
    
    # From pandas DataFrame
    df = pd.read_csv('test_data.csv')
    seed_table(conn, 'orders', df)
    
    # From list of dicts
    seed_table(conn, 'products', [
        {'id': 1, 'name': 'Widget', 'price': 9.99},
        {'id': 2, 'name': 'Gadget', 'price': 19.99}
    ])
```

**Why `seed_table()` is awesome:**
- ✅ One line to create and populate tables
- ✅ Automatic data type inference
- ✅ Handles NULL values, timestamps, special characters
- ✅ Drops existing table by default (perfect for test fixtures)
- ✅ Works with dicts, DataFrames, or list of dicts

### More Examples

Check out [examples/notebook.ipynb](examples/notebook.ipynb) for comprehensive examples including:
- Database and schema management
- Table operations and data manipulation
- String and aggregate functions
- Window functions and analytics
- Session variables
- Information schema queries
- Complex CTEs and subqueries

## Use Cases

### 1. Local Development
```python
# Develop and test Snowflake SQL without cloud access
from snowduck import start_patch_snowflake
start_patch_snowflake()

# Your existing Snowflake code works unchanged!
import snowflake.connector
conn = snowflake.connector.connect()
```

### 2. Unit Testing

**Recommended: Use the `@mock_snowflake` decorator for clean, isolated tests**

```python
import pytest
import snowflake.connector
from snowduck import mock_snowflake, seed_table

# Simple unit test - automatic setup and cleanup
@mock_snowflake
def test_query_execution():
    """Each test gets a fresh, isolated in-memory database."""
    conn = snowflake.connector.connect()
    cursor = conn.cursor()
    
    cursor.execute("CREATE TABLE users (id INT, name VARCHAR)")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    cursor.execute("SELECT * FROM users")
    
    assert cursor.fetchone() == (1, 'Alice')
    # ✅ Automatic cleanup - no side effects!

# Test with seed data
@mock_snowflake
def test_with_seed_data():
    """Use seed_table() for easy test data creation."""
    conn = snowflake.connector.connect()
    
    # One line to create and populate table!
    seed_table(conn, 'customers', {
        'id': [1, 2, 3],
        'name': ['Acme Corp', 'TechStart', 'DataCo'],
        'tier': ['enterprise', 'startup', 'enterprise']
    })
    
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM customers WHERE tier = 'enterprise'")
    assert cursor.fetchone()[0] == 2
```

**For shared fixtures across tests, use `conftest.py`:**
```python
# conftest.py
import pytest
from snowduck import patch_snowflake
import snowflake.connector

@pytest.fixture
def conn():
    """Per-test isolation with automatic cleanup."""
    with patch_snowflake():
        conn = snowflake.connector.connect()
        yield conn
        # ✅ Automatic cleanup on exit

def test_my_feature(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1
```

**Testing Patterns:**
- ✅ **Simple tests**: Use `@mock_snowflake` decorator
- ✅ **Shared setup**: Use fixture with `patch_snowflake()` context manager  
- ✅ **Isolated tests**: In-memory (default) - fresh DB per test
- ✅ **Persistent data**: File-based - `start_patch_snowflake(db_file='test.duckdb')`

### 3. CI/CD Integration
```yaml
# .github/workflows/test.yml
- name: Test SQL transformations
  run: |
    pip install snowduck pytest
    pytest tests/  # Uses SnowDuck instead of real Snowflake
```

### 4. REST API Server
```bash
# Install with server extras
uv pip install snowduck[server]

# Start the server
just serve
# Or: uvicorn snowduck.server:app --reload

# Server runs at http://localhost:8000
# - Execute SQL queries via REST API
# - Get results in Arrow IPC format
# - Multi-session support with session management
# - Compatible with Snowflake REST API clients
```

## Architecture

SnowDuck works by:

1. **Patching** - Intercepts Snowflake connector calls via `start_patch_snowflake()`
2. **Translating** - Converts Snowflake SQL dialect to DuckDB-compatible SQL
3. **Executing** - Runs queries in DuckDB's fast in-memory engine
4. **Emulating** - Mimics Snowflake's information schema and metadata

```
┌─────────────────────┐
│  Your Application   │
│  (Snowflake code)   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  SnowDuck Patch     │  ← Intercepts connector calls
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  SQL Translator     │  ← Snowflake → DuckDB dialect
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   DuckDB Engine     │  ← Fast in-memory execution
└─────────────────────┘
```

## Development

**Prerequisites**: Python 3.11+, [uv](https://github.com/astral-sh/uv), [just](https://github.com/casey/just)

```bash
# Clone and setup
git clone https://github.com/hupe1980/snowduck.git
cd snowduck
uv sync

# Run tests (128 tests, <2s)
just test

# Run all quality checks
just check

# See all commands
just --list
```

### Testing Best Practices

**When writing tests with SnowDuck:**

1. **Use `@mock_snowflake` for unit tests** - Clean, automatic cleanup
   ```python
   from snowduck import mock_snowflake
   
   @mock_snowflake
   def test_my_feature():
       conn = snowflake.connector.connect()
       # Test code here - fresh DB, auto cleanup!
   ```

2. **Use `seed_table()` for test data** - One-line table creation
   ```python
   seed_table(conn, 'test_table', {'id': [1, 2], 'name': ['a', 'b']})
   ```

3. **Choose the right storage mode:**
   - In-memory (default): Fast, isolated tests
   - File-based: Shared fixtures, persistent data
   ```python
   # Isolated: Each test gets fresh DB
   @mock_snowflake  # Uses in-memory by default
   def test_isolated(): ...
   
   # Shared: Data persists across tests
   @pytest.fixture(scope="session")
   def shared_data():
       start_patch_snowflake(db_file='fixtures.duckdb')
   ```

4. **Avoid anti-patterns:**
   - ❌ Don't mix `start_patch_snowflake()` with `@mock_snowflake`
   - ❌ Don't forget cleanup with manual `start_patch_snowflake()`
   - ✅ Use decorators or context managers for automatic cleanup

## Project Status

- ✅ **128 tests** (100% passing, <2s execution)
- ✅ **Zero linting errors** - Professional, clean codebase
- ✅ **90%+ Snowflake compatibility** - DDL, DML, functions, CTEs, window functions
- ✅ **Modern CI/CD** - PyPI trusted publishers with package attestations
- ✅ **Production-ready** for development & testing use cases
- 🚧 **Experimental** - Not for production Snowflake replacement

**What's Next:**
- Type safety improvements (mypy strict mode)
- Performance monitoring and benchmarks  
- Enhanced error messages with helpful suggestions
- API documentation with Sphinx
## Roadmap

- [ ] Additional Snowflake functions (JSON, ARRAY, etc.)
- [ ] Stored procedure emulation
- [ ] External table support
- [ ] Enhanced security features
- [ ] Performance benchmarks vs. Snowflake

## Contributing

Contributions welcome! We'd love help with:

- 🐛 Bug reports and fixes
- ✨ New Snowflake function implementations
- 📚 Documentation improvements
- 🧪 Additional test coverage

**Development Setup:**
- 📚 Documentation improvements
- 🧪 Additional test coverage

**Development Setup:**
```bash
git clone https://github.com/hupe1980/snowduck.git
cd snowduck
uv sync
just test  # Run 128 tests in <2s
```

**Code Quality:**
- Zero linting errors (ruff)
- Comprehensive test coverage
- Modern CI/CD with package attestations

## License

MIT License - see [LICENSE](LICENSE) for details.

---

**Built with ❄️ and 🦆