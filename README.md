# ❄️🦆 SnowDuck

[![CI/CD Pipeline](https://github.com/hupe1980/snowduck/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/snowduck/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

> **Run Snowflake SQL locally, powered by DuckDB**

SnowDuck is a lightweight, in-memory SQL engine that emulates Snowflake's behavior for development and testing. Write and test Snowflake SQL locally without cloud access or costs.

## Why SnowDuck?

- 🚀 **Fast Development** - Test SQL queries instantly without waiting for cloud connections
- 💰 **Zero Cloud Costs** - Develop and test locally without Snowflake compute charges
- 🧪 **Easy Testing** - Mock Snowflake databases for unit tests and CI/CD pipelines
- ⚡ **Lightning Fast** - Powered by DuckDB's in-memory execution engine
- 🔌 **Drop-in Compatible** - Uses Snowflake's connector interface - just patch and go

## Features

### Core SQL Support

| Category | Functions |
|----------|-----------|
| **DDL Operations** | CREATE/DROP DATABASE, SCHEMA, TABLE |
| **DML Operations** | INSERT, UPDATE, DELETE, MERGE |
| **Advanced SQL** | CTEs, JOINs, subqueries, CASE, QUALIFY |
| **Session Variables** | SET/SELECT \$variable syntax |
| **Information Schema** | Query metadata (databases, tables, columns) |

### Function Support

| Category | Functions |
|----------|-----------|
| **String** | CONCAT, CONCAT_WS, SPLIT, SPLIT_PART, CONTAINS, REPLACE, TRIM, LTRIM, RTRIM, LPAD, RPAD, SPACE, STRTOK, TRANSLATE, REVERSE, STARTSWITH, ENDSWITH, ASCII, CHR, INITCAP, SOUNDEX, UPPER, LOWER, LENGTH, LEN, SUBSTR, SUBSTRING, INSTR, POSITION |
| **Date/Time** | DATEADD, DATEDIFF, TIMEDIFF, DATE_TRUNC, DATE_PART, EXTRACT, LAST_DAY, ADD_MONTHS, DATE_FROM_PARTS, TIME_FROM_PARTS, TIMESTAMP_FROM_PARTS, CONVERT_TIMEZONE, TO_DATE, TO_TIMESTAMP |
| **Numeric** | ABS, CEIL, FLOOR, ROUND, MOD, SQRT, POWER, EXP, LN, LOG, SIGN, DIV0, DIV0NULL, WIDTH_BUCKET, TRUNCATE, CBRT, FACTORIAL, DEGREES, RADIANS, PI, RANDOM, GREATEST, LEAST |
| **Aggregate** | COUNT, SUM, AVG, MIN, MAX, MEDIAN, LISTAGG, ANY_VALUE, KURTOSIS, SKEW, COVAR_POP, COVAR_SAMP |
| **Window** | ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG, FIRST_VALUE, LAST_VALUE |
| **JSON** | PARSE_JSON, OBJECT_CONSTRUCT, OBJECT_INSERT, GET_PATH, TRY_PARSE_JSON, OBJECT_KEYS, CHECK_JSON, TO_JSON |
| **Array** | ARRAY_CONSTRUCT, ARRAY_SIZE, ARRAY_CONTAINS, FLATTEN, ARRAY_SLICE, ARRAY_CAT, ARRAY_APPEND, ARRAY_PREPEND, ARRAY_SORT, ARRAY_REVERSE, ARRAY_MIN, ARRAY_MAX, ARRAY_SUM, ARRAYS_OVERLAP, ARRAY_DISTINCT, ARRAY_INTERSECTION, ARRAY_EXCEPT |
| **Conditional** | NVL, NVL2, DECODE, IFF, COALESCE, NULLIF, EQUAL_NULL, ZEROIFNULL, NULLIFZERO |
| **Conversion** | TO_CHAR, TO_NUMBER, TO_BOOLEAN, TO_DATE, TRY_CAST, TRY_TO_NUMBER, TRY_TO_DATE, TRY_TO_TIMESTAMP, TRY_TO_BOOLEAN |
| **Regex** | REGEXP_LIKE, REGEXP_SUBSTR, REGEXP_REPLACE, REGEXP_COUNT |
| **Hash** | MD5, SHA1, SHA2, SHA256, HASH |
| **Encoding** | BASE64_ENCODE, BASE64_DECODE_STRING, HEX_ENCODE, HEX_DECODE_STRING |
| **Bitwise** | BITAND, BITOR, BITXOR, BITNOT, BITAND_AGG, BITOR_AGG, BITXOR_AGG |
| **Boolean Agg** | BOOLAND_AGG, BOOLOR_AGG |
| **Utility** | UUID_STRING, TYPEOF |

### Cursor Methods

SnowDuck supports all standard Snowflake cursor methods:

- `execute()` - Execute SQL statements
- `fetchone()` - Fetch a single row
- `fetchmany(size)` - Fetch multiple rows
- `fetchall()` - Fetch all rows
- `fetch_pandas_all()` - Fetch all rows as pandas DataFrame
- `fetch_pandas_batches()` - Fetch rows as iterator of DataFrames
- `get_result_batches()` - Get Arrow record batches
- `describe()` - Get result schema without execution

> **Note**: SnowDuck is designed for development and testing. Use production Snowflake for production workloads.

## Quick Start

### Installation

```bash
# Using uv (recommended)
uv pip install snowduck

# Or using pip
pip install snowduck
```

### Basic Usage

```python
import snowflake.connector
from snowduck import start_patch_snowflake

# Patch the Snowflake connector to use DuckDB
start_patch_snowflake()

# Use Snowflake connector as normal - it's now backed by DuckDB!
with snowflake.connector.connect() as conn:
    cursor = conn.cursor()
    
    cursor.execute("CREATE DATABASE my_database")
    cursor.execute("USE DATABASE my_database")
    
    cursor.execute("""
        CREATE TABLE employees (id INTEGER, name VARCHAR, salary INTEGER)
    """)
    
    cursor.execute("""
        INSERT INTO employees VALUES
        (1, 'Alice', 95000),
        (2, 'Bob', 75000),
        (3, 'Carol', 105000)
    """)
    
    cursor.execute("""
        SELECT name, salary, RANK() OVER (ORDER BY salary DESC) as rank
        FROM employees
    """)
    
    for row in cursor.fetchall():
        print(f"{row[0]}: \${row[1]:,} (Rank: {row[2]})")
```

### Data Persistence

```python
# In-memory (default) - fast, isolated
start_patch_snowflake()

# File-based - persistent across restarts
start_patch_snowflake(db_file='my_data.duckdb')

# Fresh start - reset existing data
start_patch_snowflake(db_file='my_data.duckdb', reset=True)
```

### Test Data Seeding

```python
from snowduck import seed_table

with snowflake.connector.connect() as conn:
    # From dict
    seed_table(conn, 'customers', {
        'id': [1, 2, 3],
        'name': ['Acme', 'TechStart', 'DataCo']
    })
    
    # From pandas DataFrame
    seed_table(conn, 'orders', df)
```

## Testing

### Using the Decorator

```python
from snowduck import mock_snowflake

@mock_snowflake
def test_query():
    conn = snowflake.connector.connect()
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1
```

### Using the Context Manager

```python
from snowduck import patch_snowflake

def test_with_fixture():
    with patch_snowflake():
        conn = snowflake.connector.connect()
        # Test code here
```

### pytest Fixture

```python
import pytest
from snowduck import patch_snowflake

@pytest.fixture
def conn():
    with patch_snowflake():
        yield snowflake.connector.connect()

def test_feature(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
```

## REST API Server

```bash
# Install with server extras
uv pip install snowduck[server]

# Start the server
uvicorn snowduck.server:app --reload
```

The server provides:
- Execute SQL queries via REST API
- Arrow IPC format responses
- Multi-session support

## Architecture

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

## Examples

See the [examples/](examples/) directory for Jupyter notebooks demonstrating:
- Basic operations and queries
- String, date, and numeric functions
- JSON and array operations
- Window functions
- Advanced SQL patterns

## Development

```bash
git clone https://github.com/hupe1980/snowduck.git
cd snowduck
uv sync
just test
just check
```

## Contributing

Contributions welcome! See issues for areas where help is needed.

## License

MIT License - see [LICENSE](LICENSE) for details.
