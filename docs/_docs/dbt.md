---
layout: default
title: dbt Integration
parent: Documentation
nav_order: 7
---

# dbt Integration

SnowDuck runs dbt-snowflake locally, so you can build and test dbt models
without a Snowflake account.

`dbt build`, `dbt snapshot` and `dbt docs generate` are exercised end to end
against **dbt-core 1.12 / dbt-snowflake 1.12** as part of development, covering
seeds, view / table / incremental models, snapshots, SQL function models, data
tests and the catalog.

## Setup (Recommended: Server Mode)

The simplest way to use SnowDuck with dbt is to run the server and point dbt at it.

### 1. Install Dependencies

```bash
pip install snowduck[server] dbt-snowflake
```

### 2. Start the SnowDuck Server

```bash
snowduck --port 8000
```

Or with Docker:

```bash
docker run -p 8000:8000 ghcr.io/hupe1980/snowduck:latest
```

### 3. Configure profiles.yml

Point dbt to your local SnowDuck server:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: test
      user: test
      password: test
      host: localhost           # Hostname only - the port goes in `port`
      port: 8000
      protocol: http            # Use HTTP (not HTTPS)
      database: DEV_DB
      warehouse: WH             # Any name; SnowDuck has no compute to size
      schema: PUBLIC
      threads: 4
```

{: .warning }
> Put the port in `port`, not in `host`. dbt appends `:{{ port }}` to whatever
> `host` you give it, so `host: localhost:8000` produces
> `http://localhost:8000:8000` and the connection fails to parse.

### 4. Run dbt Commands

```bash
# Create database first (one-time setup)
curl -X POST http://localhost:8000/api/v2/statements \
  -H "Content-Type: application/json" \
  -d '{"statement": "CREATE DATABASE DEV_DB"}'

# Now run dbt as normal
dbt seed
dbt run
dbt test
```

That's it! No patching, no Python wrapper scripts needed.

## Alternative: Patch Mode

If you prefer not to run a server, you can patch the connector directly:

```python
from snowduck import start_patch_snowflake
import subprocess

start_patch_snowflake()
subprocess.run(["dbt", "run"])
subprocess.run(["dbt", "test"])
```

## Testing dbt Models

### pytest Integration with Server

```python
import pytest
import subprocess
import time
import requests

@pytest.fixture(scope="session")
def dbt_server():
    """Start SnowDuck server for dbt testing."""
    # Start server in background
    server = subprocess.Popen(["snowduck", "--port", "8001"])
    time.sleep(2)  # Wait for startup
    
    # Create database
    requests.post(
        "http://localhost:8001/api/v2/statements",
        json={"statement": "CREATE DATABASE DEV_DB"}
    )
    
    yield "http://localhost:8001"
    
    server.terminate()

def test_dbt_run(dbt_server):
    """Test that dbt run completes successfully."""
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", ".", "--target", "local"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"dbt run failed: {result.stderr}"

def test_dbt_test(dbt_server):
    """Test that dbt test passes."""
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", ".", "--target", "local"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"dbt test failed: {result.stderr}"
```

## Schema Names

dbt lists a schema with `SHOW OBJECTS` and then looks each relation up under the
upper-cased name its own `Relation` renders to, so SnowDuck folds unquoted
identifiers to upper case exactly as Snowflake does.

{: .note }
> This includes a schema named `MAIN`, which is a common dbt target. DuckDB has
> an internal schema called `main` that cannot be renamed, so SnowDuck tracks
> the `CREATE SCHEMA` and reports it as `MAIN`. Earlier releases returned
> `"MY_DB"."main"."MY_MODEL"` here, which passed on a first run and then failed
> every model on the second with *"dbt found an approximate match"*.

## Supported dbt Features

| Feature | Status | Notes |
|---------|--------|-------|
| **Models** | | |
| `SELECT` statements | ✅ | |
| CTEs | ✅ | |
| Subqueries | ✅ | |
| JOINs | ✅ | All types |
| Window functions | ✅ | |
| QUALIFY | ✅ | |
| **Materializations** | | |
| `view` | ✅ | |
| `table` | ✅ | |
| `incremental` | ✅ | `merge`, `delete+insert` and `append` strategies |
| `ephemeral` | ✅ | |
| `snapshot` | ✅ | `timestamp` and `check` strategies |
| `function` (SQL) | ✅ | dbt 1.12 SQL function models; Python and JavaScript raise a clear error |
| `dynamic_table` | ❌ | Requires Snowflake's incremental compute |
| **Operations** | | |
| MERGE | ✅ | Including dbt's bare-column `WHEN NOT MATCHED ... VALUES` |
| INSERT / `INSERT OVERWRITE` | ✅ | `INSERT OVERWRITE` backs `dbt seed` |
| UPDATE / DELETE | ✅ | |
| **Metadata** | | |
| Relation listing | ✅ | `SHOW OBJECTS` with pagination (`LIMIT ... FROM`) |
| Function listing | ✅ | `SHOW USER FUNCTIONS` |
| Schema listing / existence | ✅ | `SHOW TERSE SCHEMAS`, `INFORMATION_SCHEMA.SCHEMATA` |
| Column metadata | ✅ | `DESCRIBE TABLE` |
| `dbt docs generate` | ✅ | Catalog built from `INFORMATION_SCHEMA.TABLES` / `.COLUMNS` |
| `query_tag` config | ✅ | `ALTER SESSION SET` round-trips through `SHOW PARAMETERS` |
| `persist_docs` config | ✅ | Model and column descriptions become comments, read back through `INFORMATION_SCHEMA` and `DESCRIBE TABLE`. Snowflake's multi-column `ALTER TABLE ... ALTER <col> COMMENT $$...$$` form - which sqlglot cannot parse - is re-read by SnowDuck |
| `contract` / constraints | ⚠️ | `PRIMARY KEY` and `UNIQUE` are recorded and reported through `INFORMATION_SCHEMA.TABLE_CONSTRAINTS`; `ALTER TABLE ... ADD`/`DROP CONSTRAINT` is accepted and ignored, as Snowflake does not enforce them either |
| **Macros** | | |
| `{{ ref() }}` | ✅ | |
| `{{ source() }}` | ✅ | |
| `{{ config() }}` | ✅ | |
| **Tests** | | |
| Schema tests | ✅ | unique, not_null, etc. |
| Data tests | ✅ | Custom SQL tests |
| **Seeds** | | |
| CSV seed loading | ✅ | |
| **Grants** | | |
| `grants` config | ⚠️ | `SHOW GRANTS` returns an empty result; grants are not enforced |

## Seeding Test Data

### Option 1: Use dbt Seeds (Recommended)

Create CSV files in your `seeds/` directory:

```csv
# seeds/customers.csv
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
```

Run `dbt seed`:

```bash
dbt seed
```

### Option 2: Use SQL REST API

```bash
# Insert data via REST API
curl -X POST http://localhost:8000/api/v2/statements \
  -H "Content-Type: application/json" \
  -d '{
    "statement": "CREATE TABLE raw_customers AS SELECT * FROM (VALUES (1, '\''Alice'\''), (2, '\''Bob'\'')) AS t(id, name)",
    "database": "DEV_DB",
    "schema": "PUBLIC"
  }'
```

### Option 3: Use seed_table (Patch Mode)

```python
from snowduck import start_patch_snowflake, seed_table
import snowflake.connector

start_patch_snowflake()
conn = snowflake.connector.connect(user="x", password="x", account="x")

seed_table(conn, "raw_customers", {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"]
})
```

## Example: Testing a dbt Model

### Model: `models/marts/customer_orders.sql`

```sql
{{ config(materialized='table') }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id) AS total_orders,
    SUM(o.amount) AS total_amount
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
```

### Test: `tests/test_customer_orders.py`

```python
import pytest
from snowduck import mock_snowflake, seed_table
import snowflake.connector
import subprocess

@mock_snowflake
def test_customer_orders_model():
    conn = snowflake.connector.connect(user="x", password="x", account="x")
    cursor = conn.cursor()
    
    # Set up database
    cursor.execute("CREATE DATABASE DEV_DB")
    cursor.execute("USE DATABASE DEV_DB")
    cursor.execute("CREATE SCHEMA PUBLIC")
    cursor.execute("USE SCHEMA PUBLIC")
    
    # Seed staging tables (simulating upstream models)
    seed_table(conn, "stg_customers", {
        "customer_id": [1, 2],
        "customer_name": ["Alice", "Bob"]
    })
    
    seed_table(conn, "stg_orders", {
        "order_id": [101, 102, 103],
        "customer_id": [1, 1, 2],
        "amount": [100.0, 200.0, 150.0]
    })
    
    # Run dbt model
    result = subprocess.run(
        ["dbt", "run", "--select", "customer_orders"],
        capture_output=True
    )
    assert result.returncode == 0
    
    # Verify results
    cursor.execute("SELECT * FROM customer_orders ORDER BY customer_id")
    results = cursor.fetchall()
    
    assert results[0] == (1, "Alice", 2, 300.0)  # Alice: 2 orders, $300
    assert results[1] == (2, "Bob", 1, 150.0)    # Bob: 1 order, $150
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: dbt Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      snowduck:
        image: ghcr.io/hupe1980/snowduck:latest
        ports:
          - 8000:8000
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: pip install dbt-snowflake
      
      - name: Initialize database
        run: |
          curl -X POST http://localhost:8000/api/v2/statements \
            -H "Content-Type: application/json" \
            -d '{"statement": "CREATE DATABASE DEV_DB"}'
      
      - name: Run dbt
        run: |
          dbt deps
          dbt seed
          dbt run
          dbt test
```

### Docker Compose for Local Development

```yaml
# docker-compose.yml
services:
  snowduck:
    image: ghcr.io/hupe1980/snowduck:latest
    ports:
      - "8000:8000"
    volumes:
      - snowduck-data:/data
    command: ["--host", "0.0.0.0", "--port", "8000"]

volumes:
  snowduck-data:
```

Then simply:

```bash
docker compose up -d
dbt run
```

{: .tip }
> **Performance**: SnowDuck runs dbt models 10-100x faster than cloud Snowflake, making it ideal for rapid iteration during development.

{: .warning }
> **Limitations**: Clustering, time travel, dynamic tables and stored
> procedures are not supported. See [Limitations](limitations) for the full
> list, and test those in a real Snowflake environment before production.
