---
layout: home
title: Home
nav_order: 1
permalink: /
---

# SnowDuck
{: .fs-9 }

Run Snowflake SQL locally, powered by DuckDB — lightweight in-memory SQL engine for development and testing.
{: .fs-6 .fw-300 }

[Get Started](#getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/hupe1980/snowduck){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## Why SnowDuck?

**SnowDuck** is a drop-in Snowflake mock that lets you run Snowflake SQL locally without a cloud connection. Perfect for:

- 🧪 **Unit Testing** — Test your Snowflake queries without cloud costs
- 🚀 **Local Development** — Iterate faster with sub-millisecond query times
- 🔄 **CI/CD Pipelines** — Run dbt tests without Snowflake credentials
- 📚 **Learning** — Experiment with Snowflake SQL syntax offline

## Features

| Feature | Status |
|---------|--------|
| **100+ Snowflake Functions** | ✅ String, Date, Numeric, JSON, Array, Aggregate |
| **SQL Transpilation** | ✅ Automatic Snowflake → DuckDB translation |
| **Session Variables** | ✅ SET/UNSET with context persistence |
| **Session Parameters** | ✅ ALTER SESSION SET/UNSET, read back with SHOW PARAMETERS |
| **Identifier Case** | ✅ Unquoted names fold to upper case, as in Snowflake |
| **LATERAL FLATTEN** | ✅ Full support via UNNEST |
| **QUALIFY Clause** | ✅ Native DuckDB support |
| **SHOW Commands** | ✅ Snowflake's column shapes, with TERSE / LIKE / STARTS WITH / LIMIT ... FROM |
| **Information Schema** | ✅ Per-database Snowflake-shaped INFORMATION_SCHEMA views |
| **Snowpipe Streaming API** | ✅ Full REST API mock |
| **SQL REST API** | ✅ `/api/v2/statements` endpoints |
| **dbt Compatibility** | ✅ Verified end to end against dbt-core 1.12 / dbt-snowflake 1.12 |

## Getting Started

### Installation

```bash
pip install snowduck
```

For the REST API server:
```bash
pip install snowduck[server]
```

### Quick Start

```python
import snowflake.connector
from snowduck import start_patch_snowflake, stop_patch_snowflake

# Start the mock
start_patch_snowflake()

# Use Snowflake connector as normal - it's now mocked!
conn = snowflake.connector.connect(
    user="test",
    password="test", 
    account="test"
)

cursor = conn.cursor()
cursor.execute("SELECT 'Hello, SnowDuck!' AS greeting")
print(cursor.fetchone()[0])  # Hello, SnowDuck!

# Clean up
stop_patch_snowflake()
```

### With pytest

```python
import pytest
from snowduck import mock_snowflake

@mock_snowflake
def test_my_query():
    import snowflake.connector
    conn = snowflake.connector.connect(user="x", password="x", account="x")
    cursor = conn.cursor()
    cursor.execute("SELECT UPPER('hello') AS result")
    assert cursor.fetchone()[0] == "HELLO"
```

## Docker

Run the SnowDuck server as a container:

```bash
docker run -p 8000:8000 ghcr.io/hupe1980/snowduck:latest
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Your Application                        │
│              (snowflake-connector-python)                   │
└─────────────────────┬───────────────────────────────────────┘
                      │ Patched
┌─────────────────────▼───────────────────────────────────────┐
│                      SnowDuck                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Connector  │  │   Dialect   │  │    Info Schema      │  │
│  │   Layer     │  │   Layer     │  │      Manager        │  │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘  │
│         │                │                     │             │
│         └────────────────┼─────────────────────┘             │
│                          │                                   │
│                  ┌───────▼───────┐                           │
│                  │    DuckDB     │                           │
│                  │   (Engine)    │                           │
│                  └───────────────┘                           │
└─────────────────────────────────────────────────────────────┘
```

## Test Coverage

**507 tests passing** covering:
- SQL transpilation for 100+ functions
- All DML operations (SELECT, INSERT, UPDATE, DELETE, MERGE)
- DDL operations (CREATE, DROP, ALTER)
- Session management
- dbt-snowflake compatibility (37 dedicated tests)
- Snowpipe Streaming REST API (40 tests)

## License

SnowDuck is distributed under the [MIT License](https://github.com/hupe1980/snowduck/blob/main/LICENSE).
