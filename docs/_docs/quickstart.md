---
layout: default
title: Quick Start
parent: Documentation
nav_order: 2
---

# Quick Start

This guide will get you up and running with SnowDuck in minutes.

## Basic Usage

SnowDuck patches the `snowflake-connector-python` library to redirect all SQL queries to a local DuckDB instance.

### Starting the Patch

```python
import snowflake.connector
from snowduck import start_patch_snowflake, stop_patch_snowflake

# Start the mock - all Snowflake connections are now redirected to DuckDB
start_patch_snowflake()

# Create a connection (credentials are ignored)
conn = snowflake.connector.connect(
    user="any",
    password="any",
    account="any"
)

# Execute queries as normal
cursor = conn.cursor()
cursor.execute("SELECT 1 + 1 AS result")
print(cursor.fetchone()[0])  # 2

# Stop the mock when done
stop_patch_snowflake()
```

### Using Context Manager

For automatic cleanup, use the context manager:

```python
from snowduck import patch_snowflake
import snowflake.connector

with patch_snowflake():
    conn = snowflake.connector.connect(user="x", password="x", account="x")
    cursor = conn.cursor()
    cursor.execute("SELECT UPPER('hello world')")
    print(cursor.fetchone()[0])  # HELLO WORLD
# Patch automatically stopped here
```

## Data Persistence

By default, SnowDuck uses an in-memory database. To persist data across sessions:

```python
start_patch_snowflake(db_file="my_data.duckdb")
```

This is useful for:
- Jupyter notebooks where you want to keep data between cell executions
- Development workflows where you want to inspect data

## Creating Test Data

Use the `seed_table` function to quickly create test fixtures:

```python
from snowduck import start_patch_snowflake, seed_table
import snowflake.connector

start_patch_snowflake()
conn = snowflake.connector.connect(user="x", password="x", account="x")

# Seed from a dictionary
seed_table(conn, "users", {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com"]
})

# Query the seeded data
cursor = conn.cursor()
cursor.execute("SELECT * FROM users WHERE name = 'Alice'")
print(cursor.fetchall())
```

## Setting Database Context

SnowDuck supports database and schema context:

```python
cursor = conn.cursor()

# Create and use a database
cursor.execute("CREATE DATABASE mydb")
cursor.execute("USE DATABASE mydb")

# Create and use a schema
cursor.execute("CREATE SCHEMA myschema")
cursor.execute("USE SCHEMA myschema")

# Create a table in the current context
cursor.execute("CREATE TABLE users (id INT, name VARCHAR)")
```

## Session Variables

Set and use session variables:

```python
cursor = conn.cursor()

# Set a variable
cursor.execute("SET my_var = 'hello'")

# Use the variable
cursor.execute("SELECT $my_var")
print(cursor.fetchone()[0])  # hello
```

## What's Next?

- [Testing Guide](testing) - Learn how to use SnowDuck in your test suite
- [Supported Functions](functions) - See all supported Snowflake functions
- [REST API](rest-api) - Use the REST API server for streaming applications
