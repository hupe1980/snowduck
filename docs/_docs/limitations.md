---
layout: default
title: Limitations
parent: Documentation
nav_order: 7
---

# Limitations & Compatibility

SnowDuck is designed for testing and development, not as a full Snowflake replacement. This page documents what is and isn't supported.

## Fully Supported

These features work identically to Snowflake:

### SQL Operations
- ✅ SELECT, INSERT, UPDATE, DELETE, MERGE
- ✅ CREATE/DROP DATABASE, SCHEMA, TABLE, VIEW
- ✅ CTEs (WITH clause)
- ✅ Subqueries
- ✅ All JOIN types
- ✅ UNION, INTERSECT, EXCEPT
- ✅ Window functions with PARTITION BY and ORDER BY
- ✅ QUALIFY clause
- ✅ LATERAL FLATTEN

### Data Types
- ✅ VARCHAR, TEXT, STRING
- ✅ NUMBER, INTEGER, BIGINT, FLOAT, DOUBLE
- ✅ BOOLEAN
- ✅ DATE, TIME, TIMESTAMP
- ✅ VARIANT, OBJECT, ARRAY (as JSON)
- ✅ BINARY

### Functions
- ✅ 100+ string, date, numeric, aggregate functions
- ✅ Window functions
- ✅ JSON/VARIANT functions
- ✅ Array functions
- ✅ Hash and encoding functions

## Partially Supported

These features work but with limitations:

### Information Schema
- ⚠️ Only commonly-used views are emulated
- ⚠️ Some columns may have mock values

### ALTER TABLE
- ⚠️ ADD COLUMN, DROP COLUMN work
- ⚠️ Clustering keys are ignored
- ⚠️ Some advanced options not supported

### COPY INTO
- ⚠️ Works with local stage directory
- ⚠️ Cloud storage not supported

### Transactions
- ⚠️ BEGIN, COMMIT, ROLLBACK accepted
- ⚠️ Full transaction isolation not guaranteed

## Not Supported

These Snowflake features are **not supported**:

### Enterprise Features

| Feature | Reason |
|---------|--------|
| Time Travel (AT/BEFORE) | Requires Snowflake's versioned storage |
| CLONE | Zero-copy clone is Snowflake-specific |
| Fail-safe | Snowflake infrastructure feature |
| Data Sharing | Multi-account feature |

### Administration

| Feature | Reason |
|---------|--------|
| GRANT/REVOKE | Tests run with full access |
| CREATE ROLE/USER | Single-user testing context |
| CREATE WAREHOUSE | No compute resources to manage |
| Resource Monitors | No resource limits in mock |

### Automation

| Feature | Reason |
|---------|--------|
| TASK | Scheduled jobs need Snowflake scheduler |
| STREAM | Change data capture needs versioned tables |
| ALERT | Notification system not mocked |
| PIPE (ingest) | Classic Snowpipe not emulated |

### Programmability

| Feature | Reason |
|---------|--------|
| Stored Procedures | JavaScript/Python execution not mocked |
| UDFs | User-defined functions not supported |
| External Functions | External API calls not mocked |
| Snowpark | Requires actual Snowflake runtime |

### Security

| Feature | Reason |
|---------|--------|
| Masking Policies | Security policies not needed for tests |
| Row Access Policies | Security policies not needed for tests |
| Network Policies | Network security not applicable |
| MFA | Authentication not enforced |

### Specialized Tables

| Feature | Reason |
|---------|--------|
| Dynamic Tables | Requires Snowflake's incremental compute |
| External Tables | Cloud storage integration not mocked |
| Iceberg Tables | Apache Iceberg format support limited |
| Directory Tables | Stage listing not fully emulated |

## Behavioral Differences

Some behaviors differ slightly from Snowflake:

### Case Sensitivity

```sql
-- Snowflake: Unquoted identifiers are uppercase
-- SnowDuck: Also uppercase by default

CREATE TABLE MyTable (id INT);  -- Creates "MYTABLE"
SELECT * FROM mytable;          -- Works (case-insensitive lookup)
```

### Error Messages

Error messages may differ in exact wording but should convey the same meaning.

### Performance Characteristics

- SnowDuck is typically **faster** for small datasets
- No network latency
- No query compilation overhead
- Single-node execution only

### NULL Handling

SnowDuck follows DuckDB's NULL handling, which is generally compatible with Snowflake but may differ in edge cases.

## Recommendations

{: .tip }
> **For Testing**: SnowDuck is excellent for testing SQL logic, transformations, and dbt models.

{: .warning }
> **Before Production**: Always validate critical queries against real Snowflake before deploying.

{: .note }
> **Report Issues**: If you find a compatibility issue, please [open an issue](https://github.com/hupe1980/snowduck/issues) on GitHub.

## Version Compatibility

| Component | Minimum Version |
|-----------|-----------------|
| Python | 3.11+ |
| snowflake-connector-python | 3.14+ |
| DuckDB | 1.2+ |
| dbt-snowflake | 1.9+ (for dbt testing) |
