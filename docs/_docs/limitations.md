---
layout: default
title: Limitations
parent: Documentation
nav_order: 8
---

# Limitations & Compatibility

SnowDuck is designed for testing and development, not as a full Snowflake replacement. This page documents what is and isn't supported.

## Fully Supported

These features work identically to Snowflake:

### SQL Operations
- ✅ SELECT, INSERT, UPDATE, DELETE, MERGE
- ✅ CREATE/DROP DATABASE, SCHEMA, TABLE, VIEW, SEQUENCE, STAGE
- ✅ CTEs (WITH clause)
- ✅ Subqueries
- ✅ All JOIN types
- ✅ UNION, INTERSECT, EXCEPT
- ✅ Window functions with PARTITION BY and ORDER BY
- ✅ QUALIFY clause
- ✅ LATERAL FLATTEN and TABLE(FLATTEN(...)) with all six columns (SEQ, KEY, PATH, INDEX, VALUE, THIS)
- ✅ TABLE(SPLIT_TO_TABLE(...)) and TABLE(<udf>(...))
- ✅ MERGE with qualified SET targets, UPDATE ... FROM, DELETE ... USING
- ✅ CREATE TABLE ... CLONE / LIKE, TRANSIENT and TEMPORARY tables
- ✅ GROUP BY ROLLUP / CUBE / GROUPING SETS, PIVOT / UNPIVOT
- ✅ SHOW DATABASES / SCHEMAS / OBJECTS / TABLES / COLUMNS
- ✅ LIKE ANY / LIKE ALL
- ✅ SQL UDFs (`CREATE FUNCTION ... AS $$ ... $$`), scalar and table
- ✅ Sequences (`seq.NEXTVAL`)

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
| JavaScript / Python UDFs | Only SQL UDFs are emulated; these raise a clear error |
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

SnowDuck follows DuckDB's NULL handling, which is generally compatible with
Snowflake. The places where Snowflake differs from a naive DuckDB translation
are emulated explicitly and covered by the conformance suite - see
[Snowflake Semantics](snowflake-semantics).

### Known Remaining Gaps

| Area | Difference |
|------|------------|
| `FLATTEN` `SEQ` column | Always 1. Snowflake documents it as "not necessarily gap-free or ordered", but does give each input record a distinct value |
| `COLLATE` | Only the case-insensitive specifiers (`-ci`) are emulated, by folding case. Locale-specific collation is not |
| `TO_NUMBER` format models | The model's decoration (currency symbols, group separators) is stripped rather than validated, so a malformed input parses instead of erroring |
| `ARRAY` element types | An `ARRAY` column is stored as JSON, so its elements read back as JSON values (`'10'` rather than `10`). Array literals of a single type keep their native types |
| `CLONE` | An eager copy, not Snowflake's zero-copy clone. Reads and writes behave identically; storage and timing do not |
| Named `WINDOW` clause | Not supported - and neither is it in Snowflake, whose `OVER` grammar takes only an inline window definition |

## Recommendations

{: .tip }
> **For Testing**: SnowDuck is excellent for testing SQL logic, transformations, and dbt models.

{: .warning }
> **Before Production**: Always validate critical queries against real Snowflake before deploying.

{: .note }
> **Report Issues**: If you find a compatibility issue, please [open an issue](https://github.com/hupe1980/snowduck/issues) on GitHub.

## Version Compatibility

| Component | Supported |
|-----------|-----------|
| Python | 3.11 - 3.14 |
| snowflake-connector-python | 3.17+ |
| DuckDB | 1.2+ |
| sqlglot | 30.18+ (installed as `sqlglot[c]`) |
| pyarrow | 18+ (unpinned at the top end; <19 has no cp314 wheels) |
| dbt-snowflake | 1.9+ (for dbt testing) |

{: .note }
> SnowDuck depends on `sqlglot[c]`, not `sqlglot[rs]`. sqlglot deprecated the
> Rust extra in 30.x - importing it now emits *"sqlglot[rs] is deprecated and no
> longer compatible with sqlglot"* - and replaced it with `sqlglotc`, which
> compiles the whole library (tokenizer, parser, generator, expressions) rather
> than just the tokenizer.
