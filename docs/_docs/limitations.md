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
- ✅ `INSERT OVERWRITE INTO` (truncate-and-insert)
- ✅ `TRUNCATE TABLE [IF EXISTS]`
- ✅ LIKE ANY / LIKE ALL
- ✅ SQL UDFs (`CREATE FUNCTION ... AS $$ ... $$`), scalar and table
- ✅ Sequences (`seq.NEXTVAL`)
- ✅ `ALTER SESSION SET`/`UNSET`, read back with `SHOW PARAMETERS`
- ✅ Session-scoped transactions (BEGIN / COMMIT / ROLLBACK across cursors)
- ✅ `ALTER TABLE ... SWAP WITH`, `CLUSTER BY`, `SET COMMENT`
- ✅ `ALTER TABLE ... ALTER/MODIFY COLUMN`: `SET DATA TYPE`, `SET`/`DROP NOT NULL`,
  `SET`/`DROP DEFAULT`, `COMMENT`
- ✅ Comments on tables, views and columns, inline (`COMMENT = '...'`,
  `COMMENT '...'`) or standalone (`COMMENT ON`) — what dbt's `persist_docs` writes
- ✅ `AUTOINCREMENT` and `IDENTITY(start, step)` columns
- ✅ Stage lifecycle: CREATE / LIST / REMOVE / DROP STAGE, PUT, COPY INTO
- ✅ `SET`/`UNSET` session variables, `LAST_QUERY_ID()`, `SYSTEM$TYPEOF()`
- ✅ Declared `VARCHAR(n)` / `CHAR(n)` lengths in INFORMATION_SCHEMA and `internal_size`
- ✅ `TIMESTAMP_TZ` literals with a numeric UTC offset (`+0200`)

### Catalog

`SHOW` returns Snowflake's documented column shape for each object type, not
just the columns SnowDuck can fill. Clients index into those results by name -
dbt-snowflake selects `database_name, schema_name, name, kind, is_dynamic,
is_iceberg` out of `SHOW OBJECTS` - so a missing column is a hard failure at the
client, not a degraded result.

- ✅ SHOW OBJECTS / TABLES / VIEWS / SCHEMAS / DATABASES / COLUMNS
- ✅ SHOW FUNCTIONS / USER FUNCTIONS / SEQUENCES / STAGES / WAREHOUSES
- ✅ SHOW PARAMETERS / VARIABLES
- ✅ `TERSE`, `LIKE '<pattern>'`, `STARTS WITH '<prefix>'`, `LIMIT <n> [ FROM '<name>' ]`
- ✅ Per-database `INFORMATION_SCHEMA`: DATABASES, SCHEMATA, TABLES, VIEWS,
  COLUMNS, FUNCTIONS, SEQUENCES, TABLE_CONSTRAINTS, KEY_COLUMN_USAGE,
  REFERENTIAL_CONSTRAINTS, INFORMATION_SCHEMA_CATALOG_NAME, APPLICABLE_ROLES,
  ENABLED_ROLES
- ⚠️ `INFORMATION_SCHEMA` views for object kinds with no local equivalent
  (TABLE_PRIVILEGES, USAGE_PRIVILEGES, OBJECT_PRIVILEGES, VIEW_TABLE_USAGE,
  EXTERNAL_TABLES, FILE_FORMATS, PROCEDURES, LOAD_HISTORY) return an **empty
  result with the right columns** rather than an error
- ⚠️ Object types with no local equivalent (DYNAMIC TABLES, ICEBERG TABLES,
  EXTERNAL TABLES, PROCEDURES, STREAMS, TASKS, PIPES, FILE FORMATS, GRANTS,
  PRIMARY/UNIQUE/IMPORTED KEYS, TRANSACTIONS) return an **empty result with the
  right columns** rather than an error

### Data Types
- ✅ VARCHAR, TEXT, STRING
- ✅ NUMBER, INTEGER, BIGINT, FLOAT, DOUBLE
- ✅ BOOLEAN
- ✅ DATE, TIME, TIMESTAMP
- ✅ VARIANT, OBJECT, ARRAY (as JSON)
- ✅ BINARY

### Functions
- ✅ 150+ string, date, numeric, aggregate functions
- ✅ The full VARIANT accessor and predicate families (`AS_*`, `IS_*`), with
  Snowflake's type-checking semantics rather than a coercing cast
- ✅ `TO_CHAR` / `TO_VARCHAR` numeric format models
- ✅ Window functions
- ✅ JSON/VARIANT functions
- ✅ Array functions
- ✅ Hash and encoding functions

## Partially Supported

These features work but with limitations:

### Information Schema
- ⚠️ Only the views listed above are emulated; anything else raises a catalog
  error naming the view
- ⚠️ Columns Snowflake computes from its own storage layer are mocked:
  `bytes` is NULL, `row_count` is DuckDB's estimate, `created`/`last_altered`
  are the epoch, and every owner is `SYSADMIN`

### ALTER TABLE
- ✅ ADD COLUMN, DROP COLUMN, RENAME COLUMN, RENAME TO
- ✅ ALTER/MODIFY COLUMN: SET DATA TYPE, SET/DROP NOT NULL, SET/DROP DEFAULT, COMMENT
- ⚠️ Clustering keys are ignored
- ⚠️ `ADD`/`DROP CONSTRAINT` are accepted and ignored: Snowflake records
  constraints as metadata and does not enforce them, and DuckDB has no
  `DROP CONSTRAINT` at all

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
| Zero-copy CLONE | `CLONE` runs, but as an eager copy - see Known Remaining Gaps |
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

Unquoted identifiers fold to upper case and quoted ones keep their case, as in
Snowflake:

```sql
CREATE TABLE MyTable (id INT);    -- creates MYTABLE with column ID
SELECT * FROM mytable;            -- resolves to MYTABLE
CREATE TABLE "MixedCase" (x INT); -- creates MixedCase
```

The one deviation: a *quoted* identifier still resolves case-insensitively,
because DuckDB matches identifiers that way. Snowflake would treat
`SELECT * FROM "mixedcase"` as a different object from `"MixedCase"`; SnowDuck
finds it.

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

### Parser Warnings

sqlglot logs a warning whenever it cannot model a statement and degrades it to
an opaque `Command`. SnowDuck handles several of those deliberately - the whole
`SHOW` family and Snowflake's multi-column `ALTER TABLE ... ALTER <col>
COMMENT` are re-read by their own scanners - so those warnings are held back
during parsing and dropped once the statement is recognised. A fallback SnowDuck
does *not* handle still logs, so nothing is hidden:

```python
cur.execute("SHOW USER FUNCTIONS")   # handled: no warning
cur.execute("CREATE TASK t ...")     # unhandled: warning is replayed
```

The same rule applies to sqlglot's generator: it logs
*"Unsupported ALTER COLUMN syntax"* for clauses it emits correctly anyway, so
those statements are rendered by SnowDuck instead and stay quiet. This is
pinned by `tests/conformance/test_ddl_metadata.py::TestParserWarnings`.

### Known Remaining Gaps

| Area | Difference |
|------|------------|
| `FLATTEN` `SEQ` column | Always 1. Snowflake documents it as "not necessarily gap-free or ordered", but does give each input record a distinct value |
| `COLLATE` | Only the case-insensitive specifiers (`-ci`) are emulated, by folding case. Locale-specific collation is not |
| `TO_NUMBER` format models | The model's decoration (currency symbols, group separators) is stripped rather than validated, so a malformed input parses instead of erroring |
| `ARRAY` element types | An `ARRAY` column is stored as JSON, so its elements read back as JSON values (`'10'` rather than `10`). Array literals of a single type keep their native types |
| `CLONE` | An eager copy, not Snowflake's zero-copy clone. Reads and writes behave identically; storage and timing do not |
| Named `WINDOW` clause | Not supported - and neither is it in Snowflake, whose `OVER` grammar takes only an inline window definition |
| Quoted identifier lookup | Case-insensitive, where Snowflake is case-sensitive (see above) |
| `SHOW ... STARTS WITH` | Matched case-insensitively, since identifiers are stored as written |
| `SHOW USER FUNCTIONS` `arguments` | Reports `VARIANT` for every parameter and return type: a UDF becomes a DuckDB macro, which has no typed signature |
| `SHOW STAGES` | Lists every directory under `SNOWDUCK_STAGE_DIR`, regardless of which database or schema the stage was created in |
| `ARRAY` / `OBJECT` / `VARIANT` columns | All three are stored as JSON, so `INFORMATION_SCHEMA.COLUMNS` reports `VARIANT` for all of them |
| `ALTER TABLE ... SWAP WITH` | Three renames rather than an atomic swap: a concurrent reader can observe the intermediate state |
| `CLUSTER BY`, `DATA_RETENTION_TIME_IN_DAYS` | Accepted and ignored - Snowflake storage hints with no local meaning |
| `BEGIN NAME <name>` | Named transactions are not parsed; use a bare `BEGIN` |
| `ALTER TABLE ... ALTER COLUMN` multi-column form | Only the `COMMENT` clause is recognised in the multi-column form (which is what dbt's `persist_docs` emits); combining other clauses across several columns in one statement is not |
| `LAST_QUERY_ID()` | Only the most recent statement is tracked, so the optional offset argument is ignored |
| `TIMESTAMP_LTZ` vs `TIMESTAMP_TZ` | Both map to DuckDB's `TIMESTAMPTZ`, so INFORMATION_SCHEMA reports `TIMESTAMP_TZ` for either |
| A schema named `MAIN` | DuckDB owns an internal schema called `main` in every attached database that cannot be dropped or renamed, so Snowflake's `MAIN` is stored there. SnowDuck records the `CREATE SCHEMA` and reports the name as `MAIN`, but a schema explicitly quoted as `"main"` is indistinguishable from it |
| `TEMPORARY` tables and views | A qualified `CREATE TEMPORARY <rel> <db>.<schema>.<name>` becomes a permanent relation: DuckDB keeps temporary objects in its own `temp` catalog and rejects any other qualification |
| `TRANSIENT` | Accepted and ignored - it only removes Fail-safe, which SnowDuck does not have |
| `TO_TIME` | Casts only; DuckDB has no `TIMESTAMP WITH TIME ZONE` → `TIME` cast, so a timestamp argument is not supported |
| `AS_DATE` / `AS_TIME` / `AS_TIMESTAMP_*` / `AS_BINARY` | A cast, not a type check like the rest of the `AS_` family: JSON has no such type to check against, so the VARIANT holds a string. `IS_DATE` and friends answer "a string that reads back as that type" |
| `TO_CHAR` numeric format models | `X` (hexadecimal) and the locale-aware `TME` are not modelled; a model using them is left to sqlglot, which drops it |
| `COLLATION` | Always NULL - nothing local carries a stored collation, which is what Snowflake reports for an uncollated expression |
| `HLL_ACCUMULATE` / `HLL_COMBINE` / `HLL_ESTIMATE` | The sketch is the set of distinct values, so the estimate is exact rather than approximate, and a sketch is not portable to Snowflake. `HLL` itself is DuckDB's `approx_count_distinct` |
| `GET_DDL` | Reports DuckDB's rendering of the object rather than Snowflake DDL, and reads back as NULL for an object that does not exist, where Snowflake raises. Only TABLE, VIEW, SEQUENCE and FUNCTION |
| `OBJECT_CONSTRUCT(*)` | Needs exactly one `FROM` source; over a join it raises, because DuckDB has no expression that spans both sides of one |
| Constraint names | `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` reports DuckDB's generated name (`T_ID_PKEY`), not Snowflake's `SYS_CONSTRAINT_<uuid>` |
| `CREATE SECURE VIEW` | Accepted and ignored - the view is created, but `is_secure` still reports false |
| `CREATE MATERIALIZED VIEW` | Becomes a regular view, so it reads correctly but is not maintained incrementally and appears under `SHOW VIEWS` rather than `SHOW MATERIALIZED VIEWS` |
| `INSERT ALL` / `INSERT FIRST` | Multi-table insert is not parsed |
| `PARSE_XML` / `XMLGET` / `TO_XML` | XML is not emulated; `GEOGRAPHY` and `GEOMETRY` likewise |
| Identifiers colliding with DuckDB keywords | An unquoted name DuckDB reserves and Snowflake does not (`AT`, for instance) fails to parse; quote it |

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
| snowflake-connector-python | 3.17+, including 4.x (dbt-snowflake 1.12 requires 4.2+) |
| DuckDB | 1.2+ |
| sqlglot | 30.18+ (installed as `sqlglot[c]`) |
| pyarrow | 18+ (unpinned at the top end; <19 has no cp314 wheels) |
| dbt-snowflake | 1.12 (verified end to end); 1.9+ expected to work |

{: .note }
> SnowDuck depends on `sqlglot[c]`, not `sqlglot[rs]`. sqlglot deprecated the
> Rust extra in 30.x - importing it now emits *"sqlglot[rs] is deprecated and no
> longer compatible with sqlglot"* - and replaced it with `sqlglotc`, which
> compiles the whole library (tokenizer, parser, generator, expressions) rather
> than just the tokenizer.
