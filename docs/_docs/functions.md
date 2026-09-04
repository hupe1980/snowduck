---
layout: default
title: Functions
parent: Documentation
nav_order: 4
---

# Supported Functions

SnowDuck supports 150+ Snowflake functions across all categories. Functions are
either natively supported by DuckDB or transpiled automatically.

{: .note }
> Matching a function's name is not the same as matching its behaviour. See
> [Snowflake Semantics](snowflake-semantics) for the cases where Snowflake
> differs from the obvious DuckDB translation - those are pinned by an
> executable conformance suite.

## String Functions

| Function | Status | Notes |
|----------|--------|-------|
| `CONCAT` | ✅ | |
| `CONCAT_WS` | ✅ | |
| `LENGTH` / `LEN` | ✅ | |
| `UPPER` | ✅ | |
| `LOWER` | ✅ | |
| `TRIM` / `LTRIM` / `RTRIM` | ✅ | |
| `LPAD` / `RPAD` | ✅ | |
| `LEFT` / `RIGHT` | ✅ | |
| `SUBSTRING` / `SUBSTR` | ✅ | |
| `REPLACE` | ✅ | |
| `SPLIT_PART` | ✅ | |
| `SPLIT` | ✅ | Returns array |
| `REVERSE` | ✅ | |
| `REPEAT` | ✅ | |
| `INITCAP` | ✅ | Via macro |
| `SOUNDEX` | ✅ | Via macro |
| `TRANSLATE` | ✅ | |
| `INSERT` | ✅ | Splices a substring |
| `RTRIMMED_LENGTH` | ✅ | |
| `EDITDISTANCE` | ✅ | Levenshtein |
| `JAROWINKLER_SIMILARITY` | ✅ | Integer 0-100, as Snowflake reports it |
| `PARSE_URL` | ✅ | Decodes escapes and exposes `parameters` as an object |
| `CHARINDEX` / `POSITION` | ✅ | |
| `STARTSWITH` / `ENDSWITH` | ✅ | |
| `CONTAINS` | ✅ | |
| `SPLIT_TO_TABLE` | ✅ | Via `TABLE(...)` |
| `COLLATE` | ⚠️ | Case-insensitive specifiers only (`-ci`) |

## Regular Expression Functions

| Function | Status | Notes |
|----------|--------|-------|
| `REGEXP_LIKE` / `RLIKE` | ✅ | Anchored at both ends, as in Snowflake |
| `REGEXP_REPLACE` | ✅ | Replaces all occurrences by default |
| `REGEXP_SUBSTR` | ✅ | NULL when no match; position/occurrence/group supported |
| `REGEXP_SUBSTR_ALL` | ✅ | Returns every match |
| `REGEXP_COUNT` | ✅ | Optional start position |
| `REGEXP_INSTR` | ✅ | 1-based; 0 when no match |

{: .warning }
> Snowflake's `REGEXP_LIKE` implicitly anchors its pattern, and
> `REGEXP_REPLACE` replaces *every* match by default. Both differ from most
> other SQL dialects - see [Snowflake Semantics](snowflake-semantics).

## Date/Time Functions

| Function | Status | Notes |
|----------|--------|-------|
| `CURRENT_DATE` | ✅ | |
| `CURRENT_TIME` | ✅ | |
| `CURRENT_TIMESTAMP` | ✅ | |
| `GETDATE` / `SYSDATE` | ✅ | |
| `DATE_PART` / `EXTRACT` | ✅ | |
| `DATE_TRUNC` | ✅ | |
| `DATEADD` / `TIMESTAMPADD` | ✅ | |
| `DATEDIFF` / `TIMESTAMPDIFF` | ✅ | |
| `YEAR` / `MONTH` / `DAY` | ✅ | |
| `HOUR` / `MINUTE` / `SECOND` | ✅ | |
| `DAYOFWEEK` / `DAYOFYEAR` | ✅ | |
| `WEEKOFYEAR` / `QUARTER` | ✅ | |
| `TO_DATE` | ✅ | |
| `TO_TIME` | ✅ | |
| `TO_TIMESTAMP` | ✅ | |
| `TO_CHAR` | ✅ | Date formatting |
| `LAST_DAY` | ✅ | |
| `NEXT_DAY` | ✅ | Never returns the input date |
| `PREVIOUS_DAY` | ✅ | Never returns the input date |
| `DAYNAME` / `MONTHNAME` | ✅ | Three-letter abbreviation (`Mon`, `Jan`) |
| `TIME_SLICE` | ✅ | START and END |
| `YEAROFWEEK` / `YEAROFWEEKISO` | ✅ | |
| `MONTHS_BETWEEN` | ✅ | |

### Date Literals

SnowDuck supports Snowflake's date literal syntax:

```sql
SELECT DATE '2024-01-15';
SELECT TIMESTAMP '2024-01-15 10:30:00';
```

## Numeric Functions

| Function | Status | Notes |
|----------|--------|-------|
| `ABS` | ✅ | |
| `CEIL` / `CEILING` | ✅ | |
| `FLOOR` | ✅ | |
| `ROUND` | ✅ | |
| `TRUNC` / `TRUNCATE` | ✅ | |
| `MOD` | ✅ | |
| `POWER` / `POW` | ✅ | |
| `SQRT` | ✅ | |
| `EXP` | ✅ | |
| `LN` / `LOG` | ✅ | |
| `LOG10` | ✅ | |
| `SIGN` | ✅ | |
| `RANDOM` | ✅ | |
| `UNIFORM` | ✅ | |
| `WIDTH_BUCKET` | ✅ | Via CASE expression |
| `DIV0` / `DIV0NULL` | ✅ | Safe division |
| `GREATEST` / `LEAST` | ✅ | |
| `NULLIFZERO` / `ZEROIFNULL` | ✅ | |

## Aggregate Functions

| Function | Status | Notes |
|----------|--------|-------|
| `COUNT` | ✅ | |
| `SUM` | ✅ | |
| `AVG` | ✅ | |
| `MIN` / `MAX` | ✅ | |
| `STDDEV` / `STDDEV_POP` / `STDDEV_SAMP` | ✅ | |
| `VARIANCE` / `VAR_POP` / `VAR_SAMP` | ✅ | |
| `MEDIAN` | ✅ | |
| `MODE` | ✅ | |
| `LISTAGG` | ✅ | |
| `ARRAY_AGG` | ✅ | |
| `OBJECT_AGG` | ✅ | |
| `APPROX_COUNT_DISTINCT` | ✅ | |
| `HLL` | ✅ | HyperLogLog |
| `CORR` | ✅ | Correlation |
| `COVAR_POP` / `COVAR_SAMP` | ✅ | |
| `PERCENTILE_CONT` / `PERCENTILE_DISC` | ✅ | |

## Window Functions

| Function | Status | Notes |
|----------|--------|-------|
| `ROW_NUMBER` | ✅ | |
| `RANK` | ✅ | |
| `DENSE_RANK` | ✅ | |
| `NTILE` | ✅ | |
| `LAG` / `LEAD` | ✅ | |
| `FIRST_VALUE` / `LAST_VALUE` | ✅ | |
| `NTH_VALUE` | ✅ | |
| `CUME_DIST` | ✅ | |
| `PERCENT_RANK` | ✅ | |

### QUALIFY Clause

SnowDuck fully supports the QUALIFY clause for filtering window function results:

```sql
SELECT *
FROM sales
QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) = 1;
```

## JSON Functions

| Function | Status | Notes |
|----------|--------|-------|
| `PARSE_JSON` | ✅ | |
| `TO_JSON` / `TO_VARIANT` | ✅ | |
| `GET_PATH` / `GET` | ✅ | |
| `OBJECT_CONSTRUCT` | ✅ | Drops NULL-valued keys, as in Snowflake |
| `OBJECT_CONSTRUCT_KEEP_NULL` | ✅ | Retains them |
| `OBJECT_KEYS` | ✅ | |
| `OBJECT_INSERT` | ✅ | |
| `OBJECT_DELETE` | ✅ | |
| `OBJECT_PICK` | ✅ | |
| `OBJECT_AGG` | ✅ | |
| `TO_OBJECT` | ✅ | |
| `ARRAY_CONSTRUCT` | ✅ | |
| `ARRAY_SIZE` | ✅ | |
| `ARRAY_FLATTEN` | ✅ | |
| `FLATTEN` | ✅ | `LATERAL` and `TABLE(...)`; all six columns |
| `JSON_EXTRACT_PATH_TEXT` | ✅ | |

## VARIANT Predicates and Accessors

| Function | Status | Notes |
|----------|--------|-------|
| `IS_ARRAY` / `IS_OBJECT` | ✅ | |
| `IS_NULL_VALUE` | ✅ | JSON null, not SQL NULL |
| `IS_INTEGER` / `IS_DOUBLE` / `IS_DECIMAL` | ✅ | |
| `IS_BOOLEAN` / `IS_VARCHAR` | ✅ | |
| `AS_VARCHAR` / `AS_CHAR` | ✅ | Returns the string unquoted |
| `AS_INTEGER` / `AS_DOUBLE` / `AS_DECIMAL` | ✅ | |
| `AS_BOOLEAN` / `AS_DATE` / `AS_TIMESTAMP_NTZ` | ✅ | |
| `TYPEOF` | ✅ | |

## Session Context Functions

| Function | Status | Notes |
|----------|--------|-------|
| `CURRENT_ROLE` / `CURRENT_DATABASE` / `CURRENT_SCHEMA` | ✅ | From the connection |
| `CURRENT_WAREHOUSE` / `CURRENT_SECONDARY_ROLES` | ✅ | From the connection |
| `CURRENT_VERSION` / `CURRENT_CLIENT` | ✅ | Reports the SnowDuck version |
| `CURRENT_ACCOUNT` / `CURRENT_ACCOUNT_NAME` | ✅ | Fixed mock values |
| `CURRENT_SESSION` / `CURRENT_REGION` / `CURRENT_IP_ADDRESS` | ✅ | Fixed mock values |

## User-Defined Functions

| Feature | Status | Notes |
|---------|--------|-------|
| `CREATE FUNCTION ... AS $$ ... $$` | ✅ | Compiled to a DuckDB macro |
| `CREATE FUNCTION ... AS '...'` | ✅ | Quoted body |
| Table UDFs (`RETURNS TABLE`) | ✅ | Called via `TABLE(fn())` |
| `DROP FUNCTION f(TYPE)` | ✅ | Signature accepted and ignored |
| `LANGUAGE JAVASCRIPT` / `PYTHON` | ❌ | Raises a clear error |

The UDF body is translated through the same pipeline as any other statement, so
Snowflake functions used inside a UDF are translated too.

```sql
CREATE OR REPLACE FUNCTION is_valid_id(id VARCHAR)
RETURNS BOOLEAN
AS $$ LENGTH(id) = 11 AND REGEXP_LIKE(id, '[0-9]+') $$;

SELECT is_valid_id('12345678901');  -- TRUE
```

### JSON Path Access

```sql
-- Dot notation
SELECT data:customer:name FROM my_table;

-- Bracket notation
SELECT data['customer']['name'] FROM my_table;

-- GET_PATH function
SELECT GET_PATH(data, 'customer.name') FROM my_table;
```

## Array Functions

| Function | Status | Notes |
|----------|--------|-------|
| `ARRAY_CONSTRUCT` | ✅ | |
| `ARRAY_SIZE` / `ARRAY_LENGTH` | ✅ | |
| `ARRAY_APPEND` | ✅ | |
| `ARRAY_PREPEND` | ✅ | |
| `ARRAY_CAT` | ✅ | |
| `ARRAY_SLICE` | ✅ | |
| `ARRAY_CONTAINS` | ✅ | |
| `ARRAY_POSITION` | ✅ | |
| `ARRAY_DISTINCT` | ✅ | |
| `ARRAY_FLATTEN` | ✅ | |
| `ARRAY_TO_STRING` | ✅ | |
| `ARRAYS_OVERLAP` | ✅ | |
| `ARRAY_INTERSECTION` | ✅ | |

### LATERAL FLATTEN

Full support for flattening arrays and objects:

```sql
SELECT 
    f.value::string AS item
FROM my_table,
LATERAL FLATTEN(input => items) f;
```

## Conditional Functions

| Function | Status | Notes |
|----------|--------|-------|
| `CASE` | ✅ | |
| `IFF` | ✅ | |
| `IFNULL` / `NVL` | ✅ | |
| `NVL2` | ✅ | |
| `COALESCE` | ✅ | |
| `NULLIF` | ✅ | |
| `DECODE` | ✅ | |
| `EQUAL_NULL` | ✅ | Maps to IS NOT DISTINCT FROM |
| `BOOLAND` / `BOOLOR` / `BOOLXOR` | ✅ | |
| `BOOLNOT` | ✅ | |

## Hash & Encoding Functions

| Function | Status | Notes |
|----------|--------|-------|
| `MD5` | ✅ | |
| `MD5_HEX` / `MD5_BINARY` | ✅ | |
| `SHA1` / `SHA1_HEX` | ✅ | |
| `SHA2` / `SHA2_HEX` | ✅ | |
| `HASH` | ✅ | |
| `BASE64_ENCODE` / `BASE64_DECODE` | ✅ | |
| `HEX_ENCODE` / `HEX_DECODE` | ✅ | |
| `TRY_BASE64_DECODE_STRING` | ✅ | |

## Type Conversion Functions

| Function | Status | Notes |
|----------|--------|-------|
| `CAST` | ✅ | |
| `TRY_CAST` | ✅ | |
| `TO_CHAR` / `TO_VARCHAR` | ✅ | |
| `TO_NUMBER` / `TO_DECIMAL` | ✅ | |
| `TO_DOUBLE` / `TO_FLOAT` | ✅ | |
| `TO_BOOLEAN` | ✅ | |
| `TO_BINARY` | ✅ | |
| `TRY_TO_*` variants | ✅ | |

## System Functions

| Function | Status | Notes |
|----------|--------|-------|
| `CURRENT_DATABASE` | ✅ | |
| `CURRENT_SCHEMA` | ✅ | |
| `CURRENT_USER` | ✅ | |
| `CURRENT_ROLE` | ✅ | |
| `CURRENT_WAREHOUSE` | ✅ | Returns mock value |
| `CURRENT_SESSION` | ✅ | |
| `CURRENT_ACCOUNT` | ✅ | |
| `SYSTEM$TYPEOF` | ✅ | |


## Boolean Functions

| Function | Status | Notes |
|----------|--------|-------|
| `BOOLAND` / `BOOLOR` / `BOOLXOR` / `BOOLNOT` | ✅ | Numeric inputs are treated as booleans |
| `BOOLAND_AGG` / `BOOLOR_AGG` | ✅ | |

## Predicates

| Syntax | Status | Notes |
|--------|--------|-------|
| `LIKE ANY (...)` / `LIKE ALL (...)` | ✅ | Expanded to OR / AND chains |
| `ILIKE ANY (...)` | ✅ | |
| `x REGEXP p` | ✅ | Anchored, like `REGEXP_LIKE` |
