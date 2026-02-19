---
layout: default
title: Functions
parent: Documentation
nav_order: 4
---

# Supported Functions

SnowDuck supports 100+ Snowflake functions across all categories. Functions are either natively supported by DuckDB or transpiled automatically.

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
| `REGEXP_LIKE` | ✅ | |
| `REGEXP_REPLACE` | ✅ | |
| `REGEXP_SUBSTR` | ✅ | |
| `REGEXP_COUNT` | ✅ | |
| `CHARINDEX` / `POSITION` | ✅ | |
| `STARTSWITH` / `ENDSWITH` | ✅ | |
| `CONTAINS` | ✅ | |

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
| `NEXT_DAY` | ✅ | |
| `PREVIOUS_DAY` | ✅ | |
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
| `OBJECT_CONSTRUCT` | ✅ | |
| `OBJECT_KEYS` | ✅ | |
| `OBJECT_INSERT` | ✅ | |
| `OBJECT_DELETE` | ✅ | |
| `ARRAY_CONSTRUCT` | ✅ | |
| `ARRAY_SIZE` | ✅ | |
| `FLATTEN` | ✅ | With LATERAL |
| `JSON_EXTRACT_PATH_TEXT` | ✅ | |

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
