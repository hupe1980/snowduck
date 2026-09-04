---
layout: default
title: Snowflake Semantics
parent: Documentation
nav_order: 5
---

# Snowflake Semantics

Matching Snowflake's function *names* is the easy half. This page documents the
places where Snowflake's **behaviour** differs from the obvious DuckDB
translation — the cases where the wrong answer comes back silently instead of
as an error.

Every rule on this page is pinned by an executable conformance suite in
`tests/conformance/test_snowflake_semantics.py`. If a rule here and SnowDuck
ever disagree, that is a bug — please
[open an issue](https://github.com/hupe1980/snowduck/issues).

{: .note }
> These are all cases where a naive translation *runs successfully* and returns
> something plausible but wrong. That makes them far more dangerous than a
> missing function, which fails loudly.

## Regular expressions

### REGEXP_LIKE and RLIKE anchor implicitly

Snowflake: *"The function implicitly anchors a pattern at both ends (for
example, `''` automatically becomes `'^$'`, and `'ABC'` automatically becomes
`'^ABC$'`)."*

```sql
SELECT REGEXP_LIKE('xabcx', 'a.c');      -- FALSE  (not a full match)
SELECT REGEXP_LIKE('abc',   'a.c');      -- TRUE
SELECT REGEXP_LIKE('xabcx', '.*a.c.*');  -- TRUE   (explicit wildcards)
```

This applies to `REGEXP_LIKE`, `RLIKE` and the `REGEXP` operator. It does **not**
apply to `REGEXP_SUBSTR`, `REGEXP_COUNT`, `REGEXP_INSTR` or `REGEXP_REPLACE`,
which all search for a partial match.

### No match is NULL, not an empty string

```sql
SELECT REGEXP_SUBSTR('abc', '[0-9]+');   -- NULL
```

### REGEXP_REPLACE replaces every occurrence

The `occurrence` argument defaults to `0`, which means "all".

```sql
SELECT REGEXP_REPLACE('a1b2', '[0-9]', 'X');        -- 'aXbX'
SELECT REGEXP_REPLACE('a1b2', '[0-9]', 'X', 1, 1);  -- 'aXb2'  (first only)
SELECT REGEXP_REPLACE('a1b2', '[0-9]', 'X', 3);     -- 'a1bX'  (from position 3)
```

### The `e` parameter is not a regex flag

In Snowflake, `e` means "return a capture group" — the `group_num` argument says
which, defaulting to `1`. The genuine flags are `c` (case-sensitive, the
default), `i`, `m` and `s`.

```sql
SELECT REGEXP_SUBSTR('1-1:1.8.0', '^(\d+)-(\d+):(\d+)', 1, 1, 'e', 3);  -- '1'
SELECT REGEXP_SUBSTR('12-34',     '^(\d+)-(\d+)',       1, 1, 'e');     -- '12'
```

Without `e`, the whole match is returned regardless of `group_num`.

### Positions and occurrences are 1-based

```sql
SELECT REGEXP_SUBSTR('a1b22c33', '[0-9]+', 1, 2);  -- '22'  (2nd match)
SELECT REGEXP_SUBSTR('a1b22c33', '[0-9]+', 4, 1);  -- '22'  (from char 4)
SELECT REGEXP_INSTR('abc123', '[0-9]+');           -- 4
SELECT REGEXP_INSTR('abc',    '[0-9]+');           -- 0     (no match)
SELECT REGEXP_INSTR('abc123', '[0-9]+', 1, 1, 1);  -- 7     (end of match)
```

## Numeric conversion

### TO_NUMBER defaults to NUMBER(38, 0)

Precision defaults to 38 and **scale to 0**, and reducing scale *rounds* rather
than truncating.

```sql
SELECT TO_NUMBER('123.45');         -- 123
SELECT TO_NUMBER('-2.5');           -- -3
SELECT TO_NUMBER('123.45', 10, 2);  -- 123.45
```

{: .warning }
> `TO_NUMBER(x, p, s)` produces an exact `DECIMAL`, never a `DOUBLE`. This
> matters for check-digit and currency arithmetic, where binary floating point
> silently changes results at the edges.

`TO_DECIMAL` and `TO_NUMERIC` are synonyms of `TO_NUMBER` and behave identically,
as do the `TRY_` variants (which return NULL instead of raising).

## Semi-structured data

### OBJECT_CONSTRUCT drops NULL-valued keys

```sql
SELECT OBJECT_CONSTRUCT('a', 1, 'b', NULL);            -- {"a":1}
SELECT OBJECT_CONSTRUCT_KEEP_NULL('a', 1, 'b', NULL);  -- {"a":1,"b":null}
```

The check is at runtime, not just for NULL literals — a NULL column value is
dropped too.

{: .note }
> SnowDuck implements the dropping variant with RFC 7386 merge-patch semantics,
> which recurse. A NULL nested inside an object-valued argument is therefore
> also removed, where Snowflake drops only at the top level.

### Arrays are 0-indexed

```sql
SELECT GET(ARRAY_CONSTRUCT('a', 'b', 'c'), 1);  -- 'b'
```

### VARIANT predicates and accessors

```sql
SELECT IS_ARRAY(PARSE_JSON('[1]'));        -- TRUE
SELECT IS_OBJECT(PARSE_JSON('[1]'));       -- FALSE
SELECT IS_NULL_VALUE(PARSE_JSON('null'));  -- TRUE   (JSON null, not SQL NULL)
SELECT AS_VARCHAR(PARSE_JSON('"x"'));      -- 'x'    (unquoted)
```

`IS_NULL_VALUE` returns NULL — not FALSE — for a SQL NULL input.

## Dates

### DAYNAME and MONTHNAME are abbreviated

Snowflake: *"Extracts the three-letter day-of-week name."*

```sql
SELECT DAYNAME(TO_DATE('2024-01-01'));    -- 'Mon'   (not 'Monday')
SELECT MONTHNAME(TO_DATE('2024-01-01'));  -- 'Jan'   (not 'January')
```

### NEXT_DAY / PREVIOUS_DAY never return the same day

If the input already falls on the requested weekday, the result moves a full
week.

```sql
SELECT NEXT_DAY(TO_DATE('2024-01-01'), 'Mon');      -- 2024-01-08
SELECT PREVIOUS_DAY(TO_DATE('2024-01-08'), 'Fri');  -- 2024-01-05
```

## NULL propagation

### CONCAT_WS does not skip NULLs

Snowflake: *"Unlike some implementations of the CONCAT_WS function, the
Snowflake CONCAT_WS function doesn't skip NULL values."*

```sql
SELECT CONCAT_WS('-', 'a', NULL, 'c');  -- NULL, not 'a-c'
```

### DIV0NULL returns 0, not NULL

Despite the name, `DIV0NULL` returns **0** when the divisor is 0 *or* NULL -
the `NULL` in its name refers to the divisor it guards against, not the result.

```sql
SELECT DIV0NULL(10, 0);     -- 0
SELECT DIV0NULL(10, NULL);  -- 0
```

## Arrays

### FLATTEN exposes six columns

`FLATTEN` yields `SEQ`, `KEY`, `PATH`, `INDEX`, `VALUE` and `THIS`, and `INDEX`
is 0-based:

```sql
SELECT f.INDEX, f.VALUE, f.PATH
FROM my_table, LATERAL FLATTEN(input => my_table.tags) f;
```

{: .note }
> `SEQ` is always 1 in SnowDuck; Snowflake numbers each flattened input.

### Arrays are heterogeneous

Snowflake arrays may mix types, so a mixed `ARRAY_CONSTRUCT` builds JSON rather
than a (necessarily homogeneous) DuckDB list:

```sql
SELECT ARRAY_CONSTRUCT(1, 'two', 3.5);              -- [1,"two",3.5]
SELECT ARRAY_CONTAINS('two', ARRAY_CONSTRUCT(1, 'two'));  -- TRUE
```

A bare `ARRAY` column type is likewise stored as JSON, never as a typed list —
`CREATE TABLE t (tags ARRAY)` would otherwise reject anything but integers. An
explicitly typed `INT[]` column is left as a real DuckDB list.

### Array functions work on stored ARRAY columns

SnowDuck stores an `ARRAY` column as DuckDB JSON, and coerces it back to a list
wherever an array function needs one. Elements of a stored array read back as
JSON values (`'10'` rather than `10`); array *literals* keep their native types.

```sql
CREATE TABLE t (tags ARRAY);
SELECT ARRAY_SIZE(tags), ARRAY_CONTAINS(2, tags), ARRAY_MAX(tags) FROM t;
```

`ARRAY_MIN` and `ARRAY_MAX` compare numerically, not as JSON text - otherwise
`'10'` would rank below `'2'`.

## Objects

### OBJECT_CONSTRUCT drops NULLs at the top level only

A NULL nested inside an object-valued argument is kept:

```sql
SELECT OBJECT_CONSTRUCT('a', OBJECT_CONSTRUCT_KEEP_NULL('x', NULL));
-- {"a":{"x":null}}   -- the outer key survives, and so does the inner null
```

### PARSE_URL decodes and splits the query string

```sql
SELECT PARSE_URL('https://ex.com/a%20b?q=hello%20world&n=2');
-- {"scheme":"https","host":"ex.com","path":"a b",
--  "query":"q=hello world&n=2","parameters":{"q":"hello world","n":"2"}, ...}
```

## Numbers with format models

A format model describes the decoration around the digits; it does **not** set
the scale, which still defaults to 0:

```sql
SELECT TO_NUMBER('$1,234.56', '$9,999.99');           -- 1235  (scale 0!)
SELECT TO_DECIMAL('$3,741.72', '$9,999.99', 6, 2);    -- 3741.72
```

## Session context

`CURRENT_ROLE()`, `CURRENT_DATABASE()`, `CURRENT_SCHEMA()`, `CURRENT_WAREHOUSE()`
and friends resolve to the values configured on the connection, and are
substituted **in place** — the rest of the query is untouched:

```sql
SELECT CURRENT_DATABASE(), COUNT(*) FROM my_table;  -- counts every row
```

An unaliased session function keeps Snowflake's column naming, so
`SELECT CURRENT_ROLE()` produces a column named `CURRENT_ROLE()`.

## Verifying against real Snowflake

SnowDuck is for development and testing. Before shipping anything critical,
validate against a real Snowflake account — and if you find a divergence not
listed here, it is a bug worth reporting.
