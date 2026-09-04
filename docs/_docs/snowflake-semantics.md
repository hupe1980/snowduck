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

Every rule on this page is pinned by an executable conformance suite under
`tests/conformance/` — `test_snowflake_semantics.py` for the function results,
`test_variant_accessors.py` for the VARIANT families, `test_number_formats.py`
for the format models and `test_ddl_metadata.py` for the catalog. If a rule
here and SnowDuck ever disagree, that is a bug — please
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

### The AS_ family checks rather than casts

Snowflake's `AS_<type>` functions ask *"is the VARIANT of this type?"* and
return NULL when it is not. They do **not** convert:

```sql
SELECT AS_INTEGER(PARSE_JSON('5'));      -- 5
SELECT AS_INTEGER(PARSE_JSON('"5"'));    -- NULL   (a string, not an integer)
SELECT AS_INTEGER(PARSE_JSON('1.5'));    -- NULL   (a float, not an integer)
SELECT AS_VARCHAR(PARSE_JSON('5'));      -- NULL   (a number, not a string)
SELECT AS_ARRAY(PARSE_JSON('{}'));       -- NULL   (an object, not an array)
```

Translating these to `TRY_CAST` returns a plausible wrong answer for every one
of those NULL cases, which is why they are pinned here.

The check is only possible for the types JSON itself distinguishes. Snowflake's
`DATE`, `TIME`, `TIMESTAMP` and `BINARY` are stored locally as JSON strings, so
`AS_DATE` and friends stay a cast, and `IS_DATE` and friends answer *"a string
that reads back as that type"*:

```sql
SELECT IS_DATE(PARSE_JSON('"2024-01-01"'));  -- TRUE
SELECT IS_DATE(PARSE_JSON('"xx"'));          -- FALSE
SELECT IS_DATE(PARSE_JSON('5'));             -- FALSE
```

`AS_DECIMAL` and `AS_NUMBER` take the target precision and scale as arguments
rather than in their name, and both are honoured:

```sql
SELECT AS_DECIMAL(PARSE_JSON('5'), 10, 2);   -- 5.00
```

### GET reads members as well as elements

`GET` is two functions sharing a name: with an integer it reads an array
element, with a string it reads an object member. Only the first is a DuckDB
subscript — applying one to an object raises *"Expected ARRAY, but got
OBJECT"* — so the member form becomes a JSON path.

```sql
SELECT GET(ARRAY_CONSTRUCT(10, 20), 0);       -- 10
SELECT GET(PARSE_JSON('{"a":7}'), 'a');       -- 7
SELECT GET(PARSE_JSON('{"a":7}'), 'missing'); -- NULL
```

### OBJECT_CONSTRUCT(*) builds an object from the whole row

```sql
SELECT OBJECT_CONSTRUCT(*) FROM t;            -- {"A":1,"B":"x"}
```

DuckDB has no expression that expands to the whole row, so the query's single
`FROM` source is named in a value position instead — which is what produces its
row struct. Over a join there is nothing to name, so `OBJECT_CONSTRUCT(*)` is
refused with an error rather than quietly returning one side.

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

### DATEADD keeps a DATE a DATE

Snowflake: *"If `date_or_time_part` is day or larger (for example, month,
year), the function returns a DATE value"* — anything smaller returns
`TIMESTAMP_NTZ`. DuckDB promotes `DATE + INTERVAL` to a timestamp, so the cast
back is applied wherever the input is known to be a DATE:

```sql
SELECT DATEADD(month, 1, '2024-01-31'::DATE);  -- DATE 2024-02-29
SELECT DATEADD(hour,  1, '2024-01-31'::DATE);  -- TIMESTAMP 2024-01-31 01:00
SELECT ADD_MONTHS('2024-01-31'::DATE, 1);      -- DATE 2024-02-29 (clamped)
```

{: .note }
> The input has to *say* it is a DATE — a cast, `TO_DATE`, or `CURRENT_DATE`.
> The type of a bare column is not known at translation time, so `DATEADD` over
> one keeps DuckDB's timestamp result.

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

Functions such as `SPLIT` and `OBJECT_KEYS` return a native DuckDB list rather
than JSON, so array arguments are normalised through `to_json` before being
read back as a list. That makes both representations interchangeable:

```sql
SELECT ARRAY_SIZE(SPLIT('a,b,c', ','));          -- 3
SELECT ARRAY_TO_STRING(SPLIT('a,b', ','), '-');  -- 'a-b', not '"a"-"b"'
```

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

### TO_NUMBER's format model does not set the scale

A format model describes the decoration around the digits; it does **not** set
the scale, which still defaults to 0:

```sql
SELECT TO_NUMBER('$1,234.56', '$9,999.99');           -- 1235  (scale 0!)
SELECT TO_DECIMAL('$3,741.72', '$9,999.99', 6, 2);    -- 3741.72
```

### TO_CHAR renders a numeric format model

Going the other way, the model *is* the layout. The result is right-justified
in a field as wide as the model, plus one column for the sign unless the model
places one itself, and a value too wide for the model's integer positions
renders as `#`:

```sql
SELECT TO_CHAR(-12.391,  '99.9');        -- '-12.4'
SELECT TO_CHAR(0.5,      '99.9');        -- '   .5'      (9 suppresses a leading zero)
SELECT TO_CHAR(1234.5,   '9999.99');     -- ' 1234.50'
SELECT TO_CHAR(1234.56,  '$9,999.99');   -- ' $1,234.56'
SELECT TO_CHAR(-1234.56, 'S000000.00');  -- '-001234.56'
SELECT TO_CHAR(-1234.56, '9999.99MI');   -- ' 1234.56-'
SELECT TO_CHAR(-1234.56, '9999.99PR');   -- '<1234.56>'
SELECT TO_CHAR(0,        'B9999.9');     -- ''           (blank when zero)
SELECT TO_CHAR(1234.56,  'TM9');         -- '1234.56'    (text minimum, unpadded)
SELECT TO_CHAR(12345.67, '9999.99');     -- '########'   (too wide for the model)
```

Supported model elements are `0`, `9`, `.` (`D`), `,` (`G`), `$`, `S`, `MI`,
`PR`, `B` and `TM`. `TO_VARCHAR` is the same function.

This one used to fail silently: sqlglot logs *"Argument 'format' is not
supported for expression 'ToChar'"* and drops the model, so the call returned
the unformatted number. A numeric model and a date model share the argument
position and are told apart by their alphabet — a date model needs `Y`, `H`,
`:` or `/`, none of which a numeric model may contain.

## Documented quirks

Two Snowflake behaviours are surprising enough that SnowDuck reproduces them
deliberately rather than "fixing" them.

### JAROWINKLER_SIMILARITY ignores case

Snowflake: *"The similarity computation is case-insensitive."* It returns an
integer percentage, not a ratio:

```sql
SELECT JAROWINKLER_SIMILARITY('ABC', 'abd');  -- 82, same as 'abc' vs 'abd'
SELECT JAROWINKLER_SIMILARITY('abc', 'abc');  -- 100
```

### BOOLAND rounds its arguments

`BOOLAND`, `BOOLOR`, `BOOLXOR` and `BOOLNOT` treat any non-zero number as true —
but they *round* first, so a fraction below 0.5 counts as zero. Snowflake's own
documentation shows `BOOLAND(-0.4, 5)` returning FALSE, and recommends the `AND`
operator when fractional values matter:

```sql
SELECT BOOLAND(-0.4, 5);  -- FALSE  (-0.4 rounds to 0)
SELECT BOOLAND(2, 3);     -- TRUE
```

## Types

### Declared VARCHAR lengths are kept

DuckDB has no length-limited `VARCHAR` and drops the size at DDL time, so the
declared length is recorded separately and reported back:

```sql
CREATE TABLE t (a VARCHAR(20), b VARCHAR);
SELECT column_name, character_maximum_length
FROM information_schema.columns WHERE table_name = 'T';
-- A, 20
-- B, NULL
```

`cursor.description` carries the same size in `internal_size`; an unbounded
`VARCHAR` reports Snowflake's maximum of 16,777,216.

### Comments survive the DDL that declares them

Snowflake attaches comments inline; DuckDB only has the standalone
`COMMENT ON`, and its SQL generator used to drop the inline forms with a
warning — so the DDL succeeded while every description silently went missing.
That is exactly what dbt's `persist_docs` writes.

```sql
CREATE TABLE t (id INT COMMENT 'the id') COMMENT = 'a table';
ALTER TABLE t MODIFY COLUMN id COMMENT 'renamed';
CREATE VIEW v COMMENT = 'a view' AS SELECT 1 x;

SELECT comment FROM information_schema.tables  WHERE table_name = 'T';   -- 'a table'
SELECT comment FROM information_schema.columns WHERE table_name = 'T';   -- 'renamed'
SELECT comment FROM information_schema.views   WHERE table_name = 'V';   -- 'a view'
```

### Declared lengths track ALTER TABLE

A column added or retyped by `ALTER TABLE` carries a declared size exactly as
one in a `CREATE` does, and one dropped or retyped away from `VARCHAR` forgets
the size it had — otherwise a stale length keeps being reported:

```sql
ALTER TABLE t ALTER COLUMN name SET DATA TYPE VARCHAR(30);  -- reported as 30
ALTER TABLE t ALTER COLUMN name SET DATA TYPE INT;          -- reported as NULL
```

### Numeric UTC offsets

Snowflake writes an offset as `+0200` — no colon, separated by a space — which
DuckDB reads as a *timezone name* and rejects with `Unknown TimeZone '+0200'`.
Timestamp literals are normalised to the ISO spelling first, so both work:

```sql
SELECT '2024-01-15 10:11:12 +0200'::TIMESTAMP_TZ;  -- 08:11:12 UTC
SELECT '2024-01-15 10:11:12+02:00'::TIMESTAMP_TZ;  -- 08:11:12 UTC
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
