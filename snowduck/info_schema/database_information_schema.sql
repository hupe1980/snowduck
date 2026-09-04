-- Per-database INFORMATION_SCHEMA, shaped like Snowflake's.
--
-- Snowflake gives every database its own INFORMATION_SCHEMA whose views are
-- scoped to that database. DuckDB's information_schema is account-wide and has
-- a different column set, so SnowDuck materialises Snowflake's views in a
-- hidden schema and rewrites `<db>.INFORMATION_SCHEMA.<view>` references onto
-- them (see snowduck.dialect.preprocess.info_schema).
CREATE SCHEMA IF NOT EXISTS {database}.{info_schema_name};

-- INFORMATION_SCHEMA.COLUMNS
CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._columns AS
SELECT *
FROM {account_catalog_name}.{info_schema_name}._columns
WHERE table_catalog = '{database}';

-- INFORMATION_SCHEMA.SCHEMATA
CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._schemata AS
SELECT
    s.database_name AS catalog_name,
    CASE WHEN s.schema_name = '{info_schema_name}'
         THEN 'INFORMATION_SCHEMA' ELSE s.schema_name END AS schema_name,
    'SYSADMIN' AS schema_owner,
    'NO' AS is_transient,
    'NO' AS is_managed_access,
    1 AS retention_time,
    NULL::VARCHAR AS default_character_set_catalog,
    NULL::VARCHAR AS default_character_set_schema,
    NULL::VARCHAR AS default_character_set_name,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS created,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_altered,
    s.comment AS comment
FROM duckdb_schemas() s
WHERE s.database_name = '{database}'
  AND NOT s.internal
  AND s.schema_name <> 'main';

-- INFORMATION_SCHEMA.TABLES. `row_count` is DuckDB's estimate; `bytes`,
-- `clustering_key` and the clone/iceberg flags have no local equivalent and
-- report as NULL / 'NO', which is what dbt's catalog query expects to find.
CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._tables AS
SELECT
    t.database_name AS table_catalog,
    t.schema_name AS table_schema,
    t.table_name AS table_name,
    'SYSADMIN' AS table_owner,
    CASE WHEN t.temporary THEN 'TEMPORARY TABLE' ELSE 'BASE TABLE' END AS table_type,
    CASE WHEN t.temporary THEN 'YES' ELSE 'NO' END AS is_transient,
    NULL::VARCHAR AS clustering_key,
    t.estimated_size::BIGINT AS row_count,
    NULL::BIGINT AS bytes,
    1 AS retention_time,
    NULL::VARCHAR AS self_referencing_column_name,
    NULL::VARCHAR AS reference_generation,
    NULL::VARCHAR AS user_defined_type_catalog,
    NULL::VARCHAR AS user_defined_type_schema,
    NULL::VARCHAR AS user_defined_type_name,
    'YES' AS is_insertable_into,
    'NO' AS is_typed,
    NULL::VARCHAR AS commit_action,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS created,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_altered,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_ddl,
    'SYSADMIN' AS last_ddl_by,
    'NO' AS auto_clustering_on,
    t.comment AS comment,
    CASE WHEN t.temporary THEN 'YES' ELSE 'NO' END AS is_temporary,
    'NO' AS is_iceberg,
    'NO' AS is_dynamic,
    'NO' AS is_immutable
FROM duckdb_tables() t
WHERE t.database_name = '{database}'
  AND NOT t.internal
  AND t.schema_name <> '{info_schema_name}'
UNION ALL
SELECT
    v.database_name,
    v.schema_name,
    v.view_name,
    'SYSADMIN',
    'VIEW',
    'NO',
    NULL::VARCHAR,
    NULL::BIGINT,
    NULL::BIGINT,
    1,
    NULL::VARCHAR,
    NULL::VARCHAR,
    NULL::VARCHAR,
    NULL::VARCHAR,
    NULL::VARCHAR,
    'NO',
    'NO',
    NULL::VARCHAR,
    TO_TIMESTAMP(0)::TIMESTAMPTZ,
    TO_TIMESTAMP(0)::TIMESTAMPTZ,
    TO_TIMESTAMP(0)::TIMESTAMPTZ,
    'SYSADMIN',
    'NO',
    v.comment,
    CASE WHEN v.temporary THEN 'YES' ELSE 'NO' END,
    'NO',
    'NO',
    'NO'
FROM duckdb_views() v
WHERE v.database_name = '{database}'
  AND NOT v.internal
  AND v.schema_name <> '{info_schema_name}';

-- INFORMATION_SCHEMA.VIEWS
CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._views AS
SELECT
    database_name AS table_catalog,
    schema_name AS table_schema,
    view_name AS table_name,
    'SYSADMIN' AS table_owner,
    sql AS view_definition,
    'NONE' AS check_option,
    'NO' AS is_updatable,
    'NO' AS insertable_into,
    'NO' AS is_secure,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS created,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_altered,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_ddl,
    'SYSADMIN' AS last_ddl_by,
    comment AS comment
FROM duckdb_views
WHERE database_name = '{database}'
  AND NOT internal
  AND schema_name <> '{info_schema_name}';

-- INFORMATION_SCHEMA.FUNCTIONS. SQL UDFs are DuckDB macros; SnowDuck's own
-- compatibility macros live in each database's `main` schema and are excluded.
CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._functions AS
SELECT
    f.database_name AS function_catalog,
    f.schema_name AS function_schema,
    f.function_name AS function_name,
    'SYSADMIN' AS function_owner,
    '(' || COALESCE(
        list_aggregate(
            list_transform(
                COALESCE(f.parameter_types, []),
                x -> upper(COALESCE(x, 'VARIANT'))
            ),
            'string_agg', ', '
        ), ''
    ) || ')' AS argument_signature,
    upper(COALESCE(f.return_type, 'VARIANT')) AS data_type,
    NULL::INTEGER AS character_maximum_length,
    NULL::INTEGER AS character_octet_length,
    NULL::INTEGER AS numeric_precision,
    NULL::INTEGER AS numeric_precision_radix,
    NULL::INTEGER AS numeric_scale,
    'SQL' AS function_language,
    f.macro_definition AS function_definition,
    'NO' AS is_external,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS created,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_altered,
    f.description AS comment,
    'NO' AS is_secure,
    'NO' AS is_memoizable,
    'NO' AS is_data_metric
FROM duckdb_functions() f
WHERE f.database_name = '{database}'
  AND NOT f.internal
  AND f.function_type IN ('macro', 'table_macro')
  AND f.schema_name NOT IN ('{info_schema_name}', 'main');

-- INFORMATION_SCHEMA.SEQUENCES
CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._sequences AS
SELECT
    s.database_name AS sequence_catalog,
    s.schema_name AS sequence_schema,
    s.sequence_name AS sequence_name,
    'SYSADMIN' AS sequence_owner,
    'NUMBER' AS data_type,
    38 AS numeric_precision,
    10 AS numeric_precision_radix,
    0 AS numeric_scale,
    s.start_value AS start_value,
    s.min_value AS minimum_value,
    s.max_value AS maximum_value,
    s.increment_by AS increment,
    CASE WHEN s.cycle THEN 'YES' ELSE 'NO' END AS cycle_option,
    s.comment AS comment,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS created,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_altered,
    s.start_value AS next_value
FROM duckdb_sequences() s
WHERE s.database_name = '{database}'
  AND s.schema_name <> '{info_schema_name}';
