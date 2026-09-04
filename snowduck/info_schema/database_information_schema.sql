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
    CASE WHEN s.schema_name = '{info_schema_name}' THEN 'INFORMATION_SCHEMA'
         WHEN s.schema_name = 'main' THEN 'MAIN'
         ELSE s.schema_name END AS schema_name,
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
  AND (
        (NOT s.internal AND s.schema_name <> 'main')
        -- DuckDB's internal `main` is Snowflake's MAIN, but only once asked for.
        OR (s.schema_name = 'main' AND EXISTS (
              SELECT 1 FROM {account_catalog_name}.{info_schema_name}._created_schemas c
              WHERE c.database_name = '{database}' AND c.schema_name = 'MAIN'))
  );

-- INFORMATION_SCHEMA.TABLES. `row_count` is DuckDB's estimate; `bytes`,
-- `clustering_key` and the clone/iceberg flags have no local equivalent and
-- report as NULL / 'NO', which is what dbt's catalog query expects to find.
CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._tables AS
SELECT
    t.database_name AS table_catalog,
    CASE WHEN t.schema_name = 'main' THEN 'MAIN' ELSE t.schema_name END AS table_schema,
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
    CASE WHEN v.schema_name = 'main' THEN 'MAIN' ELSE v.schema_name END AS schema_name,
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
    CASE WHEN schema_name = 'main' THEN 'MAIN' ELSE schema_name END AS table_schema,
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
    CASE WHEN f.schema_name = 'main' THEN 'MAIN' ELSE f.schema_name END AS function_schema,
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
    CASE WHEN s.schema_name = 'main' THEN 'MAIN' ELSE s.schema_name END AS sequence_schema,
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

-- INFORMATION_SCHEMA.TABLE_CONSTRAINTS. Snowflake records constraints as
-- metadata and does not enforce them; DuckDB does enforce them, so the rows
-- here are real. NOT NULL is excluded: Snowflake reports nullability through
-- COLUMNS.is_nullable, not as a constraint.
CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._table_constraints AS
SELECT
    c.database_name AS constraint_catalog,
    CASE WHEN c.schema_name = 'main' THEN 'MAIN' ELSE c.schema_name END AS constraint_schema,
    c.constraint_name AS constraint_name,
    c.database_name AS table_catalog,
    CASE WHEN c.schema_name = 'main' THEN 'MAIN' ELSE c.schema_name END AS table_schema,
    c.table_name AS table_name,
    c.constraint_type AS constraint_type,
    'NO' AS is_deferrable,
    'NO' AS initially_deferred,
    'YES' AS enforced,
    NULL::VARCHAR AS comment,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS created,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_altered,
    'SYSADMIN' AS constraint_owner,
    'YES' AS rely
FROM duckdb_constraints() c
WHERE c.database_name = '{database}'
  AND c.schema_name <> '{info_schema_name}'
  AND c.constraint_type <> 'NOT NULL';

-- INFORMATION_SCHEMA.KEY_COLUMN_USAGE
CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._key_column_usage AS
SELECT
    c.database_name AS constraint_catalog,
    CASE WHEN c.schema_name = 'main' THEN 'MAIN' ELSE c.schema_name END AS constraint_schema,
    c.constraint_name AS constraint_name,
    c.database_name AS table_catalog,
    CASE WHEN c.schema_name = 'main' THEN 'MAIN' ELSE c.schema_name END AS table_schema,
    c.table_name AS table_name,
    u.column_name AS column_name,
    u.position AS ordinal_position,
    NULL::INTEGER AS position_in_unique_constraint
FROM duckdb_constraints() c,
     UNNEST(c.constraint_column_names) WITH ORDINALITY AS u(column_name, position)
WHERE c.database_name = '{database}'
  AND c.schema_name <> '{info_schema_name}'
  AND c.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY');

-- INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._referential_constraints AS
SELECT
    c.database_name AS constraint_catalog,
    CASE WHEN c.schema_name = 'main' THEN 'MAIN' ELSE c.schema_name END AS constraint_schema,
    c.constraint_name AS constraint_name,
    c.database_name AS unique_constraint_catalog,
    CASE WHEN c.schema_name = 'main' THEN 'MAIN' ELSE c.schema_name END AS unique_constraint_schema,
    NULL::VARCHAR AS unique_constraint_name,
    'NONE' AS match_option,
    'NO ACTION' AS update_rule,
    'NO ACTION' AS delete_rule,
    NULL::VARCHAR AS comment,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS created,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_altered
FROM duckdb_constraints() c
WHERE c.database_name = '{database}'
  AND c.schema_name <> '{info_schema_name}'
  AND c.constraint_type = 'FOREIGN KEY';

-- INFORMATION_SCHEMA.INFORMATION_SCHEMA_CATALOG_NAME. One row, naming the
-- database whose INFORMATION_SCHEMA is being read.
CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._information_schema_catalog_name AS
SELECT '{database}' AS catalog_name;

-- INFORMATION_SCHEMA.APPLICABLE_ROLES / ENABLED_ROLES. SnowDuck runs every
-- statement with full access, so the one role it reports is the one in use.
CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._applicable_roles AS
SELECT
    'SYSADMIN' AS grantee,
    'SYSADMIN' AS role_name,
    'SYSADMIN' AS role_owner,
    'YES' AS is_grantable;

CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._enabled_roles AS
SELECT
    'SYSADMIN' AS role_name,
    'SYSADMIN' AS role_owner,
    NULL::VARCHAR AS comment;

-- Views for object kinds SnowDuck has no local equivalent for. They are empty
-- rather than absent so a query against them returns no rows in Snowflake's
-- column shape, which is what the object actually being missing looks like -
-- rather than a catalog error the caller has to special-case.
CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._table_privileges AS
SELECT
    NULL::VARCHAR AS grantor,
    NULL::VARCHAR AS grantee,
    NULL::VARCHAR AS table_catalog,
    NULL::VARCHAR AS table_schema,
    NULL::VARCHAR AS table_name,
    NULL::VARCHAR AS privilege_type,
    NULL::VARCHAR AS is_grantable,
    NULL::VARCHAR AS with_hierarchy
WHERE FALSE;

CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._usage_privileges AS
SELECT
    NULL::VARCHAR AS grantor,
    NULL::VARCHAR AS grantee,
    NULL::VARCHAR AS object_catalog,
    NULL::VARCHAR AS object_schema,
    NULL::VARCHAR AS object_name,
    NULL::VARCHAR AS object_type,
    NULL::VARCHAR AS privilege_type,
    NULL::VARCHAR AS is_grantable
WHERE FALSE;

CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._object_privileges AS
SELECT
    NULL::VARCHAR AS grantor,
    NULL::VARCHAR AS grantee,
    NULL::VARCHAR AS object_catalog,
    NULL::VARCHAR AS object_schema,
    NULL::VARCHAR AS object_name,
    NULL::VARCHAR AS object_type,
    NULL::VARCHAR AS privilege_type,
    NULL::VARCHAR AS is_grantable
WHERE FALSE;

CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._view_table_usage AS
SELECT
    NULL::VARCHAR AS view_catalog,
    NULL::VARCHAR AS view_schema,
    NULL::VARCHAR AS view_name,
    NULL::VARCHAR AS table_catalog,
    NULL::VARCHAR AS table_schema,
    NULL::VARCHAR AS table_name,
    NULL::VARCHAR AS column_name
WHERE FALSE;

CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._external_tables AS
SELECT
    NULL::VARCHAR AS table_catalog,
    NULL::VARCHAR AS table_schema,
    NULL::VARCHAR AS table_name,
    NULL::VARCHAR AS table_owner,
    NULL::VARCHAR AS location,
    NULL::VARCHAR AS file_format_name,
    NULL::VARCHAR AS file_format_type,
    NULL::BIGINT AS bytes,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS created,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_altered,
    NULL::VARCHAR AS comment
WHERE FALSE;

CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._file_formats AS
SELECT
    NULL::VARCHAR AS file_format_catalog,
    NULL::VARCHAR AS file_format_schema,
    NULL::VARCHAR AS file_format_name,
    NULL::VARCHAR AS file_format_owner,
    NULL::VARCHAR AS file_format_type,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS created,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_altered,
    NULL::VARCHAR AS comment
WHERE FALSE;

CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._procedures AS
SELECT
    NULL::VARCHAR AS procedure_catalog,
    NULL::VARCHAR AS procedure_schema,
    NULL::VARCHAR AS procedure_name,
    NULL::VARCHAR AS procedure_owner,
    NULL::VARCHAR AS argument_signature,
    NULL::VARCHAR AS data_type,
    NULL::VARCHAR AS procedure_language,
    NULL::VARCHAR AS procedure_definition,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS created,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_altered,
    NULL::VARCHAR AS comment
WHERE FALSE;

CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._load_history AS
SELECT
    NULL::VARCHAR AS schema_name,
    NULL::VARCHAR AS file_name,
    NULL::VARCHAR AS table_catalog_name,
    NULL::VARCHAR AS table_schema_name,
    NULL::VARCHAR AS table_name,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_load_time,
    NULL::VARCHAR AS status,
    NULL::BIGINT AS row_count,
    NULL::BIGINT AS row_parsed,
    NULL::BIGINT AS file_size,
    NULL::BIGINT AS first_error_message,
    NULL::BIGINT AS error_count
WHERE FALSE;
