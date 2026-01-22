CREATE SCHEMA IF NOT EXISTS {database}.{info_schema_name};

CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._columns AS
SELECT *
FROM {account_catalog_name}.{info_schema_name}._columns
WHERE table_catalog = '{database}';

CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._databases AS
SELECT
    catalog_name AS database_name,
    'SYSADMIN' AS database_owner,
    'NO' AS is_transient,
    NULL::VARCHAR AS comment,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS created,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_altered,
    1 AS retention_time,
    'STANDARD' AS type
FROM system.information_schema.schemata
WHERE catalog_name NOT IN ('memory', 'system', 'temp', '{account_catalog_name}')
  AND schema_name = 'main';

CREATE VIEW IF NOT EXISTS {database}.{info_schema_name}._tables AS
SELECT *
FROM system.information_schema.tables tables
LEFT JOIN {account_catalog_name}.{info_schema_name}._tables_ext ON
    tables.table_catalog = _tables_ext.ext_table_catalog
    AND tables.table_schema = _tables_ext.ext_table_schema
    AND tables.table_name = _tables_ext.ext_table_name
WHERE table_catalog = '{database}'
  AND table_schema != '{info_schema_name}';

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
    NULL::VARCHAR AS comment
FROM duckdb_views
WHERE database_name = '{database}'
  AND schema_name != '{info_schema_name}';