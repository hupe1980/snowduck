-- Every schema-level relation DuckDB knows about, shaped like a Snowflake
-- object row. Used as the source for SHOW OBJECTS / TABLES / VIEWS.
SELECT
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS created_on,
    t.database_name AS database_name,
    t.schema_name AS schema_name,
    t.table_name AS name,
    'TABLE' AS kind,
    t.comment AS comment,
    t.estimated_size::BIGINT AS "rows",
    NULL::BIGINT AS bytes,
    'SYSADMIN' AS owner,
    CASE WHEN t.temporary THEN 'Y' ELSE 'N' END AS is_temporary,
    NULL::VARCHAR AS text
FROM duckdb_tables() t
WHERE NOT t.internal
  AND t.database_name NOT IN ({excluded_databases})
  AND t.schema_name <> '{info_schema_name}'
UNION ALL
SELECT
    TO_TIMESTAMP(0)::TIMESTAMPTZ,
    v.database_name,
    v.schema_name,
    v.view_name,
    'VIEW',
    v.comment,
    NULL::BIGINT,
    NULL::BIGINT,
    'SYSADMIN',
    CASE WHEN v.temporary THEN 'Y' ELSE 'N' END,
    v.sql
FROM duckdb_views() v
WHERE NOT v.internal
  AND v.database_name NOT IN ({excluded_databases})
  AND v.schema_name <> '{info_schema_name}'
