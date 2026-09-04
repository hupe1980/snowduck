-- https://docs.snowflake.com/en/sql-reference/sql/show-schemas#output
SELECT
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS 'created_on',
    s.schema_name AS 'name',
    'N' AS 'is_default',
    CASE WHEN upper(s.schema_name) = upper({current_schema}) THEN 'Y' ELSE 'N' END AS 'is_current',
    s.database_name AS 'database_name',
    'SYSADMIN' AS 'owner',
    s.comment AS 'comment',
    '' AS 'options',
    1 AS 'retention_time',
    'ROLE' AS 'owner_role_type',
    NULL::VARCHAR AS 'budget'
FROM duckdb_schemas() s
WHERE NOT s.internal
  AND s.database_name NOT IN ({excluded_databases})
  AND s.schema_name NOT IN ('{info_schema_name}', 'main'){predicates}
{tail}
