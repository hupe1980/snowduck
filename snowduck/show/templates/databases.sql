-- https://docs.snowflake.com/en/sql-reference/sql/show-databases#output
SELECT
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS 'created_on',
    d.database_name AS 'name',
    'N' AS 'is_default',
    CASE WHEN upper(d.database_name) = upper({current_database}) THEN 'Y' ELSE 'N' END AS 'is_current',
    '' AS 'origin',
    'SYSADMIN' AS 'owner',
    d.comment AS 'comment',
    '' AS 'options',
    1 AS 'retention_time',
    'STANDARD' AS 'kind',
    NULL::VARCHAR AS 'budget',
    'ROLE' AS 'owner_role_type',
    NULL::VARCHAR AS 'object_visibility'
FROM duckdb_databases() d
WHERE d.database_name NOT IN ({excluded_databases}){predicates}
{tail}
