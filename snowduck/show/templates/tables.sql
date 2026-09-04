-- https://docs.snowflake.com/en/sql-reference/sql/show-tables#output
SELECT
    o.created_on AS 'created_on',
    o.name AS 'name',
    o.database_name AS 'database_name',
    o.schema_name AS 'schema_name',
    'TABLE' AS 'kind',
    o.comment AS 'comment',
    NULL::VARCHAR AS 'cluster_by',
    o."rows" AS 'rows',
    o.bytes AS 'bytes',
    o.owner AS 'owner',
    1 AS 'retention_time',
    'OFF' AS 'automatic_clustering',
    'OFF' AS 'change_tracking',
    'OFF' AS 'search_optimization',
    NULL::VARCHAR AS 'search_optimization_progress',
    NULL::BIGINT AS 'search_optimization_bytes',
    'N' AS 'is_external',
    'N' AS 'enable_schema_evolution',
    'ROLE' AS 'owner_role_type',
    'N' AS 'is_event',
    NULL::VARCHAR AS 'budget',
    'N' AS 'is_hybrid',
    'N' AS 'is_iceberg',
    'N' AS 'is_dynamic',
    'N' AS 'is_immutable'
FROM ({objects}) o
WHERE o.kind = 'TABLE'{predicates}
{tail}
