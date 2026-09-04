-- https://docs.snowflake.com/en/sql-reference/sql/show-objects#output
SELECT
    o.created_on AS 'created_on',
    o.name AS 'name',
    o.database_name AS 'database_name',
    o.schema_name AS 'schema_name',
    o.kind AS 'kind',
    o.comment AS 'comment',
    NULL::VARCHAR AS 'cluster_by',
    o."rows" AS 'rows',
    o.bytes AS 'bytes',
    o.owner AS 'owner',
    1 AS 'retention_time',
    'ROLE' AS 'owner_role_type',
    NULL::VARCHAR AS 'budget',
    'N' AS 'is_hybrid',
    'N' AS 'is_dynamic',
    'N' AS 'is_iceberg'
FROM ({objects}) o
WHERE TRUE{predicates}
{tail}
