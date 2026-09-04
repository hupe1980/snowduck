-- https://docs.snowflake.com/en/sql-reference/sql/show-views#output
SELECT
    o.created_on AS 'created_on',
    o.name AS 'name',
    NULL::VARCHAR AS 'reserved',
    o.database_name AS 'database_name',
    o.schema_name AS 'schema_name',
    o.owner AS 'owner',
    o.comment AS 'comment',
    o.text AS 'text',
    'false' AS 'is_secure',
    'false' AS 'is_materialized',
    'ROLE' AS 'owner_role_type',
    'OFF' AS 'change_tracking'
FROM ({objects}) o
WHERE o.kind = 'VIEW'{predicates}
{tail}
