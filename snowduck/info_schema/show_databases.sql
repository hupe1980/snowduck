SELECT
    to_timestamp(0)::timestamptz as 'created_on',
    database_name as 'name',
    'N' as 'is_default',
    'N' as 'is_current',
    '' as 'origin',
    'SYSADMIN' as 'owner',
    comment,
    '' as 'options',
    1 as 'retention_time',
    'STANDARD' as 'kind',
    NULL as 'budget',
    'ROLE' as 'owner_role_type',
    NULL as 'object_visibility'
FROM duckdb_databases()
WHERE database_name NOT IN ('memory', '{account_catalog_name}')