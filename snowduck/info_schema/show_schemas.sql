SELECT
    to_timestamp(0)::timestamptz as 'created_on',
    schema_name as 'name',
    'N' as 'is_default',
    'N' as 'is_current',
    catalog_name as 'database_name',
    'SYSADMIN' as 'owner',
    NULL as 'comment',
    '' as 'options',
    1 as 'retention_time',
    'ROLE' as 'owner_role_type'
FROM system.information_schema.schemata
WHERE catalog_name = '{database}'
  AND schema_name != '{info_schema_name}'
