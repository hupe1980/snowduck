SELECT
    to_timestamp(0)::timestamptz as 'created_on',
    table_name as 'name',
    table_schema as 'schema_name',
    table_catalog as 'database_name',
    CASE WHEN table_type = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END as 'kind',
    NULL as 'comment',
    'SYSADMIN' as 'owner',
    'ROLE' as 'owner_role_type'
FROM system.information_schema.tables
WHERE table_catalog = '{database}'
  AND table_schema = '{schema}'
  AND table_schema != '{info_schema_name}'
