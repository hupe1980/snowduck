SELECT
    table_name as 'table_name',
    table_schema as 'schema_name',
    column_name as 'column_name',
    json_object(
        'type', CASE
            WHEN data_type LIKE 'VARCHAR%' OR data_type LIKE '%CHAR%' THEN 'TEXT'
            WHEN data_type LIKE 'DECIMAL%' OR data_type IN ('BIGINT','INTEGER','SMALLINT','TINYINT','HUGEINT') THEN 'FIXED'
            WHEN data_type IN ('DOUBLE','FLOAT','REAL') THEN 'REAL'
            WHEN data_type = 'BOOLEAN' THEN 'BOOLEAN'
            WHEN data_type = 'DATE' THEN 'DATE'
            WHEN data_type LIKE 'TIMESTAMP%' THEN 'TIMESTAMP_NTZ'
            WHEN data_type = 'JSON' THEN 'VARIANT'
            ELSE data_type
        END,
        'nullable', is_nullable = 'YES'
    ) as 'data_type',
    NULL as 'null?',
    column_default as 'default',
    'N' as 'kind',
    NULL as 'expression',
    NULL as 'comment',
    table_catalog as 'database_name',
    'N' as 'autoincrement'
FROM system.information_schema.columns
WHERE table_catalog = '{database}'
  AND table_schema = '{schema}'
  AND ('{table}' = '' OR table_name = '{table}')
ORDER BY table_name, ordinal_position
