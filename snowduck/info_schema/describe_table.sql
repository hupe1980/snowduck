SELECT
    column_name AS "name",
    CASE WHEN data_type = 'NUMBER' THEN 'NUMBER(' || numeric_precision || ',' || numeric_scale || ')'
         WHEN data_type = 'TEXT' THEN 'VARCHAR(' || coalesce(character_maximum_length,16777216)  || ')'
         WHEN data_type = 'TIMESTAMP_NTZ' THEN 'TIMESTAMP_NTZ(9)'
         WHEN data_type = 'TIMESTAMP_TZ' THEN 'TIMESTAMP_TZ(9)'
         WHEN data_type = 'TIME' THEN 'TIME(9)'
         WHEN data_type = 'BINARY' THEN 'BINARY(8388608)'
        ELSE data_type END AS "type",
    'COLUMN' AS "kind",
    CASE WHEN is_nullable = 'YES' THEN 'Y' ELSE 'N' END AS "null?",
    column_default AS "default",
    'N' AS "primary key",
    'N' AS "unique key",
    NULL::VARCHAR AS "check",
    NULL::VARCHAR AS "expression",
    NULL::VARCHAR AS "comment",
    NULL::VARCHAR AS "policy name",
    NULL::JSON AS "privacy domain",
FROM {account_catalog_name}.{info_schema_name}._columns
WHERE table_catalog = '{database}' AND table_schema = '{schema}' AND table_name = '{table}'
ORDER BY ordinal_position