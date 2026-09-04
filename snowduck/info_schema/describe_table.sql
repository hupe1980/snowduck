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
    CASE WHEN EXISTS (
        SELECT 1 FROM duckdb_constraints() k
        WHERE k.constraint_type = 'PRIMARY KEY'
          AND upper(k.database_name) = upper('{database}')
          AND upper(k.table_name) = upper('{table}')
          AND list_contains(k.constraint_column_names, column_name)
    ) THEN 'Y' ELSE 'N' END AS "primary key",
    CASE WHEN EXISTS (
        SELECT 1 FROM duckdb_constraints() k
        WHERE k.constraint_type = 'UNIQUE'
          AND upper(k.database_name) = upper('{database}')
          AND upper(k.table_name) = upper('{table}')
          AND list_contains(k.constraint_column_names, column_name)
    ) THEN 'Y' ELSE 'N' END AS "unique key",
    NULL::VARCHAR AS "check",
    NULL::VARCHAR AS "expression",
    comment AS "comment",
    NULL::VARCHAR AS "policy name",
    NULL::JSON AS "privacy domain",
FROM {account_catalog_name}.{info_schema_name}._columns
-- Case-insensitive: Snowflake folds unquoted identifiers to upper case at DDL
-- time, DuckDB stores them as written, so the comparison is folded instead.
WHERE upper(table_catalog) = upper('{database}')
  AND upper(table_schema) = upper('{schema}')
  AND upper(table_name) = upper('{table}')
ORDER BY ordinal_position