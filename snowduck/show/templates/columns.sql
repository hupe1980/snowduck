-- https://docs.snowflake.com/en/sql-reference/sql/show-columns#output
-- `data_type` is a JSON blob in Snowflake, not a type name.
SELECT
    c.table_name AS 'table_name',
    c.table_schema AS 'schema_name',
    c.column_name AS 'column_name',
    CASE
        WHEN c.data_type = 'TEXT' THEN json_object(
            'type', 'TEXT',
            'length', COALESCE(c.character_maximum_length, 16777216),
            'byteLength', COALESCE(c.character_octet_length, 16777216),
            'nullable', c.is_nullable = 'YES',
            'fixed', FALSE
        )
        WHEN c.data_type = 'NUMBER' THEN json_object(
            'type', 'FIXED',
            'precision', COALESCE(c.numeric_precision, 38),
            'scale', COALESCE(c.numeric_scale, 0),
            'nullable', c.is_nullable = 'YES'
        )
        WHEN c.data_type = 'FLOAT' THEN json_object(
            'type', 'REAL', 'nullable', c.is_nullable = 'YES'
        )
        WHEN c.data_type IN ('TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ', 'TIME') THEN json_object(
            'type', c.data_type, 'precision', 0, 'scale', 9,
            'nullable', c.is_nullable = 'YES'
        )
        WHEN c.data_type = 'BINARY' THEN json_object(
            'type', 'BINARY', 'length', 8388608,
            'byteLength', 8388608, 'nullable', c.is_nullable = 'YES'
        )
        ELSE json_object('type', c.data_type, 'nullable', c.is_nullable = 'YES')
    END::VARCHAR AS 'data_type',
    CASE WHEN c.is_nullable = 'YES' THEN 'true' ELSE 'false' END AS 'null?',
    c.column_default AS 'default',
    'COLUMN' AS 'kind',
    NULL::VARCHAR AS 'expression',
    c.comment AS 'comment',
    c.table_catalog AS 'database_name',
    CASE WHEN c.is_identity = 'YES' THEN 'Y' ELSE 'N' END AS 'autoincrement',
    NULL::VARCHAR AS 'schema_evolution_record'
FROM {account_catalog_name}.{info_schema_name}._columns c
WHERE TRUE{predicates}
{tail}
