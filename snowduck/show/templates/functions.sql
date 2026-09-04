-- https://docs.snowflake.com/en/sql-reference/sql/show-user-functions#output
SELECT
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS 'created_on',
    f.function_name AS 'name',
    CASE WHEN f.internal THEN NULL
         WHEN f.schema_name = 'main' THEN 'MAIN'
         ELSE f.schema_name END AS 'schema_name',
    CASE WHEN f.internal THEN 'Y' ELSE 'N' END AS 'is_builtin',
    CASE WHEN f.function_type = 'aggregate' THEN 'Y' ELSE 'N' END AS 'is_aggregate',
    'N' AS 'is_ansi',
    len(COALESCE(f.parameters, [])) AS 'min_num_arguments',
    len(COALESCE(f.parameters, [])) AS 'max_num_arguments',
    upper(f.function_name) || '(' || COALESCE(
        list_aggregate(
            list_transform(
                COALESCE(f.parameter_types, []),
                x -> upper(COALESCE(x, 'VARIANT'))
            ),
            'string_agg', ', '
        ), ''
    ) || ') RETURN ' || upper(COALESCE(f.return_type, 'VARIANT')) AS 'arguments',
    f.description AS 'description',
    CASE WHEN f.internal THEN NULL ELSE f.database_name END AS 'catalog_name',
    CASE WHEN f.function_type IN ('table_macro', 'table') THEN 'Y' ELSE 'N' END AS 'is_table_function',
    'N' AS 'valid_for_clustering',
    'false' AS 'is_secure',
    NULL::VARCHAR AS 'secrets',
    NULL::VARCHAR AS 'external_access_integrations',
    'N' AS 'is_external_function',
    'SQL' AS 'language',
    'N' AS 'is_memoizable',
    'N' AS 'is_data_metric'
FROM duckdb_functions() f
WHERE ({selector}){predicates}
{tail}
