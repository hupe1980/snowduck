SELECT
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS 'created_on',
    d.database_name AS 'name',
    'DATABASE' AS 'kind',
    d.database_name AS 'database_name',
    NULL::VARCHAR AS 'schema_name'
FROM duckdb_databases() d
WHERE d.database_name NOT IN ({excluded_databases}){predicates}
{tail}
