SELECT
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS 'created_on',
    s.schema_name AS 'name',
    'SCHEMA' AS 'kind',
    s.database_name AS 'database_name',
    s.schema_name AS 'schema_name'
FROM duckdb_schemas() s
WHERE NOT s.internal
  AND s.database_name NOT IN ({excluded_databases})
  AND s.schema_name NOT IN ('{info_schema_name}', 'main'){predicates}
{tail}
