SELECT
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS 'created_on',
    CASE WHEN s.schema_name = 'main' THEN 'MAIN' ELSE s.schema_name END AS 'name',
    'SCHEMA' AS 'kind',
    s.database_name AS 'database_name',
    CASE WHEN s.schema_name = 'main' THEN 'MAIN' ELSE s.schema_name END AS 'schema_name'
FROM duckdb_schemas() s
WHERE (NOT s.internal
       OR (s.schema_name = 'main' AND EXISTS (
             SELECT 1 FROM {account_catalog_name}.{info_schema_name}._created_schemas c
             WHERE c.database_name = s.database_name AND c.schema_name = 'MAIN')))
  AND s.database_name NOT IN ({excluded_databases})
  -- `main` is governed by the registry clause above, not excluded outright.
  AND s.schema_name <> '{info_schema_name}'{predicates}
{tail}
