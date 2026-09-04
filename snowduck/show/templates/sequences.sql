-- https://docs.snowflake.com/en/sql-reference/sql/show-sequences#output
SELECT
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS 'created_on',
    s.sequence_name AS 'name',
    CASE WHEN s.schema_name = 'main' THEN 'MAIN' ELSE s.schema_name END AS 'schema_name',
    s.database_name AS 'database_name',
    s.start_value AS 'next_value',
    s.increment_by AS 'interval',
    'SYSADMIN' AS 'owner',
    'ROLE' AS 'owner_role_type',
    s.comment AS 'comment',
    CASE WHEN s.cycle THEN 'N' ELSE 'Y' END AS 'ordered'
FROM duckdb_sequences() s
WHERE s.database_name NOT IN ({excluded_databases})
  AND s.schema_name <> '{info_schema_name}'{predicates}
{tail}
