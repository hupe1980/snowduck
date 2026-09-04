SELECT
    o.created_on AS 'created_on',
    o.name AS 'name',
    o.kind AS 'kind',
    o.database_name AS 'database_name',
    o.schema_name AS 'schema_name'
FROM ({objects}) o
WHERE TRUE{predicates}
{tail}
