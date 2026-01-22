SELECT
    column_name AS "name",
    column_type as "type",
    'COLUMN' AS "kind",
    CASE WHEN "null" = 'YES' THEN 'Y' ELSE 'N' END AS "null?",
    NULL::VARCHAR AS "default",
    'N' AS "primary key",
    'N' AS "unique key",
    NULL::VARCHAR AS "check",
    NULL::VARCHAR AS "expression",
    NULL::VARCHAR AS "comment",
    NULL::VARCHAR AS "policy name",
    NULL::JSON AS "privacy domain",
FROM (DESCRIBE {view})