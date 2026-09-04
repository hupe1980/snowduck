CREATE SCHEMA IF NOT EXISTS {account_catalog_name}.{info_schema_name};

-- INFORMATION_SCHEMA.DATABASES. Account-wide in Snowflake, so it lives in the
-- account catalog and every database's INFORMATION_SCHEMA.DATABASES maps here.
CREATE VIEW IF NOT EXISTS {account_catalog_name}.{info_schema_name}._DATABASES AS
SELECT
    database_name AS database_name,
    'SYSADMIN' AS database_owner,
    'NO' AS is_transient,
    comment AS comment,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS created,
    TO_TIMESTAMP(0)::TIMESTAMPTZ AS last_altered,
    1 AS retention_time,
    'STANDARD' AS type
FROM duckdb_databases()
WHERE database_name NOT IN ('memory', 'system', 'temp', '{account_catalog_name}');

CREATE TABLE IF NOT EXISTS {account_catalog_name}.{info_schema_name}._tables_ext (
    ext_table_catalog VARCHAR,
    ext_table_schema VARCHAR,
    ext_table_name VARCHAR,
    comment VARCHAR,
    PRIMARY KEY (ext_table_catalog, ext_table_schema, ext_table_name)
);

CREATE TABLE IF NOT EXISTS {account_catalog_name}.{info_schema_name}._columns_ext (
    ext_table_catalog VARCHAR,
    ext_table_schema VARCHAR,
    ext_table_name VARCHAR,
    ext_column_name VARCHAR,
    ext_character_maximum_length INTEGER,
    ext_character_octet_length INTEGER,
    PRIMARY KEY (ext_table_catalog, ext_table_schema, ext_table_name, ext_column_name)
);

CREATE VIEW IF NOT EXISTS {account_catalog_name}.{info_schema_name}._columns AS
SELECT
    columns.table_catalog AS table_catalog,
    columns.table_schema AS table_schema,
    columns.table_name AS table_name,
    columns.column_name AS column_name,
    columns.ordinal_position AS ordinal_position,
    columns.column_default AS column_default,
    columns.is_nullable AS is_nullable,
    -- DuckDB type -> Snowflake type. Snowflake has one integer type (NUMBER)
    -- and one float type (FLOAT), so every width maps onto those.
    CASE
        WHEN STARTS_WITH(columns.data_type, 'DECIMAL')
          OR STARTS_WITH(columns.data_type, 'NUMERIC') THEN 'NUMBER'
        WHEN columns.data_type IN (
            'BIGINT', 'INTEGER', 'SMALLINT', 'TINYINT', 'HUGEINT',
            'UBIGINT', 'UINTEGER', 'USMALLINT', 'UTINYINT', 'UHUGEINT'
        ) THEN 'NUMBER'
        WHEN columns.data_type IN ('DOUBLE', 'FLOAT', 'REAL') THEN 'FLOAT'
        WHEN STARTS_WITH(columns.data_type, 'VARCHAR')
          OR STARTS_WITH(columns.data_type, 'CHAR')
          OR columns.data_type IN ('TEXT', 'STRING', 'UUID', 'ENUM') THEN 'TEXT'
        WHEN columns.data_type IN ('BLOB', 'BYTEA', 'BINARY', 'VARBINARY') THEN 'BINARY'
        WHEN columns.data_type = 'BOOLEAN' THEN 'BOOLEAN'
        WHEN columns.data_type = 'DATE' THEN 'DATE'
        WHEN columns.data_type LIKE 'TIMESTAMP%WITH TIME ZONE' THEN 'TIMESTAMP_TZ'
        WHEN STARTS_WITH(columns.data_type, 'TIMESTAMP') THEN 'TIMESTAMP_NTZ'
        WHEN STARTS_WITH(columns.data_type, 'TIME') THEN 'TIME'
        WHEN columns.data_type = 'JSON' THEN 'VARIANT'
        WHEN ENDS_WITH(columns.data_type, '[]')
          OR STARTS_WITH(columns.data_type, 'LIST') THEN 'ARRAY'
        WHEN STARTS_WITH(columns.data_type, 'STRUCT')
          OR STARTS_WITH(columns.data_type, 'MAP') THEN 'OBJECT'
        ELSE columns.data_type
    END AS data_type,
    ext_character_maximum_length AS character_maximum_length,
    ext_character_octet_length AS character_octet_length,
    -- Snowflake reports NUMBER(38,0) for every integer width.
    CASE
        WHEN columns.data_type IN (
            'BIGINT', 'INTEGER', 'SMALLINT', 'TINYINT', 'HUGEINT',
            'UBIGINT', 'UINTEGER', 'USMALLINT', 'UTINYINT', 'UHUGEINT'
        ) THEN 38
        WHEN columns.data_type IN ('DOUBLE', 'FLOAT', 'REAL') THEN NULL
        ELSE columns.numeric_precision
    END AS numeric_precision,
    CASE
        WHEN columns.data_type IN ('DOUBLE', 'FLOAT', 'REAL') THEN NULL
        WHEN columns.numeric_precision IS NULL THEN NULL
        ELSE 10
    END AS numeric_precision_radix,
    CASE
        WHEN columns.data_type IN ('DOUBLE', 'FLOAT', 'REAL') THEN NULL
        ELSE columns.numeric_scale
    END AS numeric_scale,
    collation_name,
    is_identity,
    identity_generation,
    identity_cycle,
    ddb_columns.comment AS comment,
    NULL::VARCHAR AS identity_start,
    NULL::VARCHAR AS identity_increment
FROM system.information_schema.columns columns
LEFT JOIN {account_catalog_name}.{info_schema_name}._columns_ext ext
    ON ext_table_catalog = columns.table_catalog
   AND ext_table_schema = columns.table_schema
   AND ext_table_name = columns.table_name
   AND ext_column_name = columns.column_name
LEFT JOIN duckdb_columns ddb_columns
    ON ddb_columns.database_name = columns.table_catalog
   AND ddb_columns.schema_name = columns.table_schema
   AND ddb_columns.table_name = columns.table_name
   AND ddb_columns.column_name = columns.column_name
WHERE columns.table_schema != '{info_schema_name}';

CREATE TABLE IF NOT EXISTS {account_catalog_name}.{info_schema_name}._users_ext (
    name VARCHAR,
    created_on TIMESTAMPTZ,
    login_name VARCHAR,
    display_name VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    mins_to_unlock VARCHAR,
    days_to_expiry VARCHAR,
    comment VARCHAR,
    disabled VARCHAR,
    must_change_password VARCHAR,
    snowflake_lock VARCHAR,
    default_warehouse VARCHAR,
    default_namespace VARCHAR,
    default_role VARCHAR,
    default_secondary_roles VARCHAR,
    ext_authn_duo VARCHAR,
    ext_authn_uid VARCHAR,
    mins_to_bypass_mfa VARCHAR,
    owner VARCHAR,
    last_success_login TIMESTAMPTZ,
    expires_at_time TIMESTAMPTZ,
    locked_until_time TIMESTAMPTZ,
    has_password VARCHAR,
    has_rsa_public_key VARCHAR
);

CREATE TABLE IF NOT EXISTS {account_catalog_name}.{info_schema_name}._warehouses_ext (
    name VARCHAR,
    state VARCHAR,
    type VARCHAR,
    size VARCHAR
    --TODO: add more columns
);