from sqlglot import parse_one

from snowduck.dialect.transforms import (
    transform_command,
    transform_copy,
    transform_create,
    transform_describe,
    transform_show,
    transform_use,
)


def test_create_transformation(dialect_context):
    expression = parse_one("CREATE DATABASE foo", read="snowflake")
    transformed_sql = transform_create(expression, context=dialect_context)
    # Unquoted identifiers are uppercased to match Snowflake behavior
    assert transformed_sql == "ATTACH DATABASE ':memory:' AS FOO"


def test_describe_transformation(dialect_context):
    expression = parse_one("DESCRIBE TABLE foo", read="snowflake")
    transformed_sql = transform_describe(expression, context=dialect_context)
    assert transformed_sql == dialect_context.info_schema_manager.describe_table_sql(
        database=dialect_context.current_database,
        schema=dialect_context.current_schema,
        table="foo",
    )


def test_use_database_transformation(dialect_context):
    expression = parse_one("USE DATABASE foo", read="snowflake")
    transformed_sql = transform_use(expression, context=dialect_context)
    assert transformed_sql == "SET schema = 'foo.PUBLIC'"


def test_show_transformation(dialect_context):
    """SHOW renders a query, not a passthrough of the SHOW statement."""
    expression = parse_one("SHOW DATABASES", read="snowflake")
    transformed_sql = transform_show(expression, context=dialect_context)
    assert "SELECT" in transformed_sql
    assert "duckdb_databases()" in transformed_sql


def test_show_schemas_transformation(dialect_context):
    expression = parse_one("SHOW SCHEMAS", read="snowflake")
    transformed_sql = transform_show(expression, context=dialect_context)
    assert "duckdb_schemas()" in transformed_sql
    assert "upper(s.database_name) = upper('test_db')" in transformed_sql


def test_show_objects_transformation(dialect_context):
    expression = parse_one(
        "SHOW OBJECTS IN SCHEMA test_db.test_schema", read="snowflake"
    )
    transformed_sql = transform_show(expression, context=dialect_context)
    assert "upper(o.database_name) = upper('test_db')" in transformed_sql
    assert "upper(o.schema_name) = upper('test_schema')" in transformed_sql


def test_show_user_functions_transformation(dialect_context):
    """sqlglot parses SHOW USER FUNCTIONS as an opaque Command, not a Show."""
    expression = parse_one(
        "SHOW USER FUNCTIONS IN test_db.test_schema", read="snowflake"
    )
    transformed_sql = transform_command(expression, context=dialect_context)
    assert "duckdb_functions()" in transformed_sql
    assert "'is_builtin'" in transformed_sql


def test_unknown_command_is_passed_through(dialect_context):
    """Only SHOW is claimed from the Command fallback."""
    expression = parse_one("VACUUM", read="snowflake")
    assert transform_command(expression, context=dialect_context) == "VACUUM"


def test_session_info_substituted_in_place(conn):
    """Session functions are replaced without disturbing the rest of the query.

    They used to be rewritten by re-rendering the whole SELECT as a string,
    which dropped everything after the projection list - so this query
    silently counted one row instead of three.
    """
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE sess_t (a INT)")
        cur.execute("INSERT INTO sess_t VALUES (1), (2), (3)")

        cur.execute("SELECT CURRENT_DATABASE(), COUNT(*) FROM sess_t")
        assert cur.fetchone()[1] == 3

        cur.execute("SELECT CURRENT_ROLE(), a FROM sess_t WHERE a > 1 ORDER BY a")
        assert [row[1] for row in cur.fetchall()] == [2, 3]


def test_session_info_values(dialect_context):
    """CURRENT_* resolve to the session's configured values."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    sql = parse_one(
        "SELECT CURRENT_ROLE() AS ROLE, CURRENT_DATABASE() AS DATABASE, "
        "CURRENT_SCHEMA() AS SCHEMA, CURRENT_WAREHOUSE() AS WAREHOUSE",
        read="snowflake",
    ).sql(dialect=dialect)

    assert "'test_role' AS ROLE" in sql
    assert "'test_db' AS DATABASE" in sql
    assert "'test_schema' AS SCHEMA" in sql
    assert "'test_warehouse' AS WAREHOUSE" in sql


def test_session_info_default_column_names(dialect_context):
    """An unaliased session function keeps Snowflake's column name."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    sql = parse_one("SELECT CURRENT_ROLE()", read="snowflake").sql(dialect=dialect)
    assert sql == "SELECT 'test_role' AS \"CURRENT_ROLE()\""


def test_transform_lateral_flatten(conn):
    """LATERAL FLATTEN unnests the input, one row per element."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT value FROM LATERAL FLATTEN(input => ARRAY_CONSTRUCT(1, 2, 3)) "
            "ORDER BY value"
        )
        assert [row[0] for row in cur.fetchall()] == [1, 2, 3]


def test_lateral_flatten_over_a_column(conn):
    """FLATTEN over a stored ARRAY column, which SnowDuck models as JSON."""
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE flat_t (id INT, tags ARRAY)")
        cur.execute("INSERT INTO flat_t VALUES (1, [10, 20])")
        cur.execute(
            "SELECT f.value FROM flat_t, LATERAL FLATTEN(input => flat_t.tags) f "
            "ORDER BY 1"
        )
        assert [str(row[0]) for row in cur.fetchall()] == ["10", "20"]


def test_transform_copy_into(dialect_context, monkeypatch):
    monkeypatch.setenv("SNOWDUCK_STAGE_DIR", "/tmp/snowduck_stage")
    expression = parse_one("COPY INTO my_table FROM @my_stage", read="snowflake")
    transformed_sql = transform_copy(expression, context=dialect_context)

    assert transformed_sql == "COPY my_table FROM '/tmp/snowduck_stage/my_stage'"
