from sqlglot import parse_one

from snowduck.dialect.transforms import (
    transform_copy,
    transform_create,
    transform_current_session_info,
    transform_describe,
    transform_lateral,
    transform_show,
    transform_use,
)


def test_create_transformation(dialect_context):
    expression = parse_one("CREATE DATABASE foo", read="snowflake")
    transformed_sql = transform_create(expression, context=dialect_context)
    assert transformed_sql == "ATTACH DATABASE ':memory:' AS foo"


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
    expression = parse_one("SHOW DATABASES", read="snowflake")
    transformed_sql = transform_show(expression, context=dialect_context)
    assert transformed_sql == dialect_context.info_schema_manager.show_databases_sql()


def test_show_schemas_transformation(dialect_context):
    expression = parse_one("SHOW SCHEMAS", read="snowflake")
    transformed_sql = transform_show(expression, context=dialect_context)
    assert transformed_sql == dialect_context.info_schema_manager.show_schemas_sql(
        database=dialect_context.current_database,
    )


def test_show_objects_transformation(dialect_context):
    expression = parse_one(
        "SHOW OBJECTS IN SCHEMA test_db.test_schema", read="snowflake"
    )
    transformed_sql = transform_show(expression, context=dialect_context)
    assert transformed_sql == dialect_context.info_schema_manager.show_objects_sql(
        database="test_db",
        schema="test_schema",
    )


def test_transform_current_session_info(dialect_context):
    # Input SQL
    input_sql = """
    SELECT 
        CURRENT_ROLE() AS ROLE, 
        CURRENT_SECONDARY_ROLES() AS SECONDARY_ROLES, 
        CURRENT_DATABASE() AS DATABASE, 
        CURRENT_SCHEMA() AS SCHEMA, 
        CURRENT_WAREHOUSE() AS WAREHOUSE
    """

    # Expected transformed SQL
    expected_sql = """
    SELECT 'test_role' AS ROLE, '{"roles": "", "value": "ALL"}' AS SECONDARY_ROLES, 'test_db' AS DATABASE, 'test_schema' AS SCHEMA, 'test_warehouse' AS WAREHOUSE
    """

    # Parse the input SQL into an expression
    expression = parse_one(input_sql)

    # Transform the expression
    transformed_sql = transform_current_session_info(expression, dialect_context)

    # Assert the transformed SQL matches the expected SQL
    assert transformed_sql.strip() == expected_sql.strip()


def test_transform_current_session_info_without_alias(dialect_context):
    input_sql = """
    SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()
    """

    expected_sql = """
    SELECT 'test_role', 'test_db', 'test_schema', 'test_warehouse'
    """

    expression = parse_one(input_sql)
    transformed_sql = transform_current_session_info(expression, dialect_context)

    assert transformed_sql.strip() == expected_sql.strip()


def test_transform_lateral_flatten(dialect_context):
    expression = parse_one(
        "SELECT value FROM LATERAL FLATTEN(input => ARRAY_CONSTRUCT(1,2))",
        read="snowflake",
    )
    lateral = expression.args["from"].this
    transformed_sql = transform_lateral(lateral, context=dialect_context)

    assert "UNNEST" in transformed_sql
    assert "1" in transformed_sql and "2" in transformed_sql


def test_transform_copy_into(dialect_context, monkeypatch):
    monkeypatch.setenv("SNOWDUCK_STAGE_DIR", "/tmp/snowduck_stage")
    expression = parse_one("COPY INTO my_table FROM @my_stage", read="snowflake")
    transformed_sql = transform_copy(expression, context=dialect_context)

    assert transformed_sql == "COPY my_table FROM '/tmp/snowduck_stage/my_stage'"
