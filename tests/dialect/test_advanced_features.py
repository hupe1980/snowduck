from sqlglot import parse_one


def test_object_construct(dialect_context):
    sql = "SELECT OBJECT_CONSTRUCT('a', 1, 'b', 'test')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    # DuckDB uses json_object for constructing JSON objects from keys/values
    # Or struct_pack. Snowflake OBJECT is usually JSON-like in usage.
    # We prefer json_object('a', 1, 'b', 'test')
    assert "json_object" in transpiled.lower()


def test_array_construct(dialect_context):
    sql = "SELECT ARRAY_CONSTRUCT(1, 2, 3)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    # DuckDB: list_value or []
    # sqlglot usually maps ARRAY_CONSTRUCT to [] or LIST_VALUE
    assert "[" in transpiled or "list_value" in transpiled.lower()


def test_time_travel_at_clause(dialect_context):
    sql = "SELECT * FROM my_table AT(TIMESTAMP => '2021-01-01'::timestamp)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    # We want to STRIP the AT clause so it runs against current data
    assert "AT" not in transpiled.upper()


def test_lateral_flatten(dialect_context):
    sql = "SELECT * FROM my_table, LATERAL FLATTEN(input => my_table.col)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    # DuckDB uses UNNEST
    # Snowflake: LATERAL FLATTEN returns columns like VALUE, etc.
    # We want to check if it transforms to something usable like UNNEST(col) AS ...
    assert "unnest" in transpiled.lower()


def test_qualify_clause(dialect_context):
    sql = "SELECT a, ROW_NUMBER() OVER (ORDER BY a) as rn FROM t QUALIFY rn = 1"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    assert "QUALIFY" in transpiled.upper()
    assert "rn = 1" in transpiled
