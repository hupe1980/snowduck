from sqlglot import parse_one


def test_parse_json_transformation(dialect_context):
    sql = "SELECT PARSE_JSON('{\"a\": 1}')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)

    transpiled = expression.sql(dialect=dialect)
    # We expect CAST(...  AS JSON) which is cleaner than JSON() function
    assert "CAST" in transpiled and "JSON" in transpiled


def test_try_parse_json_transformation(dialect_context):
    sql = "SELECT TRY_PARSE_JSON('invalid')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)

    transpiled = expression.sql(dialect=dialect)
    # We expect TRY_CAST(... AS JSON) which returns NULL on invalid JSON
    assert "TRY_CAST" in transpiled and "JSON" in transpiled
    assert "PARSE_JSON" not in transpiled.upper()
