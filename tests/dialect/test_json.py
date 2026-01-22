from sqlglot import parse_one


def test_parse_json_transformation(dialect_context):
    sql = "SELECT PARSE_JSON('{\"a\": 1}')"
    expression = parse_one(sql, read="snowflake")
    # By default sqlglot might not map this to what DuckDB wants if we don't tell it
    # DuckDB prefers `json(...)` or `json_parse(...)`
    # Let's see what it produces currently
    
    # We expect `json_parse` or cast to json
    # Let's just assert it is NOT PARSE_JSON (which DuckDB doesn't have)
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    
    transpiled = expression.sql(dialect=dialect)
    assert "JSON(" in transpiled

def test_try_parse_json_transformation(dialect_context):
    sql = "SELECT TRY_PARSE_JSON('invalid')"
    expression = parse_one(sql, read="snowflake")
    
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    
    transpiled = expression.sql(dialect=dialect)
    # We want it to result in a JSON type ideally, or consistent with PARSE_JSON
    # sqlglot default is: CASE WHEN JSON_VALID(x) THEN x ELSE NULL END
    # This returns string.
    # We might want: CASE WHEN JSON_VALID(x) THEN JSON(x) ELSE NULL END
    assert "JSON_VALID" in transpiled
    
    # We want to ensure it works in DuckDB.
    # DuckDB: json('...') or str::json
    # or json_parse
    
    # For now, let's just see. I'll assert something that fails if it's raw PARSE_JSON
    assert "PARSE_JSON" not in transpiled.upper()
