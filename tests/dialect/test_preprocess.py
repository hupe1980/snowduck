from sqlglot import parse_one

from snowduck.dialect.preprocess import preprocess_current_schema, preprocess_identifier


def test_identifier(dialect_context):
    """Test the identifier transformation."""
    expression = parse_one("SELECT * FROM IDENTIFIER('foo')", read="snowflake")
    transformed_sql = expression.transform(preprocess_identifier, context=dialect_context).sql()
    assert transformed_sql == "SELECT * FROM foo"

def test_current_schema(dialect_context):
    """Test the current schema transformation."""
    expression = parse_one("SELECT CURRENT_SCHEMA() AS current_schema", read="snowflake")
    transformed_sql = expression.transform(preprocess_current_schema, context=dialect_context).sql()
    assert transformed_sql == "SELECT 'test_schema' AS current_schema"