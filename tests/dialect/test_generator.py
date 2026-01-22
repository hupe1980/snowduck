import duckdb
import pytest
from sqlglot import parse_one

from snowduck.dialect.context import DialectContext
from snowduck.info_schema.manager import InfoSchemaManager


@pytest.fixture
def dialect_context():
    conn = duckdb.connect()
    # Pass the connection directly, not a lambda
    manager = InfoSchemaManager(conn)
    return DialectContext(info_schema_manager=manager)


def test_generator_basic(dialect_context):
    sql = "SELECT seq4() FROM TABLE(GENERATOR(ROWCOUNT => 10))"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    # We look for generate_series or range
    assert "generate_series" in transpiled.lower() or "range" in transpiled.lower()
    # default seq4 mapping
    assert "row_number()" in transpiled.lower() or "seq4" not in transpiled.lower()


def test_generator_uniform(dialect_context):
    # UNIFORM(min, max, seed)
    sql = "SELECT UNIFORM(1, 100, RANDOM()) FROM TABLE(GENERATOR(ROWCOUNT => 5))"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    # Check that UNIFORM(min, max, gen) -> floor(random() * (max - min + 1) + min) logic exists
    # Or strict replacement
    assert "random" in transpiled.lower()


def test_generator_timelimit():
    # TIMELIMIT is not supported easily in DuckDB, should be ignored or warned
    pass
