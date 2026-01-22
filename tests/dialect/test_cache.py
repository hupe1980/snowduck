from sqlglot import parse_one

from snowduck.dialect import Dialect


def test_dialect_sql_cache_reuse(dialect_context):
    Dialect.clear_cache()
    dialect = Dialect(context=dialect_context)
    expr = parse_one("SELECT 1", read="snowflake")

    sql1 = Dialect.sql_with_cache(expr, dialect)
    size1 = Dialect.cache_size()

    sql2 = Dialect.sql_with_cache(expr, dialect)
    size2 = Dialect.cache_size()

    assert sql1 == sql2
    assert size1 == 1
    assert size2 == 1
