"""Tests for Snowflake conversion function compatibility."""

import duckdb
import pytest
from sqlglot import parse_one

from snowduck.dialect.context import DialectContext
from snowduck.info_schema.manager import InfoSchemaManager


@pytest.fixture
def dialect_context():
    conn = duckdb.connect()
    manager = InfoSchemaManager(conn)
    return DialectContext(info_schema_manager=manager)


def test_to_char_integer(dialect_context):
    """Test TO_CHAR with integer input."""
    sql = "SELECT TO_CHAR(12345)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "12345"


def test_to_char_decimal(dialect_context):
    """Test TO_CHAR with decimal input."""
    sql = "SELECT TO_CHAR(123.45)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert "123.45" in res[0]


def test_to_varchar(dialect_context):
    """Test TO_VARCHAR function."""
    sql = "SELECT TO_VARCHAR(999)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "999"


def test_to_number(dialect_context):
    """TO_NUMBER defaults to NUMBER(38, 0), so a bare call rounds to an integer."""
    sql = "SELECT TO_NUMBER('123.45')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 123


def test_try_cast_valid(dialect_context):
    """Test TRY_CAST with valid conversion."""
    sql = "SELECT TRY_CAST('123' AS INTEGER)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 123


def test_try_cast_invalid(dialect_context):
    """Test TRY_CAST with invalid conversion returns NULL."""
    sql = "SELECT TRY_CAST('abc' AS INTEGER)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is None


def test_cast_date(dialect_context):
    """Test CAST to DATE."""
    sql = "SELECT CAST('2024-01-15' AS DATE)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    import datetime

    assert res[0] == datetime.date(2024, 1, 15)


def test_cast_timestamp(dialect_context):
    """Test CAST to TIMESTAMP."""
    sql = "SELECT CAST('2024-01-15 10:30:00' AS TIMESTAMP)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert "2024-01-15" in str(res[0])


def test_to_boolean_true(dialect_context):
    """Test TO_BOOLEAN with various true values."""
    sql = "SELECT TO_BOOLEAN('true'), TO_BOOLEAN('yes'), TO_BOOLEAN(1)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # First value should be True
    assert res[0] is True


def test_to_boolean_false(dialect_context):
    """Test TO_BOOLEAN with false values."""
    sql = "SELECT TO_BOOLEAN('false'), TO_BOOLEAN('no'), TO_BOOLEAN(0)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # First value should be False
    assert res[0] is False


def test_to_number_precision_and_scale(conn):
    """TO_NUMBER(expr, precision, scale) must produce an exact DECIMAL, not a DOUBLE."""
    from decimal import Decimal

    with conn.cursor() as cur:
        cur.execute("SELECT TO_NUMBER('123.45', 10, 2)")
        result = cur.fetchone()[0]
        assert isinstance(result, Decimal)
        assert result == Decimal("123.45")


def test_to_number_large_integer_is_exact(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT TO_NUMBER('12345678901234567890123', 38, 0)")
        assert str(cur.fetchone()[0]) == "12345678901234567890123"


def test_to_number_with_format_and_precision(conn):
    """A leading string argument is a format model, not the precision."""
    from decimal import Decimal

    with conn.cursor() as cur:
        cur.execute("SELECT TO_NUMBER('123.456', '999.999', 10, 2)")
        assert cur.fetchone()[0] == Decimal("123.46")


def test_to_decimal(conn):
    """TO_DECIMAL/TO_NUMERIC are synonyms of TO_NUMBER and must not reach DuckDB raw."""
    from decimal import Decimal

    with conn.cursor() as cur:
        cur.execute("SELECT TO_DECIMAL('123.45', 10, 2), TO_NUMERIC('7', 5, 0)")
        assert cur.fetchone() == (Decimal("123.45"), Decimal("7"))


def test_try_to_decimal_returns_null_on_bad_input(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT TRY_TO_DECIMAL('abc', 10, 2)")
        assert cur.fetchone()[0] is None
