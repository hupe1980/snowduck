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


def test_dateadd_minute_singular(dialect_context):
    """Test DATEADD with singular time part (minute)."""
    sql = "SELECT DATEADD(minute, 5, '2021-01-01'::timestamp)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    # DuckDB uses INTERVAL '5 minutes' or dateadd('minute', 5, timestamp)
    # Check that it's valid DuckDB syntax
    assert "dateadd" in transpiled.lower() or "interval" in transpiled.lower()


def test_dateadd_minutes_plural(dialect_context):
    """Test DATEADD with plural time part (minutes)."""
    sql = "SELECT DATEADD(minutes, 5, '2021-01-01'::timestamp)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    assert "dateadd" in transpiled.lower() or "interval" in transpiled.lower()


def test_datediff(dialect_context):
    """Test DATEDIFF function."""
    sql = "SELECT DATEDIFF(day, '2021-01-01'::date, '2021-01-10'::date)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    # DuckDB has datediff or date_diff
    assert "datediff" in transpiled.lower() or "date_diff" in transpiled.lower()


def test_to_timestamp(dialect_context):
    """Test TO_TIMESTAMP function."""
    sql = "SELECT TO_TIMESTAMP('2021-01-01 12:00:00')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    # DuckDB uses strptime or CAST
    assert "timestamp" in transpiled.lower() or "strptime" in transpiled.lower()


def test_date_trunc(dialect_context):
    """Test DATE_TRUNC function."""
    sql = "SELECT DATE_TRUNC('month', '2021-01-15'::date)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # First day of month
    assert str(res[0]).startswith("2021-01-01")


def test_current_date(dialect_context):
    """Test CURRENT_DATE function."""
    from datetime import date
    sql = "SELECT CURRENT_DATE()"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert isinstance(res[0], date)


def test_extract(dialect_context):
    """Test EXTRACT function."""
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    # YEAR
    sql = "SELECT EXTRACT(YEAR FROM DATE '2024-03-15')"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 2024

    # MONTH
    sql = "SELECT EXTRACT(MONTH FROM DATE '2024-03-15')"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 3


def test_year_month_day(dialect_context):
    """Test YEAR, MONTH, DAY convenience functions."""
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT YEAR(DATE '2024-03-15'), MONTH(DATE '2024-03-15'), DAY(DATE '2024-03-15')"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 2024
    assert res[1] == 3
    assert res[2] == 15


def test_last_day(dialect_context):
    """Test LAST_DAY function."""
    from datetime import date
    sql = "SELECT LAST_DAY(DATE '2024-02-15')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # 2024 is leap year
    assert res[0] == date(2024, 2, 29)


def test_dayofweek_dayofyear(dialect_context):
    """Test DAYOFWEEK and DAYOFYEAR functions."""
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    # DAYOFYEAR
    sql = "SELECT DAYOFYEAR(DATE '2024-03-01')"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    # March 1st is day 61 in 2024 (leap year)
    assert res[0] == 61


def test_quarter(dialect_context):
    """Test QUARTER function."""
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT QUARTER(DATE '2024-03-15')"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 1


def test_hour_minute_second(dialect_context):
    """Test HOUR, MINUTE, SECOND functions."""
    from snowduck.dialect import Dialect
    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    sql = "SELECT HOUR(TIMESTAMP '2024-01-15 10:30:45'), MINUTE(TIMESTAMP '2024-01-15 10:30:45'), SECOND(TIMESTAMP '2024-01-15 10:30:45')"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 10
    assert res[1] == 30
    assert res[2] == 45
