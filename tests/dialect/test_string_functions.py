"""Tests for Snowflake string function compatibility."""

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


def test_string_concat(dialect_context):
    """Test CONCAT function with multiple arguments."""
    sql = "SELECT CONCAT('Hello', ' ', 'World')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    # Verify it executes correctly
    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "Hello World"


def test_split_part(dialect_context):
    """Test SPLIT_PART function for string tokenization."""
    sql = "SELECT SPLIT_PART('a,b,c', ',', 2)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "b"


def test_startswith(dialect_context):
    """Test STARTSWITH function."""
    sql = "SELECT STARTSWITH('snowflake', 'snow')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is True


def test_endswith(dialect_context):
    """Test ENDSWITH function."""
    sql = "SELECT ENDSWITH('snowflake', 'flake')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is True


def test_contains(dialect_context):
    """Test CONTAINS function for substring matching."""
    sql = "SELECT CONTAINS('snowflake', 'flake')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # 'snowflake' contains 'flake'
    assert res[0] is True


def test_regexp_replace(dialect_context):
    """Test REGEXP_REPLACE for pattern-based substitution."""
    sql = "SELECT REGEXP_REPLACE('abc123def', '[0-9]+', 'X')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "abcXdef"


def test_left_right_functions(dialect_context):
    """Test LEFT and RIGHT string extraction functions."""
    sql = "SELECT LEFT('snowflake', 4), RIGHT('snowflake', 5)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "snow"
    assert res[1] == "flake"


def test_reverse(dialect_context):
    """Test REVERSE function."""
    sql = "SELECT REVERSE('abc')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "cba"


def test_repeat(dialect_context):
    """Test REPEAT function."""
    sql = "SELECT REPEAT('ab', 3)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "ababab"


def test_lpad_rpad(dialect_context):
    """Test LPAD and RPAD functions."""
    sql = "SELECT LPAD('42', 5, '0'), RPAD('hi', 5, '.')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "00042"
    assert res[1] == "hi..."


def test_trim_functions(dialect_context):
    """Test TRIM, LTRIM, RTRIM functions."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)

    # TRIM
    sql = "SELECT TRIM('  hello  ')"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "hello"

    # LTRIM
    sql = "SELECT LTRIM('  hello  ')"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "hello  "

    # RTRIM
    sql = "SELECT RTRIM('  hello  ')"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "  hello"


def test_chr_ascii(dialect_context):
    """Test CHR and ASCII functions."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)

    # CHR
    sql = "SELECT CHR(65)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "A"

    # ASCII
    sql = "SELECT ASCII('A')"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 65


def test_position_instr(dialect_context):
    """Test POSITION function (and INSTR alias)."""
    sql = "SELECT POSITION('lo' IN 'hello')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 4  # 1-indexed


def test_replace(dialect_context):
    """Test REPLACE function."""
    sql = "SELECT REPLACE('hello world', 'world', 'there')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "hello there"


def test_concat_ws(dialect_context):
    """Test CONCAT_WS function."""
    sql = "SELECT CONCAT_WS(',', 'a', 'b', 'c')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "a,b,c"


def test_substr_substring(dialect_context):
    """Test SUBSTR/SUBSTRING functions."""
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)

    # SUBSTR
    sql = "SELECT SUBSTR('hello', 2, 3)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "ell"

    # SUBSTRING
    sql = "SELECT SUBSTRING('hello', 2, 3)"
    expression = parse_one(sql, read="snowflake")
    transpiled = expression.sql(dialect=dialect)
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "ell"
