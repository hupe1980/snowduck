"""Tests for Snowflake regex function compatibility."""

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


def test_regexp_like(dialect_context):
    """Test REGEXP_LIKE function for pattern matching."""
    sql = "SELECT REGEXP_LIKE('hello123', '[a-z]+[0-9]+')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is True


def test_regexp_like_no_match(dialect_context):
    """Test REGEXP_LIKE when pattern doesn't match."""
    sql = "SELECT REGEXP_LIKE('hello', '[0-9]+')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is False


def test_regexp_substr(dialect_context):
    """Test REGEXP_SUBSTR function for extracting substrings."""
    sql = "SELECT REGEXP_SUBSTR('abc123def456', '[0-9]+')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Should extract first numeric sequence
    assert res[0] == "123"


def test_regexp_replace(dialect_context):
    """Test REGEXP_REPLACE function for pattern substitution."""
    sql = "SELECT REGEXP_REPLACE('hello123world', '[0-9]+', 'X')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "helloXworld"


def test_regexp_replace_all(dialect_context):
    """Test REGEXP_REPLACE with 'g' flag replaces all occurrences."""
    sql = "SELECT REGEXP_REPLACE('a1b2c3', '[0-9]', 'X', 'g')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "aXbXcX"


def test_regexp_count(dialect_context):
    """Test REGEXP_COUNT function for counting matches."""
    sql = "SELECT REGEXP_COUNT('abc123def456', '[0-9]+')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Two numeric sequences: 123 and 456
    assert res[0] == 2


def test_regexp_count_no_match(dialect_context):
    """Test REGEXP_COUNT with no matches."""
    sql = "SELECT REGEXP_COUNT('no numbers here', '[0-9]+')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 0


def test_rlike_alias(dialect_context):
    """RLIKE is REGEXP_LIKE, which anchors the pattern at both ends.

    Snowflake: "The function implicitly anchors a pattern at both ends
    (for example, \'ABC\' automatically becomes \'^ABC$\')." So a bare
    substring pattern does NOT match.
    """
    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    conn = duckdb.connect(":memory:")

    def run(sql):
        return conn.execute(
            parse_one(sql, read="snowflake").sql(dialect=dialect)
        ).fetchone()[0]

    assert run("SELECT 'hello' RLIKE 'ell'") is False
    assert run("SELECT 'hello' RLIKE '.*ell.*'") is True
    assert run("SELECT 'hello' RLIKE 'hello'") is True


def test_regexp_substr_extract_group(conn):
    """The 'e' parameter selects a capture group; it is not a DuckDB regex flag."""
    with conn.cursor() as cur:
        cur.execute(
            r"""SELECT REGEXP_SUBSTR('1-1:1.8.0',
                '^(\\d+)-(\\d+):(\\d+)\\.(\\d+)\\.(\\d+)$', 1, 1, 'e', 4)"""
        )
        assert cur.fetchone()[0] == "8"


def test_regexp_substr_without_e_returns_whole_match(conn):
    """Without 'e', Snowflake returns the whole match regardless of group_num."""
    with conn.cursor() as cur:
        cur.execute(r"SELECT REGEXP_SUBSTR('ab12', '([a-z]+)([0-9]+)')")
        assert cur.fetchone()[0] == "ab12"


def test_regexp_substr_occurrence(conn):
    """The occurrence argument selects the nth match, not the first."""
    with conn.cursor() as cur:
        cur.execute("SELECT REGEXP_SUBSTR('a1b22c33', '[0-9]+', 1, 2)")
        assert cur.fetchone()[0] == "22"


def test_regexp_substr_position(conn):
    """The position argument starts the search at a 1-based offset."""
    with conn.cursor() as cur:
        cur.execute("SELECT REGEXP_SUBSTR('a1b22c33', '[0-9]+', 4, 1)")
        assert cur.fetchone()[0] == "22"


def test_regexp_substr_missing_occurrence_is_null(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT REGEXP_SUBSTR('a1b22c33', '[0-9]+', 1, 9)")
        assert cur.fetchone()[0] is None


def test_regexp_substr_case_insensitive_flag(conn):
    """Real regex flags still reach DuckDB after 'e' is stripped."""
    with conn.cursor() as cur:
        cur.execute("SELECT REGEXP_SUBSTR('ABC', 'b', 1, 1, 'i')")
        assert cur.fetchone()[0] == "B"


def test_regexp_like_anchors(conn):
    """REGEXP_LIKE anchors implicitly; REGEXP_SUBSTR/COUNT/INSTR do not."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT REGEXP_LIKE('xabcx', 'a.c'),
                   REGEXP_LIKE('abc', 'a.c'),
                   REGEXP_LIKE('ABC', 'a.c', 'i'),
                   REGEXP_COUNT('xabcx', 'a.c')
        """)
        assert cur.fetchone() == (False, True, True, 1)


def test_regexp_substr_no_match_is_null(conn):
    """Snowflake returns NULL when nothing matches, not an empty string."""
    with conn.cursor() as cur:
        cur.execute("SELECT REGEXP_SUBSTR('abc', '[0-9]+')")
        assert cur.fetchone()[0] is None


def test_regexp_replace_replaces_all_by_default(conn):
    """Snowflake's occurrence argument defaults to 0, meaning 'all'."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT REGEXP_REPLACE('a1b2', '[0-9]', 'X'),
                   REGEXP_REPLACE('a1b2', '[0-9]', 'X', 1, 1),
                   REGEXP_REPLACE('a1b2', '[0-9]', 'X', 3)
        """)
        assert cur.fetchone() == ("aXbX", "aXb2", "a1bX")


def test_regexp_instr(conn):
    """REGEXP_INSTR returns a 1-based offset, or 0 when there is no match."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT REGEXP_INSTR('abc123', '[0-9]+'),
                   REGEXP_INSTR('abc', '[0-9]+'),
                   REGEXP_INSTR('a1b2', '[0-9]', 1, 2),
                   REGEXP_INSTR('abc123', '[0-9]+', 1, 1, 1),
                   REGEXP_INSTR('xxa1', '[0-9]', 3)
        """)
        assert cur.fetchone() == (4, 0, 4, 7, 4)


def test_regexp_substr_all(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT REGEXP_SUBSTR_ALL('a1b22c3', '[0-9]+')")
        assert list(cur.fetchone()[0]) == ["1", "22", "3"]
