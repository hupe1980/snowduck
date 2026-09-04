"""FLATTEN exposes Snowflake's full column set, not just VALUE.

Snowflake's FLATTEN yields SEQ, KEY, PATH, INDEX, VALUE and THIS. DuckDB has no
equivalent table function, so the columns are synthesised - previously only
VALUE was produced and any reference to the others failed to bind.
"""

import pytest

FLATTEN_COLUMNS = ["SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"]


@pytest.fixture
def flat_table(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE flatten_t (id INT, tags ARRAY)")
        cur.execute("INSERT INTO flatten_t VALUES (1, [10, 20]), (2, [30])")
    return conn


@pytest.mark.parametrize("column", FLATTEN_COLUMNS)
def test_flatten_exposes_column(flat_table, column):
    with flat_table.cursor() as cur:
        cur.execute(
            f"SELECT f.{column} FROM flatten_t, LATERAL FLATTEN(input => flatten_t.tags) f"
        )
        cur.fetchall()


def test_flatten_index_is_zero_based(flat_table):
    with flat_table.cursor() as cur:
        cur.execute("""
            SELECT flatten_t.id, f.INDEX, f.VALUE
            FROM flatten_t, LATERAL FLATTEN(input => flatten_t.tags) f
            ORDER BY flatten_t.id, f.INDEX
        """)
        rows = [(i, idx, str(v)) for i, idx, v in cur.fetchall()]

    assert rows == [(1, 0, "10"), (1, 1, "20"), (2, 0, "30")]


def test_flatten_over_a_literal_array(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT value FROM LATERAL FLATTEN(input => ARRAY_CONSTRUCT(1, 2, 3)) "
            "ORDER BY value"
        )
        assert [row[0] for row in cur.fetchall()] == [1, 2, 3]


def test_flatten_via_table_function(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM TABLE(FLATTEN(input => PARSE_JSON('[5, 6]')))")
        assert [str(row[0]) for row in cur.fetchall()] == ["5", "6"]


def test_flatten_path_and_this(flat_table):
    with flat_table.cursor() as cur:
        cur.execute("""
            SELECT f.PATH, f.THIS
            FROM flatten_t, LATERAL FLATTEN(input => flatten_t.tags) f
            WHERE flatten_t.id = 2
        """)
        path, this = cur.fetchone()

    assert path == "[0]"
    assert [str(v) for v in this] == ["30"]
