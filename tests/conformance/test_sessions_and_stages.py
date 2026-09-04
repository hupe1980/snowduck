"""Session semantics, stage lifecycle and the DDL Snowflake has that DuckDB lacks."""

import os
import tempfile

import pytest
import snowflake.connector

from snowduck import patch_snowflake


def test_transaction_is_scoped_to_the_session(conn):
    """A transaction covers the session, not one cursor.

    Each cursor used to get its own DuckDB connection, so BEGIN on one and
    INSERT on another were separate transactions and the ROLLBACK silently did
    nothing - which is what any code doing `with conn.cursor()` per statement
    does.
    """

    def run(sql):
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    run("CREATE OR REPLACE TABLE txn_t (a INT)")
    run("INSERT INTO txn_t VALUES (1)")

    run("BEGIN")
    run("INSERT INTO txn_t VALUES (2)")
    run("ROLLBACK")
    assert run("SELECT COUNT(*) FROM txn_t")[0][0] == 1

    run("BEGIN")
    run("INSERT INTO txn_t VALUES (3)")
    run("COMMIT")
    assert run("SELECT COUNT(*) FROM txn_t")[0][0] == 2


def test_cursor_results_survive_other_cursors(conn):
    """Sharing the connection must not let one cursor clobber another's rows."""
    with conn.cursor() as first:
        first.execute("SELECT 1 AS a")
        with conn.cursor() as second:
            second.execute("SELECT 2 AS b")
            assert second.fetchall() == [(2,)]
        assert first.fetchall() == [(1,)]


def test_concurrent_cursors_on_one_connection(conn):
    """Sharing the session's connection must stay safe under threads.

    Cursors share one DuckDB connection so a transaction spans the session;
    that makes the connection shared mutable state, and without serialising
    execution concurrent cursors clobbered each other's result set.
    """
    import threading

    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE concur_t (a INT)")
        cur.execute("INSERT INTO concur_t SELECT * FROM range(200)")

    failures: list[str] = []

    def worker(marker: int) -> None:
        try:
            for _ in range(25):
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM concur_t")
                    assert cursor.fetchone()[0] == 200
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"SELECT {marker} AS w, a FROM concur_t WHERE a < 5 ORDER BY a"
                    )
                    rows = cursor.fetchall()
                    assert [row[1] for row in rows] == [0, 1, 2, 3, 4]
                    # A row carrying another thread's marker means the result
                    # sets were interleaved.
                    assert all(row[0] == marker for row in rows)
        except Exception as error:  # noqa: BLE001 - reported below
            failures.append(repr(error))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not failures, failures[:3]


def test_alter_table_swap_with(conn):
    """dbt publishes a rebuilt table with SWAP WITH; DuckDB has no such statement."""
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE swap_a (x INT)")
        cur.execute("INSERT INTO swap_a VALUES (1)")
        cur.execute("CREATE OR REPLACE TABLE swap_b (y VARCHAR)")
        cur.execute("INSERT INTO swap_b VALUES ('z')")

        cur.execute("ALTER TABLE swap_a SWAP WITH swap_b")

        cur.execute("SELECT * FROM swap_a")
        assert [d.name for d in cur.description] == ["Y"]
        assert cur.fetchall() == [("z",)]

        cur.execute("SELECT * FROM swap_b")
        assert cur.fetchall() == [(1,)]


@pytest.mark.parametrize(
    "statement",
    [
        "ALTER TABLE noop_t CLUSTER BY (a)",
        "ALTER TABLE noop_t SET COMMENT = 'hello'",
        "ALTER TABLE noop_t SET DATA_RETENTION_TIME_IN_DAYS = 1",
    ],
)
def test_snowflake_only_alter_clauses_are_accepted(conn, statement):
    """Storage hints have no local meaning but must not fail the statement."""
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE noop_t (a INT)")
        cur.execute(statement)


def test_autoincrement_column(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE auto_t (id INT AUTOINCREMENT, v VARCHAR)")
        cur.execute("INSERT INTO auto_t (v) VALUES ('a'), ('b')")
        cur.execute("SELECT id, v FROM auto_t ORDER BY id")
        assert cur.fetchall() == [(1, "a"), (2, "b")]


def test_identity_column_honours_start_and_step(conn):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE OR REPLACE TABLE ident_t (id INT IDENTITY(100, 5), v VARCHAR)"
        )
        cur.execute("INSERT INTO ident_t (v) VALUES ('a'), ('b')")
        cur.execute("SELECT id FROM ident_t ORDER BY id")
        assert [row[0] for row in cur.fetchall()] == [100, 105]


def test_session_variables_can_be_unset(conn):
    with conn.cursor() as cur:
        cur.execute("SET my_var = 5")
        cur.execute("SELECT $my_var")
        assert cur.fetchone()[0] == 5

        cur.execute("UNSET my_var")
        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            cur.execute("SELECT $my_var")


def test_last_query_id(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        previous = cur.sfqid
        cur.execute("SELECT LAST_QUERY_ID()")
        assert cur.fetchone()[0] == previous


def test_system_typeof(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT SYSTEM$TYPEOF(1)")
        assert cur.fetchone()[0]


def test_stage_lifecycle(monkeypatch):
    """CREATE / LIST / REMOVE / DROP over the local stage directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("SNOWDUCK_STAGE_DIR", tmpdir)

        with patch_snowflake(), snowflake.connector.connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE OR REPLACE STAGE st_life")

            staged = os.path.join(tmpdir, "ST_LIFE")
            assert os.path.isdir(staged)
            with open(os.path.join(staged, "f.csv"), "w") as handle:
                handle.write("a,b\n1,2\n")

            cur.execute("LIST @st_life")
            rows = cur.fetchall()
            assert [row[0] for row in rows] == ["ST_LIFE/f.csv"]
            assert rows[0][1] == 8

            cur.execute("REMOVE @st_life/f.csv")
            cur.execute("LIST @st_life")
            assert cur.fetchall() == []

            cur.execute("DROP STAGE st_life")
            assert not os.path.exists(staged)
