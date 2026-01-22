from snowduck.connector import Connector


def test_create_or_replace_table():
    conn = Connector().connect()
    cur = conn.cursor()

    cur.execute("CREATE TABLE test_replace (a INTEGER)")
    cur.execute("INSERT INTO test_replace VALUES (1)")

    cur.execute("CREATE OR REPLACE TABLE test_replace (a INTEGER)")
    cur.execute("SELECT COUNT(*) FROM test_replace")

    assert cur.fetchone()[0] == 0
