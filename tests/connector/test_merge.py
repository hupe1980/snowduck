from snowduck.connector import Connector


def test_merge_statement_updates_and_inserts():
    conn = Connector().connect()
    cur = conn.cursor()

    cur.execute("CREATE TABLE target (id INTEGER, v INTEGER)")
    cur.execute("INSERT INTO target VALUES (1, 10)")
    cur.execute("CREATE TABLE source (id INTEGER, v INTEGER)")
    cur.execute("INSERT INTO source VALUES (1, 20), (2, 30)")

    cur.execute(
        "MERGE INTO target AS t USING source AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = s.v "
        "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
    )

    cur.execute("SELECT * FROM target ORDER BY id")
    assert cur.fetchall() == [(1, 20), (2, 30)]
