from snowduck.connector import Connector


def test_merge_statement_delete():
    conn = Connector().connect()
    cur = conn.cursor()

    cur.execute("CREATE TABLE target (id INTEGER, v INTEGER)")
    cur.execute("INSERT INTO target VALUES (1, 10), (2, 20)")
    cur.execute("CREATE TABLE source (id INTEGER)")
    cur.execute("INSERT INTO source VALUES (1)")

    cur.execute(
        "MERGE INTO target AS t USING source AS s ON t.id = s.id "
        "WHEN MATCHED THEN DELETE"
    )

    cur.execute("SELECT * FROM target ORDER BY id")
    assert cur.fetchall() == [(2, 20)]
