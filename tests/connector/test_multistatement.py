from snowduck.connector import Connector


def test_multiple_statements_execution():
    connector = Connector()
    conn = connector.connect()
    cur = conn.cursor()

    sql = """
    CREATE TABLE test_multi (a INT);
    INSERT INTO test_multi VALUES (1);
    INSERT INTO test_multi VALUES (2);
    SELECT count(*) FROM test_multi;
    """

    # This currently expects to handle only one statement or fail
    cur.execute(sql)

    result = cur.fetchone()
    assert result[0] == 2

    # Verify side effects
    cur.execute("SELECT count(*) FROM test_multi")
    assert cur.fetchone()[0] == 2

    cur.close()
    conn.close()
