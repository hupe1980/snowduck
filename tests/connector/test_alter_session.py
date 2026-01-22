from snowduck.connector import Connector


def test_alter_session_is_noop():
    connector = Connector()
    conn = connector.connect()
    cur = conn.cursor()

    cur.execute("ALTER SESSION SET QUERY_TAG = 'dbt'")
    result = cur.fetchone()

    assert result[0] == "Statement executed successfully."

    cur.close()
    conn.close()
