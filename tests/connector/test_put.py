from snowduck.connector import Connector


def test_put_is_noop_success():
    conn = Connector().connect()
    cur = conn.cursor()

    cur.execute("PUT file://tmp.csv @mystage")
    result = cur.fetchone()

    assert result[0] == "Statement executed successfully."
