import snowflake.connector


def test_query_request(server: dict) -> None:
    with snowflake.connector.connect(**server) as conn, conn.cursor() as cur:
        cur.execute("select 'hello world'")
        result = cur.fetchone()
        assert result[0] == "hello world"
