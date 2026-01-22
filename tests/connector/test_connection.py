import pytest
import snowflake.connector

from snowduck import mock_snowflake


@mock_snowflake
def test_connect():
    with snowflake.connector.connect() as conn:
        res = conn.cursor().execute("SELECT 'Hello snowmock!'").fetchone()
        assert res[0] == "Hello snowmock!"


def test_close_conn(conn: snowflake.connector.SnowflakeConnection):
    assert not conn.is_closed()

    conn.close()

    with pytest.raises(snowflake.connector.errors.DatabaseError) as excinfo:
        conn.execute_string("select 1")

    # actual snowflake error message is:
    # 250002 (08003): Connection is closed
    assert "250002 (08003)" in str(excinfo.value)

    assert conn.is_closed()


@mock_snowflake
def test_connect_information_schema():
    with snowflake.connector.connect(
        database="db1", schema="information_schema"
    ) as conn:
        assert conn.database == "DB1"
        assert conn.schema == "INFORMATION_SCHEMA"
        with conn, conn.cursor() as cur:
            # shouldn't fail
            cur.execute("SELECT * FROM databases")
