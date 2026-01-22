import pandas as pd
import snowflake.connector
import snowflake.connector.pandas_tools as pandas_tools

from snowduck import patch_snowflake


def test_write_pandas_inserts_rows():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    with patch_snowflake():
        conn = snowflake.connector.connect(database="db", schema="schema")
        cur = conn.cursor()
        cur.execute("CREATE TABLE db.schema.test_pandas (a INTEGER, b VARCHAR)")

        success, nchunks, nrows, output = pandas_tools.write_pandas(
            conn,
            df,
            "test_pandas",
            database="db",
            schema="schema",
        )

        assert success is True
        assert nchunks == 1
        assert nrows == 2
        assert output == []

        cur.execute("SELECT count(*) FROM db.schema.test_pandas")
        assert cur.fetchone()[0] == 2
