from snowduck.connector import Connector
from snowduck.connector.rowtype import describe_as_rowtype


def test_rowtype_supports_fixed():
    rowtype = describe_as_rowtype(
        [("range", "FIXED", "YES", None, None, None)],
        database="DB",
        schema="SCHEMA",
    )

    assert rowtype[0]["type"] == "fixed"
    assert rowtype[0]["precision"] == 38
    assert rowtype[0]["scale"] == 0
    assert rowtype[0]["database"] == "DB"
    assert rowtype[0]["schema"] == "SCHEMA"


def test_rowtype_from_integer_query():
    conn = Connector().connect()
    cur = conn.cursor()
    cur.execute("SELECT 1")

    rowtype = describe_as_rowtype(cur.describe_last_sql())
    assert rowtype[0]["type"] == "fixed"


def test_rowtype_nullable_and_varchar_length():
    rowtype = describe_as_rowtype(
        [("name", "VARCHAR(12)", "NO", None, None, None)],
        database="DB",
        schema="SCHEMA",
    )

    assert rowtype[0]["nullable"] is False
    assert rowtype[0]["length"] == 12
    assert rowtype[0]["byteLength"] == 12


def test_rowtype_table_name_from_cursor():
    conn = Connector().connect()
    cur = conn.cursor()
    cur.execute("CREATE TABLE test_meta (a INTEGER)")
    cur.execute("SELECT a FROM test_meta")

    rowtype = describe_as_rowtype(
        cur.describe_last_sql(),
        table=cur.last_table_name,
    )

    assert rowtype[0]["table"] == "test_meta"


def test_rowtype_overrides_from_info_schema():
    conn = Connector().connect(database="db", schema="schema")
    cur = conn.cursor()
    cur.execute("CREATE TABLE test_meta (a INTEGER NOT NULL, b VARCHAR(5))")
    cur.execute("SELECT a, b FROM test_meta")

    describe_results = cur.describe_last_sql()
    overrides = {c["name"]: c for c in conn.get_column_metadata("test_meta")}
    rowtype = describe_as_rowtype(
        describe_results,
        database=conn.database,
        schema=conn.schema,
        table=cur.last_table_name,
        overrides=overrides,
    )

    assert rowtype[0]["nullable"] is False
    assert rowtype[0]["precision"] == 38  # Integer precision normalized to 38

