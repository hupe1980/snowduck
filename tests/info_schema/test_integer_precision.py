import duckdb

from snowduck.info_schema import InfoSchemaManager


def test_integer_precision_normalized():
    conn = duckdb.connect(":memory:")
    mgr = InfoSchemaManager(conn)

    conn.execute("CREATE TABLE test_int (a INTEGER)")
    cols = mgr.get_table_columns(database="memory", schema="main", table="test_int")

    assert cols[0]["numeric_precision"] == 38
    assert cols[0]["numeric_scale"] == 0
