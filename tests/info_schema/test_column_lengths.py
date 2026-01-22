import duckdb

from snowduck.info_schema import InfoSchemaManager


def test_info_schema_character_max_length_from_type():
    conn = duckdb.connect(":memory:")
    mgr = InfoSchemaManager(conn)

    conn.execute("CREATE TABLE test_len (a VARCHAR(5))")
    cols = mgr.get_table_columns(database="memory", schema="main", table="test_len")

    assert cols[0]["character_maximum_length"] is None
