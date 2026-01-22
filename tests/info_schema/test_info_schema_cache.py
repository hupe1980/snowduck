import duckdb

from snowduck.info_schema import InfoSchemaManager


def test_info_schema_cache_reuse():
    conn = duckdb.connect(':memory:')
    mgr = InfoSchemaManager(conn)

    conn.execute("CREATE TABLE test_cache (a INTEGER)")
    first = mgr.get_table_columns(database="memory", schema="main", table="test_cache")
    second = mgr.get_table_columns(database="memory", schema="main", table="test_cache")

    assert first == second
