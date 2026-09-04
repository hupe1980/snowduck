"""DDL and DML shapes Snowflake accepts that DuckDB spells differently."""


def test_merge_with_qualified_set(conn):
    """Snowflake writes `SET tgt.col = ...`; DuckDB rejects a qualified target."""
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE m_src (id INT, name VARCHAR)")
        cur.execute("INSERT INTO m_src VALUES (1, 'new'), (2, 'fresh')")
        cur.execute("CREATE OR REPLACE TABLE m_tgt (id INT, name VARCHAR)")
        cur.execute("INSERT INTO m_tgt VALUES (1, 'old')")

        cur.execute("""
            MERGE INTO m_tgt USING m_src ON m_tgt.id = m_src.id
            WHEN MATCHED THEN UPDATE SET m_tgt.name = m_src.name
            WHEN NOT MATCHED THEN INSERT (id, name) VALUES (m_src.id, m_src.name)
        """)

        cur.execute("SELECT id, name FROM m_tgt ORDER BY id")
        assert cur.fetchall() == [(1, "new"), (2, "fresh")]


def test_create_table_clone(conn):
    """CLONE is emulated as an eager copy."""
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE c_src (id INT)")
        cur.execute("INSERT INTO c_src VALUES (1), (2)")
        cur.execute("CREATE OR REPLACE TABLE c_copy CLONE c_src")

        cur.execute("SELECT COUNT(*) FROM c_copy")
        assert cur.fetchone()[0] == 2

        # A clone is independent of its source.
        cur.execute("INSERT INTO c_copy VALUES (3)")
        cur.execute("SELECT COUNT(*) FROM c_src")
        assert cur.fetchone()[0] == 2


def test_show_columns(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE sc_t (id INT, label VARCHAR)")
        cur.execute("SHOW COLUMNS IN TABLE sc_t")
        columns = [row[2] for row in cur.fetchall()]

    # Snowflake folds unquoted identifiers to upper case.
    assert columns == ["ID", "LABEL"]


def test_bare_array_column_is_json(conn):
    """Snowflake ARRAY is untyped; it must not become a typed DuckDB list."""
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE arr_t (tags ARRAY, o OBJECT, v VARIANT)")
        # DuckDB stores all three as JSON ...
        cur.execute(
            "SELECT data_type FROM system.information_schema.columns "
            "WHERE table_name = 'ARR_T' ORDER BY ordinal_position"
        )
        assert [row[0] for row in cur.fetchall()] == ["JSON", "JSON", "JSON"]
        # ... and INFORMATION_SCHEMA reports the Snowflake type for them.
        cur.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'ARR_T' ORDER BY ordinal_position"
        )
        assert [row[0] for row in cur.fetchall()] == ["VARIANT"] * 3


def test_explicit_typed_array_is_preserved(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE arr_typed (n INT[])")
        cur.execute(
            "SELECT data_type FROM system.information_schema.columns "
            "WHERE table_name = 'ARR_TYPED'"
        )
        assert cur.fetchone()[0] == "INTEGER[]"
        cur.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'ARR_TYPED'"
        )
        assert cur.fetchone()[0] == "ARRAY"
