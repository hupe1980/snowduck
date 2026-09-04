"""Compatibility tests for dbt-snowflake adapter.

These tests verify that snowduck works correctly with dbt-snowflake, enabling
dbt projects to use DuckDB for local development and testing.

Test Categories:
    TestDBTConnectorCompatibility: Tests cursor/connection properties dbt relies on
    TestDBTQueryExecution: Tests SQL execution patterns used by dbt
    TestDBTServerIntegration: Integration tests with snowduck server

Requirements:
    pip install dbt-snowflake

Reference:
    https://github.com/dbt-labs/dbt-adapters/tree/main/dbt-snowflake
    https://docs.getdbt.com/docs/profile-snowflake
"""

from __future__ import annotations

import socket
import threading
import time
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

# ============================================================================
# Check for dependencies
# ============================================================================

# Check for dbt-snowflake
try:
    from dbt.adapters.snowflake.connections import (
        SnowflakeConnectionManager,
        SnowflakeCredentials,
    )

    HAS_DBT = True
except ImportError:
    HAS_DBT = False
    SnowflakeConnectionManager = None
    SnowflakeCredentials = None
    SnowflakeConnectionManager = None
    SnowflakeCredentials = None

# Check for server deps
try:
    from starlette.testclient import TestClient

    from snowduck.server import app

    HAS_SERVER = True
except ImportError:
    HAS_SERVER = False
    TestClient = None
    app = None

# Check for snowduck connector
try:
    from snowduck.connector import Connector

    HAS_CONNECTOR = True
except ImportError:
    HAS_CONNECTOR = False
    Connector = None


# ============================================================================
# Test fixtures
# ============================================================================


@pytest.fixture
def connector() -> "Iterator[Connector]":
    """Create a fresh in-memory snowduck connector."""
    conn = Connector(db_file=":memory:")
    yield conn


@pytest.fixture
def connection(connector: "Connector"):
    """Create a connection with database and schema."""
    conn = connector.connect("TEST_DB", "TEST_SCHEMA")
    yield conn
    conn.close()


def find_free_port() -> int:
    """Find a free port for the test server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_server():
    """Start a live snowduck server for dbt integration tests."""
    if not HAS_SERVER:
        pytest.skip("Server dependencies not installed")

    import uvicorn

    port = find_free_port()
    server_started = threading.Event()

    class TestServer(uvicorn.Server):
        def install_signal_handlers(self):
            pass

        async def startup(self, sockets=None):
            await super().startup(sockets)
            server_started.set()

    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = TestServer(config)
    thread = threading.Thread(target=server.run)
    thread.daemon = True
    thread.start()

    # Wait for server to start
    server_started.wait(timeout=10)
    time.sleep(0.5)  # Extra time for routes to register

    yield {"host": "127.0.0.1", "port": port}

    server.should_exit = True
    thread.join(timeout=5)


# ============================================================================
# Snowduck Connector Compatibility Tests
# ============================================================================


@pytest.mark.skipif(not HAS_CONNECTOR, reason="snowduck connector not installed")
class TestDBTConnectorCompatibility:
    """Tests for cursor/connection properties that dbt-snowflake relies on."""

    def test_cursor_has_sfqid(self, connection):
        """dbt uses cursor.sfqid for query tracking."""
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        assert hasattr(cursor, "sfqid")
        assert cursor.sfqid is not None
        # sfqid should be a string (UUID-like)
        assert isinstance(cursor.sfqid, str)
        assert len(cursor.sfqid) > 0

    def test_cursor_has_sqlstate(self, connection):
        """dbt uses cursor.sqlstate for response codes."""
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        assert hasattr(cursor, "sqlstate")
        # sqlstate is typically None or a 5-char code
        # For successful queries, it can be None or "00000"

    def test_cursor_has_rowcount(self, connection):
        """dbt uses cursor.rowcount for affected rows."""
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE test_table (id INT, name VARCHAR)")
        cursor.execute("INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        assert hasattr(cursor, "rowcount")
        # rowcount should be 3 for 3 inserted rows
        assert cursor.rowcount == 3

    def test_cursor_has_description(self, connection):
        """dbt uses cursor.description for column metadata."""
        cursor = connection.cursor()
        cursor.execute("SELECT 1 AS col_a, 'test' AS col_b")
        assert hasattr(cursor, "description")
        assert cursor.description is not None
        assert len(cursor.description) == 2
        # Each description item should have at least name and type_code
        assert cursor.description[0][0].upper() == "COL_A"
        assert cursor.description[1][0].upper() == "COL_B"

    def test_cursor_fetchone(self, connection):
        """dbt uses cursor.fetchone() for single row queries."""
        cursor = connection.cursor()
        cursor.execute("SELECT 42 AS answer")
        result = cursor.fetchone()
        assert result is not None
        assert result[0] == 42

    def test_cursor_fetchall(self, connection):
        """dbt uses cursor.fetchall() for multi-row queries."""
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM (VALUES (1), (2), (3))")
        results = cursor.fetchall()
        assert len(results) == 3

    def test_cursor_fetchmany(self, connection):
        """dbt uses cursor.fetchmany() for batched fetching."""
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM (VALUES (1), (2), (3), (4), (5))")
        results = cursor.fetchmany(2)
        assert len(results) == 2

    def test_connection_autocommit(self, connection):
        """dbt expects autocommit to be enabled by default for Snowflake."""
        # Snowflake operates in autocommit mode
        # Our connection should support this pattern
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE autocommit_test (x INT)")
        cursor.execute("INSERT INTO autocommit_test VALUES (1)")
        # Query should see the committed data
        cursor.execute("SELECT * FROM autocommit_test")
        assert cursor.fetchone()[0] == 1


@pytest.mark.skipif(not HAS_CONNECTOR, reason="snowduck connector not installed")
class TestDBTQueryExecution:
    """Tests for SQL execution patterns used by dbt."""

    def test_create_schema(self, connection):
        """dbt creates schemas for models."""
        cursor = connection.cursor()
        cursor.execute("CREATE SCHEMA IF NOT EXISTS dbt_test_schema")
        cursor.execute("SHOW SCHEMAS")
        schemas = [row[1] for row in cursor.fetchall()]
        assert "DBT_TEST_SCHEMA" in schemas

    def test_create_table_as_select(self, connection):
        """dbt uses CTAS for materializing models."""
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE dbt_model AS
            SELECT 1 AS id, 'test' AS name
        """)
        cursor.execute("SELECT * FROM dbt_model")
        result = cursor.fetchone()
        assert result[0] == 1
        assert result[1] == "test"

    def test_create_view(self, connection):
        """dbt uses views for ephemeral models."""
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE source_table AS SELECT 1 AS id")
        cursor.execute("""
            CREATE VIEW dbt_view AS
            SELECT id * 2 AS doubled_id
            FROM source_table
        """)
        cursor.execute("SELECT * FROM dbt_view")
        assert cursor.fetchone()[0] == 2

    def test_create_or_replace_view(self, connection):
        """dbt uses CREATE OR REPLACE for idempotent view creation."""
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE base AS SELECT 1 AS x")
        cursor.execute("CREATE OR REPLACE VIEW test_view AS SELECT x FROM base")
        cursor.execute(
            "CREATE OR REPLACE VIEW test_view AS SELECT x * 2 AS x FROM base"
        )
        cursor.execute("SELECT x FROM test_view")
        assert cursor.fetchone()[0] == 2

    def test_drop_if_exists(self, connection):
        """dbt uses DROP IF EXISTS for cleanup."""
        cursor = connection.cursor()
        # Should not raise error even if table doesn't exist
        cursor.execute("DROP TABLE IF EXISTS nonexistent_table")
        cursor.execute("CREATE TABLE will_drop (x INT)")
        cursor.execute("DROP TABLE IF EXISTS will_drop")
        # Verify table is gone
        cursor.execute("SHOW TABLES")
        tables = [row[1] for row in cursor.fetchall()]
        assert "WILL_DROP" not in tables

    def test_insert_overwrite(self, connection):
        """dbt incremental models may use INSERT OVERWRITE pattern."""
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE incremental_test (id INT, val VARCHAR)")
        cursor.execute("INSERT INTO incremental_test VALUES (1, 'a'), (2, 'b')")
        # Snowflake pattern: TRUNCATE + INSERT or MERGE
        cursor.execute("TRUNCATE TABLE incremental_test")
        cursor.execute("INSERT INTO incremental_test VALUES (3, 'c')")
        cursor.execute("SELECT * FROM incremental_test")
        results = cursor.fetchall()
        assert len(results) == 1
        assert results[0][0] == 3

    def test_merge_statement(self, connection):
        """dbt incremental models use MERGE for upserts."""
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE target_table (id INT, val VARCHAR)")
        cursor.execute("INSERT INTO target_table VALUES (1, 'old')")

        # MERGE pattern used by dbt
        cursor.execute("""
            MERGE INTO target_table AS target
            USING (SELECT 1 AS id, 'updated' AS val UNION ALL SELECT 2, 'new') AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET val = source.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (source.id, source.val)
        """)

        cursor.execute("SELECT * FROM target_table ORDER BY id")
        results = cursor.fetchall()
        assert len(results) == 2
        assert results[0] == (1, "updated")
        assert results[1] == (2, "new")

    def test_information_schema_columns(self, connection):
        """dbt queries INFORMATION_SCHEMA for column metadata."""
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE meta_test (id INT, name VARCHAR, active BOOLEAN)")
        # Unquoted identifiers are folded to upper case, as in Snowflake.
        cursor.execute("""
            SELECT column_name, data_type
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = 'META_TEST'
            ORDER BY ordinal_position
        """)
        results = cursor.fetchall()
        assert len(results) == 3
        assert [row[0] for row in results] == ["ID", "NAME", "ACTIVE"]

    def test_information_schema_tables(self, connection):
        """dbt queries INFORMATION_SCHEMA for table existence."""
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE table_check_test (x INT)")
        cursor.execute("""
            SELECT table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_name = 'TABLE_CHECK_TEST'
        """)
        results = cursor.fetchall()
        assert len(results) == 1
        assert results[0][0] == "TABLE_CHECK_TEST"

    def test_show_tables(self, connection):
        """dbt may use SHOW TABLES for discovery."""
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE show_test_a (x INT)")
        cursor.execute("CREATE TABLE show_test_b (y INT)")
        cursor.execute("SHOW TABLES")
        name_index = [c.name for c in cursor.description].index("name")
        table_names = [row[name_index] for row in cursor.fetchall()]
        assert "SHOW_TEST_A" in table_names
        assert "SHOW_TEST_B" in table_names

    def test_describe_table(self, connection):
        """dbt may use DESCRIBE TABLE for column info."""
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE describe_test (id INT NOT NULL, name VARCHAR)")
        cursor.execute("DESCRIBE TABLE describe_test")
        results = cursor.fetchall()
        assert len(results) == 2
        col_names = [row[0] for row in results]
        assert "ID" in col_names
        assert "NAME" in col_names

    def test_multi_statement_execution(self, connection):
        """dbt may execute multiple statements in sequence."""
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE multi_1 (x INT)")
        cursor.execute("CREATE TABLE multi_2 (y INT)")
        cursor.execute("INSERT INTO multi_1 VALUES (1)")
        cursor.execute("INSERT INTO multi_2 VALUES (2)")

        cursor.execute("SELECT COUNT(*) FROM multi_1")
        assert cursor.fetchone()[0] == 1
        cursor.execute("SELECT COUNT(*) FROM multi_2")
        assert cursor.fetchone()[0] == 1

    def test_qualified_table_names(self, connection):
        """dbt uses fully qualified table names. Uses default DATABASE.SCHEMA from connection."""
        cursor = connection.cursor()
        # The connection is created with TEST_DB and TEST_SCHEMA
        cursor.execute("CREATE TABLE local_table (x INT)")
        # Query using qualified name
        cursor.execute("SELECT * FROM TEST_DB.TEST_SCHEMA.local_table")
        # Should work without errors

    def test_cte_queries(self, connection):
        """dbt compiles models as CTEs."""
        cursor = connection.cursor()
        cursor.execute("""
            WITH base AS (
                SELECT 1 AS id, 'a' AS val
                UNION ALL
                SELECT 2, 'b'
            ),
            transformed AS (
                SELECT id, UPPER(val) AS val
                FROM base
            )
            SELECT * FROM transformed ORDER BY id
        """)
        results = cursor.fetchall()
        assert len(results) == 2
        assert results[0] == (1, "A")
        assert results[1] == (2, "B")

    def test_window_functions(self, connection):
        """dbt models often use window functions."""
        cursor = connection.cursor()
        cursor.execute("""
            SELECT 
                id,
                ROW_NUMBER() OVER (ORDER BY id) as rn,
                SUM(id) OVER () as total
            FROM (VALUES (1), (2), (3)) AS t(id)
        """)
        results = cursor.fetchall()
        assert len(results) == 3
        # Check row numbers
        assert [r[1] for r in results] == [1, 2, 3]
        # Check total (1+2+3=6)
        assert all(r[2] == 6 for r in results)


@pytest.mark.skipif(not HAS_DBT, reason="dbt-snowflake not installed")
class TestDBTServerIntegration:
    """Integration tests using dbt-snowflake with snowduck server."""

    @pytest.mark.skipif(not HAS_CONNECTOR, reason="snowduck connector not installed")
    def test_dbt_credentials_creation(self):
        """Test that we can create dbt credentials for snowduck."""
        # Create credentials pointing to local server
        creds = SnowflakeCredentials(
            account="snowduck",
            user="test_user",
            password="test_pass",
            database="TEST_DB",
            schema="TEST_SCHEMA",
        )
        assert creds.account == "snowduck"
        assert creds.database == "TEST_DB"
        assert creds.schema == "TEST_SCHEMA"
        assert creds.type == "snowflake"

    @pytest.mark.skipif(not HAS_CONNECTOR, reason="snowduck connector not installed")
    def test_dbt_response_format(self, connection):
        """Test that cursor responses match what dbt expects."""
        cursor = connection.cursor()
        cursor.execute("SELECT 42 AS answer")

        # dbt's get_response uses these properties
        assert hasattr(cursor, "sqlstate")
        assert hasattr(cursor, "sfqid")
        assert hasattr(cursor, "rowcount")

        # The response code should be usable
        code = cursor.sqlstate
        if code is None:
            code = "SUCCESS"
        assert isinstance(code, str)

    @pytest.mark.skipif(not HAS_CONNECTOR, reason="snowduck connector not installed")
    def test_dbt_split_queries_pattern(self, connection):
        """Test semicolon-separated queries (dbt splits on ;)."""
        cursor = connection.cursor()

        # dbt splits these and executes separately
        queries = [
            "CREATE TABLE split_test (x INT);",
            "INSERT INTO split_test VALUES (1);",
            "SELECT * FROM split_test;",
        ]

        for query in queries:
            # Remove trailing semicolon as dbt does
            clean_query = query.rstrip(";")
            cursor.execute(clean_query)

        results = cursor.fetchall()
        assert results[0][0] == 1

    @pytest.mark.skipif(not HAS_CONNECTOR, reason="snowduck connector not installed")
    def test_dbt_empty_query_handling(self, connection):
        """dbt should handle empty queries gracefully."""
        _cursor = connection.cursor()  # noqa: F841
        # Some dbt operations may result in empty statements
        # that get filtered out before execution


@pytest.mark.skipif(not HAS_CONNECTOR, reason="snowduck connector not installed")
class TestDBTDataTypes:
    """Test data type handling that dbt relies on."""

    def test_boolean_type(self, connection):
        """Test BOOLEAN type handling."""
        cursor = connection.cursor()
        cursor.execute("SELECT TRUE AS t, FALSE AS f")
        result = cursor.fetchone()
        assert result[0] is True
        assert result[1] is False

    def test_integer_types(self, connection):
        """Test integer type handling."""
        cursor = connection.cursor()
        cursor.execute("SELECT 42::INT, -100::BIGINT, 0::SMALLINT")
        result = cursor.fetchone()
        assert result[0] == 42
        assert result[1] == -100
        assert result[2] == 0

    def test_float_types(self, connection):
        """Test float type handling."""
        cursor = connection.cursor()
        cursor.execute("SELECT 3.14::FLOAT, 2.718::DOUBLE")
        result = cursor.fetchone()
        assert abs(result[0] - 3.14) < 0.001
        assert abs(result[1] - 2.718) < 0.001

    def test_string_types(self, connection):
        """Test string type handling."""
        cursor = connection.cursor()
        cursor.execute("SELECT 'hello'::VARCHAR, 'world'::TEXT")
        result = cursor.fetchone()
        assert result[0] == "hello"
        assert result[1] == "world"

    def test_date_types(self, connection):
        """Test date type handling."""
        cursor = connection.cursor()
        cursor.execute("SELECT DATE '2024-01-15'")
        result = cursor.fetchone()
        # Result should be a date or date-like object
        assert result[0] is not None

    def test_timestamp_types(self, connection):
        """Test timestamp type handling."""
        cursor = connection.cursor()
        cursor.execute("SELECT TIMESTAMP '2024-01-15 10:30:00'")
        result = cursor.fetchone()
        assert result[0] is not None

    def test_null_handling(self, connection):
        """Test NULL value handling."""
        cursor = connection.cursor()
        cursor.execute("SELECT NULL::INT, NULL::VARCHAR, NULL::BOOLEAN")
        result = cursor.fetchone()
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None

    def test_array_type(self, connection):
        """Test ARRAY type handling (Snowflake feature)."""
        cursor = connection.cursor()
        cursor.execute("SELECT ARRAY_CONSTRUCT(1, 2, 3) AS arr")
        result = cursor.fetchone()
        # Should return array-like structure
        assert result[0] is not None

    def test_json_type(self, connection):
        """Test JSON/VARIANT type handling."""
        cursor = connection.cursor()
        cursor.execute("""SELECT PARSE_JSON('{"key": "value"}') AS obj""")
        result = cursor.fetchone()
        assert result[0] is not None


@pytest.mark.skipif(not HAS_CONNECTOR, reason="snowduck connector not installed")
class TestDBTErrorHandling:
    """Test error handling that dbt depends on."""

    def test_programming_error_on_syntax(self, connection):
        """dbt catches ProgrammingError for SQL syntax errors."""
        import snowflake.connector.errors

        cursor = connection.cursor()
        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            cursor.execute("INVALID SQL SYNTAX HERE")

    def test_programming_error_on_missing_table(self, connection):
        """dbt catches ProgrammingError for missing objects."""
        import snowflake.connector.errors

        cursor = connection.cursor()
        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            cursor.execute("SELECT * FROM nonexistent_table_12345")

    def test_error_has_message(self, connection):
        """dbt uses error messages for debugging."""
        import snowflake.connector.errors

        cursor = connection.cursor()
        try:
            cursor.execute("SELECT * FROM missing_table")
        except snowflake.connector.errors.ProgrammingError as e:
            # Error should have a message
            assert str(e) is not None
            assert len(str(e)) > 0

    def test_error_has_sfqid(self, connection):
        """dbt logs sfqid from errors for debugging."""
        import snowflake.connector.errors

        cursor = connection.cursor()
        try:
            cursor.execute("SELECT * FROM missing")
        except snowflake.connector.errors.ProgrammingError as e:
            # Error should have sfqid attribute
            assert hasattr(e, "sfqid")


# ============================================================================
# Relation listing - the exact queries and columns dbt-snowflake indexes into
# ============================================================================

try:
    import agate

    HAS_AGATE = True
except ImportError:  # pragma: no cover - agate ships with dbt
    HAS_AGATE = False


@pytest.mark.skipif(not HAS_CONNECTOR, reason="snowduck connector not available")
class TestDBTListRelations:
    """`SnowflakeAdapter.list_relations_without_caching` and its two macros.

    dbt runs `show objects in <schema>` and `show user functions in <schema>`,
    lower-cases the column names and then `agate.Table.select()`s a fixed list
    out of each. A missing column raises `ValueError: tuple.index(x): x not in
    tuple` and takes down the whole run, so the column names are asserted
    directly rather than inferred from a successful query.
    """

    # dbt/adapters/snowflake/impl.py
    TABULAR_COLUMNS = [
        "database_name",
        "schema_name",
        "name",
        "kind",
        "is_dynamic",
        "is_iceberg",
    ]
    FUNCTION_COLUMNS = ["catalog_name", "schema_name", "name", "is_builtin"]

    @staticmethod
    def _as_agate(cursor):
        return agate.Table(
            [list(row) for row in cursor.fetchall()],
            column_names=[c.name.lower() for c in cursor.description],
            column_types=[agate.Text()] * len(cursor.description),
        )

    @pytest.fixture
    def populated(self, connection):
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE dbt_model (id INT)")
        cursor.execute("CREATE VIEW dbt_view AS SELECT * FROM dbt_model")
        cursor.execute("CREATE FUNCTION dbt_udf(x INT) RETURNS INT AS $$ x $$")
        return cursor

    @pytest.mark.skipif(not HAS_AGATE, reason="agate not installed")
    def test_show_objects_select_matches_dbt(self, populated):
        """snowflake__show_objects_sql, verbatim."""
        populated.execute("show objects in TEST_DB.TEST_SCHEMA\n    limit 10000\n    ;")
        selected = self._as_agate(populated).select(self.TABULAR_COLUMNS)
        kinds = {row["name"]: row["kind"] for row in selected}
        assert kinds["DBT_MODEL"] == "TABLE"
        assert kinds["DBT_VIEW"] == "VIEW"

    @pytest.mark.skipif(not HAS_AGATE, reason="agate not installed")
    def test_show_user_functions_select_matches_dbt(self, populated):
        """snowflake__list_function_relations_without_caching, verbatim."""
        populated.execute("show user functions in TEST_DB.TEST_SCHEMA")
        selected = self._as_agate(populated).select(self.FUNCTION_COLUMNS)
        udfs = [row for row in selected if row["is_builtin"] == "N"]
        assert [row["name"] for row in udfs] == ["DBT_UDF"]
        assert udfs[0]["catalog_name"] == "TEST_DB"
        assert udfs[0]["schema_name"] == "TEST_SCHEMA"

    def test_show_object_metadata(self, populated):
        """snowflake__show_object_metadata narrows to one relation."""
        populated.execute(
            "show objects in TEST_DB.TEST_SCHEMA starts with 'DBT_MODEL' limit 1"
        )
        names = [c.name for c in populated.description]
        rows = populated.fetchall()
        assert len(rows) == 1
        assert rows[0][names.index("name")] == "DBT_MODEL"

    def test_list_schemas(self, connection):
        """snowflake__list_schemas reads the `name` column."""
        cursor = connection.cursor()
        cursor.execute("show terse schemas in database TEST_DB\n    limit 10000")
        names = [c.name for c in cursor.description]
        assert "name" in names
        schemas = [row[names.index("name")] for row in cursor.fetchall()]
        assert "TEST_SCHEMA" in schemas

    def test_check_schema_exists(self, connection):
        """snowflake__check_schema_exists queries INFORMATION_SCHEMA.SCHEMATA."""
        cursor = connection.cursor()
        cursor.execute(
            "select count(*) from TEST_DB.information_schema.schemata "
            "where upper(schema_name) = upper('TEST_SCHEMA') "
            "and upper(catalog_name) = upper('TEST_DB')"
        )
        assert cursor.fetchone()[0] == 1

    def test_get_catalog_columns(self, connection):
        """snowflake__get_catalog_tables_sql selects these from the catalog."""
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE catalog_model (id INT)")
        cursor.execute(
            "select table_catalog, table_schema, table_name, table_type, comment, "
            "table_owner, clustering_key, row_count, bytes, last_altered, is_dynamic "
            "from TEST_DB.information_schema.tables "
            "where table_name = 'CATALOG_MODEL'"
        )
        row = dict(
            zip([c.name for c in cursor.description], cursor.fetchone(), strict=True)
        )
        assert row["table_type"] == "BASE TABLE"
        assert row["is_dynamic"] == "NO"

    def test_query_tag_round_trip(self, connection):
        """set_query_tag reads the old tag back, overwrites it and restores it."""
        cursor = connection.cursor()
        cursor.execute("show parameters like 'query_tag' in session")
        original = cursor.fetchall()[0][1]
        assert original == ""

        cursor.execute("alter session set query_tag = 'dbt-run'")
        cursor.execute("show parameters like 'query_tag' in session")
        assert cursor.fetchall()[0][1] == "dbt-run"

        cursor.execute("alter session unset query_tag")
        cursor.execute("show parameters like 'query_tag' in session")
        assert cursor.fetchall()[0][1] == ""
