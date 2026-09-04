"""SHOW output and schema-name fidelity.

These pin three regressions found running dbt against SnowDuck:

1. sqlglot's "falling back to Command" warning leaked to stderr for SHOW
   statements SnowDuck handles - about nine per dbt build.
2. A schema named MAIN lost its case, because DuckDB's own internal `main`
   schema collides with it. dbt's first run passed and every later one failed
   with "found an approximate match".
3. SHOW VIEWS once failed outright with "Table with name VIEWS does not exist".
"""

import io
import logging

import pytest
import snowflake.connector

FALLBACK_WARNING = "contains unsupported syntax"

# SHOW statements sqlglot cannot model, which SnowDuck re-reads itself.
COMMAND_SHOW_STATEMENTS = [
    "SHOW USER FUNCTIONS",
    "SHOW PARAMETERS",
    "SHOW DYNAMIC TABLES",
]

SHOW_STATEMENTS = COMMAND_SHOW_STATEMENTS + [
    "SHOW TABLES",
    "SHOW VIEWS",
    "SHOW SCHEMAS",
    "SHOW DATABASES",
    "SHOW OBJECTS",
]


@pytest.fixture
def sqlglot_log():
    """Capture everything sqlglot logs, whatever handlers are configured."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logger = logging.getLogger("sqlglot")
    logger.addHandler(handler)
    previous = logger.level
    logger.setLevel(logging.WARNING)
    try:
        yield stream
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)


@pytest.mark.parametrize("sql", SHOW_STATEMENTS)
def test_show_executes(conn, sql):
    """Every documented SHOW returns rows rather than erroring."""
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.fetchall()
        assert cur.description, f"{sql} returned no column metadata"


@pytest.mark.parametrize("sql", SHOW_STATEMENTS)
def test_show_does_not_warn(conn, sql, sqlglot_log):
    """A statement SnowDuck handles must not log a parser fallback warning."""
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.fetchall()

    assert FALLBACK_WARNING not in sqlglot_log.getvalue()


def test_unhandled_statement_still_warns(conn, sqlglot_log):
    """The warning is deferred, not silenced - an unhandled fallback replays."""
    with (
        conn.cursor() as cur,
        pytest.raises(snowflake.connector.errors.ProgrammingError),
    ):
        cur.execute("CREATE TASK t SCHEDULE = '1 minute' AS SELECT 1")

    assert FALLBACK_WARNING in sqlglot_log.getvalue()


def test_show_views_returns_snowflake_columns(conn):
    """SHOW VIEWS once failed with "Table with name VIEWS does not exist"."""
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE VIEW show_v AS SELECT 1 AS a")
        cur.execute("SHOW VIEWS")
        columns = [d.name for d in cur.description]
        names = {row[columns.index("name")] for row in cur.fetchall()}

    for expected in ("created_on", "name", "database_name", "schema_name", "text"):
        assert expected in columns
    assert "SHOW_V" in names


# ---------------------------------------------------------------------------
# A schema named MAIN
# ---------------------------------------------------------------------------


@pytest.fixture
def main_db(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE energy_kit")
        for schema in ("MAIN", "EK_TESTS"):
            cur.execute(f"CREATE SCHEMA energy_kit.{schema}")
            cur.execute(
                f"CREATE OR REPLACE TABLE energy_kit.{schema}.malo_fixtures (a INT)"
            )
    return conn


def test_fresh_database_has_no_main_schema(conn):
    """DuckDB's internal `main` must not masquerade as a Snowflake schema."""
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE fresh_db")
        cur.execute("SHOW SCHEMAS IN DATABASE fresh_db")
        names = {row[1] for row in cur.fetchall()}

    assert "MAIN" not in names


def test_create_schema_main_succeeds(conn):
    """DuckDB owns a schema called `main`; CREATE SCHEMA MAIN must still work."""
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE main_db")
        cur.execute("CREATE SCHEMA main_db.MAIN")
        cur.execute("SHOW SCHEMAS IN DATABASE main_db")
        assert "MAIN" in {row[1] for row in cur.fetchall()}


def test_duplicate_create_schema_main_fails(conn):
    """...and creating it twice still fails, as Snowflake would."""
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE dup_db")
        cur.execute("CREATE SCHEMA dup_db.MAIN")
        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            cur.execute("CREATE SCHEMA dup_db.MAIN")
        # IF NOT EXISTS stays idempotent.
        cur.execute("CREATE SCHEMA IF NOT EXISTS dup_db.MAIN")


@pytest.mark.parametrize("schema", ["MAIN", "EK_TESTS"])
def test_show_objects_reports_upper_case_schema(main_db, schema):
    """dbt looks the relation up under the upper-cased name it renders."""
    with main_db.cursor() as cur:
        cur.execute(f"SHOW OBJECTS IN energy_kit.{schema}")
        columns = [d.name for d in cur.description]
        rows = [dict(zip(columns, row, strict=False)) for row in cur.fetchall()]

    assert rows, f"no objects found in {schema}"
    for row in rows:
        assert row["schema_name"] == schema
        assert row["database_name"] == "ENERGY_KIT"


def test_information_schema_reports_upper_case_main(main_db):
    with main_db.cursor() as cur:
        cur.execute(
            "SELECT schema_name FROM energy_kit.information_schema.schemata "
            "ORDER BY schema_name"
        )
        schemata = {row[0] for row in cur.fetchall()}

        cur.execute(
            "SELECT DISTINCT table_schema FROM energy_kit.information_schema.tables"
        )
        tables = {row[0] for row in cur.fetchall()}

        cur.execute(
            "SELECT DISTINCT table_schema FROM energy_kit.information_schema.columns"
        )
        columns = {row[0] for row in cur.fetchall()}

    assert {"MAIN", "EK_TESTS"} <= schemata
    assert {"MAIN", "EK_TESTS"} == tables
    assert {"MAIN", "EK_TESTS"} == columns


def test_drop_schema_main(main_db):
    """DuckDB refuses to drop its internal `main`, so the drop is emulated."""
    with main_db.cursor() as cur:
        cur.execute("DROP SCHEMA energy_kit.MAIN")

        cur.execute("SHOW SCHEMAS IN DATABASE energy_kit")
        assert "MAIN" not in {row[1] for row in cur.fetchall()}

        cur.execute(
            "SELECT DISTINCT table_schema FROM energy_kit.information_schema.tables"
        )
        assert {row[0] for row in cur.fetchall()} == {"EK_TESTS"}


def test_show_databases_matches_across_storage_modes(tmp_path):
    """A file-backed session must not list its own storage as a database.

    DuckDB names the connection's catalog after its file, so file mode used to
    show a phantom database (`my_data`) that memory mode did not.
    """
    import snowflake.connector

    from snowduck import patch_snowflake

    def databases(**kwargs):
        with patch_snowflake(**kwargs):
            with snowflake.connector.connect(database="db") as conn:
                cur = conn.cursor()
                cur.execute("CREATE DATABASE analytics")
                cur.execute("SHOW DATABASES")
                return {row[1] for row in cur.fetchall()}

    in_memory = databases()
    on_disk = databases(db_file=str(tmp_path / "my_data.duckdb"))

    assert in_memory == on_disk
    assert "MY_DATA" not in on_disk
