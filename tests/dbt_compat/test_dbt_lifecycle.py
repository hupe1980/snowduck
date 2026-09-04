"""Regressions from running a real dbt project end to end.

Each test here corresponds to a statement dbt-snowflake 1.12 issues that
SnowDuck used to reject or mistranslate, found by running `dbt build`,
`dbt snapshot` and `dbt docs generate` against the server.
"""

import pytest

from snowduck.connector import Connector


@pytest.fixture
def cur():
    connector = Connector(db_file=":memory:")
    connection = connector.connect("DEV_DB", "PUBLIC")
    cursor = connection.cursor()
    yield cursor
    connection.close()


def columns(cursor):
    return [column.name for column in cursor.description]


# --- seeds ----------------------------------------------------------------


def test_insert_overwrite_replaces_the_table(cur):
    """dbt's seed materialization loads the first batch with INSERT OVERWRITE.

    sqlglot's DuckDB generator renders it as Hive's `INSERT OVERWRITE TABLE`,
    which DuckDB cannot parse, so the two halves are issued separately.
    """
    cur.execute("CREATE TABLE seed_t (id INT, name VARCHAR)")
    cur.execute("INSERT INTO seed_t (id, name) VALUES (1, 'old')")

    cur.execute("INSERT OVERWRITE INTO seed_t (id, name) VALUES (2, 'new'), (3, 'x')")
    assert cur.fetchall() == [(2,)]

    cur.execute("SELECT id, name FROM seed_t ORDER BY id")
    assert cur.fetchall() == [(2, "new"), (3, "x")]


def test_insert_overwrite_from_a_query(cur):
    cur.execute("CREATE TABLE ovw_t (id INT)")
    cur.execute("INSERT INTO ovw_t VALUES (1)")
    cur.execute("INSERT OVERWRITE INTO ovw_t SELECT 9")
    cur.execute("SELECT id FROM ovw_t")
    assert cur.fetchall() == [(9,)]


# --- incremental models ---------------------------------------------------


def test_qualified_temporary_relations(cur):
    """dbt stages incremental models as `create ... temporary view <db>.<schema>.<n>`.

    DuckDB keeps temporary objects in its own `temp` catalog and rejects any
    other qualification, so TEMPORARY is dropped when a name is qualified.
    """
    cur.execute("CREATE TABLE src_t (a INT)")
    cur.execute("INSERT INTO src_t VALUES (1)")

    cur.execute(
        "CREATE OR REPLACE TEMPORARY VIEW DEV_DB.PUBLIC.stage_v AS SELECT * FROM src_t"
    )
    cur.execute("SELECT a FROM DEV_DB.PUBLIC.stage_v")
    assert cur.fetchall() == [(1,)]

    cur.execute(
        "CREATE OR REPLACE TEMPORARY TABLE DEV_DB.PUBLIC.stage_t AS SELECT * FROM src_t"
    )
    cur.execute("SELECT a FROM DEV_DB.PUBLIC.stage_t")
    assert cur.fetchall() == [(1,)]


def test_unqualified_temporary_table_stays_temporary(cur):
    cur.execute("CREATE TEMPORARY TABLE local_t AS SELECT 1 AS a")
    cur.execute("SELECT a FROM local_t")
    assert cur.fetchall() == [(1,)]


def test_transient_table(cur):
    """TRANSIENT only removes Fail-safe, which SnowDuck does not have."""
    cur.execute("CREATE OR REPLACE TRANSIENT TABLE DEV_DB.PUBLIC.tr_t (a INT)")
    cur.execute("INSERT INTO tr_t VALUES (1)")
    cur.execute("SELECT a FROM tr_t")
    assert cur.fetchall() == [(1,)]


def test_merge_insert_values_resolve_against_the_source(cur):
    """dbt's incremental MERGE writes a bare column list in VALUES.

    There is no target row in a NOT MATCHED branch, so Snowflake resolves those
    against the source; DuckDB sees both relations and calls them ambiguous.
    """
    cur.execute("CREATE TABLE tgt (id INT, name VARCHAR)")
    cur.execute("INSERT INTO tgt VALUES (1, 'old')")
    cur.execute("CREATE TABLE src (id INT, name VARCHAR)")
    cur.execute("INSERT INTO src VALUES (1, 'A'), (2, 'B')")

    cur.execute(
        """
        MERGE INTO tgt AS DBT_INTERNAL_DEST
            USING src AS DBT_INTERNAL_SOURCE
            ON (DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id)
        WHEN MATCHED THEN UPDATE SET
            "NAME" = DBT_INTERNAL_SOURCE."NAME"
        WHEN NOT MATCHED THEN INSERT
            ("ID", "NAME")
        VALUES
            ("ID", "NAME")
        """
    )
    cur.execute("SELECT id, name FROM tgt ORDER BY id")
    assert cur.fetchall() == [(1, "A"), (2, "B")]


# --- snapshots ------------------------------------------------------------


@pytest.mark.parametrize(
    "expression",
    [
        "TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))",
        "TO_TIMESTAMP_LTZ(CURRENT_TIMESTAMP())",
        "TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP())",
        "TRY_TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP())",
    ],
)
def test_timestamp_conversions_of_non_literals(cur, expression):
    """`snapshot_get_time` stamps every snapshot row with the first of these.

    sqlglot only models the typed TO_TIMESTAMP_* variants when the argument is a
    literal, so the non-literal form reaches SnowDuck as an anonymous call.
    """
    cur.execute(f"SELECT {expression} AS t")
    assert cur.fetchall()[0][0] is not None


def test_to_time_of_a_non_literal(cur):
    cur.execute("CREATE TABLE times_t (raw VARCHAR)")
    cur.execute("INSERT INTO times_t VALUES ('12:34:56')")
    cur.execute("SELECT TO_TIME(raw) AS t FROM times_t")
    assert str(cur.fetchall()[0][0]) == "12:34:56"


# --- function models ------------------------------------------------------


def test_scalar_udf_with_a_query_body(cur):
    """A UDF is a table function by its signature, not by its body.

    dbt's SQL function models compile to
    `RETURNS FLOAT ... AS $$ select amount * 1.2 $$`, which is scalar.
    """
    cur.execute(
        "CREATE OR REPLACE FUNCTION add_tax(amount FLOAT) RETURNS FLOAT "
        "LANGUAGE SQL AS $$ select amount * 1.2 $$"
    )
    cur.execute("SELECT add_tax(100.0) AS v")
    assert float(cur.fetchall()[0][0]) == pytest.approx(120.0)


def test_table_udf_needs_returns_table(cur):
    cur.execute(
        "CREATE OR REPLACE FUNCTION rows_of(n INT) RETURNS TABLE(x INT) "
        "AS $$ SELECT n AS x $$"
    )
    cur.execute("SELECT * FROM TABLE(rows_of(7))")
    assert cur.fetchall() == [(7,)]


def test_show_user_functions_flags_table_functions(cur):
    cur.execute("CREATE OR REPLACE FUNCTION scalar_fn(n INT) RETURNS INT AS $$ n $$")
    cur.execute(
        "CREATE OR REPLACE FUNCTION table_fn(n INT) RETURNS TABLE(x INT) "
        "AS $$ SELECT n AS x $$"
    )
    cur.execute("SHOW USER FUNCTIONS IN DEV_DB.PUBLIC")
    names = columns(cur)
    flags = {
        row[names.index("name")]: row[names.index("is_table_function")]
        for row in cur.fetchall()
    }
    assert flags == {"SCALAR_FN": "N", "TABLE_FN": "Y"}


# --- catalog --------------------------------------------------------------


def test_create_database_gets_an_information_schema(cur):
    """CREATE DATABASE attaches the catalog through the normal DDL path.

    The information schema used to be created only when the manager did the
    ATTACH itself, so a database created with SQL had none - and every
    INFORMATION_SCHEMA query against it failed.
    """
    cur.execute("CREATE DATABASE OTHER_DB")
    cur.execute("USE DATABASE OTHER_DB")
    cur.execute("CREATE TABLE OTHER_DB.PUBLIC.t (a INT)")

    cur.execute("SELECT table_name, table_type FROM OTHER_DB.INFORMATION_SCHEMA.TABLES")
    assert cur.fetchall() == [("T", "BASE TABLE")]

    cur.execute("SELECT schema_name FROM OTHER_DB.INFORMATION_SCHEMA.SCHEMATA")
    assert "PUBLIC" in {row[0] for row in cur.fetchall()}


def test_persist_docs_round_trips_through_the_catalog(cur):
    """dbt's `persist_docs` writes descriptions as comments and reads them back.

    The SQL here is what dbt-snowflake 1.12 actually emits: `comment on ... is
    $$...$$` for the relation, and one `alter table ... alter` naming *every*
    documented column at once for the columns. sqlglot cannot parse that
    multi-column form and degrades it to an opaque Command, so it used to reach
    DuckDB verbatim and fail the whole `dbt docs generate` run.
    """
    cur.execute("CREATE TABLE documented (id INT, name VARCHAR)")
    cur.execute("comment on table documented IS $$The documented model$$")
    cur.execute(
        'alter table documented alter "ID" COMMENT $$Surrogate key$$ , '
        '"NAME" COMMENT $$Display name$$'
    )

    cur.execute(
        "SELECT comment FROM DEV_DB.INFORMATION_SCHEMA.TABLES "
        "WHERE table_name = 'DOCUMENTED'"
    )
    assert cur.fetchall() == [("The documented model",)]

    cur.execute(
        "SELECT column_name, comment FROM DEV_DB.INFORMATION_SCHEMA.COLUMNS "
        "WHERE table_name = 'DOCUMENTED' ORDER BY ordinal_position"
    )
    assert cur.fetchall() == [
        ("ID", "Surrogate key"),
        ("NAME", "Display name"),
    ]


def test_persist_docs_on_a_view(cur):
    cur.execute("CREATE VIEW documented_v COMMENT = 'A documented view' AS SELECT 1 x")
    cur.execute(
        "SELECT comment FROM DEV_DB.INFORMATION_SCHEMA.VIEWS "
        "WHERE table_name = 'DOCUMENTED_V'"
    )
    assert cur.fetchall() == [("A documented view",)]
