"""SHOW command coverage.

The column shapes asserted here are Snowflake's documented ones. They are not
cosmetic: dbt-snowflake selects fixed column *names* out of SHOW OBJECTS and
SHOW USER FUNCTIONS and fails with `tuple.index(x): x not in tuple` when one is
missing, so the shape is part of the contract.
"""

import pytest

from snowduck.connector import Connector
from snowduck.show import parse_show_text

# dbt-snowflake's SnowflakeAdapter.list_relations_without_caching selects
# exactly these, lower-cased, out of the two SHOW results.
DBT_TABULAR_COLUMNS = [
    "database_name",
    "schema_name",
    "name",
    "kind",
    "is_dynamic",
    "is_iceberg",
]
DBT_FUNCTION_COLUMNS = ["catalog_name", "schema_name", "name", "is_builtin"]


@pytest.fixture
def cur():
    connector = Connector(db_file=":memory:")
    connection = connector.connect("TEST_DB", "TEST_SCHEMA")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE customers (id INT, name VARCHAR)")
    cursor.execute("CREATE TABLE orders (id INT)")
    cursor.execute("CREATE VIEW customer_view AS SELECT * FROM customers")
    cursor.execute("CREATE FUNCTION add_one(x INT) RETURNS INT AS $$ x + 1 $$")
    yield cursor
    connection.close()


def columns(cursor):
    return [column.name for column in cursor.description]


def rows_by_name(cursor):
    names = columns(cursor)
    return {
        row[names.index("name")]: dict(zip(names, row, strict=True))
        for row in cursor.fetchall()
    }


# --- SHOW OBJECTS ---------------------------------------------------------


def test_show_objects_has_dbt_columns(cur):
    cur.execute("SHOW OBJECTS IN TEST_DB.TEST_SCHEMA")
    assert set(DBT_TABULAR_COLUMNS) <= set(columns(cur))


def test_show_objects_lists_tables_and_views(cur):
    cur.execute("SHOW OBJECTS IN TEST_DB.TEST_SCHEMA")
    found = rows_by_name(cur)
    assert found["CUSTOMERS"]["kind"] == "TABLE"
    assert found["CUSTOMER_VIEW"]["kind"] == "VIEW"
    assert found["CUSTOMERS"]["is_dynamic"] == "N"
    assert found["CUSTOMERS"]["is_iceberg"] == "N"
    assert found["CUSTOMERS"]["database_name"] == "TEST_DB"
    assert found["CUSTOMERS"]["schema_name"] == "TEST_SCHEMA"


def test_show_objects_is_ordered_by_name(cur):
    cur.execute("SHOW OBJECTS IN TEST_DB.TEST_SCHEMA")
    names = [row[columns(cur).index("name")] for row in cur.fetchall()]
    assert names == sorted(names)


def test_show_objects_limit(cur):
    cur.execute("SHOW OBJECTS IN TEST_DB.TEST_SCHEMA LIMIT 2")
    assert len(cur.fetchall()) == 2


def test_show_objects_pagination_watermark(cur):
    """dbt pages with `limit <n> from '<last name seen>'`."""
    cur.execute("SHOW OBJECTS IN TEST_DB.TEST_SCHEMA LIMIT 2")
    first_page = [row[columns(cur).index("name")] for row in cur.fetchall()]
    cur.execute(f"SHOW OBJECTS IN TEST_DB.TEST_SCHEMA LIMIT 2 FROM '{first_page[-1]}'")
    second_page = [row[columns(cur).index("name")] for row in cur.fetchall()]
    assert not set(first_page) & set(second_page)
    assert first_page + second_page == sorted(first_page + second_page)


def test_show_objects_starts_with_is_a_literal_prefix(cur):
    """`_` is a LIKE wildcard but a plain character in STARTS WITH."""
    cur.execute("SHOW OBJECTS IN TEST_DB.TEST_SCHEMA STARTS WITH 'CUSTOMER_'")
    assert list(rows_by_name(cur)) == ["CUSTOMER_VIEW"]

    cur.execute("SHOW OBJECTS LIKE 'CUSTOMER_' IN TEST_DB.TEST_SCHEMA")
    assert list(rows_by_name(cur)) == ["CUSTOMERS"]


def test_show_objects_like(cur):
    # Snowflake's grammar puts LIKE before IN.
    cur.execute("SHOW OBJECTS LIKE '%ORDER%' IN TEST_DB.TEST_SCHEMA")
    assert list(rows_by_name(cur)) == ["ORDERS"]


def test_show_terse_objects(cur):
    cur.execute("SHOW TERSE OBJECTS IN TEST_DB.TEST_SCHEMA")
    assert columns(cur) == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
    ]


def test_show_objects_defaults_to_current_schema(cur):
    cur.execute("SHOW OBJECTS")
    assert "CUSTOMERS" in rows_by_name(cur)


# --- SHOW TABLES / VIEWS --------------------------------------------------


def test_show_tables_excludes_views(cur):
    cur.execute("SHOW TABLES IN TEST_DB.TEST_SCHEMA")
    found = rows_by_name(cur)
    assert set(found) == {"CUSTOMERS", "ORDERS"}
    assert found["CUSTOMERS"]["kind"] == "TABLE"


def test_show_views_reports_the_definition(cur):
    cur.execute("SHOW VIEWS IN TEST_DB.TEST_SCHEMA")
    found = rows_by_name(cur)
    assert set(found) == {"CUSTOMER_VIEW"}
    assert "is_materialized" in found["CUSTOMER_VIEW"]
    assert "SELECT" in found["CUSTOMER_VIEW"]["text"].upper()


# --- SHOW [USER] FUNCTIONS ------------------------------------------------


def test_show_user_functions_has_dbt_columns(cur):
    cur.execute("SHOW USER FUNCTIONS IN TEST_DB.TEST_SCHEMA")
    assert set(DBT_FUNCTION_COLUMNS) <= set(columns(cur))


def test_show_user_functions_lists_udfs_only(cur):
    cur.execute("SHOW USER FUNCTIONS IN TEST_DB.TEST_SCHEMA")
    found = rows_by_name(cur)
    assert set(found) == {"ADD_ONE"}
    assert found["ADD_ONE"]["is_builtin"] == "N"
    assert found["ADD_ONE"]["catalog_name"] == "TEST_DB"
    assert found["ADD_ONE"]["schema_name"] == "TEST_SCHEMA"


def test_show_functions_includes_builtins(cur):
    cur.execute("SHOW FUNCTIONS IN TEST_DB.TEST_SCHEMA")
    flags = {row[columns(cur).index("is_builtin")] for row in cur.fetchall()}
    assert flags == {"Y", "N"}


# --- SHOW SCHEMAS / DATABASES ---------------------------------------------


def test_show_terse_schemas(cur):
    cur.execute("SHOW TERSE SCHEMAS IN DATABASE TEST_DB LIMIT 10000")
    assert columns(cur) == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
    ]
    assert "TEST_SCHEMA" in rows_by_name(cur)


def test_show_schemas_marks_the_current_one(cur):
    cur.execute("SHOW SCHEMAS IN DATABASE TEST_DB")
    found = rows_by_name(cur)
    assert found["TEST_SCHEMA"]["is_current"] == "Y"
    assert found["PUBLIC"]["is_current"] == "N"


def test_show_databases_excludes_duckdb_internals(cur):
    cur.execute("SHOW DATABASES")
    found = rows_by_name(cur)
    assert set(found) == {"TEST_DB"}
    assert found["TEST_DB"]["is_current"] == "Y"


# --- SHOW COLUMNS ---------------------------------------------------------


def test_show_columns_reports_snowflake_types(cur):
    cur.execute("SHOW COLUMNS IN TABLE TEST_DB.TEST_SCHEMA.CUSTOMERS")
    names = columns(cur)
    types = {
        row[names.index("column_name")]: row[names.index("data_type")]
        for row in cur.fetchall()
    }
    assert '"type":"FIXED"' in types["ID"]
    assert '"type":"TEXT"' in types["NAME"]


def test_show_columns_matches_case_insensitively(cur):
    """Snowflake folds unquoted names to upper case; DuckDB stores them as written."""
    cur.execute("SHOW COLUMNS IN TABLE TEST_DB.TEST_SCHEMA.customers")
    assert len(cur.fetchall()) == 2


# --- SHOW PARAMETERS / VARIABLES ------------------------------------------


def test_show_parameters_round_trips_alter_session(cur):
    cur.execute("SHOW PARAMETERS LIKE 'query_tag' IN SESSION")
    assert cur.fetchall() == [
        (
            "query_tag",
            "",
            "",
            "",
            "String tag attached to statements in this session.",
            "STRING",
        )
    ]

    cur.execute("ALTER SESSION SET QUERY_TAG = 'dbt'")
    cur.execute("SHOW PARAMETERS LIKE 'query_tag' IN SESSION")
    row = dict(zip(columns(cur), cur.fetchall()[0], strict=True))
    assert row["value"] == "dbt"
    assert row["level"] == "SESSION"

    cur.execute("ALTER SESSION UNSET QUERY_TAG")
    cur.execute("SHOW PARAMETERS LIKE 'query_tag' IN SESSION")
    assert cur.fetchall()[0][1] == ""


def test_show_variables(cur):
    cur.execute("SET my_var = 'hello'")
    cur.execute("SHOW VARIABLES")
    found = rows_by_name(cur)
    assert found["MY_VAR"]["value"] == "hello"


# --- object types with no local equivalent --------------------------------


@pytest.mark.parametrize(
    "statement, expected",
    [
        ("SHOW DYNAMIC TABLES IN TEST_DB.TEST_SCHEMA", "is_clone"),
        ("SHOW ICEBERG TABLES IN TEST_DB.TEST_SCHEMA", "external_volume_name"),
        ("SHOW PROCEDURES IN TEST_DB.TEST_SCHEMA", "is_builtin"),
        ("SHOW STREAMS IN TEST_DB.TEST_SCHEMA", "source_type"),
        ("SHOW TASKS IN TEST_DB.TEST_SCHEMA", "predecessors"),
        ("SHOW GRANTS ON TABLE TEST_DB.TEST_SCHEMA.CUSTOMERS", "grantee_name"),
        ("SHOW PRIMARY KEYS IN TEST_DB.TEST_SCHEMA", "key_sequence"),
    ],
)
def test_unsupported_object_types_return_the_right_shape(cur, statement, expected):
    """An empty result with Snowflake's columns, not a parser error."""
    cur.execute(statement)
    assert cur.fetchall() == []
    assert expected in columns(cur)


def test_unknown_object_type_reports_a_clear_error(cur):
    import snowflake.connector.errors

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        cur.execute("SHOW REPLICATION GROUPS")
    assert "not supported by SnowDuck" in str(excinfo.value)


# --- scanner --------------------------------------------------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        ("SHOW OBJECTS IN SCHEMA db.sch", ("OBJECTS", "db", "sch", None)),
        ('SHOW OBJECTS IN SCHEMA "db"."sch"', ("OBJECTS", "db", "sch", None)),
        ("SHOW USER FUNCTIONS IN sch", ("USER FUNCTIONS", None, "sch", None)),
        ("SHOW COLUMNS IN TABLE db.sch.t", ("COLUMNS", "db", "sch", "t")),
        ("SHOW IMPORTED KEYS IN TABLE db.sch.t", ("IMPORTED KEYS", "db", "sch", "t")),
        ("SHOW TERSE SCHEMAS IN DATABASE db", ("SCHEMAS", "db", None, None)),
    ],
)
def test_scanner_resolves_scope(text, expected):
    resolved = parse_show_text(text).resolved(database=None, schema=None)
    assert (
        resolved.kind,
        resolved.database,
        resolved.schema,
        resolved.table,
    ) == expected


def test_scanner_reads_every_clause():
    request = parse_show_text(
        "SHOW TERSE OBJECTS LIKE 'a%' IN SCHEMA db.sch "
        "STARTS WITH 'a' LIMIT 5 FROM 'a_1';"
    )
    assert request.terse
    assert request.kind == "OBJECTS"
    assert request.like == "a%"
    assert request.starts_with == "a"
    assert request.limit == 5
    assert request.from_ == "a_1"


def test_show_stages_reflects_newly_created_stages(monkeypatch, tmp_path, cur):
    """SHOW STAGES reads the stage directory, so its SQL must not be cached."""
    monkeypatch.setenv("SNOWDUCK_STAGE_DIR", str(tmp_path))

    cur.execute("SHOW STAGES IN TEST_DB.TEST_SCHEMA")
    assert cur.fetchall() == []

    cur.execute("CREATE STAGE my_stage")
    cur.execute("SHOW STAGES IN TEST_DB.TEST_SCHEMA")
    assert list(rows_by_name(cur)) == ["MY_STAGE"]
