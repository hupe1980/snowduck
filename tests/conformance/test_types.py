"""Type round-tripping through the connector, and result metadata."""

import datetime
import decimal

import pytest

TABLE = """
CREATE OR REPLACE TABLE types_t (
    c_num NUMBER(10,2), c_int INT, c_float FLOAT, c_str VARCHAR(20), c_bool BOOLEAN,
    c_date DATE, c_time TIME, c_ts TIMESTAMP_NTZ, c_tsltz TIMESTAMP_LTZ,
    c_tstz TIMESTAMP_TZ, c_bin BINARY, c_var VARIANT, c_arr ARRAY, c_obj OBJECT
)
"""

ROW = """
INSERT INTO types_t VALUES (
    12.34, 42, 1.5, 'hello', TRUE,
    '2024-01-15'::DATE, '10:11:12'::TIME, '2024-01-15 10:11:12'::TIMESTAMP_NTZ,
    '2024-01-15 10:11:12'::TIMESTAMP_LTZ, '2024-01-15 10:11:12 +0200'::TIMESTAMP_TZ,
    TO_BINARY('414243','HEX'), PARSE_JSON('{"k":1}'), ARRAY_CONSTRUCT(1,2),
    PARSE_JSON('{"a":1}')
)
"""

EXPECTED_PYTHON_TYPE = {
    "C_NUM": decimal.Decimal,
    "C_INT": int,
    "C_FLOAT": float,
    "C_STR": str,
    "C_BOOL": bool,
    "C_DATE": datetime.date,
    "C_TIME": datetime.time,
    "C_TS": datetime.datetime,
    "C_TSLTZ": datetime.datetime,
    "C_TSTZ": datetime.datetime,
    "C_BIN": (bytes, bytearray),
    "C_VAR": str,
    "C_ARR": str,
    "C_OBJ": str,
}


@pytest.fixture
def typed_table(conn):
    with conn.cursor() as cur:
        cur.execute(TABLE)
        cur.execute(ROW)
    return conn


@pytest.mark.parametrize("column", sorted(EXPECTED_PYTHON_TYPE))
def test_column_round_trips_to_the_right_python_type(typed_table, column):
    with typed_table.cursor() as cur:
        cur.execute(f"SELECT {column} FROM types_t")
        value = cur.fetchone()[0]

    assert isinstance(value, EXPECTED_PYTHON_TYPE[column])


def test_information_schema_reports_snowflake_types(typed_table):
    with typed_table.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'TYPES_T' ORDER BY ordinal_position"
        )
        types = dict(cur.fetchall())

    assert types["C_NUM"] == "NUMBER"
    assert types["C_STR"] == "TEXT"
    assert types["C_TS"] == "TIMESTAMP_NTZ"
    assert types["C_BIN"] == "BINARY"
    # ARRAY, OBJECT and VARIANT are all stored as JSON, so all report VARIANT.
    assert types["C_VAR"] == types["C_ARR"] == types["C_OBJ"] == "VARIANT"


# ---------------------------------------------------------------------------
# Declared VARCHAR lengths
# ---------------------------------------------------------------------------


def test_declared_varchar_length_is_reported(conn):
    """DuckDB has no length-limited VARCHAR and drops the size at DDL time."""
    with conn.cursor() as cur:
        cur.execute(
            "CREATE OR REPLACE TABLE len_t (a VARCHAR(20), b CHAR(5), c VARCHAR)"
        )
        cur.execute(
            "SELECT column_name, character_maximum_length "
            "FROM information_schema.columns WHERE table_name = 'LEN_T' "
            "ORDER BY ordinal_position"
        )
        assert cur.fetchall() == [("A", 20), ("B", 5), ("C", None)]


def test_internal_size_uses_the_declared_length(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE size_t (a VARCHAR(20), b VARCHAR)")
        cur.execute("SELECT a, b FROM size_t")
        sizes = {d.name: d.internal_size for d in cur.description}

    assert sizes["A"] == 20
    # An unbounded VARCHAR reports Snowflake's maximum.
    assert sizes["B"] == 16_777_216


def test_recreating_a_table_replaces_the_recorded_lengths(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE relen_t (a VARCHAR(20))")
        cur.execute("CREATE OR REPLACE TABLE relen_t (a VARCHAR(7))")
        cur.execute(
            "SELECT character_maximum_length FROM information_schema.columns "
            "WHERE table_name = 'RELEN_T'"
        )
        assert cur.fetchone()[0] == 7


# ---------------------------------------------------------------------------
# Timestamps with a numeric UTC offset
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "literal,expected_utc_hour",
    [
        ("2024-01-15 10:11:12 +0200", 8),
        ("2024-01-15 10:11:12 -0500", 15),
        ("2024-01-15 10:11:12+02:00", 8),
        ("2024-01-15 10:11:12 +02:00", 8),
    ],
)
def test_numeric_utc_offsets(conn, literal, expected_utc_hour):
    """Snowflake writes offsets as `+0200`; DuckDB reads that as a zone name."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT '{literal}'::TIMESTAMP_TZ")
        value = cur.fetchone()[0]

    assert value.astimezone(datetime.timezone.utc).hour == expected_utc_hour


def test_offset_normalisation_leaves_other_strings_alone(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 'not a timestamp +0200'")
        assert cur.fetchone()[0] == "not a timestamp +0200"
