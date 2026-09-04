"""The VARIANT accessor and predicate families.

Snowflake's ``AS_<type>`` functions *test* the stored type rather than coercing
to it - ``AS_INTEGER(PARSE_JSON('"5"'))`` is NULL, not 5 - and the ``IS_<type>``
functions answer the same question as a boolean. A naive translation to
TRY_CAST gets the coercion cases wrong silently, so the whole family is pinned
here.
"""

import pytest

# (id, sql, expected)
CASES: list[tuple[str, str, object]] = [
    # -- AS_ checks the stored type -----------------------------------------
    ("as_integer_match", "SELECT AS_INTEGER(PARSE_JSON('5'))", 5),
    ("as_integer_string_is_null", "SELECT AS_INTEGER(PARSE_JSON('\"5\"'))", None),
    ("as_integer_double_is_null", "SELECT AS_INTEGER(PARSE_JSON('1.5'))", None),
    ("as_varchar_match", "SELECT AS_VARCHAR(PARSE_JSON('\"x\"'))", "x"),
    ("as_varchar_number_is_null", "SELECT AS_VARCHAR(PARSE_JSON('5'))", None),
    ("as_char_alias", "SELECT AS_CHAR(PARSE_JSON('\"x\"'))", "x"),
    ("as_boolean_match", "SELECT AS_BOOLEAN(PARSE_JSON('true'))", True),
    ("as_boolean_string_is_null", "SELECT AS_BOOLEAN(PARSE_JSON('\"true\"'))", None),
    ("as_array_match", "SELECT AS_ARRAY(PARSE_JSON('[1,2]'))", "[1,2]"),
    ("as_array_object_is_null", "SELECT AS_ARRAY(PARSE_JSON('{}'))", None),
    ("as_object_match", "SELECT AS_OBJECT(PARSE_JSON('{\"a\":1}'))", '{"a":1}'),
    ("as_object_array_is_null", "SELECT AS_OBJECT(PARSE_JSON('[]'))", None),
    ("as_double_match", "SELECT AS_DOUBLE(PARSE_JSON('1.5'))", 1.5),
    ("as_double_string_is_null", "SELECT AS_DOUBLE(PARSE_JSON('\"x\"'))", None),
    ("as_real_alias", "SELECT AS_REAL(PARSE_JSON('1.5'))", 1.5),
    ("as_decimal_default", "SELECT AS_DECIMAL(PARSE_JSON('5'))", 5),
    # AS_DECIMAL states its target type in its trailing arguments.
    ("as_decimal_scale", "SELECT AS_DECIMAL(PARSE_JSON('5'), 10, 2)", "5.00"),
    ("as_number_scale", "SELECT AS_NUMBER(PARSE_JSON('5'), 12, 3)", "5.000"),
    # JSON has no date/time/binary type, so these stay a cast of the string.
    ("as_date", "SELECT AS_DATE(PARSE_JSON('\"2024-01-01\"'))", "2024-01-01"),
    ("as_date_number_is_null", "SELECT AS_DATE(PARSE_JSON('5'))", None),
    ("as_time", "SELECT AS_TIME(PARSE_JSON('\"10:00:00\"'))", "10:00:00"),
    (
        "as_timestamp_ntz",
        "SELECT AS_TIMESTAMP_NTZ(PARSE_JSON('\"2024-01-01 10:00:00\"'))",
        "2024-01-01 10:00:00",
    ),
    # -- IS_ answers the same question --------------------------------------
    ("is_array_true", "SELECT IS_ARRAY(PARSE_JSON('[1]'))", True),
    ("is_array_false", "SELECT IS_ARRAY(PARSE_JSON('{}'))", False),
    ("is_object_true", "SELECT IS_OBJECT(PARSE_JSON('{}'))", True),
    ("is_boolean_true", "SELECT IS_BOOLEAN(PARSE_JSON('true'))", True),
    ("is_varchar_true", "SELECT IS_VARCHAR(PARSE_JSON('\"a\"'))", True),
    ("is_integer_true", "SELECT IS_INTEGER(PARSE_JSON('5'))", True),
    ("is_integer_false", "SELECT IS_INTEGER(PARSE_JSON('1.5'))", False),
    ("is_double_true", "SELECT IS_DOUBLE(PARSE_JSON('1.5'))", True),
    ("is_decimal_true", "SELECT IS_DECIMAL(PARSE_JSON('5'))", True),
    ("is_real_true", "SELECT IS_REAL(PARSE_JSON('1.5'))", True),
    ("is_null_value_true", "SELECT IS_NULL_VALUE(PARSE_JSON('null'))", True),
    ("is_null_value_false", "SELECT IS_NULL_VALUE(PARSE_JSON('1'))", False),
    ("is_date_true", "SELECT IS_DATE(PARSE_JSON('\"2024-01-01\"'))", True),
    ("is_date_unparsable", "SELECT IS_DATE(PARSE_JSON('\"xx\"'))", False),
    ("is_date_number", "SELECT IS_DATE(PARSE_JSON('5'))", False),
    ("is_time_true", "SELECT IS_TIME(PARSE_JSON('\"10:00:00\"'))", True),
    ("is_time_false", "SELECT IS_TIME(PARSE_JSON('\"zz\"'))", False),
    (
        "is_timestamp_ntz_true",
        "SELECT IS_TIMESTAMP_NTZ(PARSE_JSON('\"2024-01-01 10:00:00\"'))",
        True,
    ),
    (
        "is_timestamp_ltz_true",
        "SELECT IS_TIMESTAMP_LTZ(PARSE_JSON('\"2024-01-01 10:00:00\"'))",
        True,
    ),
    (
        "is_timestamp_tz_true",
        "SELECT IS_TIMESTAMP_TZ(PARSE_JSON('\"2024-01-01 10:00:00\"'))",
        True,
    ),
    ("is_binary_true", "SELECT IS_BINARY(PARSE_JSON('\"abc\"'))", True),
    # -- GET reads members as well as elements ------------------------------
    # GET(object, key) is not an array subscript; applying one raised
    # "Expected ARRAY, but got OBJECT".
    ("get_object_member", "SELECT GET(PARSE_JSON('{\"a\":7}'), 'a')", 7),
    ("get_object_missing_member", "SELECT GET(PARSE_JSON('{\"a\":7}'), 'zz')", None),
    ("get_constructed_object", "SELECT GET(OBJECT_CONSTRUCT('a',7), 'a')", 7),
    ("get_array_element", "SELECT GET(ARRAY_CONSTRUCT(10,20), 0)", 10),
    ("get_json_array_element", "SELECT GET(PARSE_JSON('[10,20]'), 1)", 20),
    # -- Object helpers -----------------------------------------------------
    (
        "map_cat",
        "SELECT MAP_CAT(OBJECT_CONSTRUCT('a',1), OBJECT_CONSTRUCT('b',2))",
        '{"a":1,"b":2}',
    ),
    (
        "map_cat_right_wins",
        "SELECT MAP_CAT(OBJECT_CONSTRUCT('a',1), OBJECT_CONSTRUCT('a',2))",
        '{"a":2}',
    ),
]


@pytest.mark.parametrize(
    "sql,expected",
    [(sql, expected) for _, sql, expected in CASES],
    ids=[case_id for case_id, _, _ in CASES],
)
def test_variant_accessor(conn, sql, expected):
    with conn.cursor() as cur:
        cur.execute(sql)
        actual = cur.fetchone()[0]

    if expected is None:
        assert actual is None
        return
    if isinstance(expected, bool):
        assert actual is expected
        return
    assert actual == expected or str(actual) == str(expected)


def test_object_construct_star_reads_the_whole_row(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE oc (a INT, b VARCHAR)")
        cur.execute("INSERT INTO oc VALUES (1, 'x')")

        cur.execute("SELECT OBJECT_CONSTRUCT(*) FROM oc")
        assert cur.fetchone()[0] == '{"A":1,"B":"x"}'

        cur.execute("SELECT OBJECT_CONSTRUCT(*) FROM oc t")
        assert cur.fetchone()[0] == '{"A":1,"B":"x"}'

        # An unaliased subquery has no name to read the row from, so one is
        # synthesised.
        cur.execute("SELECT OBJECT_CONSTRUCT(*) FROM (SELECT 1 a, 2 b)")
        assert cur.fetchone()[0] == '{"A":1,"B":2}'


def test_object_construct_star_over_a_join_is_refused(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE l (id INT)")
        cur.execute("CREATE OR REPLACE TABLE r (id INT)")
        with pytest.raises(Exception, match="OBJECT_CONSTRUCT"):
            cur.execute("SELECT OBJECT_CONSTRUCT(*) FROM l JOIN r ON l.id = r.id")
