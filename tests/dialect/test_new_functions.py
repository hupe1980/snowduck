"""Tests for newly added functions."""

import duckdb
from sqlglot import parse_one

# =============================================================================
# STRING FUNCTIONS
# =============================================================================


def test_initcap(conn):
    """Test INITCAP capitalizes first letter of each word."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT INITCAP('hello world')")
        res = cursor.fetchone()
        assert res[0] == "Hello World"


def test_translate(dialect_context):
    """Test TRANSLATE replaces characters."""
    sql = "SELECT TRANSLATE('hello', 'el', 'ip')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "hippo"


def test_soundex(conn):
    """Test SOUNDEX returns phonetic code."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT SOUNDEX('Robert')")
        res = cursor.fetchone()
        assert res[0] == "R163"  # Standard Soundex code


def test_reverse(dialect_context):
    """Test REVERSE reverses string."""
    sql = "SELECT REVERSE('hello')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "olleh"


def test_startswith(dialect_context):
    """Test STARTSWITH checks string prefix."""
    sql = "SELECT STARTSWITH('hello world', 'hello')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is True


def test_endswith(dialect_context):
    """Test ENDSWITH checks string suffix."""
    sql = "SELECT ENDSWITH('hello world', 'world')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is True


def test_ascii(dialect_context):
    """Test ASCII returns character code."""
    sql = "SELECT ASCII('A')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 65


def test_chr(dialect_context):
    """Test CHR returns character from code."""
    sql = "SELECT CHR(65)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == "A"


# =============================================================================
# DATE/TIME CONSTRUCTION FUNCTIONS
# =============================================================================


def test_date_from_parts(dialect_context):
    """Test DATE_FROM_PARTS constructs a date."""
    sql = "SELECT DATE_FROM_PARTS(2024, 6, 15)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert "2024-06-15" in str(res[0])


def test_time_from_parts(dialect_context):
    """Test TIME_FROM_PARTS constructs a time."""
    sql = "SELECT TIME_FROM_PARTS(14, 30, 45)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert "14:30:45" in str(res[0])


def test_timestamp_from_parts(dialect_context):
    """Test TIMESTAMP_FROM_PARTS constructs a timestamp."""
    sql = "SELECT TIMESTAMP_FROM_PARTS(2024, 6, 15, 14, 30, 45)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert "2024-06-15 14:30:45" in str(res[0])


# =============================================================================
# NUMERIC FUNCTIONS
# =============================================================================


def test_cbrt(dialect_context):
    """Test CBRT returns cube root."""
    sql = "SELECT ROUND(CBRT(27), 0)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 3.0


def test_factorial(dialect_context):
    """Test FACTORIAL returns n!."""
    sql = "SELECT FACTORIAL(5)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 120


def test_degrees(dialect_context):
    """Test DEGREES converts radians to degrees."""
    sql = "SELECT ROUND(DEGREES(3.14159265359), 0)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 180


def test_radians(dialect_context):
    """Test RADIANS converts degrees to radians."""
    sql = "SELECT ROUND(RADIANS(180), 5)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert abs(res[0] - 3.14159) < 0.001


def test_pi(dialect_context):
    """Test PI returns pi constant."""
    sql = "SELECT ROUND(PI(), 5)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert abs(res[0] - 3.14159) < 0.001


# =============================================================================
# ARRAY FUNCTIONS
# =============================================================================


def test_array_cat(dialect_context):
    """Test ARRAY_CAT concatenates arrays."""
    sql = "SELECT ARRAY_CAT(ARRAY_CONSTRUCT(1, 2), ARRAY_CONSTRUCT(3, 4))"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == [1, 2, 3, 4]


def test_array_append(dialect_context):
    """Test ARRAY_APPEND adds element to array."""
    sql = "SELECT ARRAY_APPEND(ARRAY_CONSTRUCT(1, 2, 3), 4)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == [1, 2, 3, 4]


def test_array_prepend(dialect_context):
    """Test ARRAY_PREPEND adds element to start of array."""
    sql = "SELECT ARRAY_PREPEND(ARRAY_CONSTRUCT(2, 3, 4), 1)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == [1, 2, 3, 4]


def test_array_sort(dialect_context):
    """Test ARRAY_SORT sorts array."""
    sql = "SELECT ARRAY_SORT(ARRAY_CONSTRUCT(3, 1, 4, 1, 5))"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == [1, 1, 3, 4, 5]


def test_array_reverse(dialect_context):
    """Test ARRAY_REVERSE reverses array."""
    sql = "SELECT ARRAY_REVERSE(ARRAY_CONSTRUCT(1, 2, 3))"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == [3, 2, 1]


def test_array_min(dialect_context):
    """Test ARRAY_MIN returns minimum element."""
    sql = "SELECT ARRAY_MIN(ARRAY_CONSTRUCT(3, 1, 4, 1, 5))"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 1


def test_array_max(dialect_context):
    """Test ARRAY_MAX returns maximum element."""
    sql = "SELECT ARRAY_MAX(ARRAY_CONSTRUCT(3, 1, 4, 1, 5))"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 5


def test_array_sum(dialect_context):
    """Test ARRAY_SUM returns sum of elements."""
    sql = "SELECT ARRAY_SUM(ARRAY_CONSTRUCT(1, 2, 3, 4, 5))"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 15


def test_arrays_overlap(dialect_context):
    """Test ARRAYS_OVERLAP checks for common elements."""
    sql = "SELECT ARRAYS_OVERLAP(ARRAY_CONSTRUCT(1, 2, 3), ARRAY_CONSTRUCT(3, 4, 5))"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is True


def test_array_position_null_return(dialect_context):
    """Test ARRAY_POSITION returns NULL when element not found."""
    sql = "SELECT ARRAY_POSITION(99, ARRAY_CONSTRUCT(1, 2, 3))"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is None  # Should be NULL, not -1


# =============================================================================
# JSON FUNCTIONS
# =============================================================================


def test_object_keys(dialect_context):
    """Test OBJECT_KEYS returns array of keys."""
    sql = 'SELECT OBJECT_KEYS(PARSE_JSON(\'{"a": 1, "b": 2}\'))'
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    keys = res[0]
    assert "a" in keys and "b" in keys


def test_check_json_valid(dialect_context):
    """Test CHECK_JSON returns NULL for valid JSON."""
    sql = "SELECT CHECK_JSON('{\"a\": 1}')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is None  # NULL means valid


def test_check_json_invalid(dialect_context):
    """Test CHECK_JSON returns error message for invalid JSON."""
    sql = "SELECT CHECK_JSON('not json')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is not None  # Non-null means invalid


# =============================================================================
# AGGREGATE FUNCTIONS
# =============================================================================


def test_any_value(dialect_context):
    """Test ANY_VALUE returns any value from group."""
    sql = "SELECT ANY_VALUE(x) FROM (SELECT 1 as x UNION ALL SELECT 1 as x)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 1


def test_kurtosis(dialect_context):
    """Test KURTOSIS returns kurtosis of values."""
    sql = "SELECT KURTOSIS(x) FROM (VALUES (1), (2), (3), (4), (5)) as t(x)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    # Kurtosis of uniform distribution is around -1.3
    assert res[0] is not None


# =============================================================================
# MISCELLANEOUS FUNCTIONS
# =============================================================================


def test_zeroifnull(dialect_context):
    """Test ZEROIFNULL returns 0 for NULL."""
    sql = "SELECT ZEROIFNULL(NULL)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 0


def test_nullifzero(dialect_context):
    """Test NULLIFZERO returns NULL for 0."""
    sql = "SELECT NULLIFZERO(0)"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] is None


def test_try_to_number(dialect_context):
    """Test TRY_TO_NUMBER converts string to number or NULL."""
    sql = "SELECT TRY_TO_NUMBER('123.45')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert res[0] == 123.45


def test_try_to_date(dialect_context):
    """Test TRY_TO_DATE converts string to date or NULL."""
    sql = "SELECT TRY_TO_DATE('2024-06-15')"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)

    conn = duckdb.connect(":memory:")
    res = conn.execute(transpiled).fetchone()
    assert "2024-06-15" in str(res[0])


# =============================================================================
# ADDITIONAL TIME ZONE FUNCTIONS
# =============================================================================


def test_convert_timezone_two_args(conn):
    """Test CONVERT_TIMEZONE with 2 arguments (target_tz, timestamp)."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT CONVERT_TIMEZONE('America/New_York', '2024-06-15 12:00:00'::TIMESTAMP)
        """)
        res = cursor.fetchone()
        # Result should be a timestamp
        assert res[0] is not None


def test_convert_timezone_three_args(conn):
    """Test CONVERT_TIMEZONE with 3 arguments (source_tz, target_tz, timestamp)."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT CONVERT_TIMEZONE('UTC', 'America/New_York', '2024-06-15 12:00:00'::TIMESTAMP)
        """)
        res = cursor.fetchone()
        # UTC 12:00 -> EST/EDT is 08:00 (EDT) or 07:00 (EST)
        assert res[0] is not None


# =============================================================================
# ADDITIONAL ARRAY FUNCTIONS
# =============================================================================


def test_array_distinct(conn):
    """Test ARRAY_DISTINCT removes duplicates from array."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT ARRAY_DISTINCT(ARRAY_CONSTRUCT(1, 2, 2, 3, 3, 3))")
        res = cursor.fetchone()
        # Should have unique elements (order may vary)
        arr = res[0]
        assert sorted(arr) == [1, 2, 3]


def test_array_intersection(conn):
    """Test ARRAY_INTERSECTION returns common elements."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT ARRAY_INTERSECTION(
                ARRAY_CONSTRUCT(1, 2, 3),
                ARRAY_CONSTRUCT(2, 3, 4)
            )
        """)
        res = cursor.fetchone()
        arr = res[0]
        assert sorted(arr) == [2, 3]


def test_arrays_overlap_connector(conn):
    """Test ARRAYS_OVERLAP checks if arrays share elements."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT ARRAYS_OVERLAP(
                ARRAY_CONSTRUCT(1, 2, 3),
                ARRAY_CONSTRUCT(3, 4, 5)
            )
        """)
        res = cursor.fetchone()
        assert res[0] is True


def test_array_except(conn):
    """Test ARRAY_EXCEPT returns elements in first array not in second."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT ARRAY_EXCEPT(
                ARRAY_CONSTRUCT(1, 2, 3),
                ARRAY_CONSTRUCT(2, 3, 4)
            )
        """)
        res = cursor.fetchone()
        arr = res[0]
        assert arr == [1]


# =============================================================================
# ADDITIONAL CONDITIONAL FUNCTIONS
# =============================================================================


def test_iff(conn):
    """Test IFF (Snowflake's inline if)."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT IFF(1 > 0, 'yes', 'no')")
        res = cursor.fetchone()
        assert res[0] == "yes"

        cursor.execute("SELECT IFF(1 < 0, 'yes', 'no')")
        res = cursor.fetchone()
        assert res[0] == "no"


def test_equal_null(conn):
    """Test EQUAL_NULL compares including NULLs."""
    with conn.cursor() as cursor:
        # Two NULLs should be equal
        cursor.execute("SELECT EQUAL_NULL(NULL, NULL)")
        res = cursor.fetchone()
        assert res[0] is True

        # NULL and value should not be equal
        cursor.execute("SELECT EQUAL_NULL(NULL, 1)")
        res = cursor.fetchone()
        assert res[0] is False

        # Same values should be equal
        cursor.execute("SELECT EQUAL_NULL(1, 1)")
        res = cursor.fetchone()
        assert res[0] is True


# =============================================================================
# ADDITIONAL NUMERIC FUNCTIONS
# =============================================================================


def test_div0null(conn):
    """Test DIV0NULL returns NULL on divide by zero."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT DIV0NULL(10, 0)")
        res = cursor.fetchone()
        assert res[0] is None

        cursor.execute("SELECT DIV0NULL(10, 2)")
        res = cursor.fetchone()
        assert res[0] == 5


def test_ratio_to_report(dialect_context):
    """Test RATIO_TO_REPORT calculates ratio within partition."""
    sql = "SELECT RATIO_TO_REPORT(10) OVER () AS ratio"
    expression = parse_one(sql, read="snowflake")

    from snowduck.dialect import Dialect

    dialect = Dialect(context=dialect_context)
    transpiled = expression.sql(dialect=dialect)
    # Should produce valid SQL (may need window function support)
    assert "OVER" in transpiled


# =============================================================================
# HASH AND ENCODING FUNCTIONS
# =============================================================================


def test_hash(conn):
    """Test HASH returns numeric hash of input."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT HASH('hello')")
        res = cursor.fetchone()
        # Should return a bigint hash
        assert isinstance(res[0], int)


def test_sha2(conn):
    """Test SHA2 returns SHA-256 hash."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT SHA2('hello')")
        res = cursor.fetchone()
        # SHA-256 produces 64 character hex string
        assert len(res[0]) == 64


def test_base64_encode(conn):
    """Test BASE64_ENCODE encodes to base64."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT BASE64_ENCODE('hello')")
        res = cursor.fetchone()
        assert res[0] == "aGVsbG8="


def test_base64_decode(conn):
    """Test BASE64_DECODE decodes from base64."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT BASE64_DECODE_STRING('aGVsbG8=')")
        res = cursor.fetchone()
        assert res[0] == "hello"


# =============================================================================
# HEX ENCODING FUNCTIONS
# =============================================================================


def test_hex_encode(conn):
    """Test HEX_ENCODE encodes to hexadecimal."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT HEX_ENCODE('hello')")
        res = cursor.fetchone()
        assert res[0].lower() == "68656c6c6f"  # 'hello' in hex


def test_hex_decode_string(conn):
    """Test HEX_DECODE_STRING decodes hex to string."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT HEX_DECODE_STRING('68656c6c6f')")
        res = cursor.fetchone()
        assert res[0] == "hello"


# =============================================================================
# TIMEDIFF FUNCTION
# =============================================================================


def test_timediff(conn):
    """Test TIMEDIFF calculates time difference."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT TIMEDIFF(day, '2024-01-01', '2024-01-15')
        """)
        res = cursor.fetchone()
        assert res[0] == 14


def test_timediff_hours(conn):
    """Test TIMEDIFF with hours."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT TIMEDIFF('hour', '2024-01-01 00:00:00', '2024-01-01 12:00:00')
        """)
        res = cursor.fetchone()
        assert res[0] == 12


# =============================================================================
# OBJECT MANIPULATION FUNCTIONS
# =============================================================================


def test_object_insert(conn):
    """Test OBJECT_INSERT adds key-value to JSON object."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT OBJECT_INSERT(PARSE_JSON('{"a": 1}'), 'b', 2)
        """)
        res = cursor.fetchone()
        import json

        obj = json.loads(res[0])
        assert obj["a"] == 1
        assert obj["b"] == 2
