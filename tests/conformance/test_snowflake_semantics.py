"""Executable specification of Snowflake semantics.

Each case pairs a Snowflake statement with the result Snowflake's own
documentation specifies. These are the behaviours that differ from a naive
DuckDB translation - the cases where the wrong answer comes back silently
rather than as an error - so they are pinned here rather than left to the
per-function unit tests.
"""

import pytest

# (id, sql, expected)
CASES: list[tuple[str, str, object]] = [
    # -- Conversion ---------------------------------------------------------
    # TO_NUMBER defaults to NUMBER(38, 0); reducing scale rounds, not truncates.
    ("to_number_default_scale", "SELECT TO_NUMBER('123.45')", 123),
    ("to_number_rounds_half_away", "SELECT TO_NUMBER('-2.5')", -3),
    ("to_number_precision_scale", "SELECT TO_NUMBER('123.45', 10, 2)", "123.45"),
    ("to_decimal_alias", "SELECT TO_DECIMAL('7.4', 10, 1)", "7.4"),
    ("try_to_number_bad_input", "SELECT TRY_TO_NUMBER('abc')", None),
    ("to_boolean", "SELECT TO_BOOLEAN('true')", True),
    # -- Regular expressions ------------------------------------------------
    # "The function implicitly anchors a pattern at both ends."
    ("regexp_like_anchors", "SELECT REGEXP_LIKE('xabcx', 'a.c')", False),
    ("regexp_like_full", "SELECT REGEXP_LIKE('abc', 'a.c')", True),
    ("regexp_like_wildcards", "SELECT REGEXP_LIKE('xabcx', '.*a.c.*')", True),
    ("rlike_anchors", "SELECT RLIKE('xabcx', 'a.c')", False),
    ("regexp_operator_anchors", "SELECT 'xabcx' REGEXP 'a.c'", False),
    ("regexp_like_case_flag", "SELECT REGEXP_LIKE('ABC', 'a.c', 'i')", True),
    # REGEXP_SUBSTR is not anchored and returns NULL when nothing matches.
    ("regexp_substr_no_match_null", "SELECT REGEXP_SUBSTR('abc', '[0-9]+')", None),
    ("regexp_substr_first", "SELECT REGEXP_SUBSTR('a1b22', '[0-9]+')", "1"),
    (
        "regexp_substr_occurrence",
        "SELECT REGEXP_SUBSTR('a1b22c33', '[0-9]+', 1, 2)",
        "22",
    ),
    (
        "regexp_substr_position",
        "SELECT REGEXP_SUBSTR('a1b22c33', '[0-9]+', 4, 1)",
        "22",
    ),
    ("regexp_substr_past_end", "SELECT REGEXP_SUBSTR('a1', '[0-9]+', 1, 9)", None),
    # 'e' selects a capture group; group_num defaults to 1 when 'e' is given.
    (
        "regexp_substr_group",
        r"SELECT REGEXP_SUBSTR('1-1:1.8.0', '^(\\d+)-(\\d+):(\\d+)', 1, 1, 'e', 3)",
        "1",
    ),
    (
        "regexp_substr_group_default",
        r"SELECT REGEXP_SUBSTR('12-34', '^(\\d+)-(\\d+)', 1, 1, 'e')",
        "12",
    ),
    # occurrence defaults to 0, meaning "replace all".
    (
        "regexp_replace_all_default",
        "SELECT REGEXP_REPLACE('a1b2', '[0-9]', 'X')",
        "aXbX",
    ),
    (
        "regexp_replace_first_only",
        "SELECT REGEXP_REPLACE('a1b2', '[0-9]', 'X', 1, 1)",
        "aXb2",
    ),
    (
        "regexp_replace_position",
        "SELECT REGEXP_REPLACE('a1b2', '[0-9]', 'X', 3)",
        "a1bX",
    ),
    ("regexp_count", "SELECT REGEXP_COUNT('a1b2c3', '[0-9]')", 3),
    ("regexp_count_position", "SELECT REGEXP_COUNT('a1b2c3', '[0-9]', 3)", 2),
    # REGEXP_INSTR is 1-based and returns 0 when there is no match.
    ("regexp_instr", "SELECT REGEXP_INSTR('abc123', '[0-9]+')", 4),
    ("regexp_instr_no_match", "SELECT REGEXP_INSTR('abc', '[0-9]+')", 0),
    ("regexp_instr_occurrence", "SELECT REGEXP_INSTR('a1b2', '[0-9]', 1, 2)", 4),
    ("regexp_instr_option_end", "SELECT REGEXP_INSTR('abc123', '[0-9]+', 1, 1, 1)", 7),
    # -- Semi-structured ----------------------------------------------------
    # OBJECT_CONSTRUCT omits NULL-valued keys; _KEEP_NULL retains them.
    (
        "object_construct_drops_null",
        "SELECT OBJECT_CONSTRUCT('a', 1, 'b', NULL)",
        '{"a":1}',
    ),
    (
        "object_construct_keep_null",
        "SELECT OBJECT_CONSTRUCT_KEEP_NULL('a', 1, 'b', NULL)",
        '{"a":1,"b":null}',
    ),
    (
        "object_delete",
        "SELECT OBJECT_DELETE(OBJECT_CONSTRUCT('a', 1, 'b', 2), 'a')",
        '{"b":2}',
    ),
    (
        "object_pick",
        "SELECT OBJECT_PICK(OBJECT_CONSTRUCT('a', 1, 'b', 2), 'a')",
        '{"a":1}',
    ),
    ("is_array", "SELECT IS_ARRAY(PARSE_JSON('[1]'))", True),
    ("is_object", "SELECT IS_OBJECT(PARSE_JSON('[1]'))", False),
    ("is_null_value", "SELECT IS_NULL_VALUE(PARSE_JSON('null'))", True),
    ("as_varchar_unquotes", "SELECT AS_VARCHAR(PARSE_JSON('\"x\"'))", "x"),
    ("array_size", "SELECT ARRAY_SIZE(ARRAY_CONSTRUCT(1, 2, 3))", 3),
    # GET uses 0-based indexing on arrays.
    ("get_is_zero_based", "SELECT GET(ARRAY_CONSTRUCT('a', 'b', 'c'), 1)", "b"),
    # -- Date & time --------------------------------------------------------
    # DAYNAME/MONTHNAME return three-letter abbreviations.
    ("dayname_abbreviated", "SELECT DAYNAME(TO_DATE('2024-01-01'))", "Mon"),
    ("monthname_abbreviated", "SELECT MONTHNAME(TO_DATE('2024-01-01'))", "Jan"),
    ("next_day", "SELECT NEXT_DAY(TO_DATE('2024-01-01'), 'Fri')", "2024-01-05"),
    (
        "next_day_same_weekday_skips",
        "SELECT NEXT_DAY(TO_DATE('2024-01-01'), 'Mon')",
        "2024-01-08",
    ),
    ("previous_day", "SELECT PREVIOUS_DAY(TO_DATE('2024-01-08'), 'Fri')", "2024-01-05"),
    (
        "datediff",
        "SELECT DATEDIFF(day, TO_DATE('2024-01-01'), TO_DATE('2024-03-01'))",
        60,
    ),
    # -- Strings ------------------------------------------------------------
    ("insert_splices", "SELECT INSERT('abcdef', 2, 3, 'zzz')", "azzzef"),
    ("rtrimmed_length", "SELECT RTRIMMED_LENGTH('ab   ')", 2),
    ("split_part_negative", "SELECT SPLIT_PART('a,b,c', ',', -1)", "c"),
    ("initcap_delimiters", "SELECT INITCAP('ab-cd', '-')", "Ab-Cd"),
    ("editdistance", "SELECT EDITDISTANCE('kitten', 'sitting')", 3),
    # -- Conditional & boolean ---------------------------------------------
    ("booland", "SELECT BOOLAND(1, 1)", True),
    ("boolxor", "SELECT BOOLXOR(1, 0)", True),
    ("equal_null", "SELECT EQUAL_NULL(NULL, NULL)", True),
    ("div0", "SELECT DIV0(1, 0)", 0),
    ("iff", "SELECT IFF(1 = 1, 'y', 'n')", "y"),
    # -- NULL handling ------------------------------------------------------
    # "Unlike some implementations of the CONCAT_WS function, the Snowflake
    # CONCAT_WS function doesn't skip NULL values."
    ("concat_ws_null_propagates", "SELECT CONCAT_WS('-', 'a', NULL, 'c')", None),
    ("concat_ws_no_nulls", "SELECT CONCAT_WS('-', 'a', 'b')", "a-b"),
    # DIV0NULL returns 0 - not NULL - when the divisor is 0 or NULL.
    ("div0null_zero_divisor", "SELECT DIV0NULL(10, 0)", 0),
    ("div0null_null_divisor", "SELECT DIV0NULL(10, NULL)", 0),
    # -- Semi-structured edge cases -----------------------------------------
    # NULLs are dropped only at the top level; a nested null survives.
    (
        "object_construct_nested_null_kept",
        "SELECT OBJECT_CONSTRUCT('a', OBJECT_CONSTRUCT_KEEP_NULL('x', NULL))",
        '{"a":{"x":null}}',
    ),
    ("object_construct_empty", "SELECT OBJECT_CONSTRUCT()", "{}"),
    # Snowflake arrays are heterogeneous.
    (
        "array_construct_mixed_types",
        "SELECT ARRAY_SIZE(ARRAY_CONSTRUCT(1, 'two', 3.5))",
        3,
    ),
    (
        "array_contains_string",
        "SELECT ARRAY_CONTAINS('two', ARRAY_CONSTRUCT(1, 'two'))",
        True,
    ),
    # -- Documented quirks --------------------------------------------------
    # "The similarity computation is case-insensitive."
    (
        "jarowinkler_is_case_insensitive",
        "SELECT JAROWINKLER_SIMILARITY('ABC', 'abd') = JAROWINKLER_SIMILARITY('abc', 'abd')",
        True,
    ),
    ("jarowinkler_is_a_percentage", "SELECT JAROWINKLER_SIMILARITY('abc', 'abc')", 100),
    # BOOLAND rounds floating point, so a fraction below 0.5 counts as zero -
    # the docs show BOOLAND(-0.4, 5) returning FALSE.
    ("booland_rounds_fractions", "SELECT BOOLAND(-0.4, 5)", False),
    ("booland_non_zero", "SELECT BOOLAND(2, 3)", True),
    ("boolxor", "SELECT BOOLXOR(1, 0)", True),
    # -- Predicates ---------------------------------------------------------
    ("like_any", "SELECT 'abc' LIKE ANY ('a%', 'z%')", True),
    ("like_any_no_match", "SELECT 'abc' LIKE ANY ('y%', 'z%')", False),
    ("like_all", "SELECT 'abc' LIKE ALL ('a%', '%c')", True),
    ("like_all_partial", "SELECT 'abc' LIKE ALL ('a%', 'z%')", False),
]


@pytest.mark.parametrize(
    "sql,expected",
    [(sql, expected) for _, sql, expected in CASES],
    ids=[case_id for case_id, _, _ in CASES],
)
def test_snowflake_semantics(conn, sql, expected):
    with conn.cursor() as cur:
        cur.execute(sql)
        actual = cur.fetchone()[0]

    if expected is None:
        assert actual is None
        return
    if isinstance(expected, bool):
        assert actual is expected
        return
    # Compare through str so DECIMAL/date results match their documented form.
    assert actual == expected or str(actual) == str(expected)
