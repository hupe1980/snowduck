"""Tests for Snowflake SQL UDFs (CREATE FUNCTION -> DuckDB macro)."""

import pytest
import snowflake.connector


def test_udf_evaluates_body(conn: snowflake.connector.SnowflakeConnection):
    """A SQL UDF must evaluate its body, not return the body as text."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE OR REPLACE FUNCTION is_valid_len(s VARCHAR)
            RETURNS BOOLEAN
            AS $$ LENGTH(s) = 11 $$
        """)
        cur.execute("SELECT is_valid_len('12345678901'), is_valid_len('123')")
        assert cur.fetchone() == (True, False)


def test_udf_body_is_translated(conn: snowflake.connector.SnowflakeConnection):
    """Snowflake functions inside a UDF body are transpiled too."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE OR REPLACE FUNCTION first_two_sum(s VARCHAR)
            RETURNS NUMBER
            AS $$ TO_NUMBER(SUBSTR(s, 1, 1), 38, 0) + TO_NUMBER(SUBSTR(s, 2, 1), 38, 0) $$
        """)
        cur.execute("SELECT first_two_sum('47')")
        assert cur.fetchone()[0] == 11


def test_udf_quoted_body(conn: snowflake.connector.SnowflakeConnection):
    """A UDF body given as a plain string literal works like a $$-quoted one."""
    with conn.cursor() as cur:
        cur.execute(
            "CREATE OR REPLACE FUNCTION double_it(n NUMBER) RETURNS NUMBER AS 'n * 2'"
        )
        cur.execute("SELECT double_it(21)")
        assert cur.fetchone()[0] == 42


def test_udf_create_status_message(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE OR REPLACE FUNCTION noop_fn(n NUMBER) RETURNS NUMBER AS $$ n $$"
        )
        assert "successfully created" in cur.fetchone()[0]


def test_drop_function(conn: snowflake.connector.SnowflakeConnection):
    """Snowflake identifies a UDF by signature; DuckDB macros by name only."""
    with conn.cursor() as cur:
        cur.execute(
            "CREATE OR REPLACE FUNCTION tmp_fn(a VARCHAR) RETURNS NUMBER AS $$ 1 $$"
        )
        cur.execute("DROP FUNCTION tmp_fn(VARCHAR)")

        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            cur.execute("SELECT tmp_fn('x')")


def test_non_sql_udf_raises_clean_error(conn: snowflake.connector.SnowflakeConnection):
    """A JavaScript UDF cannot be emulated - fail loudly instead of silently wrong."""
    with conn.cursor() as cur:
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as exc:
            cur.execute("""
                CREATE OR REPLACE FUNCTION js_fn(a FLOAT)
                RETURNS FLOAT
                LANGUAGE JAVASCRIPT
                AS $$ return a $$
            """)

        assert "JAVASCRIPT" in str(exc.value)
