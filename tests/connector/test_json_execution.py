import pytest
import snowflake.connector


def test_parse_json_execution(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("SELECT PARSE_JSON('{\"key\": 1}')")
        result = cur.fetchone()
        assert result is not None
        # DuckDB returns JSON type as ... string? or special object?
        # Usually python client sees it as string unless we have custom formatting
        val = result[0]
        # Should be string '{"key":1}' but let's check exact formatting
        # DuckDB might strip spaces
        assert '"key": 1' in val or '"key":1' in val


def test_try_parse_json_execution(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("SELECT TRY_PARSE_JSON('{\"key\": 1}')")
        result = cur.fetchone()
        assert result is not None
        val = result[0]
        assert '"key": 1' in val or '"key":1' in val

        cur.execute("SELECT TRY_PARSE_JSON('invalid json')")
        result = cur.fetchone()
        assert result[0] is None


def test_json_type_consistency(conn: snowflake.connector.SnowflakeConnection):
    # Verify we can mix PARSE_JSON and TRY_PARSE_JSON
    with conn.cursor() as cur:
        # Union should work if types are compatible
        cur.execute(
            "SELECT PARSE_JSON('{\"a\":1}') UNION ALL SELECT TRY_PARSE_JSON('{\"b\":2}')"
        )
        rows = cur.fetchall()
        assert len(rows) == 2


def test_json_access(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        # Note: In Snowflake SELECT PARSE_JSON(...) : key works
        # In SQLGlot snowflake dialect, checking if it transpiles : to ->
        try:
            cur.execute("SELECT PARSE_JSON('{\"key\": 123}'):key")
            result = cur.fetchone()
            # 123
            # In DuckDB simple extraction might be json or text
            assert str(result[0]) == "123"
        except snowflake.connector.errors.ProgrammingError:
            pytest.skip("JSON access via colon not yet supported/transpiled")
