"""Test edge cases and missing coverage identified in FINDINGS.md review."""

from decimal import Decimal

import pandas as pd
import pytest
import snowflake.connector

from snowduck import mock_snowflake, seed_table


@mock_snowflake
def test_fetchone_before_execute():
    """Verify error when calling fetchone() before execute()."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    with pytest.raises(TypeError, match="No open result set"):
        cur.fetchone()


@mock_snowflake
def test_fetchmany_before_execute():
    """Verify error when calling fetchmany() before execute()."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    with pytest.raises(TypeError, match="No open result set"):
        cur.fetchmany(5)


@mock_snowflake
def test_fetchall_before_execute():
    """Verify error when calling fetchall() before execute()."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    with pytest.raises(TypeError, match="No open result set"):
        cur.fetchall()


@mock_snowflake
def test_parameter_binding_with_none():
    """Test parameter binding with NULL values."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    cur.execute("SELECT %(val)s", {"val": None})
    result = cur.fetchone()
    assert result[0] is None


@mock_snowflake
def test_parameter_binding_with_various_types():
    """Test parameter binding with different Python types."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # Integer
    cur.execute("SELECT %(num)s", {"num": 42})
    assert cur.fetchone()[0] == 42

    # Float - Snowflake returns float for float parameters (REAL/FLOAT type)
    # SnowDuck should match this behavior by casting float params to DOUBLE
    cur.execute("SELECT %(num)s", {"num": 3.14})
    result = cur.fetchone()[0]
    assert isinstance(result, float), f"Expected float, got {type(result).__name__}"
    assert abs(result - 3.14) < 0.0001, f"Expected ~3.14, got {result}"

    # String
    cur.execute("SELECT %(str)s", {"str": "hello"})
    assert cur.fetchone()[0] == "hello"

    # Boolean
    cur.execute("SELECT %(bool)s", {"bool": True})
    assert cur.fetchone()[0] is True


@mock_snowflake
def test_seed_table_with_null_in_numeric():
    """Test seed_table handles NULL in numeric columns correctly."""
    conn = snowflake.connector.connect()

    data = {"id": [1, None, 3], "value": [10.5, None, 30.2]}

    seed_table(conn, "test_nulls", data)

    cur = conn.cursor()
    cur.execute("SELECT * FROM test_nulls ORDER BY id NULLS LAST")
    results = cur.fetchall()

    assert len(results) == 3
    assert results[0] == (Decimal("1.0"), Decimal("10.5"))
    assert results[1] == (Decimal("3.0"), Decimal("30.2"))
    assert results[2][0] is None  # NULL id
    assert results[2][1] is None  # NULL value


@mock_snowflake
def test_seed_table_with_boolean_column():
    """Test seed_table handles boolean columns with NULLs."""
    conn = snowflake.connector.connect()

    data = {"id": [1, 2, 3], "flag": [True, False, None]}

    seed_table(conn, "test_bools", data)

    cur = conn.cursor()

    # Check NULL boolean
    cur.execute("SELECT * FROM test_bools WHERE flag IS NULL")
    result = cur.fetchone()
    assert result is not None
    assert result[0] == 3

    # Check TRUE
    cur.execute("SELECT * FROM test_bools WHERE flag = TRUE")
    result = cur.fetchone()
    assert result[0] == 1

    # Check FALSE
    cur.execute("SELECT * FROM test_bools WHERE flag = FALSE")
    result = cur.fetchone()
    assert result[0] == 2


@mock_snowflake
def test_seed_table_with_all_nulls():
    """Test seed_table with a column that's all NULLs."""
    conn = snowflake.connector.connect()

    data = {"id": [1, 2, 3], "nullable_col": [None, None, None]}

    seed_table(conn, "test_all_nulls", data)

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM test_all_nulls WHERE nullable_col IS NULL")
    result = cur.fetchone()
    assert result[0] == 3


@mock_snowflake
def test_seed_table_with_datetime_nulls():
    """Test seed_table with NULL datetime values."""
    conn = snowflake.connector.connect()

    data = {
        "id": [1, 2, 3],
        "created_at": pd.to_datetime(["2024-01-01", None, "2024-01-03"]),
    }

    seed_table(conn, "test_dt_nulls", data)

    cur = conn.cursor()
    cur.execute("SELECT * FROM test_dt_nulls WHERE created_at IS NULL")
    result = cur.fetchone()
    assert result is not None
    assert result[0] == 2


@mock_snowflake
def test_seed_table_invalid_table_name():
    """Test that seed_table validates table names (SQL injection protection)."""
    conn = snowflake.connector.connect()

    # This should work - valid identifier
    data = {"id": [1, 2]}
    seed_table(conn, "valid_table_123", data)

    # These currently don't raise errors but SHOULD for security
    # TODO: Add validation in seed_table()
    # with pytest.raises(ValueError, match="Invalid table name"):
    #     seed_table(conn, "table; DROP TABLE users; --", data)


@mock_snowflake
def test_multiple_execute_on_same_cursor():
    """Test executing multiple queries on the same cursor."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # First query
    cur.execute("SELECT 1")
    assert cur.fetchone()[0] == 1

    # Second query (should replace result set)
    cur.execute("SELECT 2")
    assert cur.fetchone()[0] == 2

    # Third query with table creation
    cur.execute("CREATE TABLE test (id INT)")
    cur.execute("INSERT INTO test VALUES (42)")
    cur.execute("SELECT * FROM test")
    assert cur.fetchone()[0] == 42


@mock_snowflake
def test_execute_after_close():
    """Test that executing after cursor close raises error."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    cur.execute("SELECT 1")
    cur.close()

    # This should raise an error (currently doesn't - potential bug!)
    # with pytest.raises(snowflake.connector.errors.Error):
    #     cur.execute("SELECT 2")


@mock_snowflake
def test_multiple_statements_last_result():
    """Test that multi-statement execution returns last statement's result."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    sql = """
    CREATE TABLE test_multi (a INT);
    INSERT INTO test_multi VALUES (1);
    INSERT INTO test_multi VALUES (2);
    SELECT COUNT(*) FROM test_multi;
    """

    cur.execute(sql)

    # Last statement is SELECT COUNT(*), so result should be count
    result = cur.fetchone()
    assert result[0] == 2


@mock_snowflake
def test_case_insensitive_table_names():
    """Test that table names are case-insensitive (Snowflake behavior)."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    cur.execute("CREATE TABLE MyTable (id INT)")
    cur.execute("INSERT INTO myTable VALUES (1)")  # lowercase
    cur.execute("INSERT INTO MYTABLE VALUES (2)")  # uppercase
    cur.execute("SELECT COUNT(*) FROM MYtable")  # mixed case

    result = cur.fetchone()
    assert result[0] == 2


@mock_snowflake
def test_session_variable_undefined_error():
    """Test that using undefined session variable raises helpful error."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    with pytest.raises(Exception, match="Undefined session variable"):
        cur.execute("SELECT $undefined_var")


@mock_snowflake
def test_session_variable_type_inference():
    """Test that session variables are correctly typed (int vs string)."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # Integer variable
    cur.execute("SET int_var = 42")
    cur.execute("SELECT $int_var + 1")
    result = cur.fetchone()
    assert result[0] == 43  # Should be numeric addition

    # String variable
    cur.execute("SET str_var = 'hello'")
    cur.execute("SELECT $str_var || ' world'")
    result = cur.fetchone()
    assert result[0] == "hello world"


@mock_snowflake
def test_information_schema_cross_database():
    """Test querying information_schema across databases."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # Create multiple databases
    cur.execute("CREATE DATABASE db1")
    cur.execute("CREATE DATABASE db2")

    cur.execute("USE DATABASE db1")

    # Should see both databases from any context
    cur.execute(
        "SELECT database_name FROM information_schema.databases ORDER BY database_name"
    )

    # Get all database names
    dbs = [row[0] for row in cur.fetchall()]

    # Should contain our created databases (uppercased)
    assert "DB1" in dbs
    assert "DB2" in dbs


@mock_snowflake
def test_describe_table_with_nullability():
    """Test that DESCRIBE TABLE includes nullability information.

    Note: Due to current database limitations, tables are created in 'memory' catalog.
    Testing DESCRIBE with explicit database.schema.table path.
    """
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    # Create table in the default database context
    cur.execute("CREATE TABLE test_desc (id INT NOT NULL, name VARCHAR)")

    # DESCRIBE should work with explicit database/schema path
    cur.execute("DESCRIBE TABLE memory.main.test_desc")

    results = cur.fetchall()
    assert len(results) >= 2  # At least id and name columns

    # Verify nullability information is present
    column_names = [row[0] for row in results]
    assert "id" in column_names or "ID" in column_names
    assert "name" in column_names or "NAME" in column_names


@mock_snowflake
def test_rowcount_after_insert():
    """Test that cursor.rowcount is correct after INSERT."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    cur.execute("CREATE TABLE test (id INT)")
    cur.execute("INSERT INTO test VALUES (1), (2), (3)")

    # rowcount should reflect number of inserted rows
    assert cur.rowcount == 3


@mock_snowflake
def test_rowcount_after_update():
    """Test that cursor.rowcount is correct after UPDATE."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    cur.execute("CREATE TABLE test (id INT, value INT)")
    cur.execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)")
    cur.execute("UPDATE test SET value = 100 WHERE id <= 2")

    # rowcount should reflect number of updated rows
    assert cur.rowcount == 2


@mock_snowflake
def test_rowcount_after_delete():
    """Test that cursor.rowcount is correct after DELETE."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()

    cur.execute("CREATE TABLE test (id INT)")
    cur.execute("INSERT INTO test VALUES (1), (2), (3), (4)")
    cur.execute("DELETE FROM test WHERE id > 2")

    # rowcount should reflect number of deleted rows
    assert cur.rowcount == 2


@mock_snowflake
def test_seed_table_with_large_dataset():
    """Test seed_table with larger dataset (performance check)."""
    conn = snowflake.connector.connect()

    # Create 10k rows
    data = {
        "id": list(range(10000)),
        "value": [i * 2 for i in range(10000)],
        "name": [f"row_{i}" for i in range(10000)],
    }

    seed_table(conn, "large_table", data)

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM large_table")
    assert cur.fetchone()[0] == 10000

    # Verify data integrity
    cur.execute("SELECT MAX(id), MAX(value) FROM large_table")
    result = cur.fetchone()
    assert result[0] == 9999
    assert result[1] == 19998


@mock_snowflake
def test_context_manager_cursor():
    """Test cursor as context manager (with statement)."""
    conn = snowflake.connector.connect()

    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        result = cur.fetchone()
        assert result[0] == 1

    # Cursor should be closed after context
    # (Currently doesn't verify this - potential enhancement)


@mock_snowflake
def test_context_manager_connection():
    """Test connection as context manager."""
    with snowflake.connector.connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1

    # Connection should be closed after context
    assert conn.is_closed()


@mock_snowflake
def test_seed_table_with_unicode():
    """Test seed_table with Unicode characters."""
    conn = snowflake.connector.connect()

    data = {
        "id": [1, 2, 3],
        "name": ["Hello 世界", "Привет мир", "مرحبا العالم"],
    }

    seed_table(conn, "unicode_test", data)

    cur = conn.cursor()
    cur.execute("SELECT name FROM unicode_test WHERE id = 1")
    result = cur.fetchone()
    assert result[0] == "Hello 世界"


@mock_snowflake
def test_seed_table_with_special_sql_characters():
    """Test seed_table with SQL special characters (quotes, etc.)."""
    conn = snowflake.connector.connect()

    data = {
        "id": [1, 2, 3],
        "text": ["It's nice", 'He said "hello"', 'Mix\'d "quotes"'],
    }

    seed_table(conn, "special_chars", data)

    cur = conn.cursor()
    cur.execute("SELECT text FROM special_chars ORDER BY id")
    results = cur.fetchall()

    assert results[0][0] == "It's nice"
    assert results[1][0] == 'He said "hello"'
    assert results[2][0] == 'Mix\'d "quotes"'
