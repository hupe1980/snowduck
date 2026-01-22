"""Test file-based storage persistence."""

import tempfile
from pathlib import Path

import pytest

from snowduck import patch_snowflake


def test_file_based_persistence():
    """Test that data persists across connections when using file-based storage."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = str(Path(tmpdir) / "test_persistence.duckdb")

        # First connection - create and populate table
        with patch_snowflake(db_file=db_file):
            import snowflake.connector

            with snowflake.connector.connect() as conn:
                cursor = conn.cursor()
                cursor.execute("CREATE TABLE test_persist (id INT, name VARCHAR)")
                cursor.execute(
                    "INSERT INTO test_persist VALUES (1, 'Alice'), (2, 'Bob')"
                )
                cursor.execute("SELECT COUNT(*) FROM test_persist")
                count = cursor.fetchone()[0]
                assert count == 2

        # Second connection - verify data persists
        with patch_snowflake(db_file=db_file):
            import snowflake.connector

            with snowflake.connector.connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM test_persist ORDER BY id")
                results = cursor.fetchall()
                assert len(results) == 2
                assert results[0] == (1, "Alice")
                assert results[1] == (2, "Bob")


def test_in_memory_isolation():
    """Test that in-memory connections are isolated."""
    with patch_snowflake():
        import snowflake.connector

        # First connection
        with snowflake.connector.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE test_isolated (id INT)")
            cursor.execute("INSERT INTO test_isolated VALUES (1)")

        # Second connection - table should NOT exist (new in-memory DB)
        with snowflake.connector.connect() as conn:
            cursor = conn.cursor()
            with pytest.raises(Exception, match="does not exist"):
                cursor.execute("SELECT * FROM test_isolated")


def test_multiple_connections_to_same_file():
    """Test multiple simultaneous connections to the same file work."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = str(Path(tmpdir) / "test_multi.duckdb")

        with patch_snowflake(db_file=db_file):
            import snowflake.connector

            # First connection creates table
            with snowflake.connector.connect() as conn1:
                cursor1 = conn1.cursor()
                cursor1.execute("CREATE TABLE shared_data (value INT)")
                cursor1.execute("INSERT INTO shared_data VALUES (42)")

            # Second connection can read the data
            with snowflake.connector.connect() as conn2:
                cursor2 = conn2.cursor()
                cursor2.execute("SELECT * FROM shared_data")
                result = cursor2.fetchone()
                assert result[0] == 42


def test_reset_clears_existing_database():
    """Test that reset=True deletes existing database file and creates fresh DB."""
    import snowflake.connector

    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = str(Path(tmpdir) / "test_reset.duckdb")

        # Create database with data using patch_snowflake context
        with patch_snowflake(db_file=db_file):
            conn = snowflake.connector.connect()
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE old_data (id INT)")
            cursor.execute("INSERT INTO old_data VALUES (123)")
            conn.close()

        # Verify file was created
        assert Path(db_file).exists(), "Database file should exist after creation"

        # Use reset=True to start fresh
        with patch_snowflake(db_file=db_file, reset=True):
            conn = snowflake.connector.connect()
            cursor = conn.cursor()

            # Old table should not exist in the fresh database
            with pytest.raises(Exception, match="does not exist"):
                cursor.execute("SELECT * FROM old_data")

            # Can create new tables in fresh database
            cursor.execute("CREATE TABLE new_data (value VARCHAR)")
            cursor.execute("INSERT INTO new_data VALUES ('fresh')")
            cursor.execute("SELECT * FROM new_data")
            assert cursor.fetchone()[0] == "fresh"

            conn.close()
