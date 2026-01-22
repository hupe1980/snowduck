"""Test data seeding utilities."""

import pandas as pd
import pytest

from snowduck import patch_snowflake, seed_table


def test_seed_table_from_dict():
    """Test seeding a table from a dict of lists."""
    with patch_snowflake():
        import snowflake.connector

        conn = snowflake.connector.connect()

        data = {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "value": [100, 200, 300],
        }

        rows = seed_table(conn, "test_data", data)

        assert rows == 3

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_data")
        assert cursor.fetchone()[0] == 3

        cursor.execute("SELECT * FROM test_data ORDER BY id")
        results = cursor.fetchall()
        assert results[0][1] == "Alice"
        assert results[1][2] == 200


def test_seed_table_from_dataframe():
    """Test seeding a table from a pandas DataFrame."""
    with patch_snowflake():
        import snowflake.connector

        conn = snowflake.connector.connect()

        df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "created": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            }
        )

        rows = seed_table(conn, "users", df)

        assert rows == 2

        cursor = conn.cursor()
        cursor.execute("SELECT name FROM users WHERE id = 2")
        assert cursor.fetchone()[0] == "Bob"


def test_seed_table_from_list_of_dicts():
    """Test seeding from list of dictionaries."""
    with patch_snowflake():
        import snowflake.connector

        conn = snowflake.connector.connect()

        data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 87},
        ]

        rows = seed_table(conn, "scores", data)

        assert rows == 2

        cursor = conn.cursor()
        cursor.execute("SELECT score FROM scores WHERE name = 'Alice'")
        assert cursor.fetchone()[0] == 95


def test_seed_table_replaces_existing():
    """Test that seed_table drops existing table by default."""
    with patch_snowflake():
        import snowflake.connector

        conn = snowflake.connector.connect()
        cursor = conn.cursor()

        # Create initial table
        seed_table(conn, "test", {"id": [1, 2]})
        cursor.execute("SELECT COUNT(*) FROM test")
        assert cursor.fetchone()[0] == 2

        # Seed again - should replace
        seed_table(conn, "test", {"id": [10, 20, 30]})
        cursor.execute("SELECT COUNT(*) FROM test")
        assert cursor.fetchone()[0] == 3


def test_seed_table_handles_null_values():
    """Test that NULL values are handled correctly."""
    with patch_snowflake():
        import snowflake.connector

        conn = snowflake.connector.connect()

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", None, "Carol"]})

        rows = seed_table(conn, "users", df)
        assert rows == 3

        cursor = conn.cursor()
        cursor.execute("SELECT name FROM users WHERE id = 2")
        result = cursor.fetchone()
        assert result[0] is None


def test_seed_table_handles_special_characters():
    """Test that strings with quotes are properly escaped."""
    with patch_snowflake():
        import snowflake.connector

        conn = snowflake.connector.connect()

        data = {"id": [1, 2], "description": ["It's great", 'Bob\'s "stuff"']}

        rows = seed_table(conn, "items", data)
        assert rows == 2

        cursor = conn.cursor()
        cursor.execute("SELECT description FROM items WHERE id = 1")
        assert cursor.fetchone()[0] == "It's great"


def test_seed_table_empty_data_raises():
    """Test that seeding with empty data raises an error."""
    with patch_snowflake():
        import snowflake.connector

        conn = snowflake.connector.connect()

        with pytest.raises(ValueError, match="empty data"):
            seed_table(conn, "test", pd.DataFrame())
