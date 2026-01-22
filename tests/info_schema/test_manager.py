from unittest.mock import mock_open, patch

import pytest

from snowduck.info_schema import InfoSchemaManager, load_sql


def test_load_sql_valid_params():
    with patch("builtins.open", new_callable=mock_open, read_data="SELECT * FROM {table}"):
        sql = load_sql("query.sql", table="users")
        assert sql == "SELECT * FROM users"


def test_load_sql_invalid_param_type():
    with patch("builtins.open", new_callable=mock_open, read_data="SELECT * FROM {table}"):
        with pytest.raises(ValueError, match="Invalid parameter type"):
            load_sql("query.sql", table={"invalid": "type"})


def test_load_sql_unsafe_characters():
    with patch("builtins.open", new_callable=mock_open, read_data="SELECT * FROM {table}"):
        with pytest.raises(ValueError, match="Unsafe characters detected"):
            load_sql("query.sql", table="users; DROP TABLE users;")


def test_attach_account_database(in_memory_duckdb_connection):
    info_schema_manager = InfoSchemaManager(in_memory_duckdb_connection)
    result = in_memory_duckdb_connection.execute("SHOW DATABASES").fetchall()
    assert (info_schema_manager.account_catalog_name,) in result


def test_create_account_information_schema(in_memory_duckdb_connection):
    info_schema_manager = InfoSchemaManager(in_memory_duckdb_connection)
    result = in_memory_duckdb_connection.execute("SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ?", [info_schema_manager.account_catalog_name]).fetchall()
    assert (info_schema_manager.info_schema_name,) in result
    

def test_has_database(in_memory_duckdb_connection):
    info_schema_manager = InfoSchemaManager(in_memory_duckdb_connection)
    result = info_schema_manager.has_database("test_db")
    assert result is False
    info_schema_manager.create_database_information_schema(database="test_db")
    result = info_schema_manager.has_database("test_db")
    assert result is True


def test_has_schema(in_memory_duckdb_connection):
    info_schema_manager = InfoSchemaManager(in_memory_duckdb_connection)
    result = info_schema_manager.has_schema("test_db", "test_schema")
    assert result is False
    info_schema_manager.create_database_information_schema(database="test_db", schema="test_schema")
    result = info_schema_manager.has_schema("test_db", "test_schema")
    assert result is True
