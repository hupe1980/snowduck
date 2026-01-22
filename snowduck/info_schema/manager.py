import os
import re
from typing import Any

from duckdb import DuckDBPyConnection

from ..helper import load_sql

ACCOUNT_CATALOG_NAME = "_snowduck_account"
INFO_SCHEMA_NAME = "_information_schema"


class InfoSchemaManager:
    def __init__(
        self, 
        duck_conn: DuckDBPyConnection,
        account_catalog_name: str = ACCOUNT_CATALOG_NAME,
        info_schema_name: str = INFO_SCHEMA_NAME,
    ) -> None:
        """
        Initializes the InfoSchemaManager with a DuckDB connection.
        """
        self._duck_conn = duck_conn
        self._account_catalog_name = account_catalog_name
        self._info_schema_name = info_schema_name
        self._columns_cache: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        self._attach_account_database()
        self._create_account_information_schema()
    
    @property
    def account_catalog_name(self) -> str:
        """
        Returns the name of the account catalog.
        """
        return self._account_catalog_name
    
    @property
    def info_schema_name(self) -> str:
        """
        Returns the name of the information schema.
        """
        return self._info_schema_name

    def _attach_account_database(self) -> None:
        """
        Attaches the account database if it does not already exist.
        """
        # Check if database is already attached (important for file-based storage)
        databases = self._execute_sql("SELECT database_name FROM duckdb_databases()").fetchall()
        db_names = [db[0] for db in databases]
        
        if self.account_catalog_name not in db_names:
            self._execute_sql(f"ATTACH DATABASE ':memory:' AS {self.account_catalog_name}")

    def _create_account_information_schema(self) -> None:
        """
        Creates the account information schema.
        """
        sql = load_sql(
            self._get_filepath("account_information_schema.sql"), 
            account_catalog_name=self.account_catalog_name, 
            info_schema_name=self.info_schema_name,
        )
        self._execute_sql(sql)

    def has_database(self, database: str) -> bool:
        """
        Checks if a database exists by querying the information schema.
        """
        query = """
            SELECT 1
            FROM information_schema.schemata
            WHERE upper(catalog_name) = upper($database)
        """
        result = self._execute_sql(query, database=database).fetchone()
        return result is not None

    def has_schema(self, database: str, schema: str) -> bool:
        """
        Checks if a schema exists in the specified database.
        """
        query = """
            SELECT 1
            FROM information_schema.schemata
            WHERE upper(catalog_name) = upper($database)
              AND upper(schema_name) = upper($schema)
        """
        result = self._execute_sql(query, database=database, schema=schema).fetchone()
        return result is not None

    def create_database_information_schema(self, *, database: str , schema: str | None = None) -> None:
        """
        Creates the database-specific information schema for the specified database.
        """
        if not database:
            raise ValueError("Database name is required")
        
        if not database.isidentifier():
            raise ValueError(f"Invalid database name: {database}")
       
        if not self.has_database(database):
            self._execute_sql(f"ATTACH DATABASE ':memory:' AS {database}")
            sql = load_sql(
                self._get_filepath("database_information_schema.sql"),
                account_catalog_name=self.account_catalog_name, 
                info_schema_name=self.info_schema_name, 
                database=database, 
            )
            self._execute_sql(sql)

        if schema:
            if not schema.isidentifier():
                raise ValueError(f"Invalid schema name: {schema}")
        
            if not self.has_schema(database, schema):
                self._execute_sql(f"CREATE SCHEMA {database}.{schema}")
            
            self._execute_sql(f"SET SCHEMA='{database}.{schema}'")

    def describe_info_schema_sql(self, view: str) -> str:
        """
        Returns the SQL to describe a specific view in the information schema.
        """
        return load_sql(
            self._get_filepath("describe_info_schema.sql"),
            view=view,
        )
    
    def describe_table_sql(self, database: str, schema: str, table: str) -> str:
        return load_sql(
            self._get_filepath("database_information_schema.sql"),
            account_catalog_name=self.account_catalog_name, 
            info_schema_name=self.info_schema_name, 
            database=database,
            schema=schema,
            table=table, 
        )
    
    def show_databases_sql(self) -> str:
        """
        Returns the SQL to show all databases.
        """
        return load_sql(
            self._get_filepath("show_databases.sql"),
            account_catalog_name=self.account_catalog_name, 
        )

    def get_table_columns(self, *, database: str, schema: str, table: str) -> list[dict[str, Any]]:
        """
        Returns column metadata for a table from the account information schema.
        """
        key = (database.upper(), schema.upper(), table.upper())
        if key in self._columns_cache:
            return self._columns_cache[key]
        query = f"""
            SELECT
                column_name,
                is_nullable,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                data_type
            FROM {self.account_catalog_name}.{self.info_schema_name}._columns
            WHERE upper(table_catalog) = upper($database)
                AND upper(table_schema) = upper($schema)
                AND upper(table_name) = upper($table)
            ORDER BY ordinal_position
        """
        rows = self._execute_sql(query, database=database, schema=schema, table=table).fetchall()
        if not rows:
            fallback = """
                SELECT
                    column_name,
                    is_nullable,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    data_type
                FROM system.information_schema.columns
                WHERE upper(table_name) = upper($table)
                ORDER BY ordinal_position
            """
            rows = self._execute_sql(fallback, table=table).fetchall()

        result = []
        for r in rows:
            char_len = r[2]
            data_type = r[5] if len(r) > 5 else None
            if char_len is None and isinstance(data_type, str):
                match = re.search(r"(VARCHAR|CHAR)\((\d+)\)", data_type, re.IGNORECASE)
                if match:
                    char_len = int(match[2])

            numeric_precision = r[3]
            numeric_scale = r[4]
            if isinstance(data_type, str) and data_type.upper() in {"INTEGER", "BIGINT", "SMALLINT", "TINYINT"}:
                numeric_precision = 38
                numeric_scale = 0

            result.append(
                {
                    "name": r[0],
                    "is_nullable": r[1],
                    "character_maximum_length": char_len,
                    "numeric_precision": numeric_precision,
                    "numeric_scale": numeric_scale,
                }
            )
        self._columns_cache[key] = result
        return result

    def clear_cache(self) -> None:
        self._columns_cache.clear()

    def show_schemas_sql(self, *, database: str) -> str:
        """
        Returns the SQL to show all schemas for a database.
        """
        return load_sql(
            self._get_filepath("show_schemas.sql"),
            database=database,
            info_schema_name=self.info_schema_name,
        )

    def show_objects_sql(self, *, database: str, schema: str) -> str:
        """
        Returns the SQL to show all objects for a schema.
        """
        return load_sql(
            self._get_filepath("show_objects.sql"),
            database=database,
            schema=schema,
            info_schema_name=self.info_schema_name,
        )
    
    def _execute_sql(self, sql: str, **params: Any) -> DuckDBPyConnection:
        """
        Executes a SQL command with optional named parameters.
        """
        return self._duck_conn.execute(sql, params)
    
    def _get_filepath(self, filename: str) -> str:
        """
        Returns the full path to a file in the same directory as this script.
        """
        return os.path.join(os.path.dirname(__file__), filename)