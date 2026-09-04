import os
import re
from pathlib import Path
from typing import Any

from duckdb import DuckDBPyConnection

from ..helper import load_sql
from ..macros import register_macros

ACCOUNT_CATALOG_NAME = "_snowduck_account"
INFO_SCHEMA_NAME = "_information_schema"


class InfoSchemaManager:
    def __init__(
        self,
        duck_conn: DuckDBPyConnection,
        account_catalog_name: str = ACCOUNT_CATALOG_NAME,
        info_schema_name: str = INFO_SCHEMA_NAME,
        storage: str = ":memory:",
    ) -> None:
        """
        Initializes the InfoSchemaManager with a DuckDB connection.

        `storage` is the connection's own database file. Every Snowflake
        database is a separate DuckDB catalog, so a file-backed session stores
        each one alongside that file - otherwise `CREATE DATABASE` would be
        silently in-memory and vanish on restart, taking every schema and table
        in it along.
        """
        self._duck_conn = duck_conn
        self._account_catalog_name = account_catalog_name
        self._info_schema_name = info_schema_name
        self._storage = storage
        self._columns_cache: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        self._attach_account_database()
        self._attach_persisted_databases()
        self._create_account_information_schema()

    @property
    def base_catalog_name(self) -> str:
        """The catalog DuckDB gives the connection's own storage.

        In-memory that is `memory`; a file-backed connection gets one named
        after the file, which would otherwise surface as a phantom Snowflake
        database in SHOW DATABASES - present in file mode but not in memory
        mode, for the same session.
        """
        if self._storage == ":memory:":
            return "memory"
        return Path(self._storage).stem

    def database_path(self, database: str) -> str:
        """Where a Snowflake database's catalog lives.

        In-memory sessions keep everything in memory. A file-backed session
        stores each catalog next to its own file, named after it, so the whole
        set travels together and `reset` can find them.
        """
        if self._storage == ":memory:":
            return ":memory:"
        base = Path(self._storage)
        return str(base.with_name(f"{base.stem}.{database.upper()}{base.suffix}"))

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
        databases = self._execute_sql(
            "SELECT database_name FROM duckdb_databases()"
        ).fetchall()
        db_names = [db[0] for db in databases]

        if self.account_catalog_name not in db_names:
            path = self.database_path(self.account_catalog_name)
            self._execute_sql(
                f"ATTACH DATABASE '{path}' AS {self.account_catalog_name}"
            )

    def _attach_persisted_databases(self) -> None:
        """Re-attach the catalogs a previous file-backed session left behind.

        Each Snowflake database is its own DuckDB file next to the connection's
        (see :meth:`database_path`), so without this a restart would come back
        with the databases missing rather than restored.
        """
        if self._storage == ":memory:":
            return

        base = Path(self._storage)
        attached = {
            row[0].upper()
            for row in self._execute_sql(
                "SELECT database_name FROM duckdb_databases()"
            ).fetchall()
        }

        for path in sorted(base.parent.glob(f"{base.stem}.*{base.suffix}")):
            database = path.name[len(base.stem) + 1 : len(path.name) - len(base.suffix)]
            if not database or not database.isidentifier():
                continue
            if database.upper() in attached:
                continue
            self._execute_sql(f"ATTACH DATABASE '{path}' AS {database.upper()}")

    def _create_account_information_schema(self) -> None:
        """
        Creates the account information schema.
        """
        sql = load_sql(
            self._get_filepath("account_information_schema.sql"),
            account_catalog_name=self.account_catalog_name,
            info_schema_name=self.info_schema_name,
            base_catalog_name=self.base_catalog_name,
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

    def has_table(self, *, database: str, schema: str, table: str) -> bool:
        """Whether a base table of that name exists.

        Views are excluded: the callers that ask (``TRUNCATE TABLE IF EXISTS``)
        are about to issue a table-only operation.
        """
        if not database or not schema or not table:
            return False
        rows = self._execute_sql(
            "SELECT 1 FROM duckdb_tables() WHERE upper(database_name) = upper($database) "
            "AND upper(schema_name) = upper($schema) "
            "AND upper(table_name) = upper($table)",
            database=database,
            schema=schema,
            table=table,
        ).fetchall()
        return bool(rows)

    def has_relation(self, *, database: str, schema: str, name: str) -> bool:
        """Whether a table or a view of that name exists."""
        if not database or not schema or not name:
            return False
        rows = self._execute_sql(
            "SELECT 1 FROM ("
            "  SELECT database_name, schema_name, table_name AS name FROM duckdb_tables()"
            "  UNION ALL"
            "  SELECT database_name, schema_name, view_name FROM duckdb_views()"
            ") WHERE upper(database_name) = upper($database) "
            "AND upper(schema_name) = upper($schema) "
            "AND upper(name) = upper($name)",
            database=database,
            schema=schema,
            name=name,
        ).fetchall()
        return bool(rows)

    def has_registered_schema(self, *, database: str, schema: str) -> bool:
        """Whether a schema was explicitly created (see register_schema)."""
        if not database or not schema:
            return False
        rows = self._execute_sql(
            f"SELECT 1 FROM {self.account_catalog_name}.{self.info_schema_name}"
            "._created_schemas WHERE database_name = $database "
            "AND schema_name = $schema",
            database=database.upper(),
            schema=schema.upper(),
        ).fetchall()
        return bool(rows)

    def clear_schema(self, *, database: str, schema: str) -> None:
        """Drop every table and view in a schema, leaving the schema itself."""
        if not database or not schema:
            return
        for source, kind in (("duckdb_views()", "VIEW"), ("duckdb_tables()", "TABLE")):
            name_column = "view_name" if kind == "VIEW" else "table_name"
            rows = self._execute_sql(
                f"SELECT {name_column} FROM {source} "
                "WHERE database_name = $database AND schema_name = $schema",
                database=database,
                schema=schema,
            ).fetchall()
            for (name,) in rows:
                self._duck_conn.execute(
                    f'DROP {kind} IF EXISTS {database}.{schema}."{name}" CASCADE'
                )

    def register_schema(self, *, database: str, schema: str) -> None:
        """Record that a schema was explicitly created.

        Only MAIN actually needs this. DuckDB's internal `main` schema exists
        in every attached database and cannot be dropped or renamed, so a
        Snowflake `CREATE SCHEMA db.MAIN` is indistinguishable from DuckDB's
        own plumbing unless the request is recorded.
        """
        if not database or not schema:
            return
        self._execute_sql(
            f"INSERT OR IGNORE INTO {self.account_catalog_name}."
            f"{self.info_schema_name}._created_schemas "
            "(database_name, schema_name) VALUES ($database, $schema)",
            database=database.upper(),
            schema=schema.upper(),
        )

    def unregister_schema(self, *, database: str, schema: str) -> None:
        """Forget a schema, so a dropped MAIN stops being reported."""
        if not database or not schema:
            return
        self._execute_sql(
            f"DELETE FROM {self.account_catalog_name}.{self.info_schema_name}"
            "._created_schemas WHERE database_name = $database "
            "AND schema_name = $schema",
            database=database.upper(),
            schema=schema.upper(),
        )

    def create_database_information_schema(
        self, *, database: str, schema: str | None = None
    ) -> None:
        """
        Creates the database-specific information schema for the specified database.
        """
        if not database:
            raise ValueError("Database name is required")

        if not database.isidentifier():
            raise ValueError(f"Invalid database name: {database}")

        if not self.has_database(database):
            path = self.database_path(database)
            self._execute_sql(f"ATTACH DATABASE '{path}' AS {database}")

        # Keyed on the info schema rather than on the database: `CREATE DATABASE`
        # attaches the catalog through the normal translation path, so by the
        # time this runs the database already exists and a database-level check
        # would skip the views entirely - leaving every INFORMATION_SCHEMA query
        # against that database failing with "Table _tables does not exist".
        if not self.has_schema(database, self.info_schema_name):
            sql = load_sql(
                self._get_filepath("database_information_schema.sql"),
                account_catalog_name=self.account_catalog_name,
                info_schema_name=self.info_schema_name,
                database=database,
            )
            self._execute_sql(sql)
            # Register Snowflake-compatible macros in the new database
            register_macros(self._duck_conn, database=database)

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
            self._get_filepath("describe_table.sql"),
            account_catalog_name=self.account_catalog_name,
            info_schema_name=self.info_schema_name,
            database=database,
            schema=schema,
            table=table,
        )

    def record_column_lengths(
        self,
        *,
        database: str,
        schema: str,
        table: str,
        lengths: dict[str, int | None],
        replace: bool = True,
    ) -> None:
        """Remember the declared length of VARCHAR/CHAR columns.

        DuckDB has no length-limited VARCHAR and drops the size at DDL time, so
        `VARCHAR(20)` came back from INFORMATION_SCHEMA with a NULL
        `character_maximum_length`. The declared size is kept here instead.

        `replace` rewrites the whole table's entries, which is what a CREATE
        wants. An ALTER passes `replace=False` and touches only the columns it
        names; a `None` length there forgets a column that lost its size (it was
        dropped, or retyped to something without one).
        """
        if not database or not schema or not table:
            return

        catalog = f"{self.account_catalog_name}.{self.info_schema_name}._columns_ext"
        if replace:
            self._execute_sql(
                f"DELETE FROM {catalog} WHERE upper(ext_table_catalog) = upper($database) "
                "AND upper(ext_table_schema) = upper($schema) "
                "AND upper(ext_table_name) = upper($table)",
                database=database,
                schema=schema,
                table=table,
            )
        for column, length in lengths.items():
            if not replace:
                self._execute_sql(
                    f"DELETE FROM {catalog} "
                    "WHERE upper(ext_table_catalog) = upper($database) "
                    "AND upper(ext_table_schema) = upper($schema) "
                    "AND upper(ext_table_name) = upper($table) "
                    "AND upper(ext_column_name) = upper($column)",
                    database=database,
                    schema=schema,
                    table=table,
                    column=column,
                )
            if length is None:
                continue
            self._execute_sql(
                f"INSERT INTO {catalog} (ext_table_catalog, ext_table_schema, "
                "ext_table_name, ext_column_name, ext_character_maximum_length, "
                "ext_character_octet_length) "
                "VALUES ($database, $schema, $table, $column, $length, $octets)",
                database=database.upper(),
                schema=schema.upper(),
                table=table.upper(),
                column=column,
                length=length,
                octets=length * 4,
            )
        self._columns_cache.pop((database.upper(), schema.upper(), table.upper()), None)

    def get_table_columns(
        self, *, database: str, schema: str, table: str
    ) -> list[dict[str, Any]]:
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
        rows = self._execute_sql(
            query, database=database, schema=schema, table=table
        ).fetchall()
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
            if isinstance(data_type, str) and data_type.upper() in {
                "INTEGER",
                "BIGINT",
                "SMALLINT",
                "TINYINT",
            }:
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
