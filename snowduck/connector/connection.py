from types import TracebackType
from typing import Any, Iterable, Self

import snowflake.connector
import sqlglot
from duckdb import DuckDBPyConnection
from snowflake.connector.cursor import DictCursor, SnowflakeCursor
from sqlglot import exp

from ..info_schema import InfoSchemaManager
from .cursor import Cursor


class Connection:
    def __init__(
        self,
        duck_conn: DuckDBPyConnection,
        info_schema_manager: InfoSchemaManager,
        database: str | None = None,
        schema: str | None = None,
        role: str | None = None,
        warehouse: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._duck_conn = duck_conn
        self._info_schema_manager = info_schema_manager
        self._is_closed = False
        self._database: str | None = None
        self._schema: str | None = None
        self._role: str | None = None
        self._warehouse: str | None = None
        self._session_variables: dict[
            str, str
        ] = {}  # Session variables (SET var = value)

        if database:
            self.use_database(database)
        if schema:
            self.use_schema(schema)

        if role or kwargs.get("role"):
            self.use_role(role or kwargs.get("role"))
        else:
            self.use_role("SYSADMIN")

        if warehouse or kwargs.get("warehouse"):
            self.use_warehouse(warehouse or kwargs.get("warehouse"))
        else:
            self.use_warehouse("DEFAULT_WAREHOUSE")

        if self._database:
            self._info_schema_manager.create_database_information_schema(
                database=self._database,
                schema=self._schema,
            )
            self._set_duckdb_schema()

        self._paramstyle = kwargs.get("paramstyle", snowflake.connector.paramstyle)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        # No cleanup required for DuckDB in-memory connection
        pass

    def autocommit(self, _mode: bool) -> None:
        pass

    def cursor(self, cursor_class: type[SnowflakeCursor] = SnowflakeCursor) -> Cursor:
        """
        Returns a new Cursor object for executing queries.
        """
        if self._is_closed:
            raise snowflake.connector.errors.DatabaseError(
                "250002 (08003): Connection is closed"
            )

        return Cursor(
            sf_conn=self,
            duck_conn=self._duck_conn,
            info_schema_manager=self._info_schema_manager,
            use_dict_result=cursor_class == DictCursor,
        )

    def execute_string(
        self,
        sql_text: str,
        remove_comments: bool = False,
        return_cursors: bool = True,
        cursor_class: type[SnowflakeCursor] = SnowflakeCursor,
        **kwargs: dict[str, Any],
    ) -> Iterable[Cursor]:
        """
        Executes a string of SQL statements, optionally removing comments.
        Returns an iterable of Cursor objects if return_cursors is True.
        """
        # Parse the SQL text into individual statements
        statements = sqlglot.parse(sql_text, read="snowflake")

        # Filter out comments or semicolon-only statements
        filtered_statements = [
            stmt for stmt in statements if stmt and not isinstance(stmt, exp.Semicolon)
        ]

        # Execute each statement and collect cursors
        cursors = [
            self.cursor(cursor_class).execute(stmt.sql(dialect="snowflake"))
            for stmt in filtered_statements
        ]

        return cursors if return_cursors else []

    def get_column_metadata(self, table: str) -> list[dict[str, Any]]:
        """Return column metadata for a table based on the info schema."""
        if not self._database or not self._schema:
            return []
        return self._info_schema_manager.get_table_columns(
            database=self._database,
            schema=self._schema,
            table=table,
        )

    def rollback(self) -> None:
        self.cursor().execute("ROLLBACK")

    def close(self) -> None:
        """
        Closes the connection.
        """
        self._duck_conn.close()
        self._is_closed = True

    def is_closed(self) -> bool:
        return self._is_closed

    @property
    def database(self) -> str | None:
        return self._database

    def use_database(self, database: str) -> None:
        """Sets the current database."""
        self._database = database.upper() if database else None
        if self._database:
            self._schema = "PUBLIC"
            self._info_schema_manager.create_database_information_schema(
                database=self._database,
                schema=self._schema,
            )
        self._set_duckdb_schema()

    @property
    def schema(self) -> str | None:
        return (
            "INFORMATION_SCHEMA"
            if self._schema == self._info_schema_manager.info_schema_name
            else self._schema
        )

    def use_schema(self, schema: str) -> None:
        """Sets the current schema."""
        self._schema = (
            self._info_schema_manager.info_schema_name
            if schema.upper() == "INFORMATION_SCHEMA"
            else schema.upper()
        )
        self._set_duckdb_schema()

    @property
    def role(self) -> str | None:
        return self._role

    def use_role(self, role: str | None) -> None:
        """Sets the current role."""
        self._role = role.upper() if role else None

    @property
    def warehouse(self) -> str | None:
        return self._warehouse

    def use_warehouse(self, warehouse: str | None) -> None:
        """Sets the current warehouse."""
        self._warehouse = warehouse.upper() if warehouse else None

    def _set_duckdb_schema(self) -> None:
        if not self._database:
            return
        schema = self._schema or "MAIN"
        try:
            self._duck_conn.execute(f"SET schema='{self._database}.{schema}'")
        except Exception:
            # Database/schema may not be attached/created yet.
            return

    @property
    def paramstyle(self) -> str:
        """
        Returns the parameter style used by the connection.
        """
        return self._paramstyle
