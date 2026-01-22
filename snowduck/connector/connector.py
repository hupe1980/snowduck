import duckdb

from ..info_schema import InfoSchemaManager
from ..macros import register_macros
from .connection import Connection


class Connector:
    def __init__(self, timezone: str = "UTC", db_file: str = ":memory:"):
        """
        Initializes the SnowDuck connector factory.

        Args:
            timezone: The default timezone to set for connections.
            db_file: The DuckDB database file to use. Defaults to ':memory:' (transient).
                     Set to a file path to enable persistence across sessions.
        """
        self._timezone = timezone
        self._db_file = db_file

        # Create a shared DuckDB connection for all connections within this Connector
        # This matches Snowflake behavior where connections in the same session share state
        self._duck_conn = duckdb.connect(database=self._db_file)
        self._duck_conn.execute(f"SET GLOBAL TimeZone = '{self._timezone}'")

        # Register Snowflake-compatible macros once
        register_macros(self._duck_conn)

        # Create shared InfoSchemaManager
        self._info_schema_manager = InfoSchemaManager(duck_conn=self._duck_conn)

    def connect(self, database: str | None = None, schema: str | None = None, **kwargs):
        """
        Create a new connection that shares the underlying DuckDB instance.

        All connections within the same Connector share database state,
        matching Snowflake's session behavior.
        """
        return Connection(
            duck_conn=self._duck_conn,
            info_schema_manager=self._info_schema_manager,
            database=database,
            schema=schema,
            owns_duck_conn=False,  # Connector owns the DuckDB conn, not Connection
            **kwargs,
        )

    def close(self) -> None:
        """
        Close the shared DuckDB connection.
        """
        if self._duck_conn:
            self._duck_conn.close()
            self._duck_conn = None
