import duckdb

from ..info_schema import InfoSchemaManager
from .connection import Connection


class Connector:
    def __init__(self, timezone: str = "UTC", db_file: str = ":memory:"):
        """
        Initializes the SnowDuck connector factory.

        Args:
            timezone: The default timezone to set for connections.
            db_file: The DuckDB database file to usage. Defaults to ':memory:' (transient).
                     Set to a file path to enable persistence.
        """
        self._timezone = timezone
        self._db_file = db_file

    def connect(self, database: str | None = None, schema: str | None = None, **kwargs):
        # Create a new connection (isolated in-memory or shared file-backed)
        duck_conn = duckdb.connect(database=self._db_file)
        duck_conn.execute(f"SET GLOBAL TimeZone = '{self._timezone}'")

        info_schema_manager = InfoSchemaManager(duck_conn=duck_conn)

        return Connection(
            duck_conn=duck_conn,
            info_schema_manager=info_schema_manager,
            database=database,
            schema=schema,
            **kwargs,
        )

    def close(self) -> None:
        """
        No-op for the factory. Individual connections must be closed by their owners.
        """
        pass
