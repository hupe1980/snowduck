import threading
from time import sleep
from typing import Any, Callable, Generator, Iterator

import duckdb
import pytest
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from snowduck import patch_snowflake
from snowduck.dialect import DialectContext
from snowduck.info_schema import InfoSchemaManager

# Conditional imports for server tests (optional dependencies)
try:
    import uvicorn

    from snowduck.server import app

    HAS_SERVER_DEPS = True
except ImportError:
    HAS_SERVER_DEPS = False


@pytest.fixture
def conn() -> Generator[SnowflakeConnection, Any, None]:
    with (
        patch_snowflake(),
        snowflake.connector.connect(database="db", schema="schema") as conn,
    ):
        yield conn


@pytest.fixture
def cursor(
    conn: snowflake.connector.SnowflakeConnection,
) -> Iterator[snowflake.connector.cursor.SnowflakeCursor]:
    with conn.cursor() as cur:
        yield cur


@pytest.fixture
def in_memory_duckdb_connection():
    """
    Provides an in-memory DuckDB connection for testing.
    """
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def dialect_context(in_memory_duckdb_connection) -> DialectContext:
    """Fixture to provide a DialectContext for testing."""
    return DialectContext(
        current_database="test_db",
        current_schema="test_schema",
        current_role="test_role",
        current_warehouse="test_warehouse",
        info_schema_manager=InfoSchemaManager(in_memory_duckdb_connection),
    )


@pytest.fixture(scope="session")
def server(unused_tcp_port_factory: Callable[[], int]) -> Iterator[dict]:
    """Start a test server for the session and provide connection details."""
    if not HAS_SERVER_DEPS:
        pytest.skip("Server dependencies (uvicorn, starlette) not installed")

    port = unused_tcp_port_factory()
    config = uvicorn.Config(app, port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name="Server", daemon=True)

    thread.start()

    # Wait until the server is fully started
    while not server.started:
        sleep(0.1)

    # Provide connection details
    yield {
        "user": "smow",
        "password": "duck",
        "account": "snowduck",
        "host": "localhost",
        "port": port,
        "protocol": "http",
        "session_parameters": {
            "CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED": False,
        },
    }

    # Graceful shutdown
    server.should_exit = True
    thread.join()
