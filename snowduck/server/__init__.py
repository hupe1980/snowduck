"""SnowDuck Server - Snowflake-compatible REST API backed by DuckDB.

Modules:
    server: Main application and CLI entry point
    connector_api: Internal connector API (used by snowflake-connector-python)
    sql_api: Public SQL REST API (/api/v2/statements)
    streaming: Snowpipe Streaming API (modular package)
    middleware: HTTP middleware (error handling, token validation)
    shared: Shared state (connector, session manager)
"""

from .connector_api import get_connector_api_routes
from .middleware import ErrorHandlingMiddleware, TokenValidationMiddleware
from .server import app, create_app
from .session_manager import SessionManager
from .shared import ServerError, session_manager, shared_connector
from .sql_api import get_sql_api_routes
from .streaming import (
    Channel,
    ChannelManager,
    ChannelStatus,
    get_streaming_routes,
    streaming_routes,
)

__all__ = [
    # Application
    "app",
    "create_app",
    # Routes
    "get_connector_api_routes",
    "get_sql_api_routes",
    "get_streaming_routes",
    "streaming_routes",
    # Middleware
    "ErrorHandlingMiddleware",
    "TokenValidationMiddleware",
    # Managers
    "Channel",
    "ChannelManager",
    "ChannelStatus",
    "SessionManager",
    "session_manager",
    # Shared state
    "ServerError",
    "shared_connector",
]
