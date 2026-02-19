"""Internal Connector API package.

Implements undocumented internal REST endpoints used by official
Snowflake connectors (Python, JDBC, ODBC, Node.js, etc.).

These endpoints are NOT part of the public Snowflake REST API.
For the public SQL REST API, see the sql_api package.

Endpoints:
    POST /session/v1/login-request - Authenticate and create session
    POST/GET /session - Session management
    POST /queries/v1/query-request - Execute SQL queries
    POST /queries/v1/abort-request - Abort running queries
    POST /telemetry/send - Receive telemetry data

Modules:
    handlers: HTTP request handlers
    routes: Route definitions
"""

from .routes import connector_api_routes, get_connector_api_routes

__all__ = [
    "get_connector_api_routes",
    "connector_api_routes",
]
