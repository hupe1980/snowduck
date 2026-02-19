"""SnowDuck Server - A Snowflake-compatible REST API server backed by DuckDB.

This server implements multiple Snowflake REST API layers:

1. Internal Connector API (connector_api.py)
   - Used by official Snowflake drivers (Python, JDBC, ODBC, Node.js)
   - Endpoints: /session/v1/*, /queries/v1/*, /telemetry/*
   - NOT part of public Snowflake REST API specs (reverse-engineered)

2. Public SQL REST API (sql_api.py)
   - Official documented API: https://docs.snowflake.com/en/developer-guide/sql-api/
   - Endpoints: /api/v2/statements*
   - Uses JWT/OAuth authentication

3. Snowpipe Streaming API (streaming.py)
   - Enables real-time data ingestion via Snowpipe Streaming SDK
   - Endpoints: /v2/streaming/*, /oauth/token
   - Uses OAuth/scoped tokens

Usage:
    # Start server with in-memory database
    snowduck-server

    # Start server with persistent database
    SNOWDUCK_DB_PATH=/path/to/db.duckdb snowduck-server

    # With custom host/port
    snowduck-server --host 0.0.0.0 --port 8080 --debug
"""

from __future__ import annotations

import argparse

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .connector_api import get_connector_api_routes
from .middleware import ErrorHandlingMiddleware, TokenValidationMiddleware
from .sql_api import get_sql_api_routes
from .streaming import streaming_routes


async def fallback_route(request: Request) -> JSONResponse:
    """Fallback route to log unmatched requests.
    
    This helps with debugging when connectors hit unexpected endpoints.
    """
    print(f"[FALLBACK] Unmatched request: {request.method} {request.url}")
    body = await request.body()
    if body:
        print(f"[FALLBACK] Request body: {body.decode('utf-8')}")
    return JSONResponse(
        {"success": False, "message": "Route not found."}, status_code=404
    )


def create_app(debug: bool = False) -> Starlette:
    """Create and configure the SnowDuck Starlette application.
    
    Args:
        debug: Enable debug mode for detailed error messages
    
    Returns:
        Configured Starlette application
    """
    # Compose routes from all API modules
    routes = [
        # Internal Connector API (used by snowflake-connector-python)
        *get_connector_api_routes(),
        # Public SQL REST API (documented Snowflake API)
        *get_sql_api_routes(),
        # Snowpipe Streaming API
        *streaming_routes,
        # Fallback for debugging
        Route("/{path:path}", fallback_route),
    ]

    app = Starlette(debug=debug, routes=routes)
    
    # Add middleware (order matters - last added runs first)
    app.add_middleware(ErrorHandlingMiddleware)
    app.add_middleware(TokenValidationMiddleware)
    
    return app


# Default application instance
app = create_app()


def main() -> None:
    """CLI entry point for the SnowDuck server."""
    try:
        from uvicorn import run
    except ImportError as e:
        raise ImportError(
            "Optional dependencies for the server are not installed. "
            "Install them using one of the following commands:\n"
            "  - With uv: 'uv sync --extra server'\n"
            "  - With pip: 'pip install snowduck[server]'"
        ) from e

    parser = argparse.ArgumentParser(
        description="SnowDuck Server - Snowflake-compatible REST API backed by DuckDB."
    )

    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host to run the server on (default: 127.0.0.1)",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to run the server on (default: 8000)",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode (default: False)",
    )

    args = parser.parse_args()

    # Recreate app with debug setting if needed
    if args.debug:
        application = create_app(debug=True)
    else:
        application = app

    print(f"Starting SnowDuck server on {args.host}:{args.port}")
    print("API Endpoints:")
    print("  - Internal Connector API: /session/v1/*, /queries/v1/*")
    print("  - SQL REST API: /api/v2/statements*")
    print("  - Snowpipe Streaming: /v2/streaming/*")
    
    run(application, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
