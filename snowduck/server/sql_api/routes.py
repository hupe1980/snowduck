"""Route definitions for SQL REST API.

Implements the Snowflake SQL REST API endpoints:
    /api/v2/statements - Statement submission and management
    /api/v2/query-history - Query history retrieval
"""

from starlette.routing import Route

from . import handlers


def get_sql_api_routes() -> list[Route]:
    """Get all SQL API routes.

    Returns:
        List of Starlette Route objects for the SQL REST API
    """
    return [
        Route("/api/v2/statements", handlers.submit_statement, methods=["POST"]),
        Route(
            "/api/v2/statements/{statementHandle}",
            handlers.get_statement_status,
            methods=["GET"],
        ),
        Route(
            "/api/v2/statements/{statementHandle}/cancel",
            handlers.cancel_statement,
            methods=["POST"],
        ),
        Route(
            "/api/v2/statements/{statementHandle}/retry",
            handlers.retry_statement,
            methods=["POST"],
        ),
        Route("/api/v2/query-history", handlers.get_query_history, methods=["GET"]),
    ]


# Convenience export
sql_api_routes = get_sql_api_routes()
