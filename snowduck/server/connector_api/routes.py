"""Route definitions for Internal Connector API.

These are internal (undocumented) endpoints used by Snowflake connectors.
"""

from starlette.routing import Route

from . import handlers


def get_connector_api_routes() -> list[Route]:
    """Get all connector API routes.

    Returns:
        List of Starlette Route objects for the connector API
    """
    return [
        # Authentication
        Route("/session/v1/login-request", handlers.login_request, methods=["POST"]),
        Route("/session", handlers.session, methods=["POST", "GET"]),
        Route(
            "/session/heartbeat-request", handlers.heartbeat_request, methods=["POST"]
        ),
        Route(
            "/session/authenticator-request",
            handlers.authenticator_request,
            methods=["POST"],
        ),
        Route("/session/parameters", handlers.set_session_parameters, methods=["POST"]),
        Route("/session/token-request", handlers.renew_session, methods=["POST"]),
        # Query execution
        Route("/queries/v1/query-request", handlers.query_request, methods=["POST"]),
        Route(
            "/queries/v1/query-request/{queryId}",
            handlers.get_query_result,
            methods=["GET"],
        ),
        Route("/queries/v1/abort-request", handlers.abort_request, methods=["POST"]),
        # Telemetry
        Route("/telemetry/send", handlers.telemetry_send, methods=["POST"]),
    ]


# Convenience export
connector_api_routes = get_connector_api_routes()
