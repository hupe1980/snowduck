"""Streaming API route definitions.

This module defines all HTTP routes for the Snowpipe Streaming API.
Routes are organized by functionality and follow RESTful conventions.
"""

from starlette.routing import Route

from . import handlers

# Base paths for different API versions
_DATA_BASE = "/v2/streaming/data"
_MGMT_BASE = "/v2/streaming"

# Common path patterns
_ORG_ACCOUNT = "org/{organization}/accounts/{account}"
_DB_SCHEMA = "databases/{databaseName}/schemas/{schemaName}"
_PIPE = "pipes/{pipeName}"
_TABLE = "tables/{tableName}"
_CHANNEL = "channels/{channelName}"


def get_streaming_routes() -> list[Route]:
    """Get all streaming API routes.

    Returns:
        List of Starlette Route objects for the streaming API.
    """
    return [
        # Auth & Discovery
        Route(f"{_MGMT_BASE}/hostname", handlers.get_hostname, methods=["GET"]),
        Route("/oauth/token", handlers.exchange_scoped_token, methods=["POST"]),
        # Telemetry
        Route("/telemetry/send/sessionless", handlers.send_telemetry, methods=["POST"]),
        # Pipe Info - Multiple path patterns for SDK compatibility
        *_pipe_info_routes(),
        # Channel Management - Multiple path patterns for SDK compatibility
        *_channel_routes(),
        # Data Operations
        *_data_routes(),
    ]


def _pipe_info_routes() -> list[Route]:
    """Routes for pipe information endpoints."""
    patterns = [
        f"{_MGMT_BASE}/{_ORG_ACCOUNT}/{_DB_SCHEMA}",
        f"{_MGMT_BASE}/{_DB_SCHEMA}",
    ]

    routes = []
    for base in patterns:
        routes.extend(
            [
                Route(
                    f"{base}/{_PIPE}:pipe-info",
                    handlers.get_pipe_info,
                    methods=["GET", "POST"],
                ),
                Route(
                    f"{base}/{_PIPE}:validate-credentials",
                    handlers.validate_credentials,
                    methods=["POST"],
                ),
                Route(
                    f"{base}/{_PIPE}:bulk-channel-status",
                    handlers.bulk_channel_status,
                    methods=["POST"],
                ),
                Route(
                    f"{base}/{_PIPE}/{_CHANNEL}",
                    handlers.open_channel,
                    methods=["PUT"],
                ),
                Route(
                    f"{base}/{_PIPE}/{_CHANNEL}",
                    handlers.drop_channel,
                    methods=["DELETE"],
                ),
                Route(
                    f"{base}/{_PIPE}/{_CHANNEL}/status",
                    handlers.get_channel_status,
                    methods=["GET"],
                ),
                Route(
                    f"{base}/{_PIPE}/{_CHANNEL}:flush",
                    handlers.flush_channel,
                    methods=["POST"],
                ),
                Route(
                    f"{base}/{_PIPE}/{_CHANNEL}/offset",
                    handlers.get_latest_committed_offset,
                    methods=["GET"],
                ),
                Route(
                    f"{base}/{_PIPE}/channels",
                    handlers.list_channels,
                    methods=["GET"],
                ),
                Route(
                    f"{base}/{_TABLE}:table-info",
                    handlers.get_table_info,
                    methods=["GET"],
                ),
                Route(
                    f"{base}/{_PIPE}/{_CHANNEL}:register-blobs",
                    handlers.register_blob,
                    methods=["POST"],
                ),
            ]
        )

    return routes


def _channel_routes() -> list[Route]:
    """Routes for channel operations (additional patterns)."""
    # These are covered in _pipe_info_routes for consistency
    return []


def _data_routes() -> list[Route]:
    """Routes for data ingestion endpoints."""
    patterns = [
        f"{_DATA_BASE}/{_ORG_ACCOUNT}/{_DB_SCHEMA}",
        f"{_DATA_BASE}/{_DB_SCHEMA}",
    ]

    routes = []
    for base in patterns:
        routes.append(
            Route(
                f"{base}/{_PIPE}/{_CHANNEL}/rows",
                handlers.append_rows,
                methods=["POST"],
            )
        )

    return routes


# Export for convenience
streaming_routes = get_streaming_routes()
