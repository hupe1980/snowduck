"""Snowpipe Streaming REST API package.

This package implements the Snowpipe Streaming API for real-time data ingestion.
The API is compatible with the official Snowflake Python SDK.

Modules:
    handlers: HTTP request handlers for streaming endpoints
    channel_manager: Channel lifecycle and state management
    auth: Token exchange and validation
    routes: Route definitions
"""

from .auth import LRUTokenCache, _LRUCache
from .channel_manager import Channel, ChannelManager, ChannelStatus
from .routes import get_streaming_routes

# Backward compatible exports
streaming_routes = get_streaming_routes()

__all__ = [
    # Main export
    "get_streaming_routes",
    "streaming_routes",
    # Channel management
    "Channel",
    "ChannelManager",
    "ChannelStatus",
    # Auth utilities
    "LRUTokenCache",
    "_LRUCache",  # Backwards compatibility alias
]
