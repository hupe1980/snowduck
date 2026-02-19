"""Channel manager for Snowpipe Streaming API."""

from __future__ import annotations

import secrets
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Optional


@dataclass
class ChannelStatus:
    """Status information for a streaming channel.

    Field names match the official Snowpipe Streaming SDK ChannelStatus.
    Reference: https://docs.snowflake.com/en/user-guide/snowpipe-streaming-sdk-python/reference/latest/api/snowflake/ingest/streaming/channel_status/index
    """

    database_name: str
    schema_name: str
    pipe_name: str
    channel_name: str
    channel_status_code: str = "ACTIVE"
    # SDK uses latest_committed_offset_token (not last_)
    latest_committed_offset_token: Optional[str] = None
    created_on_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    rows_inserted: int = 0
    rows_parsed: int = 0
    rows_error_count: int = 0
    last_error_offset_upper_bound: Optional[str] = None
    last_error_message: Optional[str] = None
    last_error_timestamp: Optional[str] = None
    snowflake_avg_processing_latency_ms: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON response.

        Field names match Snowflake's REST API format (from documentation):
        https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api

        Response fields:
        - channel_status_code: Status code (e.g., "ACTIVE")
        - last_committed_offset_token: Latest committed offset (REST API name)
        - rows_inserted, rows_parsed, rows_error_count: Counters
        - snowflake_avg_processing_latency_ms: Latency metric
        """
        result = {
            "database_name": self.database_name,
            "schema_name": self.schema_name,
            "pipe_name": self.pipe_name,
            "channel_name": self.channel_name,
            "channel_status_code": self.channel_status_code,
            # REST API uses last_committed_offset_token (documentation)
            "last_committed_offset_token": self.latest_committed_offset_token,
            "created_on_ms": self.created_on_ms,
            "rows_inserted": self.rows_inserted,
            "rows_parsed": self.rows_parsed,
            "rows_error_count": self.rows_error_count,
            "snowflake_avg_processing_latency_ms": self.snowflake_avg_processing_latency_ms,
        }
        if self.last_error_offset_upper_bound:
            result["last_error_offset_upper_bound"] = self.last_error_offset_upper_bound
        if self.last_error_message:
            result["last_error_message"] = self.last_error_message
        if self.last_error_timestamp:
            result["last_error_timestamp"] = self.last_error_timestamp
        return result


@dataclass
class Channel:
    """Represents a streaming channel."""

    database_name: str
    schema_name: str
    pipe_name: str
    channel_name: str
    status: ChannelStatus
    continuation_token: str
    client_sequencer: int = 0
    row_sequencer: int = 0

    @property
    def key(self) -> str:
        """Return the unique key for this channel."""
        return f"{self.database_name}.{self.schema_name}.{self.pipe_name}.{self.channel_name}".upper()


class ChannelManager:
    """Manages streaming channels for Snowpipe Streaming API.

    Thread-safe implementation using a lock for channel operations.
    """

    def __init__(self) -> None:
        self._channels: dict[str, Channel] = {}
        self._lock = threading.RLock()

    def _get_channel_unlocked(
        self, database: str, schema: str, pipe: str, channel_name: str
    ) -> Channel | None:
        """Get a channel by its identifiers. Must be called with lock held."""
        key = self._make_key(database, schema, pipe, channel_name)
        return self._channels.get(key)

    def _generate_continuation_token(self, channel: Channel) -> str:
        """Generate a continuation token for the channel."""
        # Token encapsulates client and row sequencers
        return f"ct_{channel.key}_{channel.client_sequencer}_{channel.row_sequencer}_{secrets.token_urlsafe(8)}"

    def _make_key(
        self, database: str, schema: str, pipe: str, channel_name: str
    ) -> str:
        """Create a unique key for a channel."""
        return f"{database}.{schema}.{pipe}.{channel_name}".upper()

    def open_channel(
        self,
        database: str,
        schema: str,
        pipe: str,
        channel_name: str,
        offset_token: Optional[str] = None,
    ) -> Channel:
        """
        Open or reopen a channel.

        If the channel already exists, bumps the client sequencer and returns it.
        Otherwise creates a new channel.
        """
        key = self._make_key(database, schema, pipe, channel_name)

        with self._lock:
            if key in self._channels:
                # Bump client sequencer for existing channel
                channel = self._channels[key]
                channel.client_sequencer += 1
                channel.continuation_token = self._generate_continuation_token(channel)
                if offset_token:
                    channel.status.latest_committed_offset_token = offset_token
                return channel

            # Create new channel
            status = ChannelStatus(
                database_name=database.upper(),
                schema_name=schema.upper(),
                pipe_name=pipe.upper(),
                channel_name=channel_name.upper(),
                latest_committed_offset_token=offset_token,
            )
            channel = Channel(
                database_name=database.upper(),
                schema_name=schema.upper(),
                pipe_name=pipe.upper(),
                channel_name=channel_name.upper(),
                status=status,
                continuation_token="",  # Will be set below
            )
            channel.continuation_token = self._generate_continuation_token(channel)
            self._channels[key] = channel
            return channel

    def get_channel(
        self, database: str, schema: str, pipe: str, channel_name: str
    ) -> Channel | None:
        """Get a channel by its identifiers."""
        with self._lock:
            return self._get_channel_unlocked(database, schema, pipe, channel_name)

    def drop_channel(
        self, database: str, schema: str, pipe: str, channel_name: str
    ) -> bool:
        """Drop a channel. Returns True if channel existed and was dropped."""
        key = self._make_key(database, schema, pipe, channel_name)
        with self._lock:
            if key in self._channels:
                del self._channels[key]
                return True
            return False

    def validate_continuation_token(
        self, database: str, schema: str, pipe: str, channel_name: str, token: str
    ) -> Channel | None:
        """
        Validate a continuation token and return the channel if valid.
        Returns None if the token is invalid or stale.
        """
        with self._lock:
            channel = self._get_channel_unlocked(database, schema, pipe, channel_name)
            if channel is None:
                return None

            # Verify the token matches the expected format and current sequencer
            expected_prefix = f"ct_{channel.key}_{channel.client_sequencer}_"
            if not token.startswith(expected_prefix):
                return None

            return channel

    def append_rows(
        self,
        database: str,
        schema: str,
        pipe: str,
        channel_name: str,
        row_count: int,
        offset_token: str | None = None,
    ) -> Channel | None:
        """
        Record that rows were appended to a channel.
        Updates counters and generates new continuation token.
        """
        with self._lock:
            channel = self._get_channel_unlocked(database, schema, pipe, channel_name)
            if channel is None:
                return None

            channel.row_sequencer += 1
            channel.status.rows_inserted += row_count
            channel.status.rows_parsed += row_count
            if offset_token:
                channel.status.latest_committed_offset_token = offset_token
            channel.continuation_token = self._generate_continuation_token(channel)
            return channel

    def get_bulk_status(
        self, database: str, schema: str, pipe: str, channel_names: list[str]
    ) -> tuple[list[dict], dict[str, dict]]:
        """Get status for multiple channels in both SDK formats.

        Returns a tuple of:
        1. List for Java SDK format (channels array)
        2. Dict for Python SDK format (channel_statuses)

        Note: channel_statuses keys use the ORIGINAL case from channel_names request
        since the SDK looks up by the key it sent.
        """
        with self._lock:
            channels_list: list[dict] = []
            channel_statuses: dict[str, dict] = {}
            for name in channel_names:
                channel = self._get_channel_unlocked(database, schema, pipe, name)
                if channel:
                    # Include full status in channels array with channel identification
                    # Include both field names for SDK compatibility
                    channels_list.append(
                        {
                            "status_code": 0,
                            "channel_name": channel.channel_name,
                            "database_name": channel.database_name,
                            "schema_name": channel.schema_name,
                            "pipe_name": channel.pipe_name,
                            # Both field names for SDK parsing
                            "persisted_offset_token": channel.status.latest_committed_offset_token,
                            "latest_committed_offset_token": channel.status.latest_committed_offset_token,
                            "offset_token": channel.status.latest_committed_offset_token,
                            "persisted_client_sequencer": channel.client_sequencer,
                            "persisted_row_sequencer": channel.row_sequencer,
                        }
                    )
                    # USE ORIGINAL CASE from channel_names request - SDK looks up by this key!
                    channel_statuses[name] = channel.status.to_dict()
            return channels_list, channel_statuses
