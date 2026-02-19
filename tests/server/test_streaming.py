"""Tests for Snowpipe Streaming REST API."""

import pytest

from snowduck.server.streaming import ChannelManager, ChannelStatus


class TestChannelManager:
    """Unit tests for ChannelManager."""

    def test_open_channel_creates_new_channel(self) -> None:
        manager = ChannelManager()
        channel = manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")

        assert channel.database_name == "DB"
        assert channel.schema_name == "SCHEMA"
        assert channel.pipe_name == "PIPE"
        assert channel.channel_name == "CHANNEL1"
        assert channel.client_sequencer == 0
        assert channel.continuation_token.startswith("ct_")
        assert channel.status.channel_status_code == "ACTIVE"

    def test_open_channel_reopens_existing_channel(self) -> None:
        manager = ChannelManager()
        channel1 = manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")
        token1 = channel1.continuation_token

        channel2 = manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")

        # Same channel object
        assert channel1.key == channel2.key
        # Client sequencer bumped
        assert channel2.client_sequencer == 1
        # New continuation token
        assert channel2.continuation_token != token1

    def test_open_channel_case_insensitive(self) -> None:
        manager = ChannelManager()
        channel1 = manager.open_channel("db", "schema", "pipe", "channel1")
        channel2 = manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")

        # Should be the same channel (case-insensitive)
        assert channel1.key == channel2.key
        assert channel2.client_sequencer == 1

    def test_open_channel_with_offset_token(self) -> None:
        manager = ChannelManager()
        channel = manager.open_channel(
            "DB", "SCHEMA", "PIPE", "CHANNEL1", offset_token="offset123"
        )

        assert channel.status.latest_committed_offset_token == "offset123"

    def test_get_channel_existing(self) -> None:
        manager = ChannelManager()
        manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")

        channel = manager.get_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")

        assert channel is not None
        assert channel.channel_name == "CHANNEL1"

    def test_get_channel_nonexistent(self) -> None:
        manager = ChannelManager()

        channel = manager.get_channel("DB", "SCHEMA", "PIPE", "NONEXISTENT")

        assert channel is None

    def test_drop_channel_existing(self) -> None:
        manager = ChannelManager()
        manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")

        result = manager.drop_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")

        assert result is True
        assert manager.get_channel("DB", "SCHEMA", "PIPE", "CHANNEL1") is None

    def test_drop_channel_nonexistent(self) -> None:
        manager = ChannelManager()

        result = manager.drop_channel("DB", "SCHEMA", "PIPE", "NONEXISTENT")

        assert result is False

    def test_validate_continuation_token_stale_after_reopen(self) -> None:
        """Token becomes stale after channel is reopened (client sequencer changes)."""
        manager = ChannelManager()
        channel = manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")
        old_token = channel.continuation_token

        # Reopen channel - this bumps client sequencer
        manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")

        # Old token should now be invalid
        validated = manager.validate_continuation_token(
            "DB", "SCHEMA", "PIPE", "CHANNEL1", old_token
        )

        assert validated is None

    def test_validate_continuation_token_valid(self) -> None:
        manager = ChannelManager()
        channel = manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")
        token = channel.continuation_token

        validated = manager.validate_continuation_token(
            "DB", "SCHEMA", "PIPE", "CHANNEL1", token
        )

        assert validated is not None
        assert validated.key == channel.key

    def test_validate_continuation_token_invalid(self) -> None:
        manager = ChannelManager()
        manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")

        validated = manager.validate_continuation_token(
            "DB", "SCHEMA", "PIPE", "CHANNEL1", "invalid_token"
        )

        assert validated is None

    def test_validate_continuation_token_channel_not_found(self) -> None:
        manager = ChannelManager()

        validated = manager.validate_continuation_token(
            "DB", "SCHEMA", "PIPE", "NONEXISTENT", "any_token"
        )

        assert validated is None

    def test_append_rows_updates_counters(self) -> None:
        manager = ChannelManager()
        channel = manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")
        initial_token = channel.continuation_token

        result = manager.append_rows("DB", "SCHEMA", "PIPE", "CHANNEL1", row_count=10)

        assert result is not None
        assert result.status.rows_inserted == 10
        assert result.status.rows_parsed == 10
        assert result.row_sequencer == 1
        assert result.continuation_token != initial_token

    def test_append_rows_with_offset_token(self) -> None:
        manager = ChannelManager()
        manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")

        result = manager.append_rows(
            "DB", "SCHEMA", "PIPE", "CHANNEL1", row_count=5, offset_token="offset_abc"
        )

        assert result is not None
        assert result.status.latest_committed_offset_token == "offset_abc"

    def test_append_rows_nonexistent_channel(self) -> None:
        manager = ChannelManager()

        result = manager.append_rows("DB", "SCHEMA", "PIPE", "NONEXISTENT", row_count=5)

        assert result is None

    def test_get_bulk_status(self) -> None:
        manager = ChannelManager()
        manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL1")
        manager.open_channel("DB", "SCHEMA", "PIPE", "CHANNEL2")
        manager.append_rows("DB", "SCHEMA", "PIPE", "CHANNEL1", row_count=100)

        channels_list, channel_statuses = manager.get_bulk_status(
            "DB", "SCHEMA", "PIPE", ["CHANNEL1", "CHANNEL2", "NONEXISTENT"]
        )

        # Check Java SDK format (channels list)
        assert len(channels_list) == 2  # NONEXISTENT not included
        assert channels_list[0]["persisted_client_sequencer"] >= 0

        # Check Python SDK format (channel_statuses dict)
        assert "CHANNEL1" in channel_statuses
        assert "CHANNEL2" in channel_statuses
        assert "NONEXISTENT" not in channel_statuses
        assert channel_statuses["CHANNEL1"]["rows_inserted"] == 100
        assert channel_statuses["CHANNEL2"]["rows_inserted"] == 0

    def test_thread_safety(self) -> None:
        """Test that ChannelManager is thread-safe."""
        import threading

        manager = ChannelManager()
        errors: list[Exception] = []

        def worker(channel_id: int) -> None:
            try:
                for _ in range(100):
                    manager.open_channel("DB", "SCHEMA", "PIPE", f"CHANNEL{channel_id}")
                    manager.append_rows(
                        "DB", "SCHEMA", "PIPE", f"CHANNEL{channel_id}", row_count=1
                    )
                    manager.get_channel("DB", "SCHEMA", "PIPE", f"CHANNEL{channel_id}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Thread safety errors: {errors}"


class TestChannelStatus:
    """Unit tests for ChannelStatus."""

    def test_to_dict(self) -> None:
        status = ChannelStatus(
            database_name="DB",
            schema_name="SCHEMA",
            pipe_name="PIPE",
            channel_name="CHANNEL",
            rows_inserted=100,
            rows_parsed=100,
        )

        result = status.to_dict()

        assert result["database_name"] == "DB"
        assert result["schema_name"] == "SCHEMA"
        assert result["pipe_name"] == "PIPE"
        assert result["channel_name"] == "CHANNEL"
        assert result["channel_status_code"] == "ACTIVE"
        assert result["rows_inserted"] == 100
        assert result["rows_parsed"] == 100
        # REST API uses last_committed_offset_token (per documentation)
        assert "last_committed_offset_token" in result
        assert "last_error_message" not in result  # None values excluded

    def test_to_dict_with_error(self) -> None:
        status = ChannelStatus(
            database_name="DB",
            schema_name="SCHEMA",
            pipe_name="PIPE",
            channel_name="CHANNEL",
            last_error_message="Something went wrong",
            last_error_offset_upper_bound="offset123",
        )

        result = status.to_dict()

        assert result["last_error_message"] == "Something went wrong"
        assert result["last_error_offset_upper_bound"] == "offset123"


class TestLRUCache:
    """Unit tests for the LRU cache used for scoped tokens."""

    def test_lru_cache_eviction(self) -> None:
        """Test that LRU cache evicts oldest entries when full."""
        from snowduck.server.streaming import _LRUCache

        cache: _LRUCache = _LRUCache(maxsize=3)
        cache["a"] = "1"
        cache["b"] = "2"
        cache["c"] = "3"

        # Adding a 4th item should evict the oldest (a)
        cache["d"] = "4"

        assert "a" not in cache
        assert "b" in cache
        assert "c" in cache
        assert "d" in cache

    def test_lru_cache_access_updates_order(self) -> None:
        """Test that accessing an item moves it to the end (most recent)."""
        from snowduck.server.streaming import _LRUCache

        cache: _LRUCache = _LRUCache(maxsize=3)
        cache["a"] = "1"
        cache["b"] = "2"
        cache["c"] = "3"

        # Access 'a' to make it most recent
        cache["a"] = "1"

        # Adding a 4th item should now evict 'b' (oldest)
        cache["d"] = "4"

        assert "a" in cache  # Still there because we accessed it
        assert "b" not in cache  # Evicted
        assert "c" in cache
        assert "d" in cache


# Integration tests for streaming API endpoints
try:
    from starlette.testclient import TestClient

    from snowduck.server import app

    HAS_SERVER_DEPS = True
except ImportError:
    HAS_SERVER_DEPS = False


@pytest.fixture
def client():
    """Create a test client for the streaming API."""
    if not HAS_SERVER_DEPS:
        pytest.skip("Server dependencies not installed")
    return TestClient(app)


@pytest.mark.skipif(not HAS_SERVER_DEPS, reason="Server dependencies not installed")
class TestStreamingEndpoints:
    """Integration tests for streaming REST API endpoints."""

    def test_get_hostname(self, client: "TestClient") -> None:
        """Test hostname endpoint returns plain text (SDK v1.1.2 format).

        Returns the host from the request so SDK uses same host for subsequent calls.
        In tests with TestClient, this is 'testserver'.
        """
        response = client.get("/v2/streaming/hostname")

        assert response.status_code == 200
        # SDK expects plain text response, not JSON
        assert "text/plain" in response.headers.get("content-type", "")
        hostname = response.text
        # Returns the host from request (testserver in tests, 127.0.0.1 in real SDK)
        assert hostname == "testserver"

    def test_exchange_scoped_token(self, client: "TestClient") -> None:
        """Test token endpoint returns plain text JWT (SDK v1.1.2 format)."""
        response = client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "scope": "snowduck.snowflakecomputing.com",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        assert response.status_code == 200
        # SDK expects plain text JWT response, not JSON
        assert "text/plain" in response.headers.get("content-type", "")
        token = response.text
        assert len(token) > 0
        # JWT format: header.payload.signature
        parts = token.split(".")
        assert len(parts) == 3

    def test_exchange_scoped_token_invalid_grant_type(
        self, client: "TestClient"
    ) -> None:
        response = client.post(
            "/oauth/token",
            data={"grant_type": "invalid", "scope": "test"},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        assert response.status_code == 400
        # Error response is still plain text
        assert "Invalid grant type" in response.text

    def test_open_channel(self, client: "TestClient") -> None:
        response = client.put(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/testpipe/channels/testchannel"
        )

        assert response.status_code == 200
        data = response.json()
        assert "next_continuation_token" in data
        assert "channel_status" in data
        assert data["channel_status"]["database_name"] == "TESTDB"
        assert data["channel_status"]["schema_name"] == "TESTSCHEMA"
        assert data["channel_status"]["pipe_name"] == "TESTPIPE"
        assert data["channel_status"]["channel_name"] == "TESTCHANNEL"
        assert (
            data["channel_status"]["channel_status_code"] == "ACTIVE"
        )  # Required by Rust SDK

    def test_open_channel_with_offset_token(self, client: "TestClient") -> None:
        response = client.put(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/testpipe/channels/channel_offset",
            json={"offset_token": "my_offset_123"},
        )

        assert response.status_code == 200
        data = response.json()
        # REST API uses last_committed_offset_token (per documentation)
        assert data["channel_status"]["last_committed_offset_token"] == "my_offset_123"

    def test_drop_channel(self, client: "TestClient") -> None:
        # First create the channel
        client.put(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/testpipe/channels/todrop"
        )

        # Then drop it
        response = client.delete(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/testpipe/channels/todrop"
        )

        # SDK expects JSON response (not 204 No Content)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "OK"

    def test_drop_channel_not_found(self, client: "TestClient") -> None:
        response = client.delete(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/testpipe/channels/nonexistent"
        )

        assert response.status_code == 404
        data = response.json()
        assert data["code"] == "CHANNEL_NOT_FOUND"

    def test_bulk_channel_status(self, client: "TestClient") -> None:
        # Create channels
        client.put(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/bulkpipe/channels/ch1"
        )
        client.put(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/bulkpipe/channels/ch2"
        )

        response = client.post(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/bulkpipe:bulk-channel-status",
            json={"channel_names": ["CH1", "CH2", "NONEXISTENT"]},
        )

        assert response.status_code == 200
        data = response.json()
        assert "channel_statuses" in data
        assert "CH1" in data["channel_statuses"]
        assert "CH2" in data["channel_statuses"]
        assert "NONEXISTENT" not in data["channel_statuses"]

    def test_append_rows_without_continuation_token(self, client: "TestClient") -> None:
        # Create channel first
        client.put(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/appendpipe/channels/appendchannel"
        )

        # Try to append without continuation token
        response = client.post(
            "/v2/streaming/data/databases/testdb/schemas/testschema/pipes/appendpipe/channels/appendchannel/rows",
            content='{"col1": "val1"}\n',
            headers={"Content-Type": "application/x-ndjson"},
        )

        assert response.status_code == 400
        data = response.json()
        assert data["code"] == "STALE_CONTINUATION_TOKEN_SEQUENCER"

    def test_get_single_channel_status(self, client: "TestClient") -> None:
        """Test getting status for a single channel."""
        # Create channel
        client.put(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/statuspipe/channels/statuschannel"
        )

        # Get status
        response = client.get(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/statuspipe/channels/statuschannel/status"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status_code"] == 0
        assert data["channel"].upper() == "STATUSCHANNEL"
        assert "channel_status" in data

    def test_get_single_channel_status_not_found(self, client: "TestClient") -> None:
        """Test getting status for non-existent channel."""
        response = client.get(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/pipe/channels/nonexistent/status"
        )

        assert response.status_code == 404
        data = response.json()
        assert data["code"] == "CHANNEL_NOT_FOUND"

    def test_flush_channel(self, client: "TestClient") -> None:
        """Test flushing a channel."""
        # Create channel
        client.put(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/flushpipe/channels/flushchannel"
        )

        # Flush it
        response = client.post(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/flushpipe/channels/flushchannel:flush"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status_code"] == 0
        assert data["message"] == "Flush completed"

    def test_flush_channel_not_found(self, client: "TestClient") -> None:
        """Test flushing non-existent channel."""
        response = client.post(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/pipe/channels/nonexistent:flush"
        )

        assert response.status_code == 404
        data = response.json()
        assert data["code"] == "CHANNEL_NOT_FOUND"

    def test_get_latest_committed_offset(self, client: "TestClient") -> None:
        """Test getting latest committed offset for a channel."""
        # Create channel with offset token
        client.put(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/offsetpipe/channels/offsetchannel",
            json={"offset_token": "initial_offset"},
        )

        # Get offset
        response = client.get(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/offsetpipe/channels/offsetchannel/offset"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status_code"] == 0
        assert data["offset_token"] == "initial_offset"

    def test_get_latest_committed_offset_not_found(self, client: "TestClient") -> None:
        """Test getting offset for non-existent channel."""
        response = client.get(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/pipe/channels/nonexistent/offset"
        )

        assert response.status_code == 404
        data = response.json()
        assert data["code"] == "CHANNEL_NOT_FOUND"

    def test_register_blob(self, client: "TestClient") -> None:
        """Test registering blobs endpoint."""
        # Create channel
        client.put(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/blobpipe/channels/blobchannel"
        )

        # Register blob (snowduck doesn't use it but should accept)
        response = client.post(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/blobpipe/channels/blobchannel:register-blobs",
            json={"blobs": ["blob1.json", "blob2.json"]},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status_code"] == 0

    def test_validate_credentials(self, client: "TestClient") -> None:
        """Test validate credentials endpoint."""
        response = client.post(
            "/v2/streaming/databases/testdb/schemas/testschema/pipes/mypipe:validate-credentials"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status_code"] == 0
        assert data["message"] == "Credentials validated successfully"
        assert data["permissions"]["can_write"] is True

    def test_list_channels_empty(self, client: "TestClient") -> None:
        """Test listing channels when no channels exist."""
        response = client.get(
            "/v2/streaming/databases/emptydb/schemas/emptyschema/pipes/emptypipe/channels"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status_code"] == 0
        assert data["channels"] == []
        assert data["total"] == 0

    def test_list_channels_with_channels(self, client: "TestClient") -> None:
        """Test listing channels after creating some."""
        # Create channels
        client.put(
            "/v2/streaming/databases/listdb/schemas/listschema/pipes/listpipe/channels/channel1"
        )
        client.put(
            "/v2/streaming/databases/listdb/schemas/listschema/pipes/listpipe/channels/channel2"
        )

        # List channels
        response = client.get(
            "/v2/streaming/databases/listdb/schemas/listschema/pipes/listpipe/channels"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status_code"] == 0
        assert data["total"] == 2
        channel_names = [c["channel_name"] for c in data["channels"]]
        assert "CHANNEL1" in channel_names
        assert "CHANNEL2" in channel_names
