"""Compatibility tests for Snowpipe Streaming Python SDK.

These tests verify that snowduck's streaming implementation is compatible
with the official Snowflake Snowpipe Streaming Python SDK.

Test Classes:
    TestRESTAPIContract: Tests the exact REST API format that the SDK expects.
        These tests run without the real SDK and verify snowduck returns
        correctly formatted responses.

    TestSDKInterface: Verifies SDK class interfaces match our implementation.
        These tests import the real SDK but don't make network calls.

    TestRealSDKIntegration: Integration tests using the real SDK against snowduck.
        Uses the SDK's InsertRows mode which sends data via HTTP - no actual S3 needed.
        The SDK validates location_type is S3/Azure/GCS, but never connects to storage
        for InsertRows (the default mode). snowduck returns fake S3 credentials.

SDK HTTP Mode Discovery:
    The SDK supports HTTP (non-TLS) connections via undocumented properties:
        scheme='http' - Use HTTP instead of HTTPS
        host='127.0.0.1' - Server hostname
        port='8000' - Server port

    Response Format Requirements:
        /v2/streaming/hostname - Returns plain text string, not JSON
        /oauth/token - Returns plain text JWT token, not JSON object
        Other endpoints - Standard JSON responses

Reference: https://docs.snowflake.com/en/user-guide/snowpipe-streaming-sdk-python/reference/latest/index
"""

from __future__ import annotations

import socket
import time
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

# ============================================================================
# Check for dependencies
# ============================================================================

# Check for server dependencies
try:
    from snowduck.server.channel_manager import ChannelManager
    from starlette.testclient import TestClient as _TestClient

    from snowduck.server import app as _app

    HAS_SERVER_DEPS = True
except ImportError:
    HAS_SERVER_DEPS = False
    _TestClient = None
    _app = None
    ChannelManager = None

# Check for real Snowpipe Streaming SDK
try:
    from snowflake.ingest.streaming import (
        ChannelStatus as SDKChannelStatus,
    )
    from snowflake.ingest.streaming import (
        StreamingIngestChannel as SDKChannel,
    )
    from snowflake.ingest.streaming import (
        StreamingIngestClient as SDKClient,
    )

    HAS_SDK = True
except ImportError:
    HAS_SDK = False
    SDKClient = None
    SDKChannel = None
    SDKChannelStatus = None


# Test RSA private key (for SDK authentication - snowduck accepts any valid JWT)
# This is a test-only key, never use in production
TEST_PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDTjlFXZXgMaoBE
byMFjySJ+GhrUtgik8gqxmb7G9WeNnp3PSY5OzPks3HNiV0Q/RjaXnqOMidTiiL2
lJ3Vbq4sQ6QabB3awS639vUchWHzcRm1OA1zc52U1hUwiSxsWD68Ov2byfb1PMxR
TvgmNpTIu8ObxPlT/lYnEWDPP/mqlSquftvGG8tr87oUZXBXkKPeRwH9nCj1oSh+
zMUcGCvKtYkQt7iekK7MlrWpVr+lFLi02cnbuoJyGR9EbfhP2rbdfAfMmHxBQ8BZ
7kIsvygcj9pYkMJ/4dVK0IVDbDbKCyPLTG8QW3yjvJW3/nkD/O2k5K/N2WgpSEvl
O6QLBaDdAgMBAAECggEACE3bUodonXAmxcjpAoRaZlIKY0AWvNC5ODiefTfb/EBe
MUUEtZVl1ZHXoz5DmXGd4qB3xhIg6g4PjkdKJMEZ9CHZlLHuwBH6L/1xJKP1stUb
MbpYCNKeemOm5EdWJ5CVMdVM9CsK1xiCWqSzZ+iXkYORS5GBSfM/VXxApRH5TmFH
SyMVXcFCDw+4EOrV7yi1hRHKyMSacxzO4G8OjZ/TG1Knkw433ubaisQClB6wlq4T
lmrWITZG7ZQ+ImG2xJ3vd1MjXSo27gwmjKNEPh4PshFRkWRsMsgEVbz6h/Y58JkL
Y0arz6BTXbHMWsL57ZoiYQhwwp38NaWA6sxv6B1bQQKBgQDvZPz0RKFukuROx1Af
VOTp0hhdii902e6SBPX4OX22BI9px+qg8qyyPgB7mPlowEn1EjEgGxFMLnKl1Q9R
CoP7z8Kiz1j9mcGRQfCT+CEcrf5YR3fnPZRM1+7TJz0V/KJUfKWlHx2asjSYNGWw
4LaV3ffsVTxbnZvgxHgEB5C8/QKBgQDiOv2ZmjHinOBWF05A1wUF2X+clVIDGdS4
4jxnz4JBUL78bvxE/Pjj3ZiH+HRbL496GFeBMh3HXsA6LnOvjB9xPO7qDycat1LB
gABhF/oQZRA5zG7tWarQ3p/KZb/IGmKXcqt1E7WyxZRTPU6TPP4ycU+aBI49GGo8
Og4kA4epYQKBgQDXJLk4hZ1XFGhebD2TiuYXRRtkpVW0/E6tqAuuQ2y48iw48tPo
RW/y2Enyyi4LeBR/TRQdOHY5Mt0SMAKN4Jdw7OyNCS9+6nnNo6ckNDD951jX2ZLm
nK70yHL4DSGW6u6wYz0ywl4GsvUVfLGPXsR4t32iRY/y/hgizi7V4D6xuQKBgQC4
Kjuemnb3uqupielrQV9WZrvK1Yfg/Fs+cvWnsLahw0DmsNbutl1K6m8saWcXgD01
sLEzfH/feFPWSVBl3RUPkwIPSmyUBB77ZN9qKyGnzQ4Lb0/yKmezBzhfhLs+A7S1
A2Vutq/Yq51WsfbQR/vLRpD9ma3NMJ3zD3PJf5IloQKBgQCa2uasbrz1v9EUclfw
8SlOrRFo+k+zHylderujXhe4A3xew5ej0uFqBuTgJCKfP40xWIFmOPPg9JmQUxBT
z2ZcMxxlHIF7JC/CHjk319IVK7ltDjYk0Qxw/LVFHGd6ME5SP9vV66pIpbrhZGvG
/3bpVzsETeGf5Rau8t2jzMGHng==
-----END PRIVATE KEY-----"""


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def reset_channel_manager() -> "Iterator[None]":
    """Reset the channel manager for each test."""
    if not HAS_SERVER_DEPS:
        pytest.skip("Server dependencies not installed")

    from snowduck.server import streaming

    streaming.channel_manager = ChannelManager()
    yield


@pytest.fixture
def test_client(reset_channel_manager: None) -> "Iterator[Any]":
    """Create a test client for REST API tests."""
    with _TestClient(_app) as client:
        yield client


@pytest.fixture(scope="class")
def server_url() -> "Iterator[str]":
    """Start snowduck server in subprocess and return URL.

    Used for tests with the real SDK that needs a live server.
    The SDK can connect via HTTP using properties: scheme='http', host, port.

    We use subprocess instead of threading because the SDK's Rust-based HTTP
    client can block/timeout when the server runs in a thread within the same
    process (GIL/event loop issues).
    """
    if not HAS_SERVER_DEPS:
        pytest.skip("Server dependencies not installed")

    import subprocess
    import sys

    import requests

    # Find an available port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        port = s.getsockname()[1]

    # Start server in subprocess (avoids threading issues with Rust SDK)
    server_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "snowduck.server:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for server to be ready
    url = f"http://127.0.0.1:{port}"
    for _ in range(50):  # 5 seconds max
        try:
            r = requests.get(f"{url}/v2/streaming/hostname", timeout=1)
            if r.status_code == 200:
                break
        except Exception:
            time.sleep(0.1)
    else:
        server_proc.kill()
        pytest.fail("Server failed to start")

    yield url

    # Cleanup
    server_proc.kill()
    server_proc.wait(timeout=5)


def _parse_server_url(url: str) -> dict[str, Any]:
    """Parse server URL into SDK properties for HTTP mode.

    The SDK supports HTTP via: scheme='http', host='...', port='...'
    """
    from urllib.parse import urlparse

    parsed = urlparse(url)
    return {
        "scheme": parsed.scheme,  # 'http'
        "host": parsed.hostname,  # '127.0.0.1'
        "port": str(parsed.port),  # port as string
    }


# ============================================================================
# REST API Contract Tests
# These verify the API responses match what the SDK expects
# ============================================================================


@pytest.mark.skipif(not HAS_SERVER_DEPS, reason="Server dependencies not installed")
class TestRESTAPIContract:
    """Tests that verify REST API responses match SDK expectations."""

    def test_get_hostname_response_format(self, test_client: Any) -> None:
        """Verify /v2/streaming/hostname returns expected format.

        SDK (v1.1.2) expects plain text string response, not JSON.
        """
        response = test_client.get("/v2/streaming/hostname")

        assert response.status_code == 200
        # SDK expects plain text response
        assert "text/plain" in response.headers.get("content-type", "")
        hostname = response.text
        assert isinstance(hostname, str)
        assert len(hostname) > 0

    def test_oauth_token_response_format(self, test_client: Any) -> None:
        """Verify /oauth/token returns expected format for SDK.

        SDK (v1.1.2) expects plain text JWT token response, not JSON.
        It parses the JWT to extract the 'exp' claim.
        """
        response = test_client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "scope": "session:role:STREAMING_INGEST",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        assert response.status_code == 200
        # SDK expects plain text JWT token
        assert "text/plain" in response.headers.get("content-type", "")
        token = response.text
        assert isinstance(token, str)
        # JWT format: header.payload.signature
        parts = token.split(".")
        assert len(parts) == 3

    def test_open_channel_response_format(self, test_client: Any) -> None:
        """Verify open channel returns expected format for SDK."""
        # First get a token
        token_response = test_client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "scope": "session:role:STREAMING_INGEST",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        token = token_response.text

        # Open channel
        response = test_client.put(
            "/v2/streaming/databases/TEST_DB/schemas/PUBLIC/pipes/TEST_PIPE/channels/channel1",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 200
        data = response.json()

        # SDK expects these fields
        assert "next_continuation_token" in data
        assert "channel_status" in data

        status = data["channel_status"]
        # Verify all ChannelStatus fields the SDK expects
        assert "database_name" in status
        assert "schema_name" in status
        assert "pipe_name" in status
        assert "channel_name" in status
        assert "channel_status_code" in status  # Required by Rust SDK
        assert "status_code" in status  # Python SDK property name
        assert "created_on_ms" in status
        assert "rows_inserted" in status
        assert "rows_parsed" in status
        assert "rows_error_count" in status
        assert "last_refreshed_on_ms" in status  # SDK requires this

    def test_bulk_channel_status_response_format(self, test_client: Any) -> None:
        """Verify bulk-channel-status returns expected format for SDK."""
        # Get token and open a channel first
        token_response = test_client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "scope": "test",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        token = token_response.text

        test_client.put(
            "/v2/streaming/databases/DB/schemas/SCHEMA/pipes/PIPE/channels/ch1",
            headers={"Authorization": f"Bearer {token}"},
        )

        # Get bulk status
        response = test_client.post(
            "/v2/streaming/databases/DB/schemas/SCHEMA/pipes/PIPE:bulk-channel-status",
            json={"channel_names": ["ch1"]},
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 200
        data = response.json()

        # SDK expects: {"channel_statuses": {"ch1": {...}, ...}}
        assert "channel_statuses" in data
        assert isinstance(data["channel_statuses"], dict)
        assert "ch1" in data["channel_statuses"]

    def test_drop_channel_response(self, test_client: Any) -> None:
        """Verify drop channel returns JSON success response."""
        token_response = test_client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "scope": "test",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        token = token_response.text

        # Create then drop
        test_client.put(
            "/v2/streaming/databases/DB/schemas/SCHEMA/pipes/PIPE/channels/to_drop",
            headers={"Authorization": f"Bearer {token}"},
        )

        response = test_client.delete(
            "/v2/streaming/databases/DB/schemas/SCHEMA/pipes/PIPE/channels/to_drop",
            headers={"Authorization": f"Bearer {token}"},
        )

        # SDK expects JSON response (not 204 No Content)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "OK"

    def test_append_rows_endpoint(self, test_client: Any) -> None:
        """Verify append rows endpoint works correctly."""
        token_response = test_client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "scope": "test",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        token = token_response.text

        # Open channel first and get continuation token
        open_response = test_client.put(
            "/v2/streaming/databases/DB/schemas/SCHEMA/pipes/PIPE/channels/append_test",
            headers={"Authorization": f"Bearer {token}"},
        )
        continuation_token = open_response.json()["next_continuation_token"]

        # Append rows using the correct API path with continuation token in query
        response = test_client.post(
            f"/v2/streaming/data/databases/DB/schemas/SCHEMA/pipes/PIPE/channels/append_test/rows?continuationToken={continuation_token}",
            content='{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}',
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/x-ndjson",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "next_continuation_token" in data
        assert "rows_inserted" in data
        assert data["rows_inserted"] == 2

    def test_open_channel_with_offset_token(self, test_client: Any) -> None:
        """Verify offset_token in body is handled."""
        token_response = test_client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "scope": "test",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        token = token_response.text

        # Open channel with offset_token in body
        response = test_client.put(
            "/v2/streaming/databases/DB/schemas/SCHEMA/pipes/PIPE/channels/offset_ch",
            json={"offset_token": "my_offset"},
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["channel_status"]["latest_committed_offset_token"] == "my_offset"

    def test_drop_nonexistent_channel_returns_404(self, test_client: Any) -> None:
        """Verify 404 for non-existent channel."""
        token_response = test_client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "scope": "test",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        token = token_response.text

        response = test_client.delete(
            "/v2/streaming/databases/DB/schemas/SCHEMA/pipes/PIPE/channels/nonexistent",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 404

    def test_append_to_nonexistent_channel_returns_400(self, test_client: Any) -> None:
        """Verify 400 when appending with invalid continuation token."""
        token_response = test_client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "scope": "test",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        token = token_response.text

        # Append to non-existent channel returns 400 (stale continuation token)
        response = test_client.post(
            "/v2/streaming/data/databases/DB/schemas/SCHEMA/pipes/PIPE/channels/missing/rows?continuationToken=invalid",
            content='{"a": 1}',
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/x-ndjson",
            },
        )

        # API returns 400 for invalid/stale continuation token
        assert response.status_code == 400
        data = response.json()
        assert data["code"] == "STALE_CONTINUATION_TOKEN_SEQUENCER"


# ============================================================================
# Real SDK Integration Tests
#
# These tests use the real Snowpipe Streaming Python SDK against snowduck.
# The SDK supports HTTP mode via properties: scheme='http', host='...', port='...'
# This allows testing without SSL/TLS certificate setup.
# ============================================================================


@pytest.mark.integration
@pytest.mark.skipif(
    not HAS_SDK or not HAS_SERVER_DEPS,
    reason="Requires snowpipe-streaming SDK and server dependencies",
)
class TestRealSDKIntegration:
    """Integration tests using the real Snowpipe Streaming SDK.

    These tests prove that snowduck can replace Snowflake for SDK testing.
    The SDK uses InsertRows mode by default which sends zstd-compressed NDJSON
    via HTTP POST - no actual S3/Azure/GCS storage is needed.

    SDK HTTP mode properties:
    - scheme='http' - Use HTTP instead of HTTPS
    - host='127.0.0.1' - Server hostname
    - port='<port>' - Server port

    Note: snowduck returns fake S3 credentials in get_pipe_info() to satisfy
    SDK validation, but InsertRows mode never actually connects to storage.
    """

    def _get_sdk_properties(self, server_url: str) -> dict[str, Any]:
        """Get SDK properties for HTTP connection to snowduck."""
        props = _parse_server_url(server_url)
        props.update(
            {
                "account": "snowduck",
                "user": "test",
                "private_key": TEST_PRIVATE_KEY,
                # Reduce timeouts for faster test execution
                "mgmt_http_timeout_seconds": "10",
                "data_http_timeout_seconds": "10",
                "mgmt_retry_total_backoff_seconds": "5",
                "data_retry_total_backoff_seconds": "5",
            }
        )
        return props

    def test_sdk_open_channel(self, server_url: str) -> None:
        """Test opening a channel with the real SDK."""
        client = SDKClient(
            client_name="test_client",
            db_name="TEST_DB",
            schema_name="PUBLIC",
            pipe_name="TEST_PIPE",
            properties=self._get_sdk_properties(server_url),
        )

        try:
            channel, status = client.open_channel("test_channel")

            # Verify SDK objects are returned
            assert isinstance(channel, SDKChannel)
            assert isinstance(status, SDKChannelStatus)

            # Verify status properties
            assert status.status_code == "ACTIVE"
            assert status.database_name.upper() == "TEST_DB"
            assert status.schema_name.upper() == "PUBLIC"

            channel.close()
        finally:
            client.close()

    def test_sdk_channel_properties(self, server_url: str) -> None:
        """Test channel properties match SDK interface."""
        client = SDKClient(
            client_name="prop_test",
            db_name="MY_DB",
            schema_name="MY_SCHEMA",
            pipe_name="MY_PIPE",
            properties=self._get_sdk_properties(server_url),
        )

        try:
            channel, _ = client.open_channel("prop_channel")

            # Verify channel properties
            assert channel.db_name.upper() == "MY_DB"
            assert channel.schema_name.upper() == "MY_SCHEMA"
            assert channel.pipe_name.upper() == "MY_PIPE"
            assert channel.channel_name == "prop_channel"
            assert channel.is_closed() is False

            channel.close()
            assert channel.is_closed() is True
        finally:
            client.close()

    @pytest.mark.xfail(
        reason="Python SDK v1.1.2 Rust core limitation: SfChannelStatus struct doesn't parse offset_token fields. "
        "Inspecting _python_ffi.abi3.so shows SfChannelStatus has 14 fields but NO offset_token variant. "
        "Server returns correct data (offset_token, persisted_offset_token, latest_committed_offset_token) "
        "but SDK ignores all of them. This is a Snowflake SDK design/bug, not a snowduck issue."
    )
    def test_sdk_get_channel_status(self, server_url: str) -> None:
        """Test getting channel status with real SDK.

        This test verifies offset_token round-trip:
        1. Open channel with offset_token="initial_offset"
        2. Call get_channel_status()
        3. Expect latest_committed_offset_token == "initial_offset"

        Known SDK limitation: Python SDK's Rust core (SfChannelStatus struct)
        doesn't include offset token fields in its JSON parsing definition.
        The Python ChannelStatus class HAS the attribute, but it's always None.

        Verified by:
        - Binary inspection of _python_ffi.abi3.so with `strings | grep SfChannelStatus`
        - HTTP trace showing server returns correct offset_token values
        - Testing with data insertion confirms SDK never populates the field
        """
        client = SDKClient(
            client_name="status_test",
            db_name="DB",
            schema_name="SCHEMA",
            pipe_name="PIPE",
            properties=self._get_sdk_properties(server_url),
        )

        try:
            channel, initial_status = client.open_channel(
                "status_channel", offset_token="initial_offset"
            )

            # Get status through channel
            status = channel.get_channel_status()
            assert isinstance(status, SDKChannelStatus)
            assert status.latest_committed_offset_token == "initial_offset"

            # Get status through client
            statuses = client.get_channel_statuses(["status_channel"])
            assert "status_channel" in statuses

            channel.close()
        finally:
            client.close()

    def test_sdk_drop_channel(self, server_url: str) -> None:
        """Test dropping a channel with real SDK.

        Note: The SDK caches channel status locally and doesn't invalidate on drop.
        This test verifies the server correctly removes the channel by making a
        direct API call to check server state.
        """
        import requests

        client = SDKClient(
            client_name="drop_test",
            db_name="DB",
            schema_name="SCHEMA",
            pipe_name="PIPE",
            properties=self._get_sdk_properties(server_url),
        )

        try:
            channel, _ = client.open_channel("to_drop")

            # Verify channel exists via SDK
            statuses = client.get_channel_statuses(["to_drop"])
            assert "to_drop" in statuses

            # Verify channel exists on server directly
            resp = requests.post(
                f"{server_url}/v2/streaming/databases/DB/schemas/SCHEMA/pipes/PIPE:bulk-channel-status",
                json={"channel_names": ["to_drop"]},
                timeout=5,
            )
            assert resp.status_code == 200
            data = resp.json()
            # Server should return 1 channel in the channels list
            assert len(data.get("channels", [])) == 1

            # Drop via SDK client
            client.drop_channel("to_drop")

            # SDK caches locally so get_channel_statuses may still return the channel.
            # Instead, verify the server actually dropped the channel by making a direct API call.
            resp = requests.post(
                f"{server_url}/v2/streaming/databases/DB/schemas/SCHEMA/pipes/PIPE:bulk-channel-status",
                json={"channel_names": ["to_drop"]},
                timeout=5,
            )
            assert resp.status_code == 200
            data = resp.json()
            # Server should return empty channels list after drop
            assert len(data.get("channels", [])) == 0, (
                f"Channel should be dropped on server, got: {data}"
            )
        finally:
            client.close()

    @pytest.mark.xfail(
        reason="Python SDK v1.1.2 Rust core limitation: SfChannelStatus struct doesn't parse offset_token. "
        "get_latest_committed_offset_tokens() internally calls get_channel_statuses() which returns None. "
        "Same root cause as test_sdk_get_channel_status - SDK design issue, not snowduck."
    )
    def test_sdk_multi_channel_workflow(self, server_url: str) -> None:
        """Test multi-channel workflow matching real SDK usage.

        This test exercises the bulk offset token retrieval API which is
        critical for Kafka connector usage patterns.

        Known SDK limitation: Same as test_sdk_get_channel_status - the Rust
        SfChannelStatus struct doesn't include offset_token fields, so
        get_latest_committed_offset_tokens() always returns {channel: None}.
        """
        client = SDKClient(
            client_name="multi_test",
            db_name="DB",
            schema_name="SCHEMA",
            pipe_name="PIPE",
            properties=self._get_sdk_properties(server_url),
        )

        try:
            # Open multiple channels
            ch1, _ = client.open_channel("ch1", offset_token="offset_1")
            ch2, _ = client.open_channel("ch2", offset_token="offset_2")
            ch3, _ = client.open_channel("ch3")

            # Bulk get statuses
            statuses = client.get_channel_statuses(["ch1", "ch2", "ch3"])
            assert len(statuses) == 3

            # Get bulk offset tokens
            tokens = client.get_latest_committed_offset_tokens(["ch1", "ch2"])
            assert tokens["ch1"] == "offset_1"
            assert tokens["ch2"] == "offset_2"

            # Close channels
            ch1.close()
            ch2.close()
            ch3.close()
        finally:
            client.close()

    def test_sdk_reopen_channel(self, server_url: str) -> None:
        """Test reopening a channel (client sequencer bump)."""
        client = SDKClient(
            client_name="reopen_test",
            db_name="DB",
            schema_name="SCHEMA",
            pipe_name="PIPE",
            properties=self._get_sdk_properties(server_url),
        )

        try:
            # First open
            ch1, status1 = client.open_channel("reopen_channel")
            ch1.close()

            # Reopen same channel
            ch2, status2 = client.open_channel("reopen_channel")

            # Should get new channel instance
            assert ch2.is_closed() is False

            ch2.close()
        finally:
            client.close()

    def test_sdk_append_row_and_flush(self, server_url: str) -> None:
        """Test appending rows and flushing with real SDK.

        This verifies the full data path:
        1. Open channel
        2. Append rows
        3. Close channel (triggers flush)
        4. Check counters are updated
        """
        import requests

        client = SDKClient(
            client_name="append_test",
            db_name="DB",
            schema_name="SCHEMA",
            pipe_name="PIPE",
            properties=self._get_sdk_properties(server_url),
        )

        try:
            channel, _ = client.open_channel("append_channel")

            # Append some rows
            channel.append_row({"id": 1, "name": "Alice"})
            channel.append_row({"id": 2, "name": "Bob"})
            channel.append_row({"id": 3, "name": "Charlie"})

            # Close channel (wait_for_flush=True by default ensures data is sent)
            channel.close()

            # Verify server received the data by checking channel status directly
            resp = requests.post(
                f"{server_url}/v2/streaming/databases/DB/schemas/SCHEMA/pipes/PIPE:bulk-channel-status",
                json={"channel_names": ["append_channel"]},
                timeout=5,
            )
            assert resp.status_code == 200
            data = resp.json()

            # Check channel_statuses dict has the channel
            channel_statuses = data.get("channel_statuses", {})
            assert "append_channel" in channel_statuses
            status = channel_statuses["append_channel"]
            assert status.get("rows_inserted", 0) == 3
            assert status.get("rows_parsed", 0) == 3

        finally:
            client.close()


# ============================================================================
# SDK Interface Verification Tests
# These verify SDK types and interfaces without network calls
# ============================================================================


@pytest.mark.skipif(not HAS_SDK, reason="Requires snowpipe-streaming SDK")
class TestSDKInterface:
    """Tests that verify SDK interface matches our implementation."""

    def test_sdk_client_has_expected_interface(self) -> None:
        """Verify StreamingIngestClient class has expected interface."""
        import inspect

        sig = inspect.signature(SDKClient.__init__)
        params = list(sig.parameters.keys())

        assert "client_name" in params
        assert "db_name" in params
        assert "schema_name" in params
        assert "pipe_name" in params
        assert "properties" in params

    def test_sdk_channel_has_expected_methods(self) -> None:
        """Verify StreamingIngestChannel has expected methods."""
        assert hasattr(SDKChannel, "close")
        assert hasattr(SDKChannel, "is_closed")
        assert callable(getattr(SDKChannel, "close", None))

    def test_sdk_module_exports(self) -> None:
        """Verify SDK module structure matches expectations."""
        from snowflake.ingest import streaming

        assert hasattr(streaming, "StreamingIngestClient")
        assert hasattr(streaming, "StreamingIngestChannel")
        assert hasattr(streaming, "ChannelStatus")
