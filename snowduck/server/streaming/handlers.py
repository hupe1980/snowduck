"""HTTP request handlers for Snowpipe Streaming API.

This module contains handler functions organized by responsibility:
- Channel handlers: open, drop, status, flush
- Data handlers: append rows
- Pipe handlers: pipe info, validate credentials
- Auth handlers: token exchange, hostname
"""

from __future__ import annotations

import datetime
import gzip
import io
import json
import os
from typing import TYPE_CHECKING, Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from .auth import generate_scoped_token, parse_form_urlencoded, store_scoped_token
from .channel_manager import ChannelManager

if TYPE_CHECKING:
    pass

# Shared channel manager instance
channel_manager = ChannelManager()


# =============================================================================
# Authentication Handlers
# =============================================================================


async def get_hostname(request: Request) -> Response:
    """GET /v2/streaming/hostname - Return streaming API hostname.
    
    The SDK uses this hostname for subsequent API calls.
    Returns plain text (not JSON) as expected by SDK v1.1.2.
    """
    # Check for environment override
    env_hostname = os.getenv("SNOWDUCK_STREAMING_HOSTNAME")
    if env_hostname:
        return Response(env_hostname, media_type="text/plain")
    
    # Return same host client connected to
    host_header = request.headers.get("host", "")
    if host_header:
        hostname = host_header.split(":")[0]
        return Response(hostname, media_type="text/plain")
    
    return Response("snowduck.snowflakecomputing.com", media_type="text/plain")


async def exchange_scoped_token(request: Request) -> Response:
    """POST /oauth/token - Exchange for scoped API token.
    
    Returns plain text JWT token as expected by SDK v1.1.2.
    """
    content_type = request.headers.get("Content-Type", "")
    body = await _decompress_body(request)
    body_str = body.decode("utf-8")
    
    # Parse request based on content type
    if "application/x-www-form-urlencoded" in content_type:
        params = parse_form_urlencoded(body_str)
        grant_type = params.get("grant_type")
        scope = params.get("scope")
    else:
        body_json = json.loads(body_str)
        grant_type = body_json.get("grant_type")
        scope = body_json.get("scope")
    
    if grant_type != "urn:ietf:params:oauth:grant-type:jwt-bearer":
        return Response("Invalid grant type", status_code=400, media_type="text/plain")
    
    # Generate and store scoped token
    scoped_token = generate_scoped_token(scope)
    original_auth = request.headers.get("Authorization", "")
    store_scoped_token(scoped_token, original_auth)
    
    return Response(scoped_token, media_type="text/plain")


# =============================================================================
# Pipe Handlers
# =============================================================================


async def get_pipe_info(request: Request) -> JSONResponse:
    """GET/POST /v2/streaming/.../pipes/{pipe}:pipe-info - Return pipe metadata.
    
    Returns fake S3 config that passes SDK validation. Data flows via HTTP
    (InsertRows mode), not cloud storage.
    """
    database = request.path_params["databaseName"]
    schema = request.path_params["schemaName"]
    pipe = request.path_params["pipeName"]
    
    now_ms = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
    expiry_ms = now_ms + 3600 * 1000  # 1 hour
    
    return JSONResponse({
        "database": database.upper(),
        "schema": schema.upper(),
        "pipe": pipe.upper(),
        "owner": "TEST_USER",
        "valid": True,
        "stage_location": _create_stage_location(database, schema, pipe, expiry_ms),
        "encryption_info": {
            "pipe_key": "",
            "pipe_key_id": "",
            "diversifier": "",
        },
        "parameter_overrides": _get_sdk_parameters(),
    })


async def validate_credentials(request: Request) -> JSONResponse:
    """POST /v2/streaming/.../pipes/{pipe}:validate-credentials - Validate access.
    
    Always returns success since snowduck uses simplified auth.
    """
    database = request.path_params["databaseName"]
    schema = request.path_params["schemaName"]
    pipe = request.path_params["pipeName"]
    
    return JSONResponse({
        "status_code": 0,
        "message": "Credentials validated successfully",
        "database": database,
        "schema": schema,
        "pipe": pipe,
        "permissions": {
            "can_write": True,
            "can_read": True,
            "can_create_channel": True,
        },
    })


async def get_table_info(request: Request) -> JSONResponse:
    """GET /v2/streaming/.../tables/{table}:table-info - Return table metadata."""
    from ..shared import shared_connector
    
    database = request.path_params["databaseName"]
    schema = request.path_params["schemaName"]
    table = request.path_params["tableName"]
    
    try:
        conn = shared_connector.connect(database, schema)
        cursor = conn.cursor()
        
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable,
                   character_maximum_length, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_catalog = '{database}'
              AND table_schema = '{schema}'
              AND table_name = '{table}'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        cursor.close()
        
        if not columns:
            return JSONResponse(
                {"code": "TABLE_NOT_FOUND", 
                 "message": f"Table {database}.{schema}.{table} not found"},
                status_code=404,
            )
        
        column_info = [
            {
                "name": col[0],
                "type": col[1],
                "nullable": col[2] == "YES",
                "length": col[3],
                "precision": col[4],
                "scale": col[5],
            }
            for col in columns
        ]
        
        return JSONResponse({
            "status_code": 0,
            "database": database,
            "schema": schema,
            "table": table,
            "columns": column_info,
            "row_count": None,
        })
        
    except Exception as e:
        return JSONResponse({"code": "ERROR", "message": str(e)}, status_code=500)


# =============================================================================
# Channel Handlers
# =============================================================================


async def open_channel(request: Request) -> JSONResponse:
    """PUT /v2/streaming/.../channels/{channel} - Open or reopen a channel."""
    database = request.path_params["databaseName"]
    schema = request.path_params["schemaName"]
    pipe = request.path_params["pipeName"]
    channel_name = request.path_params["channelName"]
    
    # Parse optional offset token from body
    offset_token = None
    body = await request.body()
    if body:
        try:
            body = await _decompress_body(request)
            body_json = json.loads(body)
            offset_token = body_json.get("offset_token")
        except (json.JSONDecodeError, Exception):
            pass
    
    channel = channel_manager.open_channel(
        database=database,
        schema=schema,
        pipe=pipe,
        channel_name=channel_name,
        offset_token=offset_token,
    )
    
    # Return combined format for SDK compatibility
    return JSONResponse({
        # Java SDK fields (flat)
        "status_code": 0,
        "message": "Success",
        "database": channel.database_name,
        "schema": channel.schema_name,
        "table": channel.pipe_name,
        "channel": channel.channel_name,
        "client_sequencer": channel.client_sequencer,
        "row_sequencer": channel.row_sequencer,
        "offset_token": channel.status.latest_committed_offset_token,
        # Python SDK fields
        "next_continuation_token": channel.continuation_token,
        "channel_status": channel.status.to_dict(),
    })


async def drop_channel(request: Request) -> JSONResponse:
    """DELETE /v2/streaming/.../channels/{channel} - Drop a channel."""
    database = request.path_params["databaseName"]
    schema = request.path_params["schemaName"]
    pipe = request.path_params["pipeName"]
    channel_name = request.path_params["channelName"]
    
    dropped = channel_manager.drop_channel(database, schema, pipe, channel_name)
    
    if not dropped:
        return JSONResponse(
            {"code": "CHANNEL_NOT_FOUND", "message": "Channel not found"},
            status_code=404,
        )
    
    return JSONResponse({"status": "OK", "message": "Channel dropped"})


async def get_channel_status(request: Request) -> JSONResponse:
    """GET /v2/streaming/.../channels/{channel}/status - Get channel status."""
    database = request.path_params["databaseName"]
    schema = request.path_params["schemaName"]
    pipe = request.path_params["pipeName"]
    channel_name = request.path_params["channelName"]
    
    channel = channel_manager.get_channel(database, schema, pipe, channel_name)
    
    if not channel:
        return JSONResponse(
            {"code": "CHANNEL_NOT_FOUND", "message": "Channel not found"},
            status_code=404,
        )
    
    return JSONResponse({
        "status_code": 0,
        "message": "Success",
        "database": channel.database_name,
        "schema": channel.schema_name,
        "table": channel.pipe_name,
        "channel": channel.channel_name,
        "client_sequencer": channel.client_sequencer,
        "row_sequencer": channel.row_sequencer,
        "offset_token": channel.status.latest_committed_offset_token,
        "channel_status": channel.status.to_dict(),
    })


async def bulk_channel_status(request: Request) -> JSONResponse:
    """POST /v2/streaming/.../pipes/{pipe}:bulk-channel-status - Bulk status.
    
    Response format matches Snowflake REST API documentation:
    https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api
    
    Request: {"channel_names": ["channel1", "channel2"]}
    Response: {"channel_statuses": {"channel1": {...}, "channel2": {...}}}
    """
    database = request.path_params["databaseName"]
    schema = request.path_params["schemaName"]
    pipe = request.path_params["pipeName"]
    
    body = await _decompress_body(request)
    try:
        body_json = json.loads(body)
    except json.JSONDecodeError as e:
        return JSONResponse(
            {"code": "INVALID_PAYLOAD", "message": f"Failed to parse: {e}"},
            status_code=400,
        )
    
    channel_names = body_json.get("channel_names", [])
    if not isinstance(channel_names, list):
        return JSONResponse(
            {"code": "INVALID_PAYLOAD", "message": "channel_names must be an array"},
            status_code=400,
        )
    
    _, channel_statuses = channel_manager.get_bulk_status(
        database, schema, pipe, channel_names
    )
    
    # Response format per REST API documentation
    return JSONResponse({"channel_statuses": channel_statuses})


async def flush_channel(request: Request) -> JSONResponse:
    """POST /v2/streaming/.../channels/{channel}:flush - Flush channel data.
    
    In snowduck, writes are synchronous, so this is a no-op.
    """
    database = request.path_params["databaseName"]
    schema = request.path_params["schemaName"]
    pipe = request.path_params["pipeName"]
    channel_name = request.path_params["channelName"]
    
    channel = channel_manager.get_channel(database, schema, pipe, channel_name)
    
    if not channel:
        return JSONResponse(
            {"code": "CHANNEL_NOT_FOUND", "message": "Channel not found"},
            status_code=404,
        )
    
    return JSONResponse({
        "status_code": 0,
        "message": "Flush completed",
        "offset_token": channel.status.latest_committed_offset_token,
    })


async def get_latest_committed_offset(request: Request) -> JSONResponse:
    """GET /v2/streaming/.../channels/{channel}/offset - Get latest offset."""
    database = request.path_params["databaseName"]
    schema = request.path_params["schemaName"]
    pipe = request.path_params["pipeName"]
    channel_name = request.path_params["channelName"]
    
    channel = channel_manager.get_channel(database, schema, pipe, channel_name)
    
    if not channel:
        return JSONResponse(
            {"code": "CHANNEL_NOT_FOUND", "message": "Channel not found"},
            status_code=404,
        )
    
    return JSONResponse({
        "status_code": 0,
        "offset_token": channel.status.latest_committed_offset_token,
        "rows_inserted": channel.status.rows_inserted,
    })


async def list_channels(request: Request) -> JSONResponse:
    """GET /v2/streaming/.../pipes/{pipe}/channels - List all channels."""
    database = request.path_params["databaseName"]
    schema = request.path_params["schemaName"]
    pipe = request.path_params["pipeName"]
    
    prefix = f"{database.upper()}.{schema.upper()}.{pipe.upper()}."
    
    channels = []
    with channel_manager._lock:
        for key, channel in channel_manager._channels.items():
            if key.startswith(prefix):
                channels.append({
                    "channel_name": channel.channel_name,
                    "client_sequencer": channel.client_sequencer,
                    "row_sequencer": channel.row_sequencer,
                    "status": channel.status.to_dict(),
                    "continuation_token": channel.continuation_token,
                })
    
    return JSONResponse({
        "status_code": 0,
        "database": database,
        "schema": schema,
        "pipe": pipe,
        "channels": channels,
        "total": len(channels),
    })


# =============================================================================
# Data Handlers
# =============================================================================


async def append_rows(request: Request) -> JSONResponse:
    """POST /v2/streaming/data/.../channels/{channel}/rows - Append data rows."""
    from ..shared import shared_connector
    
    database = request.path_params["databaseName"]
    schema = request.path_params["schemaName"]
    pipe = request.path_params["pipeName"]
    channel_name = request.path_params["channelName"]
    continuation_token = request.query_params.get("continuationToken")
    offset_token = request.query_params.get("offsetToken")
    
    # Validate continuation token
    channel = channel_manager.validate_continuation_token(
        database, schema, pipe, channel_name, continuation_token or ""
    )
    if channel is None:
        return JSONResponse(
            {
                "code": "STALE_CONTINUATION_TOKEN_SEQUENCER",
                "message": "Channel sequencer stale. Please reopen the channel",
            },
            status_code=400,
        )
    
    # Parse NDJSON body
    body = await _decompress_body(request)
    try:
        rows = _parse_ndjson(body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        return JSONResponse(
            {"code": "INVALID_PAYLOAD", "message": f"Failed to parse NDJSON: {e}"},
            status_code=400,
        )
    
    if rows:
        table_name = pipe.upper()
        conn = shared_connector.connect(database, schema)
        cursor = conn.cursor()
        
        try:
            # Auto-create table and insert rows
            _create_table_if_needed(cursor, table_name, rows[0])
            _insert_rows(cursor, table_name, rows)
        except Exception as e:
            _record_error(channel, offset_token, str(e), len(rows))
            return JSONResponse({"code": "INSERT_ERROR", "message": str(e)}, status_code=400)
        finally:
            cursor.close()
    
    # Update channel
    row_count = len(rows)
    channel = channel_manager.append_rows(
        database=database,
        schema=schema,
        pipe=pipe,
        channel_name=channel_name,
        row_count=row_count,
        offset_token=offset_token,
    )
    
    return JSONResponse({
        "next_continuation_token": channel.continuation_token,
        "rows_inserted": row_count,
    })


async def register_blob(request: Request) -> JSONResponse:
    """POST /v2/streaming/.../channels/{channel}:register-blobs - Register blobs.
    
    Acknowledge blob registration but prefer HTTP mode.
    """
    body = await _decompress_body(request)
    try:
        body_json = json.loads(body)
    except (json.JSONDecodeError, Exception):
        body_json = {}
    
    return JSONResponse({
        "status_code": 0,
        "message": "Blob registration acknowledged",
        "blobs": body_json.get("blobs", []),
    })


# =============================================================================
# Telemetry Handlers
# =============================================================================


async def send_telemetry(request: Request) -> JSONResponse:
    """POST /telemetry/send/sessionless - Receive SDK telemetry."""
    return JSONResponse({"data": "ok", "success": True})


# =============================================================================
# Helper Functions
# =============================================================================


async def _decompress_body(request: Request) -> bytes:
    """Decompress request body based on Content-Encoding header."""
    body = await request.body()
    encoding = request.headers.get("Content-Encoding", "")
    
    if encoding == "gzip":
        return gzip.decompress(body)
    elif encoding == "zstd":
        try:
            import zstandard as zstd
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(io.BytesIO(body)) as reader:
                return reader.read()
        except ImportError:
            raise ValueError("zstd decompression not available") from None
    
    return body


def _parse_ndjson(data: str) -> list[dict]:
    """Parse newline-delimited JSON format."""
    rows = []
    for line in data.strip().split("\n"):
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _create_stage_location(
    database: str, schema: str, pipe: str, expiry_ms: int
) -> dict[str, Any]:
    """Create fake S3 stage location for SDK compatibility."""
    return {
        "expiry_time_ms": expiry_ms,
        "location_type": "S3",
        "location": f"s3://snowduck-internal/{database}/{schema}/{pipe}/",
        "use_s3_regional_url": False,
        "region": "us-east-1",
        "storage_account": "",
        "storage_endpoint": "",
        "bucket_name": "snowduck-internal",
        "path_prefix": f"{database}/{schema}/{pipe}/",
        "creds": {
            "AWS_KEY_ID": "FAKE_ACCESS_KEY",
            "AWS_SECRET_KEY": "FAKE_SECRET_KEY",
            "AWS_TOKEN": "",
        },
        "parameters": _get_sdk_parameters(),
    }


def _get_sdk_parameters() -> dict[str, int]:
    """Get SDK configuration parameters."""
    return {
        "max_client_lag_seconds": 10,
        "rowset_flush_threshold_kb": 1024,
        "max_buffered_kb_for_rowset_mode": 10240,
        "rowset_threshold_lookback_seconds": 60,
        "rowset_threshold_ewma_weight": 50,
        "target_file_size_compressed_mb": 16,
        "cpu_throttle_high_watermark_percent": 90,
        "cpu_throttle_low_watermark_percent": 50,
        "mem_throttle_high_watermark_percent": 90,
        "mem_throttle_low_watermark_percent": 50,
    }


def _create_table_if_needed(cursor: Any, table_name: str, sample_row: dict) -> None:
    """Auto-create table based on first row structure."""
    col_defs = []
    for col, val in sample_row.items():
        if isinstance(val, bool):
            col_defs.append(f'"{col}" BOOLEAN')
        elif isinstance(val, int):
            col_defs.append(f'"{col}" BIGINT')
        elif isinstance(val, float):
            col_defs.append(f'"{col}" DOUBLE')
        elif isinstance(val, (dict, list)):
            col_defs.append(f'"{col}" JSON')
        else:
            col_defs.append(f'"{col}" VARCHAR')
    
    sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)})'
    cursor.execute(sql)


def _insert_rows(cursor: Any, table_name: str, rows: list[dict]) -> None:
    """Insert rows into table."""
    if not rows:
        return
    
    columns = list(rows[0].keys())
    columns_str = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join("%s" for _ in columns)
    sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'
    
    for row in rows:
        values = []
        for col in columns:
            val = row.get(col)
            if isinstance(val, (dict, list)):
                values.append(json.dumps(val))
            else:
                values.append(val)
        cursor.execute(sql, tuple(values))


def _record_error(
    channel: Any, offset_token: str | None, message: str, row_count: int
) -> None:
    """Record error in channel status."""
    import datetime
    channel.status.rows_error_count += row_count
    channel.status.last_error_message = message
    channel.status.last_error_offset_upper_bound = offset_token
    channel.status.last_error_timestamp = datetime.datetime.now(
        datetime.timezone.utc
    ).isoformat()
