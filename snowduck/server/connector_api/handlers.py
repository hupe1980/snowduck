"""HTTP request handlers for Internal Connector API.

These endpoints are NOT part of the public Snowflake REST API.
They are reverse-engineered from snowflake-connector-python to provide
compatibility with official Snowflake drivers.

Handlers:
    login_request: POST /session/v1/login-request
    session: POST/GET /session
    query_request: POST /queries/v1/query-request
    abort_request: POST /queries/v1/abort-request
    heartbeat_request: POST /session/heartbeat-request
    telemetry_send: POST /telemetry/send
"""

from __future__ import annotations

import gzip
import json
import os
import secrets
from base64 import b64encode
from typing import TYPE_CHECKING

import snowflake.connector
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse

from ...connector import describe_as_rowtype
from ..arrow import to_ipc, to_sf
from ..serializers import serialize_rowset
from ..shared import ServerError, session_manager, shared_connector

if TYPE_CHECKING:
    from starlette.requests import Request


# =============================================================================
# Authentication Handlers
# =============================================================================


async def login_request(request: "Request") -> JSONResponse:
    """Handle login requests and create a new session.

    POST /session/v1/login-request

    Creates a session token for subsequent API calls.

    Query Parameters:
        databaseName: Optional database context
        schemaName: Optional schema context
    """
    database = request.query_params.get("databaseName")
    schema = request.query_params.get("schemaName")

    body = await request.body()
    if request.headers.get("Content-Encoding") == "gzip":
        body = gzip.decompress(body)

    # Parse body for future validation
    _ = json.loads(body)

    token = secrets.token_urlsafe(32)
    connection = shared_connector.connect(database, schema)
    session_manager.create_session(token, connection)

    return JSONResponse(
        {
            "data": {
                "token": token,
                "parameters": [{"name": "AUTOCOMMIT", "value": True}],
            },
            "success": True,
        }
    )


async def session(request: "Request") -> JSONResponse:
    """Handle session management.

    GET: Returns session info
    POST: Delete session if ?delete=true
    """
    token = request.state.token

    # Handle GET for session info
    if request.method == "GET":
        return await get_session_info(request)

    if bool(request.query_params.get("delete")):
        session_manager.delete_session(token)

    return JSONResponse({"data": None, "code": None, "message": None, "success": True})


async def get_session_info(request: "Request") -> JSONResponse:
    """Get current session information.

    GET /session
    """
    token = request.state.token

    if not session_manager.session_exists(token):
        return JSONResponse(
            {
                "data": None,
                "code": "390104",
                "message": "Session no longer exists.",
                "success": False,
            },
            status_code=401,
        )

    conn = session_manager.get_session(token)

    # Get current database/schema
    database = None
    schema = None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT current_database(), current_schema()")
        result = cursor.fetchone()
        if result:
            database = result[0]
            schema = result[1]
        cursor.close()
    except Exception:
        pass

    return JSONResponse(
        {
            "data": {
                "sessionId": token[:16] + "...",
                "masterToken": token,
                "idToken": None,
                "valid": True,
                "database": database,
                "schema": schema,
                "warehouse": None,
                "role": None,
                "parameters": [
                    {"name": "AUTOCOMMIT", "value": True},
                    {"name": "TIMEZONE", "value": "UTC"},
                    {"name": "DATE_OUTPUT_FORMAT", "value": "YYYY-MM-DD"},
                    {"name": "TIME_OUTPUT_FORMAT", "value": "HH24:MI:SS"},
                    {
                        "name": "TIMESTAMP_OUTPUT_FORMAT",
                        "value": "YYYY-MM-DD HH24:MI:SS.FF3",
                    },
                ],
            },
            "success": True,
            "code": None,
            "message": None,
        }
    )


async def heartbeat_request(request: "Request") -> JSONResponse:
    """Handle session heartbeat.

    POST /session/heartbeat-request
    """
    token = request.state.token

    if not session_manager.session_exists(token):
        return JSONResponse(
            {
                "data": None,
                "code": "390104",
                "message": "Session no longer exists.",
                "success": False,
            },
            status_code=401,
        )

    return JSONResponse(
        {
            "data": {"masterValidatedTokens": {}},
            "success": True,
            "code": None,
            "message": None,
        }
    )


async def authenticator_request(request: "Request") -> JSONResponse:
    """Return available authentication methods.

    POST /session/authenticator-request
    """
    return JSONResponse(
        {
            "data": {
                "authnMethod": "PASSWORD",
                "federationInfo": None,
            },
            "success": True,
            "code": None,
            "message": None,
        }
    )


async def set_session_parameters(request: "Request") -> JSONResponse:
    """Set session parameters.

    POST /session/parameters
    """
    token = request.state.token

    if not session_manager.session_exists(token):
        return JSONResponse(
            {
                "data": None,
                "code": "390104",
                "message": "Session no longer exists.",
                "success": False,
            },
            status_code=401,
        )

    body = await request.body()
    if request.headers.get("Content-Encoding") == "gzip":
        body = gzip.decompress(body)

    body_json = json.loads(body)
    parameters = body_json.get("parameters", [])

    conn = session_manager.get_session(token)

    # Process each parameter
    applied = []
    for param in parameters:
        name = param.get("name", "").upper()
        value = param.get("value")

        try:
            if name in (
                "TIMEZONE",
                "DATE_OUTPUT_FORMAT",
                "TIME_OUTPUT_FORMAT",
                "TIMESTAMP_OUTPUT_FORMAT",
                "QUERY_TAG",
            ):
                applied.append({"name": name, "value": value, "status": "accepted"})
            elif name == "USE_DATABASE":
                cursor = conn.cursor()
                cursor.execute(f"USE {value}")
                cursor.close()
                applied.append({"name": name, "value": value, "status": "applied"})
            elif name == "USE_SCHEMA":
                cursor = conn.cursor()
                cursor.execute(f"USE SCHEMA {value}")
                cursor.close()
                applied.append({"name": name, "value": value, "status": "applied"})
            else:
                applied.append({"name": name, "value": value, "status": "ignored"})
        except Exception as e:
            applied.append(
                {"name": name, "value": value, "status": "error", "message": str(e)}
            )

    return JSONResponse(
        {
            "data": {"parameters": applied},
            "success": True,
            "code": None,
            "message": "Session parameters updated.",
        }
    )


async def renew_session(request: "Request") -> JSONResponse:
    """Renew session token.

    POST /session/token-request
    """
    token = request.state.token

    if not session_manager.session_exists(token):
        return JSONResponse(
            {
                "data": None,
                "code": "390104",
                "message": "Session no longer exists.",
                "success": False,
            },
            status_code=401,
        )

    return JSONResponse(
        {
            "data": {
                "sessionToken": token,
                "validityInSeconds": 3600,
                "masterValidatedTokens": {},
            },
            "success": True,
            "code": None,
            "message": "Session renewed successfully.",
        }
    )


# =============================================================================
# Query Handlers
# =============================================================================


async def query_request(request: "Request") -> JSONResponse:
    """Execute SQL query.

    POST /queries/v1/query-request

    Request Body:
        sqlText: SQL statement
        queryResultFormat: "arrow" or "json"
    """
    token = request.state.token
    conn = session_manager.get_session(token)
    lock = session_manager.get_lock(token)

    body = await request.body()
    if request.headers.get("Content-Encoding") == "gzip":
        body = gzip.decompress(body)

    body_json = json.loads(body)
    sql_text = body_json["sqlText"]
    query_result_format = _detect_result_format(request, body_json)

    print(body_json)

    async with lock:
        try:
            print("Executing SQL:", sql_text)
            cur = await run_in_threadpool(conn.cursor().execute, sql_text)
            describe_results = cur.describe_last_sql()
            overrides = None
            if cur.last_table_name and conn.database and conn.schema:
                columns = conn.get_column_metadata(cur.last_table_name)
                overrides = {c["name"]: c for c in columns}

            rowtype = describe_as_rowtype(
                describe_results,
                database=conn.database,
                schema=conn.schema,
                table=cur.last_table_name,
                overrides=overrides,
            )
        except snowflake.connector.errors.ProgrammingError as e:
            code = f"{e.errno:06d}"
            return JSONResponse(
                {
                    "data": {"errorCode": code, "sqlState": e.sqlstate},
                    "code": code,
                    "message": e.msg,
                    "success": False,
                }
            )
        except Exception:
            msg = f"Unhandled error during query {sql_text=}"
            raise ServerError(status_code=500, code="261000", message=msg) from None

    data = _build_query_response(cur, conn, rowtype, query_result_format)

    return JSONResponse(
        {
            "data": data,
            "success": True,
            "code": None,
            "message": None,
        }
    )


async def get_query_result(request: "Request") -> JSONResponse:
    """Get query results by query ID.

    GET /queries/v1/query-request/{queryId}
    """
    query_id = request.path_params.get("queryId")

    # In snowduck, all queries are synchronous
    return JSONResponse(
        {
            "data": {"errorCode": "000709", "sqlState": "02000"},
            "code": "000709",
            "message": f"Statement {query_id} not found",
            "success": False,
        },
        status_code=422,
    )


async def abort_request(request: "Request") -> JSONResponse:
    """Abort running query.

    POST /queries/v1/abort-request
    """
    return JSONResponse({"success": True})


# =============================================================================
# Telemetry Handlers
# =============================================================================


async def telemetry_send(request: "Request") -> JSONResponse:
    """Receive telemetry data.

    POST /telemetry/send
    """
    try:
        body = await request.body()
        if request.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)

        body_json = json.loads(body)
        print("Received telemetry data:", body_json)

        return JSONResponse({"success": True, "message": "Telemetry data received."})
    except Exception as e:
        raise ServerError(
            status_code=400,
            code="400001",
            message=f"Failed to process telemetry data: {str(e)}",
        ) from None


# =============================================================================
# Helper Functions
# =============================================================================


def _detect_result_format(request: "Request", body_json: dict) -> str:
    """Detect query result format from request."""
    query_result_format = body_json.get("queryResultFormat")

    if not query_result_format:
        accept = request.headers.get("Accept", "").lower()
        user_agent = request.headers.get("User-Agent", "").lower()
        json_user_agents = [
            "snowflake-connector-nodejs",
            "snowflake-vsc",
            "snowflake vscode",
        ]
        if "application/json" in accept or any(
            tok in user_agent for tok in json_user_agents
        ):
            query_result_format = "json"
        else:
            query_result_format = "arrow"

    return query_result_format


def _build_query_response(cur, conn, rowtype: list, query_result_format: str) -> dict:
    """Build query response data."""
    data = {
        "parameters": [],
        "rowtype": rowtype,
        "rowset": [],
        "total": 0,
        "returned": 0,
        "queryId": cur.sfqid,
        "sqlState": cur.sqlstate,
        "database": conn._database or "",
        "schema": conn._schema or "",
        "finalDatabaseName": conn._database or "",
        "finalSchemaName": conn._schema or "",
        "finalRoleName": "SYSADMIN",
        "chunkHeaders": None,
        "qrmk": None,
    }

    if cur._arrow_table:
        if query_result_format == "arrow":
            batch_bytes = to_ipc(to_sf(cur._arrow_table, rowtype))
            rowset_b64 = b64encode(batch_bytes).decode("utf-8")
            data["rowsetBase64"] = rowset_b64
            data["queryResultFormat"] = "arrow"
            data["total"] = cur.rowcount
            data["returned"] = cur.rowcount
        else:
            # JSON/native format
            rows = serialize_rowset(cur.fetchall())
            chunk_size = int(os.getenv("SNOWDUCK_CHUNK_SIZE", "1000"))

            if chunk_size > 0 and len(rows) > chunk_size:
                first_chunk = rows[:chunk_size]
                remaining = rows[chunk_size:]
                chunks = []
                for idx in range(0, len(remaining), chunk_size):
                    chunk_rows = remaining[idx : idx + chunk_size]
                    chunks.append(
                        {
                            "rowCount": len(chunk_rows),
                            "rowset": chunk_rows,
                            "chunkIndex": (idx // chunk_size) + 1,
                        }
                    )

                data["rowset"] = first_chunk
                data["chunks"] = chunks
                data["chunkHeaders"] = {
                    "chunkCount": len(chunks) + 1,
                    "rowsetSize": chunk_size,
                }
                data["total"] = len(rows)
                data["returned"] = len(first_chunk)
            else:
                data["rowset"] = rows
                data["chunkHeaders"] = None
                data["total"] = len(rows)
                data["returned"] = len(rows)

            data["queryResultFormat"] = "json"

    return data
