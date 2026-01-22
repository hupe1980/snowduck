import argparse
import gzip
import json
import os
import secrets
from base64 import b64encode
from dataclasses import dataclass
from typing import Awaitable, Callable

import snowflake.connector

from ..connector import Connector, describe_as_rowtype
from .arrow import to_ipc, to_sf
from .serializers import serialize_rowset
from .session_manager import SessionManager

try:
    from starlette.applications import Starlette
    from starlette.concurrency import run_in_threadpool
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.requests import Request
    from starlette.responses import JSONResponse, Response
    from starlette.routing import Route
    from uvicorn import run
except ImportError as e:
    raise ImportError(
        "Optional dependencies for the server are not installed. "
        "Install them using one of the following commands:\n"
        "  - With uv: 'uv sync --extra server'\n"
        "  - With pip: 'pip install snowduck[server]'"
    ) from e


shared_connector = Connector(
    db_file=os.getenv("SNOWDUCK_DB_PATH", ":memory:")
)
session_manager = SessionManager()

@dataclass
class ServerError(Exception):
    status_code: int
    code: str
    message: str


# Middleware
class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """Middleware to handle ServerError exceptions globally."""
    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        try:
            return await call_next(request)
        except ServerError as e:
            return JSONResponse(
                {"data": None, "code": e.code, "message": e.message, "success": False, "headers": None},
                status_code=e.status_code,
            )


class TokenValidationMiddleware(BaseHTTPMiddleware):
    """Middleware to validate Authorization header and extract token."""
    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        # Skip token validation for specific routes
        if request.url.path in ["/session/v1/login-request", "/telemetry/send"]:
            return await call_next(request)

        auth = request.headers.get("Authorization")
        if not auth:
            raise ServerError(status_code=401, code="390101", message="Authorization header not found in the request data.")
        
        token = auth[17:-1]
        request.state.token = token  # Store token in request state for later use

        if not session_manager.session_exists(token):
            raise ServerError(status_code=401, code="390104", message="User must login again to access the service.")

        return await call_next(request)


# Route Handlers
async def login_request(request: Request) -> JSONResponse:
    """Handles login requests and creates a new session."""
    database = request.query_params.get("databaseName")
    schema = request.query_params.get("schemaName")
    
    body = await request.body()
    if request.headers.get("Content-Encoding") == "gzip":
        body = gzip.decompress(body)

    # Parse body for future validation (currently unused)
    _ = json.loads(body)

    token = secrets.token_urlsafe(32)
    connection = shared_connector.connect(database, schema)
    session_manager.create_session(token, connection)
    
    return JSONResponse({
         "data": {
                "token": token,
                "parameters": [{"name": "AUTOCOMMIT", "value": True}],
        },
        "success": True,
    })


async def session(request: Request) -> JSONResponse:
    """Handles session management, including deletion."""
    token = request.state.token  # Token is already validated by middleware

    if bool(request.query_params.get("delete")):
        session_manager.delete_session(token)

    return JSONResponse({"data": None, "code": None, "message": None, "success": True})


async def query_request(request: Request) -> JSONResponse:
    """Handles query execution requests."""
    token = request.state.token  # Token is already validated by middleware
    conn = session_manager.get_session(token)
    lock = session_manager.get_lock(token)

    body = await request.body()
    if request.headers.get("Content-Encoding") == "gzip":
        body = gzip.decompress(body)

    body_json = json.loads(body)
    sql_text = body_json["sqlText"]
    query_result_format = body_json.get("queryResultFormat")
    if not query_result_format:
        accept = request.headers.get("Accept", "").lower()
        user_agent = request.headers.get("User-Agent", "").lower()
        json_user_agents = [
            "snowflake-connector-nodejs",
            "snowflake-vsc",
            "snowflake vscode",
        ]
        if "application/json" in accept or any(tok in user_agent for tok in json_user_agents):
            query_result_format = "json"
        else:
            query_result_format = "arrow"

    print(body_json)

    # Acquire lock for this session to prevent concurrent DuckDB access
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
                    "data": {
                        "errorCode": code,
                        "sqlState": e.sqlstate,
                    },
                    "code": code,
                    "message": e.msg,
                    "success": False,
                }
            )
        except Exception:
            msg = f"Unhandled error during query {sql_text=}"
            raise ServerError(status_code=500, code="261000", message=msg) from None

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
        # "chunks": None,  # Do not initialize chunks to None, as it breaks the Python connector
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
            # Use Cursor.fetchall to get python objects (tuples)
            # and map them to JSON-safe primitives
            rows = serialize_rowset(cur.fetchall())
            chunk_size = int(os.getenv("SNOWDUCK_CHUNK_SIZE", "1000"))

            if chunk_size > 0 and len(rows) > chunk_size:
                first_chunk = rows[:chunk_size]
                remaining = rows[chunk_size:]
                chunks = []
                for idx in range(0, len(remaining), chunk_size):
                    chunk_rows = remaining[idx:idx + chunk_size]
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
                # data["chunks"] = None # Do not set chunks to None
                data["chunkHeaders"] = None
                data["total"] = len(rows)
                data["returned"] = len(rows)

            data["queryResultFormat"] = "json"
    
    return JSONResponse(
        {
            "data": data,
            "success": True,
            "code": None,
            "message": None,
        }
    )


async def abort_request(request: Request) -> JSONResponse:
    """Handles query abort requests."""
    return JSONResponse({"success": True})


async def telemetry_send(request: Request) -> JSONResponse:
    """Handles telemetry data submission."""
    try:
        body = await request.body()
        if request.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)

        body_json = json.loads(body)
        
        print("Received telemetry data:", body_json)

        return JSONResponse({"success": True, "message": "Telemetry data received."})
    except Exception as e:
        raise ServerError(status_code=400, code="400001", message=f"Failed to process telemetry data: {str(e)}") from None


async def fallback_route(request: Request) -> JSONResponse:
    """Fallback route to log unmatched requests."""
    print(f"Received unmatched request: {request.method} {request.url}")
    body = await request.body()
    print(f"Request body: {body.decode('utf-8') if body else 'No body'}")
    return JSONResponse({"success": False, "message": "Route not found."}, status_code=404)


# Application and Routes
routes = [
    Route("/session/v1/login-request", login_request, methods=["POST"]),
    Route("/session", session, methods=["POST"]),
    Route("/queries/v1/query-request", query_request, methods=["POST"]),
    Route("/queries/v1/abort-request", abort_request, methods=["POST"]),
    Route("/telemetry/send", telemetry_send, methods=["POST"]),
    Route("/{path:path}", fallback_route),  # Fallback route
]

app = Starlette(debug=False, routes=routes)
app.add_middleware(ErrorHandlingMiddleware)
app.add_middleware(TokenValidationMiddleware)


# CLI Entry Point
def main() -> None:
    parser = argparse.ArgumentParser(description="Run the SnowMock server.")
    
    parser.add_argument(
        "--host", type=str, default="127.0.0.1", help="Host to run the server on (default: 127.0.0.1)"
    )
    
    parser.add_argument(
        "--port", type=int, default=8000, help="Port to run the server on (default: 8000)"
    )
    
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug mode (default: False)"
    )

    args = parser.parse_args()

    app.debug = args.debug

    # Run the server with the provided arguments
    run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()

