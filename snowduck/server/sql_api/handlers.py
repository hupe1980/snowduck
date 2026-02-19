"""HTTP request handlers for SQL REST API.

Implements the Snowflake SQL REST API spec:
https://docs.snowflake.com/en/developer-guide/sql-api/index.html

Handlers:
    submit_statement: POST /api/v2/statements
    get_statement_status: GET /api/v2/statements/{handle}
    cancel_statement: POST /api/v2/statements/{handle}/cancel
    retry_statement: POST /api/v2/statements/{handle}/retry
    get_query_history: GET /api/v2/query-history
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import snowflake.connector

from .statement_manager import statement_manager
from .types import build_row_type, format_row

if TYPE_CHECKING:
    from starlette.requests import Request
    from starlette.responses import JSONResponse


async def submit_statement(request: "Request") -> "JSONResponse":
    """Submit a SQL statement for execution.
    
    POST /api/v2/statements
    
    Supports:
        - Bind parameters via "bindings" field
        - Async execution via ?async=true query parameter
        - Multi-statement execution via "parameters.multi_statement_count"
    
    Request Body:
        statement: SQL text
        database: Database context
        schema: Schema context
        warehouse: Warehouse context (informational)
        role: Role context (informational)
        bindings: Positional bind parameters
        parameters: Additional execution parameters
    
    Query Parameters:
        async: If "true", return immediately with handle
        nullable: If "false", format nulls as "null" string
    """
    from starlette.responses import JSONResponse

    from ..shared import shared_connector

    # Parse request body
    body = await request.json()
    sql = body.get("statement", "")
    database = body.get("database")
    schema = body.get("schema")
    warehouse = body.get("warehouse")
    role = body.get("role")
    bindings = body.get("bindings", {})
    is_async = request.query_params.get("async", "false").lower() == "true"
    nullable = request.query_params.get("nullable", "true").lower() != "false"

    if not sql:
        return JSONResponse(
            {
                "code": "422000",
                "message": "SQL statement is required",
                "sqlState": "42000",
            },
            status_code=422,
        )

    # Process bind parameters
    bind_values = _convert_bindings(bindings) if bindings else None

    # Create statement record
    stmt = statement_manager.create_statement(
        sql=sql,
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role,
    )

    # For async mode, return immediately with handle
    if is_async:
        stmt.status = "running"
        statement_manager.update_statement(stmt)
        return JSONResponse(
            {
                "code": "333334",  # ASYNC_EXECUTION_IN_PROGRESS
                "sqlState": "00000",
                "message": "Asynchronous execution in progress.",
                "statementHandle": stmt.handle,
                "createdOn": stmt.created_on,
                "statementStatusUrl": f"/api/v2/statements/{stmt.handle}",
            },
            status_code=202,
        )

    # Execute synchronously
    try:
        conn = shared_connector.connect(database, schema)
        cur = conn.cursor()

        # Execute with or without bind parameters
        if bind_values:
            cur.execute(sql, bind_values)
        else:
            cur.execute(sql)

        # Fetch results
        rows = cur.fetchall()
        description = cur.description or []

        stmt.status = "success"
        stmt.result_data = [list(row) for row in rows]
        stmt.result_meta = _build_result_metadata(description, rows, stmt)
        stmt.num_rows = len(rows)

        # Collect DML stats if applicable
        stmt.stats = _collect_dml_stats(sql, cur)

        statement_manager.update_statement(stmt)

        # Build response with partition 0 data
        response_data = {
            "code": "090001",
            "sqlState": "00000",
            "message": "Statement executed successfully.",
            "statementHandle": stmt.handle,
            "createdOn": stmt.created_on,
            "statementStatusUrl": f"/api/v2/statements/{stmt.handle}",
            "resultSetMetaData": stmt.result_meta,
            "data": [format_row(row, nullable) for row in stmt.get_partition(0)],
        }

        if stmt.stats:
            response_data["stats"] = stmt.stats

        headers = _build_link_headers(stmt)
        return JSONResponse(response_data, headers=headers)

    except snowflake.connector.errors.ProgrammingError as e:
        return _handle_programming_error(stmt, e)
    except Exception as e:
        return _handle_generic_error(stmt, e)


async def get_statement_status(request: "Request") -> "JSONResponse":
    """Get statement status and results.
    
    GET /api/v2/statements/{statementHandle}
    
    Query Parameters:
        partition: Partition number to return (0-indexed)
        nullable: If "false", format nulls as "null" string
    """
    from starlette.responses import JSONResponse

    handle = request.path_params["statementHandle"]
    partition = int(request.query_params.get("partition", 0))
    nullable = request.query_params.get("nullable", "true").lower() != "false"

    stmt = statement_manager.get_statement(handle)
    if not stmt:
        return JSONResponse(
            {
                "code": "000404",
                "message": f"Statement with handle {handle} not found",
                "sqlState": "02000",
            },
            status_code=404,
        )

    # Handle non-success states
    if stmt.status == "running":
        return JSONResponse(
            {
                "code": "333334",
                "sqlState": "00000",
                "message": "Statement execution in progress.",
                "statementHandle": stmt.handle,
                "createdOn": stmt.created_on,
                "statementStatusUrl": f"/api/v2/statements/{stmt.handle}",
            },
            status_code=202,
        )

    if stmt.status == "failed":
        return JSONResponse(
            {
                "code": stmt.error_code,
                "sqlState": stmt.sql_state,
                "message": stmt.error_message,
                "statementHandle": stmt.handle,
                "createdOn": stmt.created_on,
            },
            status_code=422,
        )

    if stmt.status == "cancelled":
        return JSONResponse(
            {
                "code": "000604",
                "sqlState": "57014",
                "message": "Statement was cancelled.",
                "statementHandle": stmt.handle,
                "createdOn": stmt.created_on,
            },
            status_code=422,
        )

    # Validate partition number
    partition_count = stmt.get_partition_count()
    if partition_count > 0 and partition >= partition_count:
        return JSONResponse(
            {
                "code": "000001",
                "message": f"Invalid partition {partition}. Valid range: 0-{partition_count - 1}",
                "sqlState": "HY000",
            },
            status_code=422,
        )

    # Get partition data
    partition_data = stmt.get_partition(partition)

    # Build response
    response_data = {
        "code": "090001",
        "sqlState": "00000",
        "message": "Statement executed successfully.",
        "statementHandle": stmt.handle,
        "createdOn": stmt.created_on,
        "statementStatusUrl": f"/api/v2/statements/{stmt.handle}",
        "resultSetMetaData": stmt.result_meta,
        "data": [format_row(row, nullable) for row in partition_data],
    }

    if stmt.stats:
        response_data["stats"] = stmt.stats

    headers = _build_partition_headers(stmt, partition)
    return JSONResponse(response_data, headers=headers)


async def cancel_statement(request: "Request") -> "JSONResponse":
    """Cancel a running statement.
    
    POST /api/v2/statements/{statementHandle}/cancel
    """
    from starlette.responses import JSONResponse

    handle = request.path_params["statementHandle"]

    stmt = statement_manager.get_statement(handle)
    if not stmt:
        return JSONResponse(
            {
                "code": "000404",
                "message": f"Statement with handle {handle} not found",
                "sqlState": "02000",
            },
            status_code=404,
        )

    if statement_manager.cancel_statement(handle):
        return JSONResponse({
            "code": "000000",
            "sqlState": "00000",
            "message": "Statement cancelled successfully.",
            "statementHandle": handle,
            "statementStatusUrl": f"/api/v2/statements/{handle}",
        })
    else:
        return JSONResponse(
            {
                "code": "000605",
                "message": f"Statement {handle} cannot be cancelled (status: {stmt.status})",
                "sqlState": "HY000",
            },
            status_code=422,
        )


async def retry_statement(request: "Request") -> "JSONResponse":
    """Retry a failed statement.
    
    POST /api/v2/statements/{statementHandle}/retry
    """
    from starlette.responses import JSONResponse

    from ..shared import shared_connector

    handle = request.path_params["statementHandle"]

    stmt = statement_manager.get_statement(handle)
    if not stmt:
        return JSONResponse(
            {
                "code": "000404",
                "message": f"Statement with handle {handle} not found",
                "sqlState": "02000",
            },
            status_code=404,
        )

    # Only failed statements can be retried
    if stmt.status != "failed":
        return JSONResponse(
            {
                "code": "000606",
                "message": f"Statement {handle} cannot be retried (status: {stmt.status})",
                "sqlState": "HY000",
            },
            status_code=422,
        )

    # Reset status and re-execute
    stmt.status = "running"
    stmt.error_code = None
    stmt.error_message = None
    stmt.sql_state = None
    stmt.created_on = int(time.time() * 1000)

    try:
        conn = shared_connector.connect(stmt.database, stmt.schema)
        cursor = conn.cursor()
        cursor.execute(stmt.sql)

        # Fetch results
        results = cursor.fetchall()
        description = cursor.description or []

        stmt.status = "success"
        stmt.result_data = [list(row) for row in results]
        stmt.num_rows = len(results)
        stmt.result_meta = {
            "numRows": len(results),
            "format": "jsonv2",
            "rowType": build_row_type(description),
            "partitionInfo": [{
                "rowCount": len(results),
                "compressedSize": 0,
                "uncompressedSize": 0,
            }] if results else [],
        }

        cursor.close()
        statement_manager.update_statement(stmt)

        return JSONResponse({
            "code": "090001",
            "sqlState": "00000",
            "message": "Statement retried successfully.",
            "statementHandle": stmt.handle,
            "createdOn": stmt.created_on,
            "statementStatusUrl": f"/api/v2/statements/{stmt.handle}",
            "resultSetMetaData": stmt.result_meta,
            "data": [format_row(row) for row in stmt.result_data],
        })

    except snowflake.connector.errors.ProgrammingError as e:
        stmt.status = "failed"
        stmt.error_code = str(e.errno) if hasattr(e, "errno") else "100000"
        stmt.error_message = str(e)
        stmt.sql_state = "42000"
        statement_manager.update_statement(stmt)

        return JSONResponse(
            {
                "code": stmt.error_code,
                "message": stmt.error_message,
                "sqlState": stmt.sql_state,
                "statementHandle": stmt.handle,
            },
            status_code=422,
        )


async def get_query_history(request: "Request") -> "JSONResponse":
    """Get history of recent queries.
    
    GET /api/v2/query-history
    
    Query Parameters:
        limit: Maximum number of results (default: 100)
        status: Filter by status (pending, running, success, failed, cancelled)
    """
    from starlette.responses import JSONResponse

    limit = int(request.query_params.get("limit", "100"))
    status_filter = request.query_params.get("status")

    statements = statement_manager.list_statements(status=status_filter, limit=limit)

    # Build response
    queries = []
    for stmt in statements:
        query_info = {
            "statementHandle": stmt.handle,
            "status": stmt.status,
            "sqlText": stmt.sql[:1000] + "..." if len(stmt.sql) > 1000 else stmt.sql,
            "createdOn": stmt.created_on,
            "database": stmt.database,
            "schema": stmt.schema,
            "warehouse": stmt.warehouse,
            "role": stmt.role,
        }

        if stmt.status == "success":
            query_info["rowsReturned"] = stmt.num_rows
        elif stmt.status == "failed":
            query_info["errorCode"] = stmt.error_code
            query_info["errorMessage"] = stmt.error_message

        if stmt.stats:
            query_info["stats"] = stmt.stats

        queries.append(query_info)

    return JSONResponse({
        "code": "000000",
        "sqlState": "00000",
        "message": "Query history retrieved successfully.",
        "queries": queries,
        "total": len(queries),
    })


# =============================================================================
# Helper Functions
# =============================================================================


def _convert_bindings(bindings: dict) -> tuple | None:
    """Convert binding parameters to tuple for SQL execution.
    
    Format: {"1":{"type":"FIXED","value":"123"}, "2":{"type":"TEXT","value":"hello"}}
    """
    bind_values = []
    for key in sorted(bindings.keys(), key=lambda x: int(x)):
        binding = bindings[key]
        value = binding.get("value")
        bind_type = binding.get("type", "TEXT").upper()

        # Convert based on type
        if value is None:
            bind_values.append(None)
        elif bind_type in ("FIXED", "INTEGER", "BIGINT"):
            bind_values.append(int(value))
        elif bind_type in ("REAL", "FLOAT", "DOUBLE"):
            bind_values.append(float(value))
        elif bind_type == "BOOLEAN":
            bind_values.append(str(value).lower() in ("true", "1", "yes"))
        else:
            bind_values.append(str(value))

    return tuple(bind_values)


def _build_result_metadata(description: list, rows: list, stmt: Any) -> dict:
    """Build result set metadata."""
    row_type = build_row_type(description) if description else []
    partition_count = stmt.get_partition_count()
    
    return {
        "numRows": len(rows),
        "format": "jsonv2",
        "rowType": row_type,
        "partitionInfo": [
            {"rowCount": min(stmt.partition_size, len(rows) - i * stmt.partition_size)}
            for i in range(partition_count)
        ] if partition_count > 1 else None,
    }


def _collect_dml_stats(sql: str, cursor: Any) -> dict | None:
    """Collect DML operation statistics."""
    if not hasattr(cursor, "rowcount") or cursor.rowcount < 0:
        return None
    
    sql_upper = sql.strip().upper()
    if sql_upper.startswith("INSERT"):
        return {"numRowsInserted": cursor.rowcount}
    elif sql_upper.startswith("UPDATE"):
        return {"numRowsUpdated": cursor.rowcount}
    elif sql_upper.startswith("DELETE"):
        return {"numRowsDeleted": cursor.rowcount}
    elif sql_upper.startswith("MERGE"):
        return {"numRowsUpdated": cursor.rowcount}
    
    return None


def _build_link_headers(stmt: Any) -> dict:
    """Build Link headers for partitioned results."""
    partition_count = stmt.get_partition_count()
    if partition_count <= 1:
        return {}
    
    links = [
        f'</api/v2/statements/{stmt.handle}?partition=0>; rel="first"',
        f'</api/v2/statements/{stmt.handle}?partition={partition_count - 1}>; rel="last"',
    ]
    if partition_count > 1:
        links.append(f'</api/v2/statements/{stmt.handle}?partition=1>; rel="next"')
    
    return {"Link": ", ".join(links)}


def _build_partition_headers(stmt: Any, partition: int) -> dict:
    """Build Link headers for a specific partition."""
    partition_count = stmt.get_partition_count()
    if partition_count <= 1:
        return {}
    
    links = [
        f'</api/v2/statements/{stmt.handle}?partition=0>; rel="first"',
        f'</api/v2/statements/{stmt.handle}?partition={partition_count - 1}>; rel="last"',
    ]
    if partition > 0:
        links.append(f'</api/v2/statements/{stmt.handle}?partition={partition - 1}>; rel="prev"')
    if partition < partition_count - 1:
        links.append(f'</api/v2/statements/{stmt.handle}?partition={partition + 1}>; rel="next"')
    
    return {"Link": ", ".join(links)}


def _handle_programming_error(stmt: Any, e: Any) -> "JSONResponse":
    """Handle ProgrammingError during execution."""
    from starlette.responses import JSONResponse
    
    stmt.status = "failed"
    stmt.error_code = f"{e.errno:06d}" if e.errno else "000001"
    stmt.error_message = str(e.msg)
    stmt.sql_state = "42000"
    statement_manager.update_statement(stmt)

    return JSONResponse(
        {
            "code": stmt.error_code,
            "sqlState": stmt.sql_state,
            "message": stmt.error_message,
            "statementHandle": stmt.handle,
            "createdOn": stmt.created_on,
        },
        status_code=422,
    )


def _handle_generic_error(stmt: Any, e: Exception) -> "JSONResponse":
    """Handle generic exception during execution."""
    from starlette.responses import JSONResponse
    
    stmt.status = "failed"
    stmt.error_code = "000001"
    stmt.error_message = str(e)
    stmt.sql_state = "42000"
    statement_manager.update_statement(stmt)

    return JSONResponse(
        {
            "code": stmt.error_code,
            "sqlState": stmt.sql_state,
            "message": stmt.error_message,
            "statementHandle": stmt.handle,
            "createdOn": stmt.created_on,
        },
        status_code=422,
    )
