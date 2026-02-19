"""SQL REST API package.

Implements the Snowflake SQL REST API:
https://docs.snowflake.com/en/developer-guide/sql-api/index.html

Endpoints:
    POST /api/v2/statements - Submit SQL statement
    GET /api/v2/statements/{handle} - Get statement status/results
    POST /api/v2/statements/{handle}/cancel - Cancel statement
    POST /api/v2/statements/{handle}/retry - Retry failed statement
    GET /api/v2/query-history - Get query history

Modules:
    handlers: HTTP request handlers
    statement_manager: Statement storage and lifecycle
    types: Type conversion utilities
    routes: Route definitions
"""

from .routes import get_sql_api_routes, sql_api_routes
from .statement_manager import StatementManager, StatementResult, statement_manager
from .types import build_row_type, format_row, format_value, type_code_to_name

__all__ = [
    # Routes
    "get_sql_api_routes",
    "sql_api_routes",
    # Statement management
    "StatementManager",
    "StatementResult",
    "statement_manager",
    # Type utilities
    "build_row_type",
    "format_row",
    "format_value",
    "type_code_to_name",
]
