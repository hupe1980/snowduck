"""Middleware classes for the SnowDuck server.

This module contains HTTP middleware for:
- Error handling: Converts ServerError exceptions to JSON responses
- Token validation: Validates Authorization headers for connector API routes
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from .shared import ServerError, session_manager

if TYPE_CHECKING:
    from starlette.requests import Request
    from starlette.responses import Response


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """Middleware to handle ServerError exceptions globally.
    
    Catches ServerError exceptions and converts them to JSON responses
    with the appropriate HTTP status code and Snowflake error format.
    """

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        try:
            return await call_next(request)
        except ServerError as e:
            return JSONResponse(
                {
                    "data": None,
                    "code": e.code,
                    "message": e.message,
                    "success": False,
                    "headers": None,
                },
                status_code=e.status_code,
            )


class TokenValidationMiddleware(BaseHTTPMiddleware):
    """Middleware to validate Authorization header and extract token.
    
    This middleware validates session tokens for the internal connector API.
    It extracts the token from the Authorization header and verifies it
    exists in the session manager.
    
    Routes that skip validation:
    - Login request (no token yet)
    - Streaming hostname endpoint (public)
    - OAuth token endpoint (uses different auth)
    - Telemetry endpoints (fire-and-forget)
    - All /v2/streaming/ routes (use OAuth/scoped tokens)
    - All /api/v2/ routes (use JWT auth, handled separately)
    """

    # Exact paths that skip token validation
    SKIP_PATHS = frozenset([
        "/session/v1/login-request",
        "/v2/streaming/hostname",
        "/oauth/token",
    ])
    
    # Path prefixes that skip token validation
    SKIP_PREFIXES = (
        "/telemetry/",
        "/v2/streaming/",  # Snowpipe Streaming uses OAuth/scoped tokens
        "/api/v2/",  # SQL REST API uses JWT auth, handled separately
    )

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        path = request.url.path
        
        # Skip validation for specific routes
        if path in self.SKIP_PATHS or any(
            path.startswith(prefix) for prefix in self.SKIP_PREFIXES
        ):
            return await call_next(request)

        auth = request.headers.get("Authorization")
        if not auth:
            raise ServerError(
                status_code=401,
                code="390101",
                message="Authorization header not found in the request data.",
            )

        # Extract token from "Snowflake Token=\"...\"" format
        token = auth[17:-1]
        request.state.token = token  # Store token in request state for later use

        if not session_manager.session_exists(token):
            raise ServerError(
                status_code=401,
                code="390104",
                message="User must login again to access the service.",
            )

        return await call_next(request)
