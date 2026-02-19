"""Shared state and utilities for the SnowDuck server.

This module contains:
- ServerError exception class
- Shared connector and session manager instances
- Common utilities used across routes
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from ..connector import Connector
from .session_manager import SessionManager

# Shared DuckDB connector instance
# Use SNOWDUCK_DB_PATH environment variable for persistence, or in-memory by default
shared_connector = Connector(db_file=os.getenv("SNOWDUCK_DB_PATH", ":memory:"))

# Shared session manager for tracking active sessions
session_manager = SessionManager()


@dataclass
class ServerError(Exception):
    """Exception raised for server errors with HTTP status code and Snowflake error code."""

    status_code: int
    code: str
    message: str
