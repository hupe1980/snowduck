"""Statement management for SQL REST API.

This module provides storage and lifecycle management for SQL statement
execution results, with LRU eviction for memory management.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from threading import Lock
from typing import Any


@dataclass
class StatementResult:
    """Stores the result of a SQL statement execution.
    
    Attributes:
        handle: Unique identifier for the statement
        status: Current execution status (pending, running, success, failed, cancelled)
        sql: The SQL statement text
        database: Database context for execution
        schema: Schema context for execution
        warehouse: Warehouse context (informational in snowduck)
        role: Role context (informational in snowduck)
        created_on: Timestamp when statement was created (ms since epoch)
        result_data: Raw result rows
        result_meta: Result metadata (column types, row count, etc.)
        num_rows: Number of rows in result
        partition_size: Rows per partition for large results
        error_code: Error code (on failure)
        error_message: Error message (on failure)
        sql_state: SQL state code (on failure)
        stats: DML operation statistics (rows inserted/updated/deleted)
    """
    
    handle: str
    status: str  # "pending", "running", "success", "failed", "cancelled"
    sql: str
    database: str | None = None
    schema: str | None = None
    warehouse: str | None = None
    role: str | None = None
    created_on: int = field(default_factory=lambda: int(time.time() * 1000))
    
    # Result data (populated on success)
    result_data: list[list[Any]] | None = None
    result_meta: dict[str, Any] | None = None
    num_rows: int = 0
    
    # Partition info for large results
    partition_size: int = 10000  # Number of rows per partition
    
    # Error info (populated on failure)
    error_code: str | None = None
    error_message: str | None = None
    sql_state: str | None = None
    
    # DML stats
    stats: dict[str, int] | None = None
    
    def get_partition_count(self) -> int:
        """Get the number of partitions for this result set."""
        if not self.result_data:
            return 0
        return (len(self.result_data) + self.partition_size - 1) // self.partition_size
    
    def get_partition(self, partition: int) -> list[list[Any]]:
        """Get data for a specific partition.
        
        Args:
            partition: Zero-indexed partition number
            
        Returns:
            List of rows in the partition
        """
        if not self.result_data:
            return []
        start = partition * self.partition_size
        end = start + self.partition_size
        return self.result_data[start:end]


class StatementManager:
    """Manages SQL statement execution and result storage.
    
    Provides thread-safe storage for statement results with LRU eviction
    to prevent unbounded memory growth.
    
    Attributes:
        max_statements: Maximum number of statements to retain
    """
    
    def __init__(self, max_statements: int = 1000) -> None:
        """Initialize the statement manager.
        
        Args:
            max_statements: Maximum number of statements to store before eviction
        """
        self._statements: dict[str, StatementResult] = {}
        self._order: list[str] = []
        self._max_statements = max_statements
        self._lock = Lock()
    
    def create_statement(
        self,
        sql: str,
        database: str | None = None,
        schema: str | None = None,
        warehouse: str | None = None,
        role: str | None = None,
    ) -> StatementResult:
        """Create a new pending statement.
        
        Args:
            sql: The SQL statement text
            database: Database context
            schema: Schema context
            warehouse: Warehouse context (informational)
            role: Role context (informational)
            
        Returns:
            New StatementResult in pending status
        """
        handle = str(uuid.uuid4())
        stmt = StatementResult(
            handle=handle,
            status="pending",
            sql=sql,
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role,
        )
        
        with self._lock:
            # Evict oldest if at capacity
            while len(self._statements) >= self._max_statements and self._order:
                oldest = self._order.pop(0)
                self._statements.pop(oldest, None)
            
            self._statements[handle] = stmt
            self._order.append(handle)
        
        return stmt
    
    def get_statement(self, handle: str) -> StatementResult | None:
        """Get a statement by handle.
        
        Args:
            handle: Statement handle
            
        Returns:
            StatementResult if found, None otherwise
        """
        with self._lock:
            return self._statements.get(handle)
    
    def update_statement(self, stmt: StatementResult) -> None:
        """Update a statement's status/results.
        
        Args:
            stmt: Statement to update
        """
        with self._lock:
            self._statements[stmt.handle] = stmt
    
    def cancel_statement(self, handle: str) -> bool:
        """Cancel a statement.
        
        Args:
            handle: Statement handle
            
        Returns:
            True if found and cancelled, False otherwise
        """
        with self._lock:
            stmt = self._statements.get(handle)
            if stmt and stmt.status in ("pending", "running"):
                stmt.status = "cancelled"
                return True
            return False
    
    def list_statements(
        self,
        status: str | None = None,
        limit: int = 100,
    ) -> list[StatementResult]:
        """List statements with optional filtering.
        
        Args:
            status: Filter by status
            limit: Maximum number to return
            
        Returns:
            List of matching statements, most recent first
        """
        with self._lock:
            statements = list(self._statements.values())
        
        if status:
            statements = [s for s in statements if s.status == status]
        
        # Sort by created_on descending
        statements.sort(key=lambda s: s.created_on, reverse=True)
        
        return statements[:limit]


# Global statement manager instance
statement_manager = StatementManager()
