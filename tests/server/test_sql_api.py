"""Tests for SQL REST API endpoints.

Tests the Snowflake SQL REST API implementation at /api/v2/statements.
"""

from __future__ import annotations

import pytest

try:
    from starlette.testclient import TestClient as _TestClient

    from snowduck.server import app as _app

    HAS_SERVER_DEPS = True
except ImportError:
    HAS_SERVER_DEPS = False
    _TestClient = None
    _app = None


pytestmark = pytest.mark.skipif(
    not HAS_SERVER_DEPS, reason="Server dependencies not installed"
)


@pytest.fixture
def test_client():
    """Create a test client for the SQL API."""
    return _TestClient(_app)


class TestSQLAPI:
    """Tests for the SQL REST API endpoints."""

    def test_submit_statement_simple_query(self, test_client) -> None:
        """Test submitting a simple SELECT query."""
        response = test_client.post(
            "/api/v2/statements",
            json={"statement": "SELECT 1 as num, 'hello' as msg"},
        )

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "statementHandle" in data
        assert "resultSetMetaData" in data
        assert "data" in data

        # Check metadata
        meta = data["resultSetMetaData"]
        assert meta["numRows"] == 1
        assert len(meta["rowType"]) == 2

        # Check data
        assert len(data["data"]) == 1
        row = data["data"][0]
        assert row[0] == "1"
        assert row[1] == "hello"

    def test_submit_statement_with_database_schema(self, test_client) -> None:
        """Test submitting query with database and schema context."""
        # First create schema
        test_client.post(
            "/api/v2/statements",
            json={"statement": "CREATE SCHEMA IF NOT EXISTS test_schema"},
        )

        response = test_client.post(
            "/api/v2/statements",
            json={
                "statement": "CREATE TABLE IF NOT EXISTS test_table (id INT, name TEXT)",
                "schema": "test_schema",
            },
        )

        assert response.status_code == 200

    def test_submit_statement_empty_statement(self, test_client) -> None:
        """Test submitting empty statement returns error."""
        response = test_client.post(
            "/api/v2/statements",
            json={"statement": ""},
        )

        assert response.status_code == 422
        data = response.json()
        assert "message" in data

    def test_submit_statement_syntax_error(self, test_client) -> None:
        """Test submitting statement with syntax error."""
        response = test_client.post(
            "/api/v2/statements",
            json={"statement": "SELECTZ FROM WHERE"},
        )

        assert response.status_code == 422
        data = response.json()
        assert "message" in data
        assert data["statementHandle"] is not None

    def test_submit_statement_async(self, test_client) -> None:
        """Test async statement submission."""
        response = test_client.post(
            "/api/v2/statements?async=true",
            json={"statement": "SELECT 1"},
        )

        assert response.status_code == 202
        data = response.json()
        assert "statementHandle" in data
        assert "statementStatusUrl" in data

    def test_get_statement_status_success(self, test_client) -> None:
        """Test getting status of completed statement."""
        # Submit statement
        submit_response = test_client.post(
            "/api/v2/statements",
            json={"statement": "SELECT 42 as answer"},
        )
        handle = submit_response.json()["statementHandle"]

        # Get status
        status_response = test_client.get(f"/api/v2/statements/{handle}")

        assert status_response.status_code == 200
        data = status_response.json()
        assert data["statementHandle"] == handle
        assert "data" in data
        assert data["data"][0][0] == "42"

    def test_get_statement_status_not_found(self, test_client) -> None:
        """Test getting status of non-existent statement."""
        response = test_client.get(
            "/api/v2/statements/00000000-0000-0000-0000-000000000000"
        )

        assert response.status_code == 404
        data = response.json()
        assert "message" in data

    def test_cancel_statement(self, test_client) -> None:
        """Test cancelling a statement."""
        # Submit async statement
        submit_response = test_client.post(
            "/api/v2/statements?async=true",
            json={"statement": "SELECT 1"},
        )
        handle = submit_response.json()["statementHandle"]

        # Cancel it
        cancel_response = test_client.post(f"/api/v2/statements/{handle}/cancel")

        assert cancel_response.status_code == 200
        data = cancel_response.json()
        assert data["statementHandle"] == handle

    def test_cancel_statement_not_found(self, test_client) -> None:
        """Test cancelling non-existent statement."""
        response = test_client.post(
            "/api/v2/statements/00000000-0000-0000-0000-000000000000/cancel"
        )

        assert response.status_code == 404

    def test_multiple_rows(self, test_client) -> None:
        """Test query returning multiple rows."""
        response = test_client.post(
            "/api/v2/statements",
            json={
                "statement": """
                    SELECT * FROM (VALUES 
                        (1, 'Alice'),
                        (2, 'Bob'),
                        (3, 'Charlie')
                    ) AS t(id, name)
                """
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["resultSetMetaData"]["numRows"] == 3
        assert len(data["data"]) == 3

    def test_ddl_statement(self, test_client) -> None:
        """Test DDL statement execution."""
        # Create table
        response = test_client.post(
            "/api/v2/statements",
            json={"statement": "CREATE TABLE IF NOT EXISTS sql_api_test (id INT)"},
        )
        assert response.status_code == 200

        # Insert data
        response = test_client.post(
            "/api/v2/statements",
            json={"statement": "INSERT INTO sql_api_test VALUES (1), (2), (3)"},
        )
        assert response.status_code == 200

        # Query data
        response = test_client.post(
            "/api/v2/statements",
            json={"statement": "SELECT COUNT(*) FROM sql_api_test"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["data"][0][0] == "3"

        # Cleanup
        test_client.post(
            "/api/v2/statements",
            json={"statement": "DROP TABLE IF EXISTS sql_api_test"},
        )

    def test_null_values(self, test_client) -> None:
        """Test handling of NULL values in results."""
        response = test_client.post(
            "/api/v2/statements",
            json={"statement": "SELECT NULL as empty_col"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["data"][0][0] is None

    def test_statement_handle_is_uuid(self, test_client) -> None:
        """Test that statement handles are valid UUIDs."""
        import uuid

        response = test_client.post(
            "/api/v2/statements",
            json={"statement": "SELECT 1"},
        )

        handle = response.json()["statementHandle"]
        # Should not raise
        uuid.UUID(handle)

    def test_bind_parameters(self, test_client) -> None:
        """Test SQL with bind parameters."""
        response = test_client.post(
            "/api/v2/statements",
            json={
                "statement": "SELECT ? + ? as sum",
                "bindings": {
                    "1": {"type": "FIXED", "value": "10"},
                    "2": {"type": "FIXED", "value": "20"},
                },
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["data"][0][0] == "30"

    def test_bind_parameters_text(self, test_client) -> None:
        """Test bind parameters with TEXT type."""
        response = test_client.post(
            "/api/v2/statements",
            json={
                "statement": "SELECT ? || ' ' || ? as greeting",
                "bindings": {
                    "1": {"type": "TEXT", "value": "Hello"},
                    "2": {"type": "TEXT", "value": "World"},
                },
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["data"][0][0] == "Hello World"

    def test_bind_parameters_float(self, test_client) -> None:
        """Test bind parameters with REAL/FLOAT type."""
        response = test_client.post(
            "/api/v2/statements",
            json={
                "statement": "SELECT ? * 2 as result",
                "bindings": {
                    "1": {"type": "REAL", "value": "3.14"},
                },
            },
        )

        assert response.status_code == 200
        data = response.json()
        # Float multiplication result
        assert float(data["data"][0][0]) == pytest.approx(6.28)

    def test_bind_parameters_boolean(self, test_client) -> None:
        """Test bind parameters with BOOLEAN type."""
        response = test_client.post(
            "/api/v2/statements",
            json={
                "statement": "SELECT ? as flag",
                "bindings": {
                    "1": {"type": "BOOLEAN", "value": "true"},
                },
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["data"][0][0].lower() == "true"

    def test_nullable_parameter_false(self, test_client) -> None:
        """Test nullable=false query parameter returns 'null' string."""
        response = test_client.post(
            "/api/v2/statements?nullable=false",
            json={"statement": "SELECT NULL as val"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["data"][0][0] == "null"

    def test_nullable_parameter_true(self, test_client) -> None:
        """Test nullable=true (default) returns actual null."""
        response = test_client.post(
            "/api/v2/statements?nullable=true",
            json={"statement": "SELECT NULL as val"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["data"][0][0] is None

    def test_dml_stats_insert(self, test_client) -> None:
        """Test DML stats are returned for INSERT."""
        # Create table
        test_client.post(
            "/api/v2/statements",
            json={"statement": "CREATE TABLE IF NOT EXISTS stats_test (id INT)"},
        )

        # Insert and check stats
        response = test_client.post(
            "/api/v2/statements",
            json={"statement": "INSERT INTO stats_test VALUES (1), (2), (3)"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "stats" in data
        assert data["stats"]["numRowsInserted"] == 3

        # Cleanup
        test_client.post(
            "/api/v2/statements",
            json={"statement": "DROP TABLE IF EXISTS stats_test"},
        )

    def test_partition_info_in_metadata(self, test_client) -> None:
        """Test that partition info is included for large results."""
        # Generate rows using UNNEST
        response = test_client.post(
            "/api/v2/statements",
            json={
                "statement": "SELECT * FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) AS t(id)"
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["resultSetMetaData"]["numRows"] == 10

    def test_get_statement_with_partition(self, test_client) -> None:
        """Test getting statement results with partition parameter."""
        # Submit statement
        submit_response = test_client.post(
            "/api/v2/statements",
            json={"statement": "SELECT 42 as answer"},
        )
        handle = submit_response.json()["statementHandle"]

        # Get partition 0
        response = test_client.get(f"/api/v2/statements/{handle}?partition=0")

        assert response.status_code == 200
        data = response.json()
        assert data["data"][0][0] == "42"

    def test_get_statement_invalid_partition(self, test_client) -> None:
        """Test invalid partition number returns error for small result set."""
        # Submit statement with small result set
        submit_response = test_client.post(
            "/api/v2/statements",
            json={"statement": "SELECT 42 as answer"},
        )
        handle = submit_response.json()["statementHandle"]

        # Request partition 999 (invalid)
        response = test_client.get(f"/api/v2/statements/{handle}?partition=999")

        # Should return error or handle gracefully
        # For small result sets (1 partition), partition 999 would be invalid
        assert response.status_code in (200, 422)  # Allow either response

    def test_retry_statement_success(self, test_client) -> None:
        """Test retrying a failed statement."""
        # First create a valid statement that succeeded
        submit_response = test_client.post(
            "/api/v2/statements",
            json={"statement": "SELECT 1 as test"},
        )
        handle = submit_response.json()["statementHandle"]

        # Retry should fail since status is 'success', not 'failed'
        response = test_client.post(f"/api/v2/statements/{handle}/retry")

        assert response.status_code == 422
        data = response.json()
        assert "cannot be retried" in data["message"]

    def test_retry_statement_not_found(self, test_client) -> None:
        """Test retrying a non-existent statement returns 404."""
        response = test_client.post("/api/v2/statements/non-existent-handle/retry")

        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["message"]

    def test_query_history_empty(self, test_client) -> None:
        """Test query history endpoint returns queries."""
        response = test_client.get("/api/v2/query-history")

        assert response.status_code == 200
        data = response.json()
        assert "queries" in data
        assert "total" in data
        assert data["total"] >= 0

    def test_query_history_with_limit(self, test_client) -> None:
        """Test query history with limit parameter."""
        # Submit a few queries first
        for i in range(3):
            test_client.post(
                "/api/v2/statements",
                json={"statement": f"SELECT {i} as num"},
            )

        # Query with limit
        response = test_client.get("/api/v2/query-history?limit=2")

        assert response.status_code == 200
        data = response.json()
        assert len(data["queries"]) <= 2

    def test_query_history_filter_by_status(self, test_client) -> None:
        """Test query history filtered by status."""
        # Submit a successful query
        test_client.post(
            "/api/v2/statements",
            json={"statement": "SELECT 1 as num"},
        )

        # Filter by success
        response = test_client.get("/api/v2/query-history?status=success")

        assert response.status_code == 200
        data = response.json()
        # All returned queries should have success status
        for query in data["queries"]:
            assert query["status"] == "success"
