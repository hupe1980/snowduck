import asyncio

import pytest

from snowduck.connector import Connector
from snowduck.server.session_manager import SessionManager


@pytest.mark.asyncio
async def test_concurrent_requests_same_session():
    """Verify that concurrent requests to the same session are safe."""
    manager = SessionManager()
    connector = Connector(db_file=":memory:")
    
    # Create a session
    token = "test_token_concurrent"
    connection = connector.connect("test_db", "test_schema")
    manager.create_session(token, connection)
    
    # Get the session's connection
    conn = manager.get_session(token)
    cursor = conn.cursor()
    
    # Create a test table
    cursor.execute("CREATE TABLE test_table (id INTEGER, value VARCHAR)")
    cursor.execute("INSERT INTO test_table VALUES (1, 'initial')")
    
    # Define concurrent operations
    async def update_value(val: str):
        # Simulate concurrent update
        await asyncio.sleep(0.01)  # Small delay to increase chance of race
        cursor.execute(f"UPDATE test_table SET value = '{val}' WHERE id = 1")
        result = cursor.execute("SELECT value FROM test_table WHERE id = 1").fetchone()
        return result[0]
    
    # Execute concurrent updates
    tasks = [update_value(f"value_{i}") for i in range(10)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Check that we didn't get any exceptions (though values may vary due to race)
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0, f"Got exceptions during concurrent execution: {exceptions}"
    
    # Cleanup
    cursor.close()
    conn.close()
