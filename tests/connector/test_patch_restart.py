"""Tests for patch restart behavior."""

import pytest
import snowflake.connector

from snowduck import start_patch_snowflake, stop_patch_snowflake, seed_table


def test_start_patch_twice_with_reset():
    """Test that calling start_patch_snowflake twice with reset works."""
    try:
        # First patch
        start_patch_snowflake()
        with snowflake.connector.connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE test1 (id INT)")
            cur.execute("INSERT INTO test1 VALUES (1)")
        
        # Verify data exists
        with snowflake.connector.connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM test1")
            assert len(cur.fetchall()) == 1
        
        # Second patch with reset (should clear everything)
        start_patch_snowflake(reset=True)
        
        # Should fail - table doesn't exist anymore
        with snowflake.connector.connect() as conn:
            cur = conn.cursor()
            with pytest.raises(snowflake.connector.errors.ProgrammingError, match="Table.*does not exist"):
                cur.execute("SELECT * FROM test1")
    finally:
        stop_patch_snowflake()


def test_start_patch_file_persistence_between_restarts():
    """Test that file-based persistence survives patch restart.
    
    Note: Data persists, but the "database" context (which is an ATTACH in DuckDB)
    must be re-created. Tables are stored in the main schema of the file.
    """
    import tempfile
    import os
    
    db_file = tempfile.mktemp(suffix=".duckdb")
    
    try:
        # First patch - create data in the main schema (no separate database)
        start_patch_snowflake(db_file=db_file)
        with snowflake.connector.connect() as conn:
            cur = conn.cursor()
            # Create table in main schema (persists in file)
            cur.execute("CREATE TABLE persisted (id INT)")
            cur.execute("INSERT INTO persisted VALUES (42)")
        stop_patch_snowflake()
        
        # Second patch - data should still exist (no reset)
        start_patch_snowflake(db_file=db_file)
        with snowflake.connector.connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM persisted")
            rows = cur.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 42
    finally:
        stop_patch_snowflake()
        if os.path.exists(db_file):
            os.remove(db_file)


def test_cross_connection_state_shared():
    """Test that connections share database state within same patch."""
    try:
        start_patch_snowflake()
        
        # First connection creates data
        with snowflake.connector.connect() as conn:
            cur = conn.cursor()
            cur.execute("CREATE DATABASE shared_test")
            cur.execute("USE DATABASE shared_test")
            cur.execute("CREATE TABLE shared_data (val INT)")
            cur.execute("INSERT INTO shared_data VALUES (100)")
        
        # Second connection can see it
        with snowflake.connector.connect() as conn:
            cur = conn.cursor()
            cur.execute("USE DATABASE shared_test")
            cur.execute("SELECT * FROM shared_data")
            rows = cur.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 100
    finally:
        stop_patch_snowflake()
