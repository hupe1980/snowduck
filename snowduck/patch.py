import atexit
import uuid
from contextlib import ExitStack, contextmanager
from unittest.mock import patch as mock_patch

from .connector import Connector

_patch_ctx = None  # Global variable to track context


def write_pandas(
    conn,
    df,
    table_name: str,
    database: str | None = None,
    schema: str | None = None,
    **_kwargs,
):
    """
    Minimal Snowflake write_pandas replacement using DuckDB insertion.

    Returns a tuple consistent with snowflake.connector.pandas_tools.write_pandas:
    (success, nchunks, nrows, output)
    """
    duck_conn = conn._duck_conn
    resolved_db = database or conn.database
    resolved_schema = schema or conn.schema or "main"

    if resolved_db and resolved_schema:
        qualified_table = f"{resolved_db}.{resolved_schema}.{table_name}"
    elif resolved_schema:
        qualified_table = f"{resolved_schema}.{table_name}"
    else:
        qualified_table = table_name

    temp_name = f"_snowduck_df_{uuid.uuid4().hex}"
    view_created = False

    try:
        if hasattr(duck_conn, "from_df"):
            try:
                relation = duck_conn.from_df(df)
                if hasattr(relation, "create_temp_view"):
                    relation.create_temp_view(temp_name)
                else:
                    relation.create_view(temp_name)
                view_created = True
                duck_conn.execute(f"INSERT INTO {qualified_table} SELECT * FROM {temp_name}")
                return True, 1, len(df), []
            except Exception:
                view_created = False

        columns = ", ".join(df.columns)
        placeholders = ", ".join(["?"] * len(df.columns))
        sql = f"INSERT INTO {qualified_table} ({columns}) VALUES ({placeholders})"
        duck_conn.executemany(sql, list(df.itertuples(index=False, name=None)))
    finally:
        if view_created:
            duck_conn.execute(f"DROP VIEW IF EXISTS {temp_name}")

    return True, 1, len(df), []

@contextmanager
def patch_snowflake(db_file: str = ':memory:', reset: bool = False):
    """
    Context manager to patch Snowflake-related functionality with SnowDuck.
    
    Args:
        db_file: Path to DuckDB database file. Use ':memory:' for in-memory (default),
                 or provide a file path for persistent storage (e.g., 'test_data.duckdb').
        reset: If True, deletes the database file before starting (default: False).
    """
    import glob
    import os
    
    if reset and db_file != ':memory:':
        # Delete the main file and any related files (.wal, .tmp, etc.)
        for pattern in [db_file, f"{db_file}.wal", f"{db_file}.tmp"]:
            for file in glob.glob(pattern):
                if os.path.exists(file):
                    os.remove(file)
    
    connector = Connector(db_file=db_file)
    targets = {
        'snowflake.connector.connect': connector.connect,
        'snowflake.connector.pandas_tools.write_pandas': write_pandas,
    }

    with ExitStack() as stack:
        for target, mock_func in targets.items():
            p = mock_patch(target, side_effect=mock_func)
            stack.enter_context(p)
        try:
            yield
        finally:
            connector.close()


def start_patch_snowflake(db_file: str = ':memory:', reset: bool = False):
    """
    Start the Snowflake patching context and register cleanup.
    
    Args:
        db_file: Path to DuckDB database file. Use ':memory:' for in-memory (default),
                 or provide a file path for persistent storage (e.g., 'test_data.duckdb').
        reset: If True, deletes the database file before starting (default: False).
               Useful for notebooks/scripts that need a fresh start.
    
    Example::
    
        # In-memory (data lost on exit):
        start_patch_snowflake()
        
        # Persistent storage (data saved to file):
        start_patch_snowflake(db_file='my_test_data.duckdb')
        
        # Fresh start (delete existing file):
        start_patch_snowflake(db_file='test.duckdb', reset=True)
    """
    global _patch_ctx
    if _patch_ctx is None:  # Ensure we don't register multiple times
        _patch_ctx = patch_snowflake(db_file=db_file, reset=reset)
        _patch_ctx.__enter__()
        atexit.register(stop_patch_snowflake)  # Register cleanup when starting


def stop_patch_snowflake():
    """Stop the Snowflake patching context."""
    global _patch_ctx
    if _patch_ctx:
        _patch_ctx.__exit__(None, None, None)
        _patch_ctx = None  # Reset for future use