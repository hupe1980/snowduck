"""Data seeding utilities for SnowDuck - making test data easy!"""
from typing import Any

import pandas as pd


def seed_table(
    conn,
    table_name: str,
    data: pd.DataFrame | dict[str, list] | list[dict[str, Any]],
    drop_if_exists: bool = True,
) -> int:
    """
    Seed a table with data from a pandas DataFrame or dict.
    
    This is the recommended way to create test fixtures in SnowDuck!
    
    Args:
        conn: SnowDuck connection object
        table_name: Name of the table to create/populate
        data: Data as pandas DataFrame, dict of lists, or list of dicts
        drop_if_exists: If True, drops existing table first (default: True)
    
    Returns:
        Number of rows inserted
    
    Example:
        >>> import snowflake.connector
        >>> from snowduck import seed_table
        >>> 
        >>> conn = snowflake.connector.connect()
        >>> 
        >>> # From dict of lists
        >>> seed_table(conn, 'employees', {
        ...     'id': [1, 2, 3],
        ...     'name': ['Alice', 'Bob', 'Carol'],
        ...     'salary': [95000, 75000, 105000]
        ... })
        >>> 
        >>> # From pandas DataFrame
        >>> import pandas as pd
        >>> df = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})
        >>> seed_table(conn, 'test_data', df)
    """
    cursor = conn.cursor()
    
    # Convert to DataFrame if needed
    if isinstance(data, dict):
        df = pd.DataFrame(data)
    elif isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data
    
    if len(df) == 0:
        raise ValueError("Cannot seed table with empty data")
    
    # Drop existing table if requested
    if drop_if_exists:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    # Build CREATE TABLE AS SELECT FROM VALUES
    # This is the most reliable approach for all data types
    rows = []
    for _, row in df.iterrows():
        values = []
        for _, value in row.items():
            if pd.isna(value):
                values.append("NULL")
            elif isinstance(value, (int, float)):
                values.append(str(value))
            elif isinstance(value, bool):
                values.append("TRUE" if value else "FALSE")
            elif isinstance(value, pd.Timestamp):
                values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'::TIMESTAMP")
            else:
                # String - escape single quotes
                escaped = str(value).replace("'", "''")
                values.append(f"'{escaped}'")
        rows.append(f"({', '.join(values)})")
    
    # Build column list
    columns = ', '.join(df.columns)
    
    # Build VALUES rows
    values_rows = ',\n            '.join(rows)
    
    # Create table
    sql = f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM (VALUES
            {values_rows}
        ) AS t({columns})
    """
    
    cursor.execute(sql)
    
    return len(df)
