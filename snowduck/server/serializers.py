from datetime import date, datetime
from decimal import Decimal
from typing import Any, List


def serialize_item(item: Any) -> Any:
    """
    Serializes a single cell value to a JSON-compatible format 
    matching Snowflake's expected string representations.
    """
    if item is None:
        return None
    if isinstance(item, (date, datetime)):
        # Snowflake returns ISO 8601 strings
        return item.isoformat()
    if isinstance(item, Decimal):
        # Snowflake typically returns high-precision numbers as strings/numbers
        # using str() preserves precision
        return str(item)
    if isinstance(item, bytes):
        # Binary data -> Hex string (Snowflake default)
        return item.hex()
    return item


def serialize_rowset(rows: List[tuple]) -> List[List[Any]]:
    """
    Converts a list of row tuples (from Cursor.fetchall) into a list of list
    structures for Snowflake JSON response.
    """
    return [[serialize_item(cell) for cell in row] for row in rows]
