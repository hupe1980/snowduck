"""Type conversion utilities for SQL API.

Maps between DuckDB/Python types and Snowflake type system for
proper result metadata generation.
"""

from __future__ import annotations

from typing import Any

# Mapping from DuckDB/Python types to Snowflake types
TYPE_MAP = {
    "INTEGER": "FIXED",
    "INT": "FIXED",
    "BIGINT": "FIXED",
    "SMALLINT": "FIXED",
    "TINYINT": "FIXED",
    "HUGEINT": "FIXED",
    "UBIGINT": "FIXED",
    "UINTEGER": "FIXED",
    "USMALLINT": "FIXED",
    "UTINYINT": "FIXED",
    "NUMBER": "FIXED",
    "DECIMAL": "FIXED",
    "NUMERIC": "FIXED",
    "FLOAT": "REAL",
    "DOUBLE": "REAL",
    "REAL": "REAL",
    "VARCHAR": "TEXT",
    "STRING": "TEXT",
    "TEXT": "TEXT",
    "CHAR": "TEXT",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP_NTZ",
    "TIMESTAMP_NS": "TIMESTAMP_NTZ",
    "TIMESTAMP_MS": "TIMESTAMP_NTZ",
    "TIMESTAMP_S": "TIMESTAMP_NTZ",
    "TIMESTAMPTZ": "TIMESTAMP_TZ",
    "TIME": "TIME",
    "BOOLEAN": "BOOLEAN",
    "BOOL": "BOOLEAN",
    "BLOB": "BINARY",
    "BYTEA": "BINARY",
    "JSON": "VARIANT",
    "LIST": "ARRAY",
    "ARRAY": "ARRAY",
    "STRUCT": "OBJECT",
    "MAP": "OBJECT",
}


def type_code_to_name(type_code: int | str | type) -> str:
    """Convert type to Snowflake type name.

    Args:
        type_code: Type code (int), type name (str), or type class

    Returns:
        Snowflake type name string
    """
    # Handle type objects directly
    if isinstance(type_code, type):
        type_str = type_code.__name__.upper()
    else:
        type_str = str(type_code).upper()

    # Check for exact match or partial match
    for key, value in TYPE_MAP.items():
        if key in type_str:
            return value

    return "TEXT"


def build_row_type(description: list) -> list[dict[str, Any]]:
    """Build row type metadata from cursor description.

    Converts DuckDB/snowduck cursor description to Snowflake-compatible
    column metadata format.

    Args:
        description: Cursor description list (ResultMetadata or tuples)

    Returns:
        List of column metadata dictionaries
    """
    from snowflake.connector.cursor import ResultMetadata

    row_types = []
    for col in description:
        # Handle ResultMetadata objects (snowduck cursor)
        if isinstance(col, ResultMetadata):
            row_types.append(
                {
                    "name": col.name,
                    "type": type_code_to_name(col.type_code),
                    "length": col.internal_size or 0,
                    "precision": col.precision or 0,
                    "scale": col.scale or 0,
                    "nullable": col.is_nullable
                    if col.is_nullable is not None
                    else True,
                }
            )
        # Handle tuple format (regular DBAPI cursor)
        elif isinstance(col, (tuple, list)):
            if len(col) >= 2:
                name = col[0]
                type_info = col[1]
                null_ok = col[5] if len(col) > 5 else True
            else:
                name = str(col[0]) if col else "column"
                type_info = "TEXT"
                null_ok = True

            row_types.append(
                {
                    "name": str(name),
                    "type": type_code_to_name(type_info),
                    "length": 0,
                    "precision": 0,
                    "scale": 0,
                    "nullable": null_ok if null_ok is not None else True,
                }
            )
        else:
            # Fallback for unknown formats
            row_types.append(
                {
                    "name": str(col),
                    "type": "TEXT",
                    "length": 0,
                    "precision": 0,
                    "scale": 0,
                    "nullable": True,
                }
            )

    return row_types


def format_value(value: Any, nullable: bool = True) -> Any:
    """Format a value for JSON response.

    Args:
        value: The value to format
        nullable: If False, return "null" string instead of None

    Returns:
        Formatted value suitable for JSON
    """
    if value is None:
        return None if nullable else "null"
    return str(value)


def format_row(row: list[Any], nullable: bool = True) -> list[Any]:
    """Format a row's values for JSON response.

    Args:
        row: List of values
        nullable: If False, return "null" string instead of None

    Returns:
        Formatted row values
    """
    return [format_value(v, nullable) for v in row]
