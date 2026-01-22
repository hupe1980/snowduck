import re
from typing import Optional, TypedDict

from snowflake.connector.cursor import ResultMetadata


def convert_dbapi_description_to_describe(description):
    type_mapping = {
        "STRING": "VARCHAR",
        "BIGINT": "FIXED",
        "DOUBLE": "REAL",
        "BOOLEAN": "BOOLEAN",
    }

    converted = []
    for column in description:
        column_name, type_name, *_rest, null_ok = (
            column  # Extract the last value for nullability
        )
        sql_type = type_mapping.get(type_name, type_name)  # Convert type
        nullability = (
            "YES" if null_ok is None or null_ok else "NO"
        )  # Use YES for None or True

        converted.append((column_name, sql_type, nullability, None, None, None))

    return converted


class ColumnInfo(TypedDict):
    """Represents metadata for a database column in Snowflake."""

    name: str
    database: str
    schema: str
    table: str
    nullable: bool
    type: str
    byteLength: Optional[int]
    length: Optional[int]
    scale: Optional[int]
    precision: Optional[int]
    collation: Optional[str]


DUCKDB_TO_SF_TYPE = {
    "FIXED": "fixed",
    "BIGINT": "fixed",
    "BLOB": "binary",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "DECIMAL": "fixed",
    "DOUBLE": "real",
    "REAL": "real",
    "HUGEINT": "fixed",
    "INTEGER": "fixed",
    "JSON": "variant",
    "TIME": "time",
    "TIMESTAMP WITH TIME ZONE": "timestamp_tz",
    "TIMESTAMP_NS": "timestamp_ntz",
    "TIMESTAMP": "timestamp_ntz",
    "UBIGINT": "fixed",
    "VARCHAR": "text",
}


def describe_as_rowtype(
    describe_results: list[tuple[str, str, str, str, str, str]],
    database: str | None = None,
    schema: str | None = None,
    table: str | None = None,
    overrides: dict[str, dict[str, int | str | None]] | None = None,
) -> list[ColumnInfo]:
    """
    Convert DuckDB column types to Snowflake row types returned by the API.

    Args:
        describe_results (list[tuple]): A list of tuples containing column metadata.
        database (str, optional): The database name to populate in metadata.
        schema (str, optional): The schema name to populate in metadata.
        table (str, optional): The table name to populate in metadata.

    Returns:
        list[ColumnInfo]: A list of column metadata in Snowflake-compatible format.

    Raises:
        NotImplementedError: If the column type is not recognized.
    """

    def normalize_nullable(value: str | bool | None) -> bool:
        if value is None:
            return True
        if isinstance(value, bool):
            return value
        return value.upper() == "YES"

    def as_column_info(
        column_name: str, column_type: str, nullability: str | None
    ) -> ColumnInfo:
        column_type_str = str(column_type)
        if column_type_str.startswith("DECIMAL"):
            normalized_type = "DECIMAL"
        elif column_type_str.upper().startswith("VARCHAR("):
            normalized_type = "VARCHAR"
        elif column_type_str.upper().startswith("BINARY("):
            normalized_type = "BINARY"
        else:
            normalized_type = column_type_str
        sf_type = DUCKDB_TO_SF_TYPE.get(normalized_type)

        if sf_type is None:
            raise NotImplementedError(f"Unsupported column type: {column_type}")

        info: ColumnInfo = {
            "name": column_name,
            "database": database or "",
            "schema": schema or "",
            "table": table or "",
            "nullable": normalize_nullable(nullability),
            "type": sf_type,
            "byteLength": None,
            "length": None,
            "scale": None,
            "precision": None,
            "collation": None,
        }

        if column_type_str.startswith("DECIMAL"):
            match = re.search(r"\((\d+),(\d+)\)", column_type_str)
            info["precision"] = int(match[1]) if match else 38
            info["scale"] = int(match[2]) if match else 0
        elif sf_type == "fixed":
            info["precision"] = 38
            info["scale"] = 0
        elif sf_type == "text":
            match = re.search(r"VARCHAR\((\d+)\)", column_type_str, re.IGNORECASE)
            length = int(match[1]) if match else 16_777_216
            info["byteLength"] = length
            info["length"] = length
        elif sf_type.startswith("time"):
            info["precision"] = 0
            info["scale"] = 9
        elif sf_type == "binary":
            match = re.search(r"BINARY\((\d+)\)", column_type_str, re.IGNORECASE)
            length = int(match[1]) if match else 8_388_608
            info["byteLength"] = length
            info["length"] = length

        if overrides and column_name in overrides:
            meta = overrides[column_name]
            if "is_nullable" in meta and meta["is_nullable"] is not None:
                info["nullable"] = str(meta["is_nullable"]).upper() == "YES"
            if (
                "character_maximum_length" in meta
                and meta["character_maximum_length"] is not None
            ):
                info["length"] = int(meta["character_maximum_length"])
                info["byteLength"] = int(meta["character_maximum_length"])
            if "numeric_precision" in meta and meta["numeric_precision"] is not None:
                info["precision"] = int(meta["numeric_precision"])
            if "numeric_scale" in meta and meta["numeric_scale"] is not None:
                info["scale"] = int(meta["numeric_scale"])

        return info

    return [
        as_column_info(name, col_type, nullability)
        for name, col_type, nullability, *_ in describe_results
    ]


def describe_as_result_metadata(
    describe_results: list[tuple[str, str, str, str, str, str]],
    database: str | None = None,
    schema: str | None = None,
    table: str | None = None,
) -> list[ResultMetadata]:
    """
    Convert describe results to Snowflake-compatible result metadata.

    Args:
        describe_results (list[tuple]): A list of tuples containing column metadata.
        database (str, optional): The database name.
        schema (str, optional): The schema name.

    Returns:
        list[ResultMetadata]: A list of Snowflake result metadata.
    """
    return [
        ResultMetadata.from_column(c)
        for c in describe_as_rowtype(describe_results, database, schema, table)
    ]  # pyright: ignore[reportArgumentType]
