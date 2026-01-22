from typing import cast

import pyarrow as pa
import pyarrow.compute as pc

from ..connector import ColumnInfo


def to_sf_schema(schema: pa.Schema, rowtype: list[ColumnInfo]) -> pa.Schema:
    """
    Convert a PyArrow schema to a Snowflake-compatible schema.

    Args:
        schema (pa.Schema): The PyArrow schema.
        rowtype (list[ColumnInfo]): Column metadata information.

    Returns:
        pa.Schema: A Snowflake-compatible schema.

    Raises:
        ValueError: If schema and rowtype lengths do not match.
    """
    if len(schema) != len(rowtype):
        raise ValueError(
            f"Schema and rowtype must have the same length: {len(schema)=}, {len(rowtype)=}"
        )

    def sf_field(field: pa.Field, column: ColumnInfo) -> pa.Field:
        """
        Convert a PyArrow field to a Snowflake-compatible field.

        Args:
            field (pa.Field): The PyArrow field.
            column (ColumnInfo): Column metadata.

        Returns:
            pa.Field: A transformed Snowflake-compatible field.
        """
        if isinstance(field.type, pa.TimestampType):
            fields = [
                pa.field("epoch", pa.int64(), nullable=False),
                pa.field("fraction", pa.int32(), nullable=False),
            ]
            if field.type.tz:
                fields.append(pa.field("timezone", pa.int32(), nullable=False))
            field = field.with_type(pa.struct(fields))
        elif isinstance(field.type, pa.Time64Type):
            field = field.with_type(pa.int64())
        elif pa.types.is_uint64(field.type):
            field = field.with_type(pa.int64())

        return field.with_metadata(
            {
                "logicalType": column["type"].upper(),
                "precision": str(column["precision"] or 38),
                "scale": str(column["scale"] or 0),
                "charLength": str(column["length"] or 0),
            }
        )

    return pa.schema([sf_field(schema.field(i), c) for i, c in enumerate(rowtype)])


def to_ipc(table: pa.Table) -> pa.Buffer:
    """
    Convert a PyArrow table to an IPC buffer.

    Args:
        table (pa.Table): The PyArrow table.

    Returns:
        pa.Buffer: The serialized IPC buffer.

    Raises:
        NotImplementedError: If the table contains more than one batch.
    """
    batches = table.to_batches()
    if len(batches) != 1:
        raise NotImplementedError(f"Expected 1 batch, but got {len(batches)}.")

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_batch(batches[0])

    return sink.getvalue()


def to_sf(table: pa.Table, rowtype: list[ColumnInfo]) -> pa.Table:
    """
    Convert a PyArrow table to a Snowflake-compatible format.

    Args:
        table (pa.Table): The input table.
        rowtype (list[ColumnInfo]): Column metadata information.

    Returns:
        pa.Table: A transformed Snowflake-compatible table.
    """

    def to_sf_col(col: pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
        """
        Transform a PyArrow column to Snowflake-compatible format.

        Args:
            col (pa.ChunkedArray): The input column.

        Returns:
            pa.Array | pa.ChunkedArray: The transformed column.
        """
        if pa.types.is_timestamp(col.type):
            return timestamp_to_sf_struct(col)
        if pa.types.is_time(col.type):
            return pc.multiply(col.cast(pa.int64()), 1000)  # Convert to nanoseconds
        return col

    return pa.Table.from_arrays(
        [to_sf_col(col) for col in table.columns],
        schema=to_sf_schema(table.schema, rowtype),
    )


def timestamp_to_sf_struct(ts: pa.Array | pa.ChunkedArray) -> pa.Array:
    """
    Convert a timestamp column into a Snowflake-compatible struct.

    Args:
        ts (pa.Array | pa.ChunkedArray): The timestamp column.

    Returns:
        pa.Array: The transformed timestamp column.

    Raises:
        ValueError: If the input column is not of type Timestamp.
        AssertionError: If a timezone other than UTC is encountered.
    """
    if isinstance(ts, pa.ChunkedArray):
        ts = cast(pa.Array, ts.combine_chunks())

    if not isinstance(ts.type, pa.TimestampType):
        raise ValueError(f"Expected TimestampArray, got {type(ts)}")

    tsa_without_us = pc.floor_temporal(ts, unit="second")  # Strip subseconds
    epoch = pc.divide(tsa_without_us.cast(pa.int64()), 1_000_000)
    fraction = pc.multiply(pc.subsecond(ts), 1_000_000_000).cast(pa.int32())

    if ts.type.tz:
        if ts.type.tz != "UTC":
            raise ValueError(
                f"Unsupported timezone: {ts.type.tz}. Only UTC is supported."
            )
        timezone = pa.array([1440] * len(ts), type=pa.int32())

        return pa.StructArray.from_arrays(
            [epoch, fraction, timezone],
            fields=[
                pa.field("epoch", pa.int64(), nullable=False),
                pa.field("fraction", pa.int32(), nullable=False),
                pa.field("timezone", pa.int32(), nullable=False),
            ],
        )

    return pa.StructArray.from_arrays(
        [epoch, fraction],
        fields=[
            pa.field("epoch", pa.int64(), nullable=False),
            pa.field("fraction", pa.int32(), nullable=False),
        ],
    )
