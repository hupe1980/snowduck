from typing import Any, Mapping

# Characters that would let a caller-supplied identifier break out of the
# surrounding SQL. Identifiers reaching load_sql come from parsed object names,
# never from user data, but a stray quote would still corrupt the statement.
_UNSAFE = (";", "--", "/*", "*/", "'", '"')


def sql_literal(value: str | int | float | None) -> str:
    """Render a value as a SQL literal, escaping embedded quotes."""
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def load_sql(
    filepath: str,
    *,
    fragments: Mapping[str, str] | None = None,
    **params: Any,
) -> str:
    """Load a SQL template and fill in its placeholders.

    ``params`` are identifiers (database, schema, table names). They are
    rejected if they contain anything that could terminate or comment out the
    surrounding statement.

    ``fragments`` are pre-built SQL snippets - predicates, ORDER BY/LIMIT tails
    - that legitimately contain quoted literals. Build them with
    :func:`sql_literal` so the values inside them are escaped.
    """
    for key, value in params.items():
        if not isinstance(value, (str, int, float)):
            raise ValueError(f"Invalid parameter type for {key}: {type(value)}")
        if isinstance(value, str) and any(char in value for char in _UNSAFE):
            raise ValueError(f"Unsafe characters detected in parameter {key}: {value}")

    with open(filepath, "r", encoding="utf-8") as file:
        sql = file.read()
    return sql.format(**params, **(fragments or {}))
