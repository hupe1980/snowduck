from typing import Any


def load_sql(filepath: str, **params: Any) -> str:
    """
    Loads SQL queries from external files and replaces placeholders dynamically.
    Ensures that the parameters do not contain unsafe characters to prevent SQL injection.
    """
    # Validate params to ensure no unsafe characters
    for key, value in params.items():
        if not isinstance(value, (str, int, float)):
            raise ValueError(f"Invalid parameter type for {key}: {type(value)}")
        if isinstance(value, str) and any(
            char in value for char in [";", "--", "/*", "*/", "'"]
        ):
            raise ValueError(f"Unsafe characters detected in parameter {key}: {value}")

    with open(filepath, "r", encoding="utf-8") as file:
        sql = file.read()
    return sql.format(**params)
