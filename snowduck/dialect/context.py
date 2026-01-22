from dataclasses import dataclass, field

from ..info_schema import InfoSchemaManager


@dataclass(kw_only=True)
class DialectContext:
    current_database: str | None = None
    current_schema: str | None = None
    current_role: str | None = None
    current_warehouse: str | None = None
    info_schema_manager: InfoSchemaManager
    # Session variables (SET var = value)
    session_variables: dict[str, str] = field(default_factory=dict)