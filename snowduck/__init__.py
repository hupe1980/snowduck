from .decorators import mock_snowflake
from .patch import patch_snowflake, start_patch_snowflake, stop_patch_snowflake


# Lazy import for seeding (requires pandas)
def __getattr__(name: str):
    if name == "seed_table":
        from .seeding import seed_table
        return seed_table
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "mock_snowflake",
    "patch_snowflake",
    "seed_table",
    "start_patch_snowflake",
    "stop_patch_snowflake",
]
