from functools import wraps
from typing import Any, Callable, TypeVar, cast

from .patch import patch_snowflake

F = TypeVar("F", bound=Callable[..., Any])


def mock_snowflake(func: F) -> F:
    """
    Decorator to apply SnowDuck as a mock for Snowflake.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with patch_snowflake():
            return func(*args, **kwargs)

    return cast(F, wrapper)
