from functools import wraps

from .patch import patch_snowflake


def mock_snowflake(func):
    """
    Decorator to apply SnowDuck as a mock for Snowflake.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        with patch_snowflake():
            return func(*args, **kwargs)

    return wrapper
