"""Query errors must not be reported as retryable HTTP 5xx responses.

The Snowflake connector classifies every 5xx as retryable, so a query failure
returned as a 500 is silently re-executed until the network timeout expires
instead of failing the query.
"""

import time

import pytest
import snowflake.connector

import snowduck.connector.cursor as cursor_module


def test_query_error_is_not_retried(server: dict) -> None:
    with (
        snowflake.connector.connect(**server, network_timeout=15) as conn,
        conn.cursor() as cur,
    ):
        started = time.monotonic()
        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            cur.execute("SELECT * FROM this_table_does_not_exist")

        # A retry storm would burn the whole network timeout before giving up.
        assert time.monotonic() - started < 5


def test_unhandled_error_is_not_retried(
    server: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Even an unexpected server-side failure must fail fast, not retry."""
    calls = {"n": 0}
    original = cursor_module.Cursor.execute

    def exploding_execute(self, command, *args, **kwargs):
        if "SELECT 1" in command:
            calls["n"] += 1
            raise RuntimeError("boom")
        return original(self, command, *args, **kwargs)

    monkeypatch.setattr(cursor_module.Cursor, "execute", exploding_execute)

    with (
        snowflake.connector.connect(**server, network_timeout=15) as conn,
        conn.cursor() as cur,
    ):
        started = time.monotonic()
        with pytest.raises(snowflake.connector.errors.Error) as exc:
            cur.execute("SELECT 1")

        assert time.monotonic() - started < 5
        assert calls["n"] == 1, "statement was retried"
        assert not isinstance(exc.value, snowflake.connector.errors.InternalServerError)
        assert "boom" in str(exc.value)
