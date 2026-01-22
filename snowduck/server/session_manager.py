import asyncio
from typing import Dict

from ..connector import Connection


class SessionManager:
    def __init__(self) -> None:
        self._sessions: Dict[str, Connection] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def create_session(self, token: str, connection: Connection) -> None:
        """Creates a new session."""
        self._sessions[token] = connection
        self._locks[token] = asyncio.Lock()

    def get_session(self, token: str) -> Connection:
        """Retrieves a session by token."""
        if token not in self._sessions:
            raise ValueError("Session not found. User must log in again.")
        return self._sessions[token]

    def delete_session(self, token: str) -> None:
        """Deletes a session by token and closes the connection."""
        if token in self._sessions:
            conn = self._sessions[token]
            try:
                # Connection might not expose close directly if not wrapped, but our Connection does
                if hasattr(conn, "close"):
                    conn.close()
            except Exception:
                pass
            del self._sessions[token]
            if token in self._locks:
                del self._locks[token]

    def session_exists(self, token: str) -> bool:
        """Checks if a session exists for the given token."""
        return token in self._sessions

    def get_lock(self, token: str) -> asyncio.Lock:
        """Gets the lock for a session."""
        if token not in self._locks:
            raise ValueError("Session not found. User must log in again.")
        return self._locks[token]
