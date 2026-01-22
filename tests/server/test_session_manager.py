import pytest

from snowduck.server import SessionManager


class MockConnection:
    """Mock class for Connection to use in tests."""

    pass


@pytest.fixture
def session_manager():
    """Fixture to create a fresh SessionManager instance for each test."""
    return SessionManager()


@pytest.fixture
def mock_connection():
    """Fixture to create a mock Connection instance."""
    return MockConnection()


def test_create_session(session_manager, mock_connection):
    """Test creating a new session."""
    token = "test_token"
    session_manager.create_session(token, mock_connection)
    assert session_manager.get_session(token) == mock_connection


def test_get_session(session_manager, mock_connection):
    """Test retrieving an existing session."""
    token = "test_token"
    session_manager.create_session(token, mock_connection)
    retrieved_connection = session_manager.get_session(token)
    assert retrieved_connection == mock_connection


def test_get_session_not_found(session_manager):
    """Test retrieving a session that does not exist."""
    token = "nonexistent_token"
    with pytest.raises(ValueError, match="Session not found. User must log in again."):
        session_manager.get_session(token)


def test_delete_session(session_manager, mock_connection):
    """Test deleting an existing session."""
    token = "test_token"
    session_manager.create_session(token, mock_connection)
    session_manager.delete_session(token)
    assert not session_manager.session_exists(token)


def test_delete_nonexistent_session(session_manager):
    """Test deleting a session that does not exist."""
    token = "nonexistent_token"
    session_manager.delete_session(token)  # Should not raise an error
    assert not session_manager.session_exists(token)


def test_session_exists(session_manager, mock_connection):
    """Test checking if a session exists."""
    token = "test_token"
    session_manager.create_session(token, mock_connection)
    assert session_manager.session_exists(token)
    session_manager.delete_session(token)
    assert not session_manager.session_exists(token)
