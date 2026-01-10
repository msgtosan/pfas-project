"""
Unit tests for session module.

Tests session management with automatic timeout.
"""

import pytest
from datetime import datetime, timedelta

from pfas.core.session import (
    SessionManager,
    Session,
    hash_password,
    verify_password,
    SESSION_TIMEOUT_MINUTES,
)
from pfas.core.exceptions import SessionExpiredError


@pytest.fixture
def test_user(db_connection):
    """Create a test user to satisfy FK constraint on sessions."""
    cursor = db_connection.cursor()
    cursor.execute(
        """
        INSERT INTO users (pan_encrypted, pan_salt, name, email)
        VALUES (?, ?, ?, ?)
        """,
        (b"encrypted", b"salt", "Test User", "test@example.com"),
    )
    db_connection.commit()
    return cursor.lastrowid


@pytest.fixture
def session_manager(db_connection, test_user):
    """Provide a SessionManager instance with a test user created."""
    manager = SessionManager(db_connection)
    manager._test_user_id = test_user
    return manager


class TestSessionCreation:
    """Tests for session creation."""

    def test_create_session(self, session_manager):
        """Test creating a new session."""
        token = session_manager.create_session(user_id=session_manager._test_user_id)

        assert token is not None
        assert len(token) == 64  # 32 bytes in hex

    def test_create_session_with_custom_duration(self, session_manager):
        """Test creating session with custom duration."""
        token = session_manager.create_session(user_id=session_manager._test_user_id, duration_hours=48)

        session = session_manager.get_session(token)
        expected_expiry = datetime.now() + timedelta(hours=48)

        # Check expiry is approximately 48 hours from now
        assert abs((session.expires_at - expected_expiry).total_seconds()) < 60


class TestSessionValidation:
    """Tests for session validation."""

    def test_is_valid(self, session_manager):
        """Test session validity check."""
        token = session_manager.create_session(user_id=session_manager._test_user_id)
        assert session_manager.is_valid(token) is True

    def test_is_valid_invalid_token(self, session_manager):
        """Test validity check with invalid token."""
        assert session_manager.is_valid("invalid_token") is False

    def test_session_timeout(self, session_manager):
        """Test session expiration after 15 minutes (TC-CORE-007)."""
        token = session_manager.create_session(user_id=session_manager._test_user_id)
        assert session_manager.is_valid(token) is True

        # Simulate 16 minutes passing
        session_manager._update_last_activity(
            token, datetime.now() - timedelta(minutes=16)
        )

        # Session should be expired
        assert session_manager.is_valid(token) is False

    def test_session_not_expired_within_timeout(self, session_manager):
        """Test session valid within timeout period."""
        token = session_manager.create_session(user_id=session_manager._test_user_id)

        # Simulate 10 minutes passing (within 15 min timeout)
        session_manager._update_last_activity(
            token, datetime.now() - timedelta(minutes=10)
        )

        assert session_manager.is_valid(token) is True

    def test_session_max_duration_expiry(self, session_manager):
        """Test session expires at max duration even if active."""
        token = session_manager.create_session(user_id=session_manager._test_user_id, duration_hours=1)

        # Manually set expires_at to past
        session_manager.conn.execute(
            "UPDATE sessions SET expires_at = ? WHERE token = ?",
            ((datetime.now() - timedelta(hours=1)).isoformat(), token),
        )
        session_manager.conn.commit()

        assert session_manager.is_valid(token) is False


class TestSessionTouch:
    """Tests for session activity updates."""

    def test_touch_updates_last_activity(self, session_manager):
        """Test that touch updates last_activity timestamp."""
        token = session_manager.create_session(user_id=session_manager._test_user_id)

        # Set last_activity to 5 minutes ago
        old_time = datetime.now() - timedelta(minutes=5)
        session_manager._update_last_activity(token, old_time)

        # Touch the session
        result = session_manager.touch(token)
        assert result is True

        # Verify last_activity was updated
        session = session_manager.get_session(token)
        assert session.last_activity > old_time

    def test_touch_invalid_token(self, session_manager):
        """Test touch with invalid token returns False."""
        result = session_manager.touch("invalid_token")
        assert result is False


class TestSessionInvalidation:
    """Tests for session invalidation."""

    def test_invalidate(self, session_manager):
        """Test invalidating a session (logout)."""
        token = session_manager.create_session(user_id=session_manager._test_user_id)
        assert session_manager.is_valid(token) is True

        result = session_manager.invalidate(token)
        assert result is True

        assert session_manager.is_valid(token) is False

    def test_invalidate_invalid_token(self, session_manager):
        """Test invalidating non-existent token."""
        result = session_manager.invalidate("invalid_token")
        assert result is False

    def test_invalidate_all_user_sessions(self, session_manager):
        """Test invalidating all sessions for a user."""
        user_id = session_manager._test_user_id
        # Create multiple sessions for same user
        token1 = session_manager.create_session(user_id=user_id)
        token2 = session_manager.create_session(user_id=user_id)

        count = session_manager.invalidate_all_user_sessions(user_id=user_id)

        assert count == 2
        assert session_manager.is_valid(token1) is False
        assert session_manager.is_valid(token2) is False


class TestSessionRetrieval:
    """Tests for session retrieval."""

    def test_get_session(self, session_manager):
        """Test getting a session by token."""
        user_id = session_manager._test_user_id
        token = session_manager.create_session(user_id=user_id)
        session = session_manager.get_session(token)

        assert session is not None
        assert isinstance(session, Session)
        assert session.user_id == user_id
        assert session.token == token
        assert session.is_active is True

    def test_get_session_not_found(self, session_manager):
        """Test getting non-existent session returns None."""
        session = session_manager.get_session("invalid_token")
        assert session is None

    def test_get_user_sessions(self, session_manager):
        """Test getting all sessions for a user."""
        user_id = session_manager._test_user_id
        session_manager.create_session(user_id=user_id)
        session_manager.create_session(user_id=user_id)

        sessions = session_manager.get_user_sessions(user_id=user_id)

        assert len(sessions) == 2
        for s in sessions:
            assert s.user_id == user_id

    def test_get_user_sessions_active_only(self, session_manager):
        """Test getting only active sessions."""
        user_id = session_manager._test_user_id
        token1 = session_manager.create_session(user_id=user_id)
        session_manager.create_session(user_id=user_id)

        # Invalidate one
        session_manager.invalidate(token1)

        active_sessions = session_manager.get_user_sessions(user_id=user_id, active_only=True)
        all_sessions = session_manager.get_user_sessions(user_id=user_id, active_only=False)

        assert len(active_sessions) == 1
        assert len(all_sessions) == 2


class TestSessionCleanup:
    """Tests for session cleanup."""

    def test_cleanup_expired_sessions(self, session_manager):
        """Test cleaning up expired sessions."""
        user_id = session_manager._test_user_id
        token1 = session_manager.create_session(user_id=user_id)
        token2 = session_manager.create_session(user_id=user_id)

        # Expire one session
        session_manager._update_last_activity(
            token1, datetime.now() - timedelta(minutes=20)
        )

        count = session_manager.cleanup_expired_sessions()

        assert count == 1
        assert session_manager.is_valid(token1) is False
        assert session_manager.is_valid(token2) is True


class TestRequireValidSession:
    """Tests for require_valid_session method."""

    def test_require_valid_session_success(self, session_manager):
        """Test require_valid_session with valid token."""
        user_id = session_manager._test_user_id
        token = session_manager.create_session(user_id=user_id)

        session = session_manager.require_valid_session(token)

        assert session is not None
        assert session.user_id == user_id

    def test_require_valid_session_expired(self, session_manager):
        """Test require_valid_session with expired token raises error."""
        token = session_manager.create_session(user_id=session_manager._test_user_id)

        # Expire the session
        session_manager._update_last_activity(
            token, datetime.now() - timedelta(minutes=20)
        )

        with pytest.raises(SessionExpiredError):
            session_manager.require_valid_session(token)

    def test_require_valid_session_invalid(self, session_manager):
        """Test require_valid_session with invalid token raises error."""
        with pytest.raises(SessionExpiredError):
            session_manager.require_valid_session("invalid_token")


class TestSessionDataclass:
    """Tests for Session dataclass."""

    def test_is_expired_property(self, session_manager):
        """Test Session.is_expired property."""
        token = session_manager.create_session(user_id=session_manager._test_user_id)
        session = session_manager.get_session(token)

        assert session.is_expired is False

        # Expire the session
        session_manager._update_last_activity(
            token, datetime.now() - timedelta(minutes=20)
        )
        session = session_manager.get_session(token)

        assert session.is_expired is True


class TestPasswordHashing:
    """Tests for password hashing functions."""

    def test_hash_password(self):
        """Test password hashing."""
        password = "secure_password_123"

        hashed, salt = hash_password(password)

        assert hashed is not None
        assert len(hashed) == 32  # SHA-256 produces 32 bytes
        assert len(salt) == 16

    def test_hash_password_with_salt(self):
        """Test password hashing with provided salt."""
        password = "secure_password_123"
        fixed_salt = b"fixed_salt_here!"

        hash1, salt1 = hash_password(password, fixed_salt)
        hash2, salt2 = hash_password(password, fixed_salt)

        assert hash1 == hash2
        assert salt1 == salt2

    def test_verify_password_correct(self):
        """Test verifying correct password."""
        password = "secure_password_123"
        hashed, salt = hash_password(password)

        assert verify_password(password, hashed, salt) is True

    def test_verify_password_incorrect(self):
        """Test verifying incorrect password."""
        password = "secure_password_123"
        hashed, salt = hash_password(password)

        assert verify_password("wrong_password", hashed, salt) is False

    def test_different_passwords_different_hashes(self):
        """Test that different passwords produce different hashes."""
        salt = b"fixed_salt_here!"

        hash1, _ = hash_password("password1", salt)
        hash2, _ = hash_password("password2", salt)

        assert hash1 != hash2
