"""
Session management with automatic timeout.

Provides user authentication session tracking with 15-minute idle timeout.
"""

import secrets
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
import sqlite3

from pfas.core.exceptions import SessionExpiredError, AuthenticationError


# Session configuration
SESSION_TIMEOUT_MINUTES = 15
TOKEN_LENGTH = 32


@dataclass
class Session:
    """Represents a user session."""

    id: int
    user_id: int
    token: str
    created_at: datetime
    last_activity: datetime
    expires_at: datetime
    is_active: bool

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Session":
        """Create Session from database row."""
        return cls(
            id=row["id"],
            user_id=row["user_id"],
            token=row["token"],
            created_at=datetime.fromisoformat(row["created_at"]) if isinstance(row["created_at"], str) else row["created_at"],
            last_activity=datetime.fromisoformat(row["last_activity"]) if isinstance(row["last_activity"], str) else row["last_activity"],
            expires_at=datetime.fromisoformat(row["expires_at"]) if isinstance(row["expires_at"], str) else row["expires_at"],
            is_active=bool(row["is_active"]),
        )

    @property
    def is_expired(self) -> bool:
        """Check if session has expired due to timeout or explicit expiration."""
        now = datetime.now()
        idle_timeout = self.last_activity + timedelta(minutes=SESSION_TIMEOUT_MINUTES)
        return now > self.expires_at or now > idle_timeout


class SessionManager:
    """
    Manager for user sessions with idle timeout.

    Usage:
        manager = SessionManager(connection)

        # Create a session after login
        token = manager.create_session(user_id=1)

        # Validate session on each request
        if manager.is_valid(token):
            # Allow access
            manager.touch(token)  # Update last activity
        else:
            # Require re-authentication
            pass

        # End session on logout
        manager.invalidate(token)
    """

    def __init__(
        self,
        db_connection: sqlite3.Connection,
        timeout_minutes: int = SESSION_TIMEOUT_MINUTES,
    ):
        """
        Initialize the session manager.

        Args:
            db_connection: SQLite database connection
            timeout_minutes: Idle timeout in minutes (default 15)
        """
        self.conn = db_connection
        self.timeout_minutes = timeout_minutes

    def create_session(
        self,
        user_id: int,
        duration_hours: int = 24,
    ) -> str:
        """
        Create a new session for a user.

        Args:
            user_id: User ID
            duration_hours: Session max duration in hours (default 24)

        Returns:
            Session token
        """
        # Generate secure token
        token = secrets.token_hex(TOKEN_LENGTH)

        now = datetime.now()
        expires_at = now + timedelta(hours=duration_hours)

        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO sessions (user_id, token, created_at, last_activity, expires_at, is_active)
            VALUES (?, ?, ?, ?, ?, 1)
            """,
            (
                user_id,
                token,
                now.isoformat(),
                now.isoformat(),
                expires_at.isoformat(),
            ),
        )
        self.conn.commit()

        return token

    def get_session(self, token: str) -> Optional[Session]:
        """
        Get a session by token.

        Args:
            token: Session token

        Returns:
            Session object or None if not found
        """
        cursor = self.conn.execute(
            "SELECT * FROM sessions WHERE token = ?", (token,)
        )
        row = cursor.fetchone()
        if row:
            return Session.from_row(row)
        return None

    def is_valid(self, token: str) -> bool:
        """
        Check if a session token is valid.

        A session is valid if:
        - It exists
        - It is marked as active
        - It hasn't exceeded the idle timeout
        - It hasn't exceeded the max duration

        Args:
            token: Session token

        Returns:
            True if session is valid, False otherwise
        """
        session = self.get_session(token)
        if not session:
            return False

        if not session.is_active:
            return False

        if session.is_expired:
            # Auto-invalidate expired session
            self._deactivate_session(token)
            return False

        return True

    def touch(self, token: str) -> bool:
        """
        Update last activity time for a session.

        Call this on each user action to prevent idle timeout.

        Args:
            token: Session token

        Returns:
            True if session was updated, False if session not found
        """
        now = datetime.now()
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE sessions
            SET last_activity = ?
            WHERE token = ? AND is_active = 1
            """,
            (now.isoformat(), token),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def invalidate(self, token: str) -> bool:
        """
        Invalidate a session (logout).

        Args:
            token: Session token

        Returns:
            True if session was invalidated, False if not found
        """
        return self._deactivate_session(token)

    def _deactivate_session(self, token: str) -> bool:
        """Deactivate a session in the database."""
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE sessions SET is_active = 0 WHERE token = ?",
            (token,),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def invalidate_all_user_sessions(self, user_id: int) -> int:
        """
        Invalidate all sessions for a user.

        Useful when user changes password or for security purposes.

        Args:
            user_id: User ID

        Returns:
            Number of sessions invalidated
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE sessions SET is_active = 0 WHERE user_id = ? AND is_active = 1",
            (user_id,),
        )
        self.conn.commit()
        return cursor.rowcount

    def get_user_sessions(self, user_id: int, active_only: bool = True) -> list:
        """
        Get all sessions for a user.

        Args:
            user_id: User ID
            active_only: Only return active sessions

        Returns:
            List of Session objects
        """
        if active_only:
            cursor = self.conn.execute(
                "SELECT * FROM sessions WHERE user_id = ? AND is_active = 1",
                (user_id,),
            )
        else:
            cursor = self.conn.execute(
                "SELECT * FROM sessions WHERE user_id = ?",
                (user_id,),
            )

        return [Session.from_row(row) for row in cursor.fetchall()]

    def cleanup_expired_sessions(self) -> int:
        """
        Clean up all expired sessions.

        Should be called periodically to remove old session records.

        Returns:
            Number of sessions cleaned up
        """
        now = datetime.now()
        idle_cutoff = now - timedelta(minutes=self.timeout_minutes)

        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE sessions
            SET is_active = 0
            WHERE is_active = 1 AND (expires_at < ? OR last_activity < ?)
            """,
            (now.isoformat(), idle_cutoff.isoformat()),
        )
        self.conn.commit()
        return cursor.rowcount

    def _update_last_activity(self, token: str, timestamp: datetime) -> None:
        """
        Update last activity to a specific timestamp (for testing).

        Args:
            token: Session token
            timestamp: Timestamp to set
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE sessions SET last_activity = ? WHERE token = ?",
            (timestamp.isoformat(), token),
        )
        self.conn.commit()

    def require_valid_session(self, token: str) -> Session:
        """
        Get a session, raising an exception if invalid.

        Args:
            token: Session token

        Returns:
            Session object

        Raises:
            SessionExpiredError: If session is invalid or expired
        """
        if not self.is_valid(token):
            raise SessionExpiredError("Session has expired or is invalid")

        session = self.get_session(token)
        if not session:
            raise SessionExpiredError("Session not found")

        return session


def hash_password(password: str, salt: bytes = None) -> tuple:
    """
    Hash a password using PBKDF2-SHA256.

    Args:
        password: Plain text password
        salt: Optional salt (generated if not provided)

    Returns:
        Tuple of (hashed_password, salt)
    """
    if salt is None:
        salt = secrets.token_bytes(16)

    hashed = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        100000,  # iterations
    )

    return hashed, salt


def verify_password(password: str, hashed: bytes, salt: bytes) -> bool:
    """
    Verify a password against its hash.

    Args:
        password: Plain text password to verify
        hashed: Stored password hash
        salt: Salt used for hashing

    Returns:
        True if password matches, False otherwise
    """
    new_hash, _ = hash_password(password, salt)
    return secrets.compare_digest(new_hash, hashed)
