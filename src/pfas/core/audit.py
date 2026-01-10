"""
Audit logging for compliance and data tracking.

Captures all data changes with table, record ID, action, old/new values, and timestamp.
Essential for tax compliance and audit trails.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any
import sqlite3


@dataclass
class AuditLogEntry:
    """Represents an audit log entry."""

    id: int
    table_name: str
    record_id: int
    action: str
    old_values: Optional[Dict[str, Any]]
    new_values: Optional[Dict[str, Any]]
    user_id: Optional[int]
    ip_address: Optional[str]
    timestamp: datetime

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "AuditLogEntry":
        """Create AuditLogEntry from database row."""
        return cls(
            id=row["id"],
            table_name=row["table_name"],
            record_id=row["record_id"],
            action=row["action"],
            old_values=json.loads(row["old_values"]) if row["old_values"] else None,
            new_values=json.loads(row["new_values"]) if row["new_values"] else None,
            user_id=row["user_id"],
            ip_address=row["ip_address"],
            timestamp=datetime.fromisoformat(row["timestamp"]) if isinstance(row["timestamp"], str) else row["timestamp"],
        )


class AuditLogger:
    """
    Logger for tracking all data changes in the system.

    Usage:
        logger = AuditLogger(connection)

        # Log an insert
        logger.log_change(
            table_name="users",
            record_id=1,
            action="INSERT",
            new_values={"name": "John", "email": "john@example.com"}
        )

        # Log an update
        logger.log_change(
            table_name="users",
            record_id=1,
            action="UPDATE",
            old_values={"email": "john@example.com"},
            new_values={"email": "john.doe@example.com"}
        )
    """

    VALID_ACTIONS = ("INSERT", "UPDATE", "DELETE")

    def __init__(
        self,
        db_connection: sqlite3.Connection,
        user_id: int = None,
        ip_address: str = None,
    ):
        """
        Initialize the audit logger.

        Args:
            db_connection: SQLite database connection
            user_id: Default user ID for log entries
            ip_address: Default IP address for log entries
        """
        self.conn = db_connection
        self.user_id = user_id
        self.ip_address = ip_address

    def log_change(
        self,
        table_name: str,
        record_id: int,
        action: str,
        old_values: Dict[str, Any] = None,
        new_values: Dict[str, Any] = None,
        user_id: int = None,
        ip_address: str = None,
    ) -> int:
        """
        Log a data change.

        Args:
            table_name: Name of the table being modified
            record_id: ID of the record being modified
            action: One of INSERT, UPDATE, DELETE
            old_values: Previous values (for UPDATE/DELETE)
            new_values: New values (for INSERT/UPDATE)
            user_id: User who made the change (overrides default)
            ip_address: IP address (overrides default)

        Returns:
            Audit log entry ID

        Raises:
            ValueError: If action is not valid
        """
        if action not in self.VALID_ACTIONS:
            raise ValueError(f"Invalid action: {action}. Must be one of {self.VALID_ACTIONS}")

        cursor = self.conn.cursor()

        cursor.execute(
            """
            INSERT INTO audit_log
            (table_name, record_id, action, old_values, new_values, user_id, ip_address)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                table_name,
                record_id,
                action,
                json.dumps(old_values) if old_values else None,
                json.dumps(new_values) if new_values else None,
                user_id or self.user_id,
                ip_address or self.ip_address,
            ),
        )

        self.conn.commit()
        return cursor.lastrowid

    def log_insert(
        self,
        table_name: str,
        record_id: int,
        new_values: Dict[str, Any],
        user_id: int = None,
    ) -> int:
        """Convenience method to log an INSERT."""
        return self.log_change(
            table_name=table_name,
            record_id=record_id,
            action="INSERT",
            new_values=new_values,
            user_id=user_id,
        )

    def log_update(
        self,
        table_name: str,
        record_id: int,
        old_values: Dict[str, Any],
        new_values: Dict[str, Any],
        user_id: int = None,
    ) -> int:
        """Convenience method to log an UPDATE."""
        return self.log_change(
            table_name=table_name,
            record_id=record_id,
            action="UPDATE",
            old_values=old_values,
            new_values=new_values,
            user_id=user_id,
        )

    def log_delete(
        self,
        table_name: str,
        record_id: int,
        old_values: Dict[str, Any],
        user_id: int = None,
    ) -> int:
        """Convenience method to log a DELETE."""
        return self.log_change(
            table_name=table_name,
            record_id=record_id,
            action="DELETE",
            old_values=old_values,
            user_id=user_id,
        )

    def get_log_entry(self, log_id: int) -> Optional[AuditLogEntry]:
        """
        Get a specific audit log entry.

        Args:
            log_id: Audit log entry ID

        Returns:
            AuditLogEntry or None if not found
        """
        cursor = self.conn.execute(
            "SELECT * FROM audit_log WHERE id = ?", (log_id,)
        )
        row = cursor.fetchone()
        if row:
            return AuditLogEntry.from_row(row)
        return None

    def get_record_history(
        self,
        table_name: str,
        record_id: int,
    ) -> List[AuditLogEntry]:
        """
        Get all audit log entries for a specific record.

        Args:
            table_name: Table name
            record_id: Record ID

        Returns:
            List of AuditLogEntry objects in chronological order
        """
        cursor = self.conn.execute(
            """
            SELECT * FROM audit_log
            WHERE table_name = ? AND record_id = ?
            ORDER BY timestamp ASC
            """,
            (table_name, record_id),
        )
        return [AuditLogEntry.from_row(row) for row in cursor.fetchall()]

    def get_table_history(
        self,
        table_name: str,
        start_time: datetime = None,
        end_time: datetime = None,
        limit: int = 100,
    ) -> List[AuditLogEntry]:
        """
        Get audit log entries for a table within a time range.

        Args:
            table_name: Table name
            start_time: Start of time range (optional)
            end_time: End of time range (optional)
            limit: Maximum number of entries to return

        Returns:
            List of AuditLogEntry objects in reverse chronological order
        """
        query = "SELECT * FROM audit_log WHERE table_name = ?"
        params: list = [table_name]

        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time.isoformat())

        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time.isoformat())

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        cursor = self.conn.execute(query, params)
        return [AuditLogEntry.from_row(row) for row in cursor.fetchall()]

    def get_user_activity(
        self,
        user_id: Optional[int],
        start_time: datetime = None,
        end_time: datetime = None,
        limit: int = 100,
    ) -> List[AuditLogEntry]:
        """
        Get all audit log entries for a specific user.

        Args:
            user_id: User ID (None to find entries with no user)
            start_time: Start of time range (optional)
            end_time: End of time range (optional)
            limit: Maximum number of entries to return

        Returns:
            List of AuditLogEntry objects in reverse chronological order
        """
        if user_id is None:
            query = "SELECT * FROM audit_log WHERE user_id IS NULL"
            params: list = []
        else:
            query = "SELECT * FROM audit_log WHERE user_id = ?"
            params = [user_id]

        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time.isoformat())

        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time.isoformat())

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        cursor = self.conn.execute(query, params)
        return [AuditLogEntry.from_row(row) for row in cursor.fetchall()]

    def mask_sensitive_values(self, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mask sensitive values before logging.

        Sensitive fields: pan, aadhaar, bank_account, password, etc.

        Args:
            values: Dictionary of values to mask

        Returns:
            Dictionary with sensitive values masked
        """
        sensitive_fields = {
            "pan", "pan_encrypted", "aadhaar", "aadhaar_encrypted",
            "bank_account", "password", "pin", "token",
        }

        masked = {}
        for key, value in values.items():
            if key.lower() in sensitive_fields:
                masked[key] = "****"
            else:
                masked[key] = value

        return masked
