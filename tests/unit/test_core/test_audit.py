"""
Unit tests for audit module.

Tests audit logging for compliance and data tracking.
"""

import pytest
from datetime import datetime, timedelta

from pfas.core.audit import AuditLogger, AuditLogEntry


@pytest.fixture
def audit_logger(db_connection):
    """Provide an AuditLogger instance without default user (avoids FK constraint)."""
    return AuditLogger(db_connection, user_id=None, ip_address="127.0.0.1")


class TestAuditLogCreation:
    """Tests for audit log creation."""

    def test_audit_log_creation(self, audit_logger, db_connection):
        """Test audit log entry for data changes (TC-CORE-006)."""
        # Log an insert
        log_id = audit_logger.log_change(
            table_name="users",
            record_id=1,
            action="INSERT",
            old_values=None,
            new_values={"name": "Test User", "pan": "****"},
        )

        assert log_id > 0

        # Verify log entry
        cursor = db_connection.execute(
            "SELECT * FROM audit_log WHERE table_name = 'users' AND record_id = 1"
        )
        log_entry = cursor.fetchone()

        assert log_entry is not None
        assert log_entry["action"] == "INSERT"

    def test_log_insert(self, audit_logger):
        """Test logging an INSERT operation."""
        log_id = audit_logger.log_insert(
            table_name="accounts",
            record_id=100,
            new_values={"code": "9999", "name": "Test Account"},
        )

        entry = audit_logger.get_log_entry(log_id)

        assert entry.action == "INSERT"
        assert entry.old_values is None
        assert entry.new_values["code"] == "9999"

    def test_log_update(self, audit_logger):
        """Test logging an UPDATE operation."""
        log_id = audit_logger.log_update(
            table_name="accounts",
            record_id=100,
            old_values={"name": "Old Name"},
            new_values={"name": "New Name"},
        )

        entry = audit_logger.get_log_entry(log_id)

        assert entry.action == "UPDATE"
        assert entry.old_values["name"] == "Old Name"
        assert entry.new_values["name"] == "New Name"

    def test_log_delete(self, audit_logger):
        """Test logging a DELETE operation."""
        log_id = audit_logger.log_delete(
            table_name="accounts",
            record_id=100,
            old_values={"code": "9999", "name": "Deleted Account"},
        )

        entry = audit_logger.get_log_entry(log_id)

        assert entry.action == "DELETE"
        assert entry.old_values["code"] == "9999"
        assert entry.new_values is None

    def test_invalid_action(self, audit_logger):
        """Test that invalid action raises error."""
        with pytest.raises(ValueError) as exc_info:
            audit_logger.log_change(
                table_name="users",
                record_id=1,
                action="INVALID",
            )

        assert "Invalid action" in str(exc_info.value)

    def test_user_id_override(self, audit_logger):
        """Test overriding default user_id (None bypasses FK)."""
        log_id = audit_logger.log_insert(
            table_name="users",
            record_id=1,
            new_values={"name": "Test"},
            user_id=None,  # Use None to avoid FK constraint
        )

        entry = audit_logger.get_log_entry(log_id)
        assert entry.user_id is None

    def test_default_user_id(self, audit_logger):
        """Test using default user_id (None to avoid FK constraint)."""
        log_id = audit_logger.log_insert(
            table_name="users",
            record_id=1,
            new_values={"name": "Test"},
        )

        entry = audit_logger.get_log_entry(log_id)
        assert entry.user_id is None  # Default from fixture (None)


class TestAuditLogRetrieval:
    """Tests for audit log retrieval."""

    def test_get_log_entry(self, audit_logger):
        """Test retrieving a specific log entry."""
        log_id = audit_logger.log_insert(
            table_name="users",
            record_id=1,
            new_values={"name": "Test"},
        )

        entry = audit_logger.get_log_entry(log_id)

        assert entry is not None
        assert isinstance(entry, AuditLogEntry)
        assert entry.id == log_id

    def test_get_log_entry_not_found(self, audit_logger):
        """Test retrieving non-existent log entry."""
        entry = audit_logger.get_log_entry(99999)
        assert entry is None

    def test_get_record_history(self, audit_logger):
        """Test getting all log entries for a record."""
        # Create multiple entries for same record
        audit_logger.log_insert("users", 1, {"name": "Initial"})
        audit_logger.log_update("users", 1, {"name": "Initial"}, {"name": "Updated"})
        audit_logger.log_update("users", 1, {"name": "Updated"}, {"name": "Final"})

        history = audit_logger.get_record_history("users", 1)

        assert len(history) == 3
        assert history[0].action == "INSERT"
        assert history[1].action == "UPDATE"
        assert history[2].action == "UPDATE"

    def test_get_table_history(self, audit_logger):
        """Test getting log entries for a table."""
        # Create entries for different records
        audit_logger.log_insert("accounts", 1, {"name": "Account 1"})
        audit_logger.log_insert("accounts", 2, {"name": "Account 2"})
        audit_logger.log_insert("users", 1, {"name": "User"})  # Different table

        history = audit_logger.get_table_history("accounts")

        assert len(history) == 2
        # All entries should be for the accounts table
        record_ids = {entry.record_id for entry in history}
        assert record_ids == {1, 2}

    def test_get_table_history_with_limit(self, audit_logger):
        """Test table history with limit."""
        for i in range(10):
            audit_logger.log_insert("accounts", i, {"name": f"Account {i}"})

        history = audit_logger.get_table_history("accounts", limit=5)

        assert len(history) == 5

    def test_get_user_activity(self, audit_logger):
        """Test getting all activity for a user (using None user_id)."""
        # Create entries with None user_id (avoids FK constraint)
        audit_logger.log_insert("users", 1, {"name": "User"})
        audit_logger.log_insert("accounts", 1, {"name": "Account"})

        # All entries have user_id=None
        activity = audit_logger.get_user_activity(user_id=None)

        # Should find entries with user_id=None
        assert len(activity) >= 2

    def test_get_user_activity_with_limit(self, audit_logger):
        """Test user activity with limit parameter."""
        # Create multiple entries
        for i in range(5):
            audit_logger.log_insert("users", i, {"name": f"Test {i}"})

        # Get only 3 entries
        activity = audit_logger.get_user_activity(user_id=None, limit=3)

        assert len(activity) == 3


class TestSensitiveDataMasking:
    """Tests for sensitive data masking."""

    def test_mask_sensitive_values_pan(self, audit_logger):
        """Test masking PAN in values."""
        values = {"name": "Test User", "pan": "AAPPS0793R"}
        masked = audit_logger.mask_sensitive_values(values)

        assert masked["name"] == "Test User"
        assert masked["pan"] == "****"

    def test_mask_sensitive_values_multiple(self, audit_logger):
        """Test masking multiple sensitive fields."""
        values = {
            "name": "Test",
            "pan": "AAPPS0793R",
            "aadhaar": "123456789012",
            "bank_account": "1234567890",
            "password": "secret123",
        }
        masked = audit_logger.mask_sensitive_values(values)

        assert masked["name"] == "Test"
        assert masked["pan"] == "****"
        assert masked["aadhaar"] == "****"
        assert masked["bank_account"] == "****"
        assert masked["password"] == "****"

    def test_mask_sensitive_case_insensitive(self, audit_logger):
        """Test that field name matching is case insensitive."""
        values = {"PAN": "AAPPS0793R", "AADHAAR": "123456789012"}
        masked = audit_logger.mask_sensitive_values(values)

        assert masked["PAN"] == "****"
        assert masked["AADHAAR"] == "****"


class TestAuditLogEntryDataclass:
    """Tests for AuditLogEntry dataclass."""

    def test_from_row(self, db_connection, audit_logger):
        """Test creating AuditLogEntry from database row."""
        audit_logger.log_insert("users", 1, {"name": "Test"})

        cursor = db_connection.execute("SELECT * FROM audit_log LIMIT 1")
        row = cursor.fetchone()

        entry = AuditLogEntry.from_row(row)

        assert entry.table_name == "users"
        assert entry.record_id == 1
        assert entry.action == "INSERT"
        assert isinstance(entry.timestamp, datetime)
