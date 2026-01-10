"""
Unit tests for database module.

Tests SQLCipher encrypted database creation and schema initialization.
"""

import pytest
from pfas.core.database import DatabaseManager, get_connection, SCHEMA_SQL
from pfas.core.exceptions import DatabaseError


class TestDatabaseManager:
    """Tests for DatabaseManager class."""

    def test_singleton_pattern(self, db_manager):
        """Test that DatabaseManager follows singleton pattern."""
        another_manager = DatabaseManager()
        assert db_manager is another_manager

    def test_database_encryption_init(self, db_manager):
        """Test SQLCipher encrypted database creation (TC-CORE-001)."""
        db_path = ":memory:"
        password = "test_password_123"

        conn = db_manager.init(db_path, password)

        # Verify tables exist
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = [row[0] for row in cursor.fetchall()]

        assert "users" in tables
        assert "accounts" in tables
        assert "journals" in tables
        assert "journal_entries" in tables
        assert "audit_log" in tables
        assert "exchange_rates" in tables
        assert "sessions" in tables

    def test_get_tables(self, db_connection, db_manager):
        """Test get_tables method returns all expected tables."""
        tables = db_manager.get_tables()

        expected_tables = [
            "users", "accounts", "journals", "journal_entries",
            "exchange_rates", "audit_log", "sessions"
        ]

        for table in expected_tables:
            assert table in tables

    def test_connection_property_before_init(self):
        """Test that accessing connection before init raises error."""
        DatabaseManager.reset_instance()
        manager = DatabaseManager()

        with pytest.raises(DatabaseError) as exc_info:
            _ = manager.connection

        assert "not initialized" in str(exc_info.value).lower()
        DatabaseManager.reset_instance()

    def test_execute_and_commit(self, db_connection, db_manager):
        """Test execute and commit methods."""
        # Insert a test account
        db_manager.execute(
            "INSERT INTO accounts (code, name, account_type) VALUES (?, ?, ?)",
            ("9999", "Test Account", "ASSET"),
        )
        db_manager.commit()

        # Verify insert
        cursor = db_manager.execute(
            "SELECT * FROM accounts WHERE code = ?", ("9999",)
        )
        row = cursor.fetchone()

        assert row is not None
        assert row["name"] == "Test Account"

    def test_rollback(self, db_connection, db_manager):
        """Test rollback method."""
        # Insert a test account
        db_manager.execute(
            "INSERT INTO accounts (code, name, account_type) VALUES (?, ?, ?)",
            ("9998", "Rollback Test", "ASSET"),
        )

        # Rollback instead of commit
        db_manager.rollback()

        # Verify insert was rolled back
        cursor = db_manager.execute(
            "SELECT * FROM accounts WHERE code = ?", ("9998",)
        )
        row = cursor.fetchone()

        assert row is None

    def test_row_factory(self, db_connection, db_manager):
        """Test that row factory allows dict-like access."""
        db_manager.execute(
            "INSERT INTO accounts (code, name, account_type) VALUES (?, ?, ?)",
            ("9997", "Row Factory Test", "LIABILITY"),
        )
        db_manager.commit()

        cursor = db_manager.execute(
            "SELECT * FROM accounts WHERE code = ?", ("9997",)
        )
        row = cursor.fetchone()

        # Should be able to access by column name
        assert row["code"] == "9997"
        assert row["name"] == "Row Factory Test"
        assert row["account_type"] == "LIABILITY"

    def test_foreign_keys_enabled(self, db_connection):
        """Test that foreign keys are enabled."""
        cursor = db_connection.execute("PRAGMA foreign_keys")
        result = cursor.fetchone()
        assert result[0] == 1  # Foreign keys should be ON


class TestGetConnection:
    """Tests for get_connection function."""

    def test_get_connection_after_init(self, db_connection):
        """Test get_connection returns valid connection after init."""
        conn = get_connection()
        assert conn is not None
        assert conn is db_connection

    def test_get_connection_before_init(self):
        """Test get_connection raises error before init."""
        DatabaseManager.reset_instance()

        with pytest.raises(DatabaseError):
            get_connection()

        DatabaseManager.reset_instance()
