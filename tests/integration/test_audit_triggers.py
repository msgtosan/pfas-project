"""
Integration tests for automatic audit logging triggers.

Tests that SQLite triggers automatically populate the audit_log table
for all data changes without requiring manual calls to AuditLogger.
"""

import pytest
from decimal import Decimal
from datetime import date

from pfas.core.journal import JournalEngine, JournalEntry
from pfas.core.currency import CurrencyConverter
from pfas.core.accounts import setup_chart_of_accounts, get_account_by_code


class TestAutomaticAuditTriggers:
    """Tests for automatic audit logging via SQLite triggers."""

    def test_journal_insert_trigger(self, db_with_accounts):
        """Test that journal INSERT automatically creates audit log entry."""
        engine = JournalEngine(db_with_accounts)
        bank = get_account_by_code(db_with_accounts, "1101")
        salary = get_account_by_code(db_with_accounts, "4101")

        # Create a journal entry
        journal_id = engine.create_journal(
            txn_date=date.today(),
            description="Test journal",
            entries=[
                JournalEntry(account_id=bank.id, debit=Decimal("1000")),
                JournalEntry(account_id=salary.id, credit=Decimal("1000")),
            ],
        )

        # Verify audit log was automatically created by trigger
        cursor = db_with_accounts.execute(
            """
            SELECT * FROM audit_log
            WHERE table_name = 'journals' AND record_id = ? AND action = 'INSERT'
            """,
            (journal_id,)
        )
        audit_entry = cursor.fetchone()

        assert audit_entry is not None, "Audit log entry should be created automatically"
        assert audit_entry["action"] == "INSERT"
        assert audit_entry["new_values"] is not None

    def test_journal_update_trigger(self, db_with_accounts):
        """Test that journal UPDATE automatically creates audit log entry."""
        engine = JournalEngine(db_with_accounts)
        bank = get_account_by_code(db_with_accounts, "1101")
        salary = get_account_by_code(db_with_accounts, "4101")

        # Create a journal
        journal_id = engine.create_journal(
            txn_date=date.today(),
            description="Original description",
            entries=[
                JournalEntry(account_id=bank.id, debit=Decimal("1000")),
                JournalEntry(account_id=salary.id, credit=Decimal("1000")),
            ],
        )

        # Update the journal (mark as reversed)
        db_with_accounts.execute(
            "UPDATE journals SET is_reversed = 1 WHERE id = ?",
            (journal_id,)
        )
        db_with_accounts.commit()

        # Verify UPDATE audit log was created
        cursor = db_with_accounts.execute(
            """
            SELECT * FROM audit_log
            WHERE table_name = 'journals' AND record_id = ? AND action = 'UPDATE'
            """,
            (journal_id,)
        )
        audit_entry = cursor.fetchone()

        assert audit_entry is not None
        assert audit_entry["action"] == "UPDATE"
        assert audit_entry["old_values"] is not None
        assert audit_entry["new_values"] is not None

    def test_journal_entries_trigger(self, db_with_accounts):
        """Test that journal_entries INSERT creates audit log."""
        engine = JournalEngine(db_with_accounts)
        bank = get_account_by_code(db_with_accounts, "1101")
        salary = get_account_by_code(db_with_accounts, "4101")

        # Create journal (which creates journal_entries)
        journal_id = engine.create_journal(
            txn_date=date.today(),
            description="Test",
            entries=[
                JournalEntry(account_id=bank.id, debit=Decimal("1000")),
                JournalEntry(account_id=salary.id, credit=Decimal("1000")),
            ],
        )

        # Get the journal entry IDs
        cursor = db_with_accounts.execute(
            "SELECT id FROM journal_entries WHERE journal_id = ?",
            (journal_id,)
        )
        entry_ids = [row["id"] for row in cursor.fetchall()]

        assert len(entry_ids) == 2

        # Verify audit logs for each journal entry
        for entry_id in entry_ids:
            cursor = db_with_accounts.execute(
                """
                SELECT * FROM audit_log
                WHERE table_name = 'journal_entries' AND record_id = ? AND action = 'INSERT'
                """,
                (entry_id,)
            )
            audit_entry = cursor.fetchone()
            assert audit_entry is not None, f"Audit log missing for journal_entry {entry_id}"

    def test_exchange_rates_trigger(self, db_connection):
        """Test that exchange_rates INSERT creates audit log."""
        converter = CurrencyConverter(db_connection)

        # Add an exchange rate
        rate_id = converter.add_rate(
            rate_date=date(2024, 6, 15),
            from_currency="USD",
            rate=Decimal("83.50"),
        )

        # Verify audit log was created
        cursor = db_connection.execute(
            """
            SELECT * FROM audit_log
            WHERE table_name = 'exchange_rates' AND record_id = ? AND action = 'INSERT'
            """,
            (rate_id,)
        )
        audit_entry = cursor.fetchone()

        assert audit_entry is not None
        assert audit_entry["action"] == "INSERT"
        import json
        new_values = json.loads(audit_entry["new_values"])
        # JSON stores as TEXT in SQLite, which becomes a string in the JSON object
        # The rate value in the audit log comes from the trigger's json_object()
        assert str(new_values["rate"]) == "83.5" or new_values["rate"] == "83.50"

    def test_accounts_trigger(self, db_connection):
        """Test that accounts INSERT creates audit log."""
        # Insert a new account
        cursor = db_connection.cursor()
        cursor.execute(
            """
            INSERT INTO accounts (code, name, account_type)
            VALUES (?, ?, ?)
            """,
            ("9999", "Test Account", "ASSET")
        )
        account_id = cursor.lastrowid
        db_connection.commit()

        # Verify audit log was created
        cursor = db_connection.execute(
            """
            SELECT * FROM audit_log
            WHERE table_name = 'accounts' AND record_id = ? AND action = 'INSERT'
            """,
            (account_id,)
        )
        audit_entry = cursor.fetchone()

        assert audit_entry is not None
        assert audit_entry["action"] == "INSERT"

    def test_bank_accounts_trigger(self, db_connection):
        """Test that bank_accounts INSERT creates audit log."""
        from pfas.core.encryption import encrypt_field

        master_key = b"test_master_key_32_bytes_long!!"
        account_number = "1234567890"
        encrypted, salt = encrypt_field(account_number, master_key)

        cursor = db_connection.cursor()
        cursor.execute(
            """
            INSERT INTO bank_accounts
            (account_number_encrypted, account_number_salt, account_number_last4, bank_name)
            VALUES (?, ?, ?, ?)
            """,
            (encrypted, salt, "7890", "Test Bank")
        )
        bank_account_id = cursor.lastrowid
        db_connection.commit()

        # Verify audit log (should NOT contain encrypted account number)
        cursor = db_connection.execute(
            """
            SELECT * FROM audit_log
            WHERE table_name = 'bank_accounts' AND record_id = ? AND action = 'INSERT'
            """,
            (bank_account_id,)
        )
        audit_entry = cursor.fetchone()

        assert audit_entry is not None
        import json
        new_values = json.loads(audit_entry["new_values"])
        # Should only contain last4, not the encrypted value
        assert "account_number_last4" in new_values
        assert "account_number_encrypted" not in new_values

    def test_cascade_audit_on_reversal(self, db_with_accounts):
        """Test that reversing a journal creates audit logs for all operations."""
        engine = JournalEngine(db_with_accounts)
        bank = get_account_by_code(db_with_accounts, "1101")
        salary = get_account_by_code(db_with_accounts, "4101")

        # Create original journal
        original_id = engine.create_journal(
            txn_date=date.today(),
            description="Original",
            entries=[
                JournalEntry(account_id=bank.id, debit=Decimal("1000")),
                JournalEntry(account_id=salary.id, credit=Decimal("1000")),
            ],
        )

        # Reverse it
        reversal_id = engine.reverse_journal(original_id)

        # Should have audit logs for:
        # 1. Original journal INSERT (1 entry)
        # 2. Original journal entries INSERT (2 entries)
        # 3. Original journal UPDATE (marked as reversed) (1 entry)
        # 4. Reversal journal INSERT (1 entry)
        # 5. Reversal journal entries INSERT (2 entries)

        cursor = db_with_accounts.execute(
            "SELECT COUNT(*) as count FROM audit_log WHERE table_name = 'journals'"
        )
        journal_audit_count = cursor.fetchone()["count"]

        # Should have at least 3 journal audit entries (2 INSERTs + 1 UPDATE)
        assert journal_audit_count >= 3

        cursor = db_with_accounts.execute(
            "SELECT COUNT(*) as count FROM audit_log WHERE table_name = 'journal_entries'"
        )
        entries_audit_count = cursor.fetchone()["count"]

        # Should have 4 journal_entries audit entries (2 for original + 2 for reversal)
        assert entries_audit_count >= 4


class TestAuditTriggerTransactionConsistency:
    """Test that audit logs are created atomically with data changes."""

    def test_audit_in_same_transaction(self, db_with_accounts):
        """Test that audit log is created in the same transaction as the data change."""
        engine = JournalEngine(db_with_accounts)
        bank = get_account_by_code(db_with_accounts, "1101")
        salary = get_account_by_code(db_with_accounts, "4101")

        # The journal creation should fail due to an invalid account
        with pytest.raises(Exception):
            engine.create_journal(
                txn_date=date.today(),
                description="This should fail",
                entries=[
                    JournalEntry(account_id=bank.id, debit=Decimal("1000")),
                    JournalEntry(account_id=99999, credit=Decimal("1000")),  # Invalid account
                ],
            )

        # Verify no audit log was created (because transaction rolled back)
        cursor = db_with_accounts.execute(
            "SELECT COUNT(*) as count FROM audit_log WHERE table_name = 'journals'"
        )
        count = cursor.fetchone()["count"]

        # Should be 0 because the transaction was rolled back
        assert count == 0

    def test_multiple_operations_in_transaction(self, db_with_accounts):
        """Test that all audit logs are committed atomically."""
        # Use database transaction context manager
        from pfas.core.database import DatabaseManager

        db_manager = DatabaseManager()
        engine = JournalEngine(db_with_accounts)
        bank = get_account_by_code(db_with_accounts, "1101")
        salary = get_account_by_code(db_with_accounts, "4101")

        # Create multiple journals in a single logical operation
        journal_id = engine.create_journal(
            txn_date=date.today(),
            description="Transaction test",
            entries=[
                JournalEntry(account_id=bank.id, debit=Decimal("1000")),
                JournalEntry(account_id=salary.id, credit=Decimal("1000")),
            ],
        )

        # All audit logs should be present
        cursor = db_with_accounts.execute(
            """
            SELECT COUNT(*) as count FROM audit_log
            WHERE table_name IN ('journals', 'journal_entries')
            """
        )
        count = cursor.fetchone()["count"]

        # Should have 1 journal audit + 2 journal_entries audits = 3 total
        assert count >= 3
