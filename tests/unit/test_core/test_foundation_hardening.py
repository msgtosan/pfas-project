"""
Unit tests for Foundation Hardening components.

Tests cover:
- Security module (UserContext, @require_user_context)
- Audit logging with source tracking
- NAV service with interpolation
- Transaction service with idempotency
- Batch ingester with atomic rollback
- Idempotency key generation
"""

import pytest
import sqlite3
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch
import tempfile
import os

# Import modules under test
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from pfas.core.security import (
    UserContext,
    UserContextManager,
    UserContextError,
    require_user_context,
    validate_user_owns_record,
    get_user_filter_clause,
)
from pfas.core.audit import AuditLogger, AuditLogEntry
from pfas.core.exceptions import IdempotencyError, BatchIngestionError
from pfas.services.nav_service import NAVService, NAVRecord
from pfas.core.transaction_service import (
    TransactionService,
    TransactionResult,
    TransactionSource,
    IdempotencyKeyGenerator,
)
from pfas.core.journal import JournalEntry
from pfas.parsers.idempotency import (
    IdempotencyMixin,
    mf_idempotency_key,
    stock_idempotency_key,
    bank_idempotency_key,
)
from pfas.services.batch_ingester import (
    BatchIngester,
    BatchResult,
    FileResult,
    FileStatus,
)


@pytest.fixture
def db_connection():
    """Create in-memory SQLite database with required schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create minimal schema for testing
    conn.executescript("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            account_type TEXT NOT NULL,
            parent_id INTEGER,
            currency TEXT DEFAULT 'INR',
            description TEXT,
            is_active INTEGER DEFAULT 1,
            user_id INTEGER
        );

        CREATE TABLE journals (
            id INTEGER PRIMARY KEY,
            date DATE NOT NULL,
            description TEXT,
            reference_type TEXT,
            reference_id INTEGER,
            created_by INTEGER,
            is_reversed INTEGER DEFAULT 0
        );

        CREATE TABLE journal_entries (
            id INTEGER PRIMARY KEY,
            journal_id INTEGER NOT NULL,
            account_id INTEGER NOT NULL,
            debit DECIMAL(15,2) DEFAULT 0,
            credit DECIMAL(15,2) DEFAULT 0,
            currency TEXT DEFAULT 'INR',
            exchange_rate DECIMAL(10,6) DEFAULT 1,
            narration TEXT,
            FOREIGN KEY (journal_id) REFERENCES journals(id),
            FOREIGN KEY (account_id) REFERENCES accounts(id)
        );

        CREATE TABLE mf_schemes (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            isin TEXT,
            asset_class TEXT
        );

        CREATE TABLE mf_folios (
            id INTEGER PRIMARY KEY,
            scheme_id INTEGER NOT NULL,
            folio_number TEXT,
            user_id INTEGER NOT NULL,
            FOREIGN KEY (scheme_id) REFERENCES mf_schemes(id)
        );

        CREATE TABLE mf_transactions (
            id INTEGER PRIMARY KEY,
            folio_id INTEGER NOT NULL,
            date DATE NOT NULL,
            units DECIMAL(12,4),
            nav DECIMAL(12,4),
            amount DECIMAL(15,2),
            transaction_type TEXT,
            FOREIGN KEY (folio_id) REFERENCES mf_folios(id)
        );

        CREATE TABLE audit_log (
            id INTEGER PRIMARY KEY,
            table_name TEXT NOT NULL,
            record_id INTEGER,
            action TEXT,
            old_values JSON,
            new_values JSON,
            user_id INTEGER,
            ip_address TEXT,
            source TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Insert test data
        INSERT INTO users (id, name) VALUES (1, 'Test User');
        INSERT INTO accounts (id, code, name, account_type) VALUES
            (1, '1101', 'Bank Savings', 'ASSET'),
            (2, '4101', 'Salary Income', 'INCOME');
        INSERT INTO mf_schemes (id, name, isin) VALUES (1, 'Test Scheme', 'INF123456789');
        INSERT INTO mf_folios (id, scheme_id, folio_number, user_id) VALUES (1, 1, 'FOLIO123', 1);
    """)

    yield conn
    conn.close()


# ============================================================
# SECURITY MODULE TESTS
# ============================================================

class TestUserContext:
    """Tests for UserContext class."""

    def test_set_and_get_current(self):
        """Test setting and getting current user."""
        UserContext.clear()
        assert UserContext.get_current() is None

        UserContext.set_current(123)
        assert UserContext.get_current() == 123

        UserContext.clear()
        assert UserContext.get_current() is None

    def test_set_current_none_raises(self):
        """Test that setting None raises error."""
        with pytest.raises(UserContextError):
            UserContext.set_current(None)

    def test_context_manager(self):
        """Test scoped context manager."""
        UserContext.set_current(100)

        with UserContext.set(200):
            assert UserContext.get_current() == 200

        # Should restore previous value
        assert UserContext.get_current() == 100
        UserContext.clear()


class TestRequireUserContext:
    """Tests for @require_user_context decorator."""

    def test_decorator_with_user_id_kwarg(self):
        """Test decorator passes with user_id in kwargs."""

        @require_user_context
        def my_func(user_id: int, data: str):
            return f"user={user_id}, data={data}"

        result = my_func(user_id=1, data="test")
        assert result == "user=1, data=test"

    def test_decorator_with_user_id_positional(self):
        """Test decorator passes with user_id as positional arg."""

        @require_user_context
        def my_func(user_id: int, data: str):
            return f"user={user_id}, data={data}"

        result = my_func(1, "test")
        assert result == "user=1, data=test"

    def test_decorator_raises_without_user_id(self):
        """Test decorator raises when user_id missing."""
        UserContext.clear()

        @require_user_context
        def my_func(user_id: int):
            return user_id

        with pytest.raises(UserContextError):
            my_func(user_id=None)

    def test_decorator_uses_global_context(self):
        """Test decorator falls back to global context."""

        @require_user_context
        def my_func(user_id: int = None):
            return user_id

        UserContext.set_current(999)
        result = my_func()
        assert result == 999
        UserContext.clear()

    def test_decorator_validates_user_id_type(self):
        """Test decorator validates user_id is int."""

        @require_user_context
        def my_func(user_id: int):
            return user_id

        with pytest.raises(UserContextError, match="must be an integer"):
            my_func(user_id="not_an_int")

    def test_decorator_validates_positive_user_id(self):
        """Test decorator validates user_id is positive."""

        @require_user_context
        def my_func(user_id: int):
            return user_id

        with pytest.raises(UserContextError, match="must be positive"):
            my_func(user_id=0)


# ============================================================
# AUDIT LOGGER TESTS
# ============================================================

class TestAuditLogger:
    """Tests for AuditLogger class."""

    def test_log_insert(self, db_connection):
        """Test logging an INSERT operation."""
        logger = AuditLogger(db_connection, user_id=1, source="test")

        log_id = logger.log_insert(
            table_name="users",
            record_id=100,
            new_values={"name": "John", "email": "john@example.com"}
        )

        assert log_id > 0

        # Verify log entry
        cursor = db_connection.execute(
            "SELECT * FROM audit_log WHERE id = ?", (log_id,)
        )
        row = cursor.fetchone()
        assert row["table_name"] == "users"
        assert row["action"] == "INSERT"
        assert row["user_id"] == 1
        assert row["source"] == "test"

    def test_log_update(self, db_connection):
        """Test logging an UPDATE operation."""
        logger = AuditLogger(db_connection, user_id=1)

        log_id = logger.log_update(
            table_name="users",
            record_id=100,
            old_values={"email": "old@example.com"},
            new_values={"email": "new@example.com"},
            source="api"
        )

        cursor = db_connection.execute(
            "SELECT * FROM audit_log WHERE id = ?", (log_id,)
        )
        row = cursor.fetchone()
        assert row["action"] == "UPDATE"
        assert "old@example.com" in row["old_values"]
        assert "new@example.com" in row["new_values"]

    def test_log_delete(self, db_connection):
        """Test logging a DELETE operation."""
        logger = AuditLogger(db_connection, user_id=1)

        log_id = logger.log_delete(
            table_name="users",
            record_id=100,
            old_values={"name": "Deleted User"}
        )

        cursor = db_connection.execute(
            "SELECT action FROM audit_log WHERE id = ?", (log_id,)
        )
        assert cursor.fetchone()["action"] == "DELETE"

    def test_invalid_action_raises(self, db_connection):
        """Test that invalid action raises ValueError."""
        logger = AuditLogger(db_connection)

        with pytest.raises(ValueError, match="Invalid action"):
            logger.log_change(
                table_name="users",
                record_id=1,
                action="INVALID"
            )


# ============================================================
# NAV SERVICE TESTS
# ============================================================

class TestNAVService:
    """Tests for NAVService class."""

    def test_store_and_retrieve_nav(self, db_connection):
        """Test storing and retrieving NAV."""
        service = NAVService(db_connection)

        # Store NAV
        service.store_nav(
            scheme_id=1,
            nav_date=date(2024, 3, 15),
            nav=Decimal("123.4567"),
            source="test"
        )

        # Retrieve NAV
        nav = service.get_nav_at(scheme_id=1, as_of=date(2024, 3, 15))
        assert nav == Decimal("123.4567")

    def test_nav_interpolation(self, db_connection):
        """Test NAV interpolation between dates."""
        service = NAVService(db_connection)

        # Store two NAVs 10 days apart
        service.store_nav(1, date(2024, 3, 1), Decimal("100.0000"))
        service.store_nav(1, date(2024, 3, 11), Decimal("110.0000"))

        # Get interpolated NAV at midpoint
        nav = service.get_nav_at(1, date(2024, 3, 6), allow_interpolation=True)

        # Should be approximately 105 (midpoint)
        assert nav is not None
        assert Decimal("104") < nav < Decimal("106")

    def test_nav_fallback_to_nearest(self, db_connection):
        """Test fallback to nearest NAV when interpolation not possible."""
        service = NAVService(db_connection)

        service.store_nav(1, date(2024, 3, 1), Decimal("100.0000"))

        # Request date after only available NAV
        nav = service.get_nav_at(1, date(2024, 3, 15), allow_interpolation=False)
        assert nav == Decimal("100.0000")

    def test_backfill_from_transactions(self, db_connection):
        """Test backfilling NAV from transactions."""
        # Insert test transactions
        db_connection.execute("""
            INSERT INTO mf_transactions (folio_id, date, units, nav, amount)
            VALUES (1, '2024-03-01', 100, 50.5, 5050),
                   (1, '2024-03-15', 50, 51.0, 2550)
        """)
        db_connection.commit()

        service = NAVService(db_connection)
        count = service.backfill_from_transactions()

        assert count == 2

        # Verify NAVs were stored
        nav1 = service.get_nav_at(1, date(2024, 3, 1))
        nav2 = service.get_nav_at(1, date(2024, 3, 15))
        assert nav1 == Decimal("50.5")
        assert nav2 == Decimal("51.0")


# ============================================================
# TRANSACTION SERVICE TESTS
# ============================================================

class TestTransactionService:
    """Tests for TransactionService class."""

    def test_record_balanced_transaction(self, db_connection):
        """Test recording a balanced transaction."""
        service = TransactionService(db_connection)

        entries = [
            JournalEntry(account_id=1, debit=Decimal("1000")),
            JournalEntry(account_id=2, credit=Decimal("1000")),
        ]

        result = service.record(
            user_id=1,
            entries=entries,
            description="Test transaction",
            source=TransactionSource.MANUAL,
            idempotency_key="test:001"
        )

        assert result.result == TransactionResult.SUCCESS
        assert result.journal_id is not None

    def test_duplicate_detection(self, db_connection):
        """Test duplicate transaction detection."""
        service = TransactionService(db_connection)

        entries = [
            JournalEntry(account_id=1, debit=Decimal("1000")),
            JournalEntry(account_id=2, credit=Decimal("1000")),
        ]

        # First transaction
        result1 = service.record(
            user_id=1,
            entries=entries,
            description="First",
            source=TransactionSource.MANUAL,
            idempotency_key="test:duplicate"
        )
        assert result1.result == TransactionResult.SUCCESS

        # Duplicate transaction
        result2 = service.record(
            user_id=1,
            entries=entries,
            description="Duplicate",
            source=TransactionSource.MANUAL,
            idempotency_key="test:duplicate"
        )
        assert result2.result == TransactionResult.DUPLICATE

    def test_unbalanced_transaction_fails(self, db_connection):
        """Test that unbalanced transactions fail."""
        service = TransactionService(db_connection)

        entries = [
            JournalEntry(account_id=1, debit=Decimal("1000")),
            JournalEntry(account_id=2, credit=Decimal("500")),  # Unbalanced!
        ]

        result = service.record(
            user_id=1,
            entries=entries,
            description="Unbalanced",
            source=TransactionSource.MANUAL,
            idempotency_key="test:unbalanced"
        )

        assert result.result == TransactionResult.VALIDATION_ERROR

    def test_get_transaction_by_key(self, db_connection):
        """Test retrieving transaction by idempotency key."""
        service = TransactionService(db_connection)

        entries = [
            JournalEntry(account_id=1, debit=Decimal("1000")),
            JournalEntry(account_id=2, credit=Decimal("1000")),
        ]

        service.record(
            user_id=1,
            entries=entries,
            description="Findable",
            source=TransactionSource.MANUAL,
            idempotency_key="test:findme"
        )

        txn = service.get_transaction_by_key("test:findme")
        assert txn is not None
        assert txn["user_id"] == 1


# ============================================================
# IDEMPOTENCY KEY GENERATOR TESTS
# ============================================================

class TestIdempotencyKeyGenerator:
    """Tests for IdempotencyKeyGenerator class."""

    def test_mf_transaction_key(self):
        """Test MF transaction key generation."""
        key = IdempotencyKeyGenerator.mf_transaction(
            folio_number="FOLIO123",
            txn_date=date(2024, 3, 15),
            amount=Decimal("10000.50"),
            units=Decimal("100.123"),
            txn_type="PURCHASE"
        )
        assert key == "mf:FOLIO123:2024-03-15:10000.50:100.123:PURCHASE"

    def test_stock_trade_key(self):
        """Test stock trade key generation."""
        key = IdempotencyKeyGenerator.stock_trade(
            broker="zerodha",
            trade_id="T12345"
        )
        assert key == "stock:zerodha:T12345"

    def test_bank_transaction_key(self):
        """Test bank transaction key generation."""
        key = IdempotencyKeyGenerator.bank_transaction(
            account_number="1234567890",
            txn_date=date(2024, 3, 15),
            ref_no="REF001",
            amount=Decimal("5000")
        )
        # Account number should be hashed
        assert key.startswith("bank:")
        assert "1234567890" not in key  # Account number should be hashed
        assert "2024-03-15" in key
        assert "REF001" in key


# ============================================================
# PARSER IDEMPOTENCY MIXIN TESTS
# ============================================================

class TestIdempotencyMixin:
    """Tests for IdempotencyMixin class."""

    def test_mixin_generates_keys(self, db_connection):
        """Test mixin key generation."""

        class TestParser(IdempotencyMixin):
            def __init__(self, conn):
                self.conn = conn
                self._init_idempotency()

        parser = TestParser(db_connection)
        key = parser.generate_key("test", "part1", date(2024, 3, 15), Decimal("100.50"))

        assert key == "test:part1:2024-03-15:100.50"

    def test_mixin_deduplication(self, db_connection):
        """Test mixin deduplication."""

        class TestParser(IdempotencyMixin):
            def __init__(self, conn):
                self.conn = conn
                self._init_idempotency()

        parser = TestParser(db_connection)

        # First check - not duplicate
        assert not parser.is_duplicate("unique:key:001")

        # Record as processed
        parser.record_processed("unique:key:001", record_id=1)

        # Second check - is duplicate
        assert parser.is_duplicate("unique:key:001")


# ============================================================
# BATCH INGESTER TESTS
# ============================================================

class TestBatchIngester:
    """Tests for BatchIngester class."""

    def test_file_hash_calculation(self, db_connection):
        """Test file hash calculation."""
        ingester = BatchIngester(db_connection, user_id=1)

        # Create temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"test content")
            temp_path = Path(f.name)

        try:
            hash1 = ingester.calculate_file_hash(temp_path)
            hash2 = ingester.calculate_file_hash(temp_path)

            # Same file should have same hash
            assert hash1 == hash2
            assert len(hash1) == 32  # MD5 hex length
        finally:
            os.unlink(temp_path)

    def test_duplicate_file_detection(self, db_connection):
        """Test duplicate file detection."""
        ingester = BatchIngester(db_connection, user_id=1)

        # Record a processed file
        db_connection.execute("""
            INSERT INTO processed_files (file_hash, file_name, user_id, status)
            VALUES ('abc123', 'test.xlsx', 1, 'success')
        """)
        db_connection.commit()

        assert ingester.is_file_processed("abc123")
        assert not ingester.is_file_processed("xyz789")

    def test_batch_result_tracking(self):
        """Test BatchResult tracking."""
        result = BatchResult(
            success=False,
            total_files=3,
            files_processed=0,
            files_failed=0,
            files_skipped=0,
            total_records=0
        )

        # Add success
        result.add_file_result(FileResult(
            file_path=Path("file1.xlsx"),
            status=FileStatus.SUCCESS,
            records_processed=10
        ))

        # Add failure
        result.add_file_result(FileResult(
            file_path=Path("file2.xlsx"),
            status=FileStatus.FAILED,
            error_message="Parse error"
        ))

        # Add skip
        result.add_file_result(FileResult(
            file_path=Path("file3.xlsx"),
            status=FileStatus.SKIPPED
        ))

        assert result.files_processed == 1
        assert result.files_failed == 1
        assert result.files_skipped == 1
        assert result.total_records == 10


# ============================================================
# STANDALONE KEY FUNCTION TESTS
# ============================================================

class TestStandaloneKeyFunctions:
    """Tests for standalone idempotency key functions."""

    def test_mf_key(self):
        key = mf_idempotency_key("FOLIO1", date(2024, 1, 1), Decimal("1000"), Decimal("10"), "BUY")
        assert key == "mf:FOLIO1:2024-01-01:1000:10:BUY"

    def test_stock_key(self):
        key = stock_idempotency_key("zerodha", "T001")
        assert key == "stock:zerodha:T001"

    def test_bank_key_hashes_account(self):
        key = bank_idempotency_key("1234567890", date(2024, 1, 1), "REF", Decimal("100"))
        assert "1234567890" not in key
        assert "2024-01-01" in key


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
