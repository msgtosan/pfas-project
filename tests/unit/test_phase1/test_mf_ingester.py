"""Tests for MF Statement Ingester."""

import pytest
from pathlib import Path
from decimal import Decimal
import tempfile
import sqlite3

from pfas.core.database import DatabaseManager
from pfas.parsers.mf.ingester import MFIngester, IngestionResult


@pytest.fixture
def test_db():
    """Create in-memory test database."""
    db = DatabaseManager()
    DatabaseManager.reset_instance()  # Reset singleton
    db = DatabaseManager()
    conn = db.init(":memory:", "test_password")

    # Create test user
    conn.execute(
        "INSERT INTO users (name, pan_encrypted, pan_salt) VALUES (?, ?, ?)",
        ("TestUser", b"encrypted", b"salt")
    )
    conn.commit()

    yield conn

    db.close()
    DatabaseManager.reset_instance()


@pytest.fixture
def temp_inbox():
    """Create temporary inbox directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        inbox = Path(tmpdir) / "inbox" / "Mutual-Fund"
        (inbox / "CAMS").mkdir(parents=True)
        yield inbox


class TestMFIngester:
    """Test suite for MFIngester."""

    def test_ingester_init(self, test_db, temp_inbox):
        """Test ingester initialization."""
        ingester = MFIngester(test_db, 1, temp_inbox)

        assert ingester.conn == test_db
        assert ingester.user_id == 1
        assert ingester.inbox_path == temp_inbox

    def test_ingest_empty_inbox(self, test_db, temp_inbox):
        """Test ingesting from empty inbox."""
        ingester = MFIngester(test_db, 1, temp_inbox)
        result = ingester.ingest()

        assert result.success is True
        assert result.files_processed == 0
        assert len(result.warnings) > 0  # "No files found" warning

    def test_is_already_processed(self, test_db, temp_inbox):
        """Test duplicate detection by file hash."""
        ingester = MFIngester(test_db, 1, temp_inbox)

        # Insert a processed file record
        test_db.execute(
            """
            INSERT INTO ingestion_log
            (user_id, source_file, file_hash, asset_type, status)
            VALUES (?, ?, ?, ?, 'COMPLETED')
            """,
            (1, "test.pdf", "abc123hash", "Mutual-Fund")
        )
        test_db.commit()

        # Check if detected as processed
        assert ingester._is_already_processed("abc123hash") is True
        assert ingester._is_already_processed("different_hash") is False

    def test_create_ingestion_log(self, test_db, temp_inbox):
        """Test ingestion log entry creation."""
        from pfas.parsers.mf.scanner import ScannedFile, RTA, FileType

        # Create a test file
        test_file = temp_inbox / "CAMS" / "test.xlsx"
        test_file.write_text("test content")

        scanned = ScannedFile(
            path=test_file,
            rta=RTA.CAMS,
            file_type=FileType.XLSX
        )

        ingester = MFIngester(test_db, 1, temp_inbox)
        log_id = ingester._create_ingestion_log(scanned)

        assert log_id > 0

        # Verify log entry
        cursor = test_db.execute(
            "SELECT * FROM ingestion_log WHERE id = ?",
            (log_id,)
        )
        row = cursor.fetchone()

        assert row is not None
        assert row['status'] == 'PENDING'
        assert row['rta_source'] == 'CAMS'

    def test_update_ingestion_status(self, test_db, temp_inbox):
        """Test ingestion status update."""
        # Create log entry
        test_db.execute(
            """
            INSERT INTO ingestion_log
            (user_id, source_file, file_hash, asset_type, status)
            VALUES (?, ?, ?, ?, 'PENDING')
            """,
            (1, "test.pdf", "hash123", "Mutual-Fund")
        )
        test_db.commit()

        ingester = MFIngester(test_db, 1, temp_inbox)
        ingester._update_ingestion_status(
            1, 'COMPLETED',
            records_processed=10,
            records_skipped=2
        )

        cursor = test_db.execute(
            "SELECT status, records_processed, records_skipped FROM ingestion_log WHERE id = 1"
        )
        row = cursor.fetchone()

        assert row['status'] == 'COMPLETED'
        assert row['records_processed'] == 10
        assert row['records_skipped'] == 2

    def test_get_ingestion_history(self, test_db, temp_inbox):
        """Test retrieving ingestion history."""
        # Insert some history
        for i in range(3):
            test_db.execute(
                """
                INSERT INTO ingestion_log
                (user_id, source_file, file_hash, asset_type, status)
                VALUES (?, ?, ?, ?, 'COMPLETED')
                """,
                (1, f"file{i}.pdf", f"hash{i}", "Mutual-Fund")
            )
        test_db.commit()

        ingester = MFIngester(test_db, 1, temp_inbox)
        history = ingester.get_ingestion_history(limit=10)

        assert len(history) == 3

    def test_get_pending_files(self, test_db, temp_inbox):
        """Test retrieving pending/failed files."""
        # Insert mixed status entries
        test_db.execute(
            """
            INSERT INTO ingestion_log
            (user_id, source_file, file_hash, asset_type, status)
            VALUES
            (?, 'completed.pdf', 'hash1', 'Mutual-Fund', 'COMPLETED'),
            (?, 'failed.pdf', 'hash2', 'Mutual-Fund', 'FAILED'),
            (?, 'pending.pdf', 'hash3', 'Mutual-Fund', 'PENDING')
            """,
            (1, 1, 1)
        )
        test_db.commit()

        ingester = MFIngester(test_db, 1, temp_inbox)
        pending = ingester.get_pending_files()

        assert len(pending) == 2
        statuses = [p['status'] for p in pending]
        assert 'FAILED' in statuses
        assert 'PENDING' in statuses
        assert 'COMPLETED' not in statuses


class TestIngestionResult:
    """Test suite for IngestionResult."""

    def test_default_success(self):
        """Test default success state."""
        result = IngestionResult()
        assert result.success is True

    def test_add_error_sets_failure(self):
        """Test that adding error sets success to False."""
        result = IngestionResult()
        result.add_error("Test error")

        assert result.success is False
        assert len(result.errors) == 1

    def test_add_warning_keeps_success(self):
        """Test that warnings don't affect success."""
        result = IngestionResult()
        result.add_warning("Test warning")

        assert result.success is True
        assert len(result.warnings) == 1
