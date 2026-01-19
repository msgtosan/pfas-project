"""Tests for MF Statement Scanner."""

import pytest
from pathlib import Path
from decimal import Decimal
import tempfile
import os

from pfas.parsers.mf.scanner import (
    MFStatementScanner,
    ScannedFile,
    ScanResult,
    RTA,
    FileType,
    scan_mf_inbox
)


@pytest.fixture
def temp_inbox():
    """Create a temporary inbox directory structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        inbox = Path(tmpdir) / "inbox" / "Mutual-Fund"

        # Create RTA folders
        (inbox / "CAMS").mkdir(parents=True)
        (inbox / "KARVY").mkdir(parents=True)

        yield inbox


@pytest.fixture
def temp_inbox_with_files(temp_inbox):
    """Create temp inbox with sample files."""
    # Create sample files
    (temp_inbox / "CAMS" / "cas_statement.xlsx").write_text("dummy excel")
    (temp_inbox / "CAMS" / "cas_report.pdf").write_bytes(b"dummy pdf content")
    (temp_inbox / "KARVY" / "karvy_statement.xlsx").write_text("dummy excel")

    return temp_inbox


class TestMFStatementScanner:
    """Test suite for MFStatementScanner."""

    def test_scan_empty_inbox(self, temp_inbox):
        """Test scanning empty inbox returns empty result."""
        scanner = MFStatementScanner(temp_inbox)
        result = scanner.scan()

        assert result.success is True
        assert len(result.files) == 0
        assert result.total_scanned == 0

    def test_scan_detects_rta_from_path(self, temp_inbox_with_files):
        """Test RTA detection from folder path."""
        scanner = MFStatementScanner(temp_inbox_with_files)
        result = scanner.scan()

        assert result.success is True
        assert len(result.files) == 3

        cams_files = [f for f in result.files if f.rta == RTA.CAMS]
        karvy_files = [f for f in result.files if f.rta == RTA.KARVY]

        assert len(cams_files) == 2
        assert len(karvy_files) == 1

        # Verify detected_from is "path"
        for f in result.files:
            assert f.detected_from == "path"

    def test_scan_detects_file_types(self, temp_inbox_with_files):
        """Test file type detection."""
        scanner = MFStatementScanner(temp_inbox_with_files)
        result = scanner.scan()

        pdf_files = result.pdf_files
        excel_files = result.excel_files

        assert len(pdf_files) == 1
        assert len(excel_files) == 2

        assert pdf_files[0].file_type == FileType.PDF
        assert all(f.file_type in (FileType.XLSX, FileType.XLS) for f in excel_files)

    def test_scan_calculates_file_hash(self, temp_inbox_with_files):
        """Test that file hash is calculated."""
        scanner = MFStatementScanner(temp_inbox_with_files)
        result = scanner.scan()

        for f in result.files:
            assert f.file_hash != ""
            assert len(f.file_hash) == 64  # SHA256 hex length

    def test_scan_nonexistent_path(self):
        """Test scanning nonexistent path returns error."""
        scanner = MFStatementScanner(Path("/nonexistent/path"))
        result = scanner.scan()

        assert result.success is False
        assert len(result.errors) > 0
        assert "does not exist" in result.errors[0]

    def test_scan_recursive_subfolders(self, temp_inbox):
        """Test recursive scanning of subfolders."""
        # Create nested structure
        (temp_inbox / "CAMS" / "2024").mkdir(parents=True)
        (temp_inbox / "CAMS" / "2024" / "statement.xlsx").write_text("nested")

        scanner = MFStatementScanner(temp_inbox)
        result = scanner.scan()

        assert len(result.files) == 1
        assert result.files[0].rta == RTA.CAMS

    def test_scan_ignores_unsupported_files(self, temp_inbox):
        """Test that unsupported file types are ignored."""
        (temp_inbox / "CAMS" / "readme.txt").write_text("readme")
        (temp_inbox / "CAMS" / "image.png").write_bytes(b"png")
        (temp_inbox / "CAMS" / "valid.xlsx").write_text("excel")

        scanner = MFStatementScanner(temp_inbox)
        result = scanner.scan()

        # Only xlsx should be found
        assert len(result.files) == 1
        assert result.files[0].path.suffix == ".xlsx"


class TestScannedFile:
    """Test suite for ScannedFile dataclass."""

    def test_file_hash_calculation(self, temp_inbox):
        """Test that file hash is calculated on init."""
        test_file = temp_inbox / "test.xlsx"
        test_file.write_text("test content")

        scanned = ScannedFile(
            path=test_file,
            rta=RTA.CAMS,
            file_type=FileType.XLSX
        )

        assert scanned.file_hash != ""
        assert scanned.size_bytes > 0

    def test_same_content_same_hash(self, temp_inbox):
        """Test that same content produces same hash."""
        content = "identical content"

        file1 = temp_inbox / "file1.xlsx"
        file2 = temp_inbox / "file2.xlsx"

        file1.write_text(content)
        file2.write_text(content)

        scanned1 = ScannedFile(path=file1, rta=RTA.CAMS, file_type=FileType.XLSX)
        scanned2 = ScannedFile(path=file2, rta=RTA.CAMS, file_type=FileType.XLSX)

        assert scanned1.file_hash == scanned2.file_hash


class TestScanResult:
    """Test suite for ScanResult."""

    def test_success_with_no_errors(self):
        """Test success is True when no errors."""
        result = ScanResult()
        assert result.success is True

    def test_cams_files_filter(self):
        """Test cams_files property filters correctly."""
        result = ScanResult()
        result.files = [
            ScannedFile(path=Path("a.pdf"), rta=RTA.CAMS, file_type=FileType.PDF),
            ScannedFile(path=Path("b.pdf"), rta=RTA.KARVY, file_type=FileType.PDF),
            ScannedFile(path=Path("c.pdf"), rta=RTA.CAMS, file_type=FileType.PDF),
        ]

        assert len(result.cams_files) == 2
        assert len(result.karvy_files) == 1


def test_convenience_function(temp_inbox_with_files):
    """Test scan_mf_inbox convenience function."""
    result = scan_mf_inbox(temp_inbox_with_files)

    assert result.success is True
    assert len(result.files) == 3
