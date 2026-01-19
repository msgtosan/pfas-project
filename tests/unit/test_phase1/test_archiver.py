"""Tests for File Archiver."""

import pytest
from pathlib import Path
from datetime import date
import tempfile

from pfas.services.archiver import FileArchiver, ArchiveResult, archive_processed_files


@pytest.fixture
def temp_dirs():
    """Create temporary inbox and archive directories."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        inbox = base / "inbox"
        archive = base / "archive"

        # Create structure
        (inbox / "Mutual-Fund" / "CAMS").mkdir(parents=True)
        (inbox / "Mutual-Fund" / "KARVY").mkdir(parents=True)
        (archive / "Mutual-Fund" / "CAMS").mkdir(parents=True)
        (archive / "Mutual-Fund" / "KARVY").mkdir(parents=True)

        yield inbox, archive


@pytest.fixture
def temp_dirs_with_files(temp_dirs):
    """Create temp dirs with sample files."""
    inbox, archive = temp_dirs

    # Create test files
    (inbox / "Mutual-Fund" / "CAMS" / "cas_statement.pdf").write_text("CAMS content")
    (inbox / "Mutual-Fund" / "KARVY" / "karvy_report.xlsx").write_text("KARVY content")

    return inbox, archive


class TestFileArchiver:
    """Test suite for FileArchiver."""

    def test_archiver_init(self, temp_dirs):
        """Test archiver initialization."""
        inbox, archive = temp_dirs
        archiver = FileArchiver(inbox, archive, "TestUser")

        assert archiver.inbox_base == inbox
        assert archiver.archive_base == archive
        assert archiver.user_name == "TestUser"

    def test_archive_single_file(self, temp_dirs_with_files):
        """Test archiving a single file."""
        inbox, archive = temp_dirs_with_files
        source_file = inbox / "Mutual-Fund" / "CAMS" / "cas_statement.pdf"

        archiver = FileArchiver(inbox, archive, "Sanjay")
        archived_path = archiver.archive_file(source_file)

        assert archived_path is not None
        assert archived_path.exists()
        assert not source_file.exists()  # Original moved

        # Check naming convention
        today = date.today().isoformat()
        assert today in archived_path.name
        assert "Sanjay" in archived_path.name

    def test_archive_preserves_structure(self, temp_dirs_with_files):
        """Test that archive preserves folder structure."""
        inbox, archive = temp_dirs_with_files
        source_file = inbox / "Mutual-Fund" / "CAMS" / "cas_statement.pdf"

        archiver = FileArchiver(inbox, archive, "Sanjay")
        archived_path = archiver.archive_file(source_file)

        # Should be in archive/Mutual-Fund/CAMS/
        assert "Mutual-Fund" in str(archived_path.parent)
        assert "CAMS" in str(archived_path.parent)

    def test_archive_naming_convention(self, temp_dirs_with_files):
        """Test standardized naming: YYYY-MM-DD_User_Source_Name.ext"""
        inbox, archive = temp_dirs_with_files
        source_file = inbox / "Mutual-Fund" / "CAMS" / "cas_statement.pdf"

        archiver = FileArchiver(inbox, archive, "Sanjay")
        archived_path = archiver.archive_file(source_file, source_type="CAMS")

        name = archived_path.name
        today = date.today().isoformat()

        assert name.startswith(today)
        assert "_Sanjay_" in name
        assert "_CAMS_" in name
        assert name.endswith(".pdf")

    def test_archive_handles_duplicate_names(self, temp_dirs_with_files):
        """Test handling of duplicate archive names."""
        inbox, archive = temp_dirs_with_files

        # Create two files with same name
        file1 = inbox / "Mutual-Fund" / "CAMS" / "statement.pdf"
        file2 = inbox / "Mutual-Fund" / "KARVY" / "statement.pdf"
        file1.write_text("content1")
        file2.write_text("content2")

        archiver = FileArchiver(inbox, archive, "Sanjay")

        # Archive to same destination folder
        archived1 = archiver.archive_file(file1)
        archived2 = archiver.archive_file(file2)

        # Both should exist with unique names
        assert archived1.exists()
        assert archived2.exists()
        assert archived1 != archived2

    def test_archive_nonexistent_file(self, temp_dirs):
        """Test archiving nonexistent file returns None."""
        inbox, archive = temp_dirs
        archiver = FileArchiver(inbox, archive, "TestUser")

        result = archiver.archive_file(Path("/nonexistent/file.pdf"))

        assert result is None

    def test_archive_multiple_files(self, temp_dirs_with_files):
        """Test archiving multiple files."""
        inbox, archive = temp_dirs_with_files

        files = [
            inbox / "Mutual-Fund" / "CAMS" / "cas_statement.pdf",
            inbox / "Mutual-Fund" / "KARVY" / "karvy_report.xlsx",
        ]

        archiver = FileArchiver(inbox, archive, "Sanjay")
        result = archiver.archive_files(files)

        assert result.success is True
        assert result.files_archived == 2
        assert result.files_failed == 0
        assert len(result.archived_paths) == 2

    def test_archive_detects_source_from_path(self, temp_dirs_with_files):
        """Test automatic source detection from path."""
        inbox, archive = temp_dirs_with_files
        source_file = inbox / "Mutual-Fund" / "CAMS" / "cas_statement.pdf"

        archiver = FileArchiver(inbox, archive, "Sanjay")
        archived_path = archiver.archive_file(source_file)

        # Should detect CAMS from path
        assert "_CAMS_" in archived_path.name

    def test_unique_path_generation(self, temp_dirs):
        """Test _get_unique_path method."""
        inbox, archive = temp_dirs
        archiver = FileArchiver(inbox, archive, "TestUser")

        # Create existing file
        test_file = archive / "test.pdf"
        test_file.write_text("existing")

        unique_path = archiver._get_unique_path(test_file)

        assert unique_path != test_file
        assert "test_1.pdf" in str(unique_path)


class TestArchiveResult:
    """Test suite for ArchiveResult."""

    def test_default_success(self):
        """Test default success state."""
        result = ArchiveResult()
        assert result.success is True

    def test_success_with_failures(self):
        """Test success is partial when some failures."""
        result = ArchiveResult()
        result.files_archived = 2
        result.files_failed = 1
        result.success = result.files_archived > 0

        assert result.success is True


def test_convenience_function(temp_dirs_with_files):
    """Test archive_processed_files convenience function."""
    inbox, archive = temp_dirs_with_files

    files = [
        inbox / "Mutual-Fund" / "CAMS" / "cas_statement.pdf",
    ]

    result = archive_processed_files(
        processed_files=files,
        inbox_base=inbox,
        archive_base=archive,
        user_name="Sanjay"
    )

    assert result.success is True
    assert result.files_archived == 1
