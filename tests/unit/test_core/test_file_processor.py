"""
Tests for MultiFileProcessor utility.
"""

import pytest
from pathlib import Path
from datetime import date
from pfas.core.file_processor import (
    MultiFileProcessor,
    sort_files_by_date,
    detect_fy,
    get_fy_from_date
)


class TestFYDetection:
    """Test financial year detection from filenames."""

    @pytest.mark.parametrize("filename,expected_fy", [
        ("EPF_FY2024-25.pdf", "2024-25"),
        ("EPF_FY24-25.pdf", "2024-25"),
        ("Statement_2024-25.xlsx", "2024-25"),
        ("NPS_2024-2025.csv", "2024-25"),
        ("PPF_FY2425.pdf", "2024-25"),
        ("Passbook_2024.pdf", "2024-25"),
        ("EPF_Statement_FY_2023-24.pdf", "2023-24"),
    ])
    def test_detect_fy_from_filename(self, filename, expected_fy, tmp_path):
        """Test FY detection from various filename patterns."""
        file_path = tmp_path / filename
        file_path.write_text("test")

        result = detect_fy(file_path)
        assert result == expected_fy

    def test_detect_fy_unknown(self, tmp_path):
        """Test FY detection returns UNKNOWN for unrecognized patterns."""
        file_path = tmp_path / "random_file.pdf"
        file_path.write_text("test")

        result = detect_fy(file_path)
        assert result == "UNKNOWN"


class TestFYFromDate:
    """Test FY calculation from transaction dates."""

    @pytest.mark.parametrize("txn_date,expected_fy", [
        (date(2024, 4, 1), "2024-25"),   # Start of FY
        (date(2024, 12, 31), "2024-25"), # End of calendar year
        (date(2025, 1, 1), "2024-25"),   # Jan in same FY
        (date(2025, 3, 31), "2024-25"),  # End of FY
        (date(2025, 4, 1), "2025-26"),   # Start of next FY
    ])
    def test_get_fy_from_date(self, txn_date, expected_fy):
        """Test FY calculation from dates."""
        result = get_fy_from_date(txn_date)
        assert result == expected_fy


class TestFileSorting:
    """Test chronological sorting of files."""

    def test_sort_by_fy_in_filename(self, tmp_path):
        """Test sorting files by FY extracted from filename."""
        # Create files with FY in name
        files = []
        for fy in ["2024-25", "2022-23", "2023-24"]:
            f = tmp_path / f"EPF_FY{fy}.pdf"
            f.write_text("test")
            files.append(f)

        sorted_files = sort_files_by_date(files)

        # Should be sorted oldest to newest
        assert "2022-23" in sorted_files[0].name
        assert "2023-24" in sorted_files[1].name
        assert "2024-25" in sorted_files[2].name

    def test_sort_by_fy_reverse(self, tmp_path):
        """Test sorting files newest first."""
        files = []
        for fy in ["2022-23", "2024-25", "2023-24"]:
            f = tmp_path / f"EPF_FY{fy}.pdf"
            f.write_text("test")
            files.append(f)

        sorted_files = sort_files_by_date(files, reverse=True)

        # Should be sorted newest to oldest
        assert "2024-25" in sorted_files[0].name
        assert "2023-24" in sorted_files[1].name
        assert "2022-23" in sorted_files[2].name


class TestGroupByFY:
    """Test grouping files by financial year."""

    def test_group_by_fy(self, tmp_path):
        """Test grouping multiple files by FY."""
        files = []
        # Multiple files for 2024-25
        for i in range(3):
            f = tmp_path / f"Statement_{i}_FY2024-25.pdf"
            f.write_text("test")
            files.append(f)
        # One file for 2023-24
        f = tmp_path / "Statement_FY2023-24.pdf"
        f.write_text("test")
        files.append(f)

        grouped = MultiFileProcessor.group_by_fy(files)

        assert len(grouped["2024-25"]) == 3
        assert len(grouped["2023-24"]) == 1


class TestDeduplication:
    """Test record deduplication."""

    def test_deduplicate_records(self):
        """Test deduplication with key function."""
        records = [
            {"id": 1, "date": "2024-01-01", "amount": 100},
            {"id": 2, "date": "2024-01-02", "amount": 200},
            {"id": 1, "date": "2024-01-01", "amount": 100},  # Duplicate
            {"id": 3, "date": "2024-01-03", "amount": 300},
        ]

        unique = MultiFileProcessor.deduplicate_records(
            records,
            key_func=lambda r: (r["id"], r["date"])
        )

        assert len(unique) == 3
        assert unique[0]["id"] == 1
        assert unique[1]["id"] == 2
        assert unique[2]["id"] == 3


class TestGetLatestFile:
    """Test getting the most recent file."""

    def test_get_latest_file(self, tmp_path):
        """Test finding the most recent file."""
        import time

        # Create files with different timestamps
        f1 = tmp_path / "old_FY2022-23.pdf"
        f1.write_text("old")
        time.sleep(0.1)

        f2 = tmp_path / "new_FY2024-25.pdf"
        f2.write_text("new")

        latest = MultiFileProcessor.get_latest_file(
            tmp_path,
            extensions=['.pdf']
        )

        assert latest is not None
        assert "2024-25" in latest.name

    def test_get_latest_file_no_match(self, tmp_path):
        """Test returns None when no files match."""
        result = MultiFileProcessor.get_latest_file(
            tmp_path,
            extensions=['.pdf']
        )
        assert result is None

    def test_get_latest_file_directory_not_exists(self, tmp_path):
        """Test returns None for non-existent directory."""
        result = MultiFileProcessor.get_latest_file(
            tmp_path / "nonexistent",
            extensions=['.pdf']
        )
        assert result is None
