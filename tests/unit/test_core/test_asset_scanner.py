"""
Tests for Universal Asset Scanner with hybrid statement detection.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, MagicMock
from pfas.core.asset_scanner import (
    AssetScanner,
    AssetScanResult,
    ScannedAssetFile,
    AssetType,
    scan_asset_inbox,
    DUAL_STATEMENT_ASSETS,
    TRANSACTION_ONLY_ASSETS
)
from pfas.core.statement_detector import StatementType, DetectionMethod


class TestAssetScanner:
    """Test universal asset scanner."""

    @pytest.fixture
    def mock_resolver(self, tmp_path):
        """Create mock PathResolver."""
        resolver = Mock()
        resolver.root = tmp_path
        resolver.inbox.return_value = tmp_path / "inbox"
        resolver.archive.return_value = tmp_path / "archive"
        resolver.user_config_dir.return_value = tmp_path / "config"

        # Create inbox structure
        (tmp_path / "inbox").mkdir()
        (tmp_path / "config").mkdir()

        return resolver

    @pytest.fixture
    def scanner(self, mock_resolver):
        """Create scanner with mock resolver."""
        return AssetScanner(mock_resolver)

    # ===== Basic Scanning Tests =====

    def test_scan_empty_asset_folder(self, scanner, mock_resolver):
        """Scanning empty asset folder should return empty result."""
        result = scanner.scan_asset("Mutual-Fund")

        assert result.success
        assert len(result.files) == 0
        assert len(result.warnings) > 0  # Warning about no folder

    def test_scan_flat_folder_structure(self, scanner, mock_resolver, tmp_path):
        """Test scanning flat folder (no transactions/holdings subfolders)."""
        mf_folder = tmp_path / "inbox" / "Mutual-Fund"
        mf_folder.mkdir(parents=True)

        # Create test files with unique content to avoid duplicate hash
        (mf_folder / "txn_report.xlsx").write_text("transaction content 1")
        (mf_folder / "holdings_summary.xlsx").write_text("holdings content 2")

        result = scanner.scan_asset(AssetType.MUTUAL_FUND)

        assert result.success
        assert len(result.files) == 2
        assert len(result.transaction_files) == 1
        assert len(result.holdings_files) == 1

    def test_scan_with_subfolders(self, scanner, mock_resolver, tmp_path):
        """Test scanning with transactions/ and holdings/ subfolders."""
        mf_folder = tmp_path / "inbox" / "Mutual-Fund"
        txn_folder = mf_folder / "transactions"
        holdings_folder = mf_folder / "holdings"

        txn_folder.mkdir(parents=True)
        holdings_folder.mkdir(parents=True)

        # Files in appropriate subfolders with unique content
        (txn_folder / "statement1.xlsx").write_text("txn content 1")
        (txn_folder / "statement2.xlsx").write_text("txn content 2")
        (holdings_folder / "portfolio.xlsx").write_text("holdings content 3")

        result = scanner.scan_asset(AssetType.MUTUAL_FUND)

        assert result.success
        assert len(result.transaction_files) == 2
        assert len(result.holdings_files) == 1

    def test_scan_mixed_structure(self, scanner, mock_resolver, tmp_path):
        """Test scanning with both subfolders and flat files."""
        mf_folder = tmp_path / "inbox" / "Mutual-Fund"
        txn_folder = mf_folder / "transactions"

        mf_folder.mkdir(parents=True)
        txn_folder.mkdir()

        # File in transactions subfolder with unique content
        (txn_folder / "explicit_txn.xlsx").write_text("explicit txn content")
        # File in flat folder with txn keyword
        (mf_folder / "auto_txn_report.xlsx").write_text("auto txn content")
        # File in flat folder with holdings keyword
        (mf_folder / "auto_holdings.xlsx").write_text("auto holdings content")

        result = scanner.scan_asset(AssetType.MUTUAL_FUND)

        assert result.success
        assert len(result.transaction_files) == 2
        assert len(result.holdings_files) == 1

    # ===== Multi-File Handling Tests =====

    def test_files_sorted_by_date(self, scanner, mock_resolver, tmp_path):
        """Test that files are sorted chronologically."""
        mf_folder = tmp_path / "inbox" / "Mutual-Fund"
        mf_folder.mkdir(parents=True)

        # Create files with FY in name and unique content
        for i, fy in enumerate(["2022-23", "2024-25", "2023-24"]):
            (mf_folder / f"txn_FY{fy}.xlsx").write_text(f"content for FY {fy} - {i}")

        result = scanner.scan_asset(AssetType.MUTUAL_FUND)
        sorted_files = result.get_sorted_files()

        # Should be oldest first
        assert "2022-23" in sorted_files[0].path.name
        assert "2023-24" in sorted_files[1].path.name
        assert "2024-25" in sorted_files[2].path.name

    def test_group_by_financial_year(self, scanner, mock_resolver, tmp_path):
        """Test grouping files by FY."""
        mf_folder = tmp_path / "inbox" / "Mutual-Fund"
        mf_folder.mkdir(parents=True)

        # Create files for different FYs with unique content
        (mf_folder / "txn_FY2024-25_1.xlsx").write_text("FY2024 file 1")
        (mf_folder / "txn_FY2024-25_2.xlsx").write_text("FY2024 file 2")
        (mf_folder / "txn_FY2023-24.xlsx").write_text("FY2023 file")

        result = scanner.scan_asset(AssetType.MUTUAL_FUND)
        by_fy = result.by_financial_year

        assert "2024-25" in by_fy
        assert len(by_fy["2024-25"]) == 2
        assert "2023-24" in by_fy
        assert len(by_fy["2023-24"]) == 1

    def test_duplicate_files_skipped(self, scanner, mock_resolver, tmp_path):
        """Test that duplicate files (same hash) are skipped."""
        mf_folder = tmp_path / "inbox" / "Mutual-Fund"
        txn_folder = mf_folder / "transactions"

        mf_folder.mkdir(parents=True)
        txn_folder.mkdir()

        # Same content in two locations
        content = "identical content for hash test"
        (mf_folder / "file1.xlsx").write_text(content)
        (txn_folder / "file2.xlsx").write_text(content)

        result = scanner.scan_asset(AssetType.MUTUAL_FUND)

        # Should only have one file (duplicate skipped)
        assert len(result.files) == 1
        assert any("Duplicate" in w for w in result.warnings)

    # ===== Asset Type Tests =====

    @pytest.mark.parametrize("asset_type", [
        "Mutual-Fund", "Indian-Stocks", "USA-Stocks",
        "PPF", "NPS", "Bank"
    ])
    def test_scan_various_asset_types(self, scanner, mock_resolver, tmp_path, asset_type):
        """Test scanning various asset types."""
        asset_folder = tmp_path / "inbox" / asset_type
        asset_folder.mkdir(parents=True)

        (asset_folder / "statement.xlsx").write_text(f"content for {asset_type}")

        result = scanner.scan_asset(asset_type)

        assert result.success
        assert len(result.files) == 1

    def test_scan_epf_asset(self, scanner, mock_resolver, tmp_path):
        """Test scanning EPF (PDF only)."""
        epf_folder = tmp_path / "inbox" / "EPF"
        epf_folder.mkdir(parents=True)

        # EPF only supports PDF
        (epf_folder / "passbook.pdf").write_bytes(b"%PDF-1.4 test content")

        result = scanner.scan_asset("EPF")

        assert result.success
        assert len(result.files) == 1

    def test_scan_all_assets(self, scanner, mock_resolver, tmp_path):
        """Test scanning all asset types at once."""
        # Create folders for a few asset types with unique content
        for i, asset in enumerate(["Mutual-Fund", "Bank"]):
            folder = tmp_path / "inbox" / asset
            folder.mkdir(parents=True)
            (folder / f"{asset.lower()}_stmt.xlsx").write_text(f"content {i} for {asset}")

        # EPF needs PDF
        epf_folder = tmp_path / "inbox" / "EPF"
        epf_folder.mkdir(parents=True)
        (epf_folder / "epf_stmt.pdf").write_bytes(b"%PDF-1.4 EPF content")

        results = scanner.scan_all_assets()

        assert AssetType.MUTUAL_FUND in results
        assert AssetType.BANK in results
        assert AssetType.EPF in results
        assert len(results) == 3

    # ===== Archive Scanning Tests =====

    def test_scan_with_archive(self, mock_resolver, tmp_path):
        """Test scanning both inbox and archive."""
        # Create inbox and archive folders
        inbox_mf = tmp_path / "inbox" / "Mutual-Fund"
        archive_mf = tmp_path / "archive" / "Mutual-Fund"

        inbox_mf.mkdir(parents=True)
        archive_mf.mkdir(parents=True)

        (inbox_mf / "new_txn.xlsx").write_text("inbox content")
        (archive_mf / "old_txn.xlsx").write_text("archive content")

        scanner = AssetScanner(mock_resolver, include_archive=True)
        result = scanner.scan_asset(AssetType.MUTUAL_FUND)

        assert len(result.files) == 2

    # ===== Subfolder Scanning Tests =====

    def test_scan_specific_subfolders(self, scanner, mock_resolver, tmp_path):
        """Test scanning specific RTA subfolders (CAMS, KARVY)."""
        mf_folder = tmp_path / "inbox" / "Mutual-Fund"
        cams_folder = mf_folder / "CAMS"
        karvy_folder = mf_folder / "KARVY"

        cams_folder.mkdir(parents=True)
        karvy_folder.mkdir(parents=True)

        (cams_folder / "cams_txn.xlsx").write_text("CAMS txn content")
        (karvy_folder / "karvy_txn.xlsx").write_text("KARVY txn content")
        (mf_folder / "other_file.xlsx").write_text("other content")

        result = scanner.scan_asset(
            AssetType.MUTUAL_FUND,
            subfolders=["CAMS", "KARVY"]
        )

        # Should only find files in specified subfolders
        assert len(result.files) == 2

    # ===== Failed Folder Tests =====

    def test_move_to_failed(self, scanner, mock_resolver, tmp_path):
        """Test moving failed file to failed/ subfolder."""
        mf_folder = tmp_path / "inbox" / "Mutual-Fund"
        mf_folder.mkdir(parents=True)

        file_path = mf_folder / "bad_file.xlsx"
        file_path.write_text("test")

        new_path = scanner.move_to_failed(file_path)

        assert new_path is not None
        assert new_path.parent.name == "failed"
        assert not file_path.exists()
        assert new_path.exists()

    def test_failed_folder_excluded_from_scan(self, scanner, mock_resolver, tmp_path):
        """Test that failed/ folder is excluded from scanning."""
        mf_folder = tmp_path / "inbox" / "Mutual-Fund"
        failed_folder = mf_folder / "failed"

        mf_folder.mkdir(parents=True)
        failed_folder.mkdir()

        (mf_folder / "good_file.xlsx").write_text("test")
        (failed_folder / "bad_file.xlsx").write_text("test")

        result = scanner.scan_asset(AssetType.MUTUAL_FUND)

        # Should only find the good file
        assert len(result.files) == 1
        assert "good_file" in result.files[0].path.name


class TestScannedAssetFile:
    """Test ScannedAssetFile dataclass."""

    def test_file_hash_calculated(self, tmp_path):
        """Test that file hash is calculated on creation."""
        from pfas.core.statement_detector import DetectionResult, DetectionMethod

        file_path = tmp_path / "test.xlsx"
        file_path.write_text("test content")

        detection = DetectionResult(
            statement_type=StatementType.TRANSACTIONS,
            detection_method=DetectionMethod.FILENAME
        )

        scanned = ScannedAssetFile(
            path=file_path,
            asset_type=AssetType.MUTUAL_FUND,
            statement_type=StatementType.TRANSACTIONS,
            detection_result=detection
        )

        assert scanned.file_hash != ""
        assert scanned.size_bytes > 0

    def test_financial_year_detected(self, tmp_path):
        """Test that FY is detected from filename."""
        from pfas.core.statement_detector import DetectionResult, DetectionMethod

        file_path = tmp_path / "statement_FY2024-25.xlsx"
        file_path.write_text("test")

        detection = DetectionResult(
            statement_type=StatementType.TRANSACTIONS,
            detection_method=DetectionMethod.FILENAME
        )

        scanned = ScannedAssetFile(
            path=file_path,
            asset_type=AssetType.MUTUAL_FUND,
            statement_type=StatementType.TRANSACTIONS,
            detection_result=detection
        )

        assert scanned.financial_year == "2024-25"

    def test_is_transaction_property(self, tmp_path):
        """Test is_transaction property."""
        from pfas.core.statement_detector import DetectionResult, DetectionMethod

        file_path = tmp_path / "test.xlsx"
        file_path.write_text("test")

        detection = DetectionResult(
            statement_type=StatementType.TRANSACTIONS,
            detection_method=DetectionMethod.FILENAME
        )

        scanned = ScannedAssetFile(
            path=file_path,
            asset_type=AssetType.MUTUAL_FUND,
            statement_type=StatementType.TRANSACTIONS,
            detection_result=detection
        )

        assert scanned.is_transaction is True
        assert scanned.is_holding is False


class TestConvenienceFunction:
    """Test convenience functions."""

    def test_scan_asset_inbox_function(self, tmp_path):
        """Test scan_asset_inbox convenience function."""
        resolver = Mock()
        resolver.root = tmp_path
        resolver.inbox.return_value = tmp_path / "inbox"
        resolver.archive.return_value = tmp_path / "archive"
        resolver.user_config_dir.return_value = tmp_path / "config"

        (tmp_path / "inbox" / "Mutual-Fund").mkdir(parents=True)
        (tmp_path / "inbox" / "Mutual-Fund" / "test.xlsx").write_text("test")
        (tmp_path / "config").mkdir()

        result = scan_asset_inbox(resolver, "Mutual-Fund")

        assert result.success
        assert len(result.files) == 1
