"""
Tests for Statement Type Detector - Hybrid detection for transactions vs holdings.
"""

import pytest
import json
from pathlib import Path
from pfas.core.statement_detector import (
    StatementTypeDetector,
    StatementType,
    DetectionMethod,
    DetectionResult,
    StatementRulesConfig,
    detect_statement_type,
    create_default_config
)


class TestStatementTypeDetection:
    """Test hybrid statement type detection."""

    @pytest.fixture
    def detector(self):
        """Create detector with default config."""
        return StatementTypeDetector()

    @pytest.fixture
    def custom_config(self):
        """Create custom config for testing."""
        return StatementRulesConfig(
            transactions_keywords=["txn", "trade", "buy", "sell"],
            holdings_keywords=["holding", "portfolio", "snapshot"],
            file_overrides={"special_file.xlsx": "holdings"},
            transactions_folders=["transactions", "trades"],
            holdings_folders=["holdings", "portfolio"]
        )

    # ===== Folder Detection Tests =====

    def test_detect_from_transactions_folder(self, detector, tmp_path):
        """Files in transactions/ folder should be detected as transactions."""
        txn_folder = tmp_path / "inbox" / "Mutual-Fund" / "transactions"
        txn_folder.mkdir(parents=True)
        file_path = txn_folder / "statement.xlsx"
        file_path.write_text("test")

        result = detector.detect(file_path)

        assert result.statement_type == StatementType.TRANSACTIONS
        assert result.detection_method == DetectionMethod.FOLDER
        assert result.confidence == 1.0

    def test_detect_from_holdings_folder(self, detector, tmp_path):
        """Files in holdings/ folder should be detected as holdings."""
        holdings_folder = tmp_path / "inbox" / "Mutual-Fund" / "holdings"
        holdings_folder.mkdir(parents=True)
        file_path = holdings_folder / "portfolio.xlsx"
        file_path.write_text("test")

        result = detector.detect(file_path)

        assert result.statement_type == StatementType.HOLDINGS
        assert result.detection_method == DetectionMethod.FOLDER
        assert result.confidence == 1.0

    def test_detect_from_nested_transactions_folder(self, detector, tmp_path):
        """Files nested inside transactions/ folder should be detected."""
        nested = tmp_path / "inbox" / "transactions" / "CAMS"
        nested.mkdir(parents=True)
        file_path = nested / "cams_statement.xlsx"
        file_path.write_text("test")

        result = detector.detect(file_path)

        assert result.statement_type == StatementType.TRANSACTIONS
        assert result.detection_method == DetectionMethod.FOLDER

    # ===== Filename Detection Tests =====

    @pytest.mark.parametrize("filename,expected_type", [
        ("CAMS_txn_report.xlsx", StatementType.TRANSACTIONS),
        ("trade_history_2024.csv", StatementType.TRANSACTIONS),
        ("buy_sell_report.pdf", StatementType.TRANSACTIONS),
        ("capital_gains_FY2024.xlsx", StatementType.TRANSACTIONS),
        ("dividend_statement.pdf", StatementType.TRANSACTIONS),
        ("holding_summary.xlsx", StatementType.HOLDINGS),
        ("portfolio_valuation.pdf", StatementType.HOLDINGS),
        ("consolidated_statement.xlsx", StatementType.HOLDINGS),
        ("current_value_report.csv", StatementType.HOLDINGS),
    ])
    def test_detect_from_filename_keywords(self, detector, tmp_path, filename, expected_type):
        """Test filename keyword detection for various patterns."""
        file_path = tmp_path / filename
        file_path.write_text("test")

        result = detector.detect(file_path)

        assert result.statement_type == expected_type
        assert result.detection_method == DetectionMethod.FILENAME

    def test_filename_ambiguous_prefers_transactions(self, detector, tmp_path):
        """When filename has both keywords, should use count or default."""
        # More transaction keywords
        file_path = tmp_path / "buy_sell_holding.xlsx"
        file_path.write_text("test")

        result = detector.detect(file_path)

        # Has buy, sell (2 txn) vs holding (1 holding) -> transactions
        assert result.statement_type == StatementType.TRANSACTIONS
        assert "Ambiguous" in str(result.warnings) or result.confidence < 0.8

    # ===== Content Detection Tests =====

    def test_detect_from_excel_content_transactions(self, detector, tmp_path):
        """Test content-based detection for transaction Excel files."""
        import pandas as pd

        file_path = tmp_path / "unknown_statement.xlsx"
        df = pd.DataFrame({
            'Transaction Date': ['2024-01-15', '2024-02-20'],
            'Transaction Type': ['Purchase', 'Redemption'],
            'Units': [100.5, -50.25],
            'Amount': [10000, 5500]
        })
        df.to_excel(file_path, index=False)

        result = detector.detect(file_path)

        # Should detect as transactions from content
        assert result.statement_type == StatementType.TRANSACTIONS
        assert result.detection_method == DetectionMethod.CONTENT

    def test_detect_from_excel_content_holdings(self, detector, tmp_path):
        """Test content-based detection for holdings Excel files."""
        import pandas as pd

        file_path = tmp_path / "unknown_report.xlsx"
        df = pd.DataFrame({
            'Scheme Name': ['HDFC Equity'],
            'Current NAV': [150.50],
            'Market Value': [100000],
            'As On': ['2024-03-31']
        })
        df.to_excel(file_path, index=False)

        result = detector.detect(file_path)

        assert result.statement_type == StatementType.HOLDINGS
        assert result.detection_method == DetectionMethod.CONTENT

    # ===== Config Override Tests =====

    def test_file_override_in_config(self, tmp_path):
        """Test that file-specific overrides in config work."""
        config = StatementRulesConfig(
            file_overrides={"special_file.xlsx": "holdings"}
        )
        detector = StatementTypeDetector(config=config)

        file_path = tmp_path / "special_file.xlsx"
        file_path.write_text("test")

        result = detector.detect(file_path)

        assert result.statement_type == StatementType.HOLDINGS
        assert result.detection_method == DetectionMethod.CONFIG

    def test_custom_keywords_config(self, tmp_path):
        """Test custom keywords in config."""
        config = StatementRulesConfig(
            transactions_keywords=["mycustomtxn"],
            holdings_keywords=["mycustomholding"]
        )
        detector = StatementTypeDetector(config=config)

        txn_file = tmp_path / "report_mycustomtxn.xlsx"
        txn_file.write_text("test")

        result = detector.detect(txn_file)

        assert result.statement_type == StatementType.TRANSACTIONS
        assert "mycustomtxn" in result.matched_keywords

    # ===== Default Fallback Tests =====

    def test_default_fallback_to_transactions(self, detector, tmp_path):
        """Unknown files should default to transactions with warning."""
        file_path = tmp_path / "random_file_12345.xlsx"
        file_path.write_text("test")

        result = detector.detect(file_path)

        assert result.statement_type == StatementType.TRANSACTIONS
        assert result.detection_method == DetectionMethod.DEFAULT
        assert result.confidence < 0.7
        assert len(result.warnings) > 0

    def test_default_configurable(self, tmp_path):
        """Default type should be configurable."""
        config = StatementRulesConfig(default_type="holdings")
        detector = StatementTypeDetector(config=config)

        file_path = tmp_path / "random_xyz_123.xlsx"
        file_path.write_text("test")

        result = detector.detect(file_path)

        assert result.statement_type == StatementType.HOLDINGS
        assert result.detection_method == DetectionMethod.DEFAULT

    # ===== Batch Detection Tests =====

    def test_batch_detection(self, detector, tmp_path):
        """Test detecting multiple files at once."""
        files = []
        for name in ["txn_report.xlsx", "holdings_summary.xlsx", "random.csv"]:
            f = tmp_path / name
            f.write_text("test")
            files.append(f)

        results = detector.detect_batch(files)

        assert len(results) == 3
        assert results[files[0]].statement_type == StatementType.TRANSACTIONS
        assert results[files[1]].statement_type == StatementType.HOLDINGS

    def test_get_transactions_files(self, detector, tmp_path):
        """Test filtering to only transaction files."""
        files = []
        for name in ["txn_report.xlsx", "holdings_summary.xlsx", "trade_history.xlsx"]:
            f = tmp_path / name
            f.write_text("test")
            files.append(f)

        txn_files = detector.get_transactions_files(files)

        assert len(txn_files) == 2
        assert all("txn" in f.name or "trade" in f.name for f in txn_files)

    def test_get_holdings_files(self, detector, tmp_path):
        """Test filtering to only holdings files."""
        files = []
        for name in ["txn_report.xlsx", "holdings_summary.xlsx", "portfolio_snapshot.xlsx"]:
            f = tmp_path / name
            f.write_text("test")
            files.append(f)

        holdings_files = detector.get_holdings_files(files)

        assert len(holdings_files) == 2


class TestStatementRulesConfig:
    """Test config loading and merging."""

    def test_default_config_creation(self):
        """Test default config has expected values."""
        config = StatementRulesConfig()

        assert "txn" in config.transactions_keywords
        assert "holding" in config.holdings_keywords
        assert config.default_type == "transactions"
        assert config.min_content_confidence == 0.6

    def test_config_from_dict(self):
        """Test creating config from dictionary."""
        data = {
            "transactions_keywords": ["custom_txn"],
            "holdings_keywords": ["custom_holding"],
            "default_type": "holdings"
        }

        config = StatementRulesConfig.from_dict(data)

        assert config.transactions_keywords == ["custom_txn"]
        assert config.holdings_keywords == ["custom_holding"]
        assert config.default_type == "holdings"

    def test_config_merge(self):
        """Test merging two configs."""
        base = StatementRulesConfig(
            transactions_keywords=["txn", "trade"],
            holdings_keywords=["holding"],
            file_overrides={"a.xlsx": "transactions"}
        )
        override = StatementRulesConfig(
            transactions_keywords=["custom_txn"],
            file_overrides={"b.xlsx": "holdings"}
        )

        merged = base.merge_with(override)

        # Keywords should be combined
        assert "txn" in merged.transactions_keywords
        assert "custom_txn" in merged.transactions_keywords
        # File overrides should be merged
        assert merged.file_overrides["a.xlsx"] == "transactions"
        assert merged.file_overrides["b.xlsx"] == "holdings"

    def test_load_config_from_file(self, tmp_path):
        """Test loading config from JSON file."""
        config_data = {
            "transactions_keywords": ["file_txn"],
            "holdings_keywords": ["file_holding"]
        }

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_file = config_dir / "statement_rules.json"
        config_file.write_text(json.dumps(config_data))

        detector = StatementTypeDetector(project_root=tmp_path)

        assert "file_txn" in detector.config.transactions_keywords

    def test_create_default_config_dict(self):
        """Test creating default config as dict for saving."""
        config_dict = create_default_config()

        assert "transactions_keywords" in config_dict
        assert "holdings_keywords" in config_dict
        assert "default_type" in config_dict
        assert isinstance(config_dict["transactions_keywords"], list)


class TestDetectionPriority:
    """Test that detection priority order is correct."""

    def test_folder_beats_filename(self, tmp_path):
        """Folder detection should take priority over filename."""
        # File has holdings keyword but is in transactions folder
        txn_folder = tmp_path / "transactions"
        txn_folder.mkdir()
        file_path = txn_folder / "holdings_report.xlsx"
        file_path.write_text("test")

        detector = StatementTypeDetector()
        result = detector.detect(file_path)

        assert result.statement_type == StatementType.TRANSACTIONS
        assert result.detection_method == DetectionMethod.FOLDER

    def test_config_override_beats_all(self, tmp_path):
        """Config file override should beat everything."""
        # File in transactions folder with txn keyword, but overridden in config
        txn_folder = tmp_path / "transactions"
        txn_folder.mkdir()
        file_path = txn_folder / "txn_report.xlsx"
        file_path.write_text("test")

        config = StatementRulesConfig(
            file_overrides={"txn_report.xlsx": "holdings"}
        )
        detector = StatementTypeDetector(config=config)
        result = detector.detect(file_path)

        assert result.statement_type == StatementType.HOLDINGS
        assert result.detection_method == DetectionMethod.CONFIG

    def test_filename_beats_content(self, tmp_path):
        """Filename detection should take priority over content."""
        import pandas as pd

        # File with txn filename but holdings content
        file_path = tmp_path / "txn_statement.xlsx"
        df = pd.DataFrame({
            'Current NAV': [150.50],
            'Market Value': [100000],
            'As On': ['2024-03-31']  # Holdings keywords in content
        })
        df.to_excel(file_path, index=False)

        detector = StatementTypeDetector()
        result = detector.detect(file_path)

        assert result.statement_type == StatementType.TRANSACTIONS
        assert result.detection_method == DetectionMethod.FILENAME
