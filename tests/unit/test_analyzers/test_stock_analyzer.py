"""
Unit Tests for Stock Analyzer - PFAS

Tests for broker detection, field normalization, XIRR calculation, and capital gains.
"""

import pytest
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
import pandas as pd

from pfas.analyzers.stock_analyzer import (
    StockAnalyzer,
    StockStatementScanner,
    StockFieldNormalizer,
    StockDBIngester,
    BrokerDetector,
    XIRRCalculator,
    NormalizedHolding,
    NormalizedTransaction,
    ScannedFile,
    BrokerType,
    StatementType,
    GainType,
    INDIAN_FY_QUARTERS
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "brokers": {
            "icicidirect": {
                "name": "ICICI Direct",
                "code": "ICICI",
                "folder_patterns": ["ICICIDirect", "ICICI_Direct"],
                "file_patterns": {
                    "holdings": ["*holding*.xlsx"],
                    "transactions": ["*capital_gain*.xlsx", "*cg_report*.xlsx"]
                }
            },
            "zerodha": {
                "name": "Zerodha",
                "code": "ZERODHA",
                "folder_patterns": ["Zerodha", "Kite"],
                "file_patterns": {
                    "holdings": ["*holding*.xlsx"],
                    "transactions": ["*taxpnl*.xlsx", "*pnl*.xlsx"]
                }
            }
        },
        "broker_detection": {
            "keywords": {
                "icicidirect": ["ICICI Direct", "ICICIdirect", "I-Sec"],
                "zerodha": ["Zerodha", "Kite"]
            },
            "column_signatures": {
                "icicidirect": ["Block For Margin", "Price as on 31st Jan 2018"],
                "zerodha": ["Unrealized P&L Pct.", "Quantity Discrepant"]
            }
        },
        "field_mappings": {
            "holdings": {
                "icicidirect": {
                    "Stock Name": "company_name",
                    "Stock ISIN": "isin",
                    "Allocated Quantity": "quantity_held",
                    "Current Market Price": "current_price",
                    "Market Value": "market_value"
                },
                "zerodha": {
                    "Symbol": "symbol",
                    "ISIN": "isin",
                    "Quantity Available": "quantity_held",
                    "Average Price": "average_buy_price",
                    "Previous Closing Price": "current_price",
                    "Unrealized P&L": "unrealized_pnl"
                }
            },
            "transactions": {
                "icicidirect": {
                    "Stock Symbol": "symbol",
                    "ISIN": "isin",
                    "Qty": "quantity",
                    "Sale Date": "sell_date",
                    "Purchase Date": "buy_date",
                    "Sale Value": "sell_value",
                    "Purchase Value": "buy_value",
                    "Profit/Loss(-)": "profit_loss"
                },
                "zerodha": {
                    "Symbol": "symbol",
                    "ISIN": "isin",
                    "Entry Date": "buy_date",
                    "Exit Date": "sell_date",
                    "Quantity": "quantity",
                    "Buy Value": "buy_value",
                    "Sell Value": "sell_value",
                    "Profit": "profit_loss",
                    "Period of Holding": "holding_period_days"
                }
            }
        },
        "capital_gains_rules": {
            "ltcg_threshold_days": 365,
            "ltcg_exemption_limit": 125000,
            "ltcg_tax_rate": 0.125,
            "stcg_tax_rate": 0.20
        },
        "processing": {
            "scan_recursive": True,
            "detect_duplicates": True,
            "duplicate_key": ["user_id", "isin", "buy_date", "sell_date", "quantity"]
        }
    }


@pytest.fixture
def mock_path_resolver(tmp_path):
    """Mock PathResolver for testing."""
    resolver = Mock()
    resolver.root = tmp_path
    resolver.inbox.return_value = tmp_path / "inbox"
    resolver.archive.return_value = tmp_path / "archive"
    resolver.reports.return_value = tmp_path / "reports"
    resolver.db_path.return_value = tmp_path / "db" / "test.db"

    # Create directories
    (tmp_path / "inbox" / "Indian-Stocks").mkdir(parents=True)
    (tmp_path / "reports").mkdir(parents=True)

    return resolver


# =============================================================================
# Broker Detection Tests
# =============================================================================

class TestBrokerDetector:
    """Tests for broker detection logic."""

    def test_detect_from_folder_icicidirect(self, sample_config, tmp_path):
        """Test detection from ICICI Direct folder."""
        detector = BrokerDetector(sample_config)

        icici_folder = tmp_path / "ICICIDirect"
        icici_folder.mkdir()
        file_path = icici_folder / "holdings.xlsx"
        file_path.touch()

        broker, method = detector.detect(file_path)

        assert broker == BrokerType.ICICIDIRECT
        assert "folder" in method

    def test_detect_from_folder_zerodha(self, sample_config, tmp_path):
        """Test detection from Zerodha folder."""
        detector = BrokerDetector(sample_config)

        zerodha_folder = tmp_path / "Zerodha"
        zerodha_folder.mkdir()
        file_path = zerodha_folder / "taxpnl.xlsx"
        file_path.touch()

        broker, method = detector.detect(file_path)

        assert broker == BrokerType.ZERODHA
        assert "folder" in method

    def test_detect_from_filename_keywords(self, sample_config, tmp_path):
        """Test detection from filename keywords."""
        detector = BrokerDetector(sample_config)

        # File with ICICI keyword in name but not in broker folder
        file_path = tmp_path / "ICICI_Direct_holdings_2024.xlsx"
        file_path.touch()

        broker, method = detector.detect(file_path)

        assert broker == BrokerType.ICICIDIRECT
        assert "filename" in method

    def test_detect_from_content_columns(self, sample_config, tmp_path):
        """Test detection from file content (column signatures)."""
        detector = BrokerDetector(sample_config)

        # Create Excel file with Zerodha-specific columns
        file_path = tmp_path / "unknown_statement.xlsx"
        df = pd.DataFrame({
            "Symbol": ["RELIANCE"],
            "ISIN": ["INE002A01018"],
            "Quantity Discrepant": [0],
            "Unrealized P&L Pct.": [5.5]
        })
        df.to_excel(file_path, index=False)

        broker, method = detector.detect(file_path)

        assert broker == BrokerType.ZERODHA
        assert "content" in method

    def test_detect_statement_type_holdings(self, sample_config, tmp_path):
        """Test statement type detection for holdings."""
        detector = BrokerDetector(sample_config)

        file_path = tmp_path / "demat_holding_statement.xlsx"
        file_path.touch()

        stmt_type = detector.detect_statement_type(file_path, BrokerType.ICICIDIRECT)

        assert stmt_type == StatementType.HOLDINGS

    def test_detect_statement_type_transactions(self, sample_config, tmp_path):
        """Test statement type detection for transactions."""
        detector = BrokerDetector(sample_config)

        file_path = tmp_path / "capital_gain_report.xlsx"
        file_path.touch()

        stmt_type = detector.detect_statement_type(file_path, BrokerType.ICICIDIRECT)

        assert stmt_type == StatementType.TRANSACTIONS

    def test_detect_unknown_broker(self, sample_config, tmp_path):
        """Test handling of unknown broker."""
        detector = BrokerDetector(sample_config)

        file_path = tmp_path / "random_statement_xyz.xlsx"
        file_path.touch()

        broker, method = detector.detect(file_path)

        assert broker == BrokerType.UNKNOWN
        assert method == "undetected"


# =============================================================================
# Field Normalization Tests
# =============================================================================

class TestStockFieldNormalizer:
    """Tests for field normalization."""

    def test_normalize_icici_holdings(self, sample_config):
        """Test normalization of ICICI Direct holdings."""
        normalizer = StockFieldNormalizer(sample_config)

        df = pd.DataFrame({
            "Stock Name": ["Reliance Industries Ltd"],
            "Stock ISIN": ["INE002A01018"],
            "Allocated Quantity": [100],
            "Current Market Price": ["₹2,500.50"],
            "Market Value": ["₹2,50,050.00"]
        })

        holdings = normalizer.normalize_holdings(df, BrokerType.ICICIDIRECT, "test.xlsx")

        assert len(holdings) == 1
        h = holdings[0]
        assert h.company_name == "Reliance Industries Ltd"
        assert h.isin == "INE002A01018"
        assert h.quantity_held == 100
        assert h.current_price == Decimal("2500.50")
        assert h.market_value == Decimal("250050.00")

    def test_normalize_zerodha_holdings(self, sample_config):
        """Test normalization of Zerodha holdings."""
        normalizer = StockFieldNormalizer(sample_config)

        df = pd.DataFrame({
            "Symbol": ["RELIANCE"],
            "ISIN": ["INE002A01018"],
            "Quantity Available": [50],
            "Average Price": [2200.00],
            "Previous Closing Price": [2500.50],
            "Unrealized P&L": [15025.00]
        })

        holdings = normalizer.normalize_holdings(df, BrokerType.ZERODHA, "test.xlsx")

        assert len(holdings) == 1
        h = holdings[0]
        assert h.symbol == "RELIANCE"
        assert h.quantity_held == 50
        assert h.average_buy_price == Decimal("2200.00")
        assert h.unrealized_pnl == Decimal("15025.00")

    def test_normalize_zerodha_transactions(self, sample_config):
        """Test normalization of Zerodha transactions."""
        normalizer = StockFieldNormalizer(sample_config)

        df = pd.DataFrame({
            "Symbol": ["INFY"],
            "ISIN": ["INE009A01021"],
            "Entry Date": ["15-Jan-2024"],
            "Exit Date": ["15-Jul-2024"],
            "Quantity": [10],
            "Buy Value": [15000.00],
            "Sell Value": [17500.00],
            "Profit": [2500.00],
            "Period of Holding": [182]
        })

        transactions = normalizer.normalize_transactions(
            df, BrokerType.ZERODHA, "test.xlsx", "2024-25"
        )

        assert len(transactions) == 1
        t = transactions[0]
        assert t.symbol == "INFY"
        assert t.quantity == 10
        assert t.buy_value == Decimal("15000.00")
        assert t.sell_value == Decimal("17500.00")
        assert t.profit_loss == Decimal("2500.00")
        assert t.holding_period_days == 182
        assert t.is_long_term is False  # < 365 days
        assert t.gain_type == GainType.STCG

    def test_ltcg_classification(self, sample_config):
        """Test long-term capital gains classification."""
        normalizer = StockFieldNormalizer(sample_config)

        df = pd.DataFrame({
            "Symbol": ["TCS"],
            "ISIN": ["INE467B01029"],
            "Entry Date": ["01-Jan-2023"],
            "Exit Date": ["15-Jul-2024"],
            "Quantity": [5],
            "Buy Value": [15000.00],
            "Sell Value": [20000.00],
            "Profit": [5000.00],
            "Period of Holding": [561]
        })

        transactions = normalizer.normalize_transactions(
            df, BrokerType.ZERODHA, "test.xlsx", "2024-25"
        )

        assert len(transactions) == 1
        t = transactions[0]
        assert t.holding_period_days == 561
        assert t.is_long_term is True
        assert t.gain_type == GainType.LTCG

    def test_currency_cleaning(self, sample_config):
        """Test currency symbol cleaning."""
        normalizer = StockFieldNormalizer(sample_config)

        # Test various currency formats
        assert normalizer._get_decimal("₹1,234.56") == Decimal("1234.56")
        assert normalizer._get_decimal("Rs. 1,234.56") == Decimal("1234.56")
        assert normalizer._get_decimal("INR 1234.56") == Decimal("1234.56")
        assert normalizer._get_decimal("1,234.56") == Decimal("1234.56")
        assert normalizer._get_decimal("-") == Decimal("0")
        assert normalizer._get_decimal(None) == Decimal("0")

    def test_date_parsing(self, sample_config):
        """Test date parsing for various formats."""
        normalizer = StockFieldNormalizer(sample_config)

        # Test various date formats
        assert normalizer._get_date("15-Jan-2024") == date(2024, 1, 15)
        assert normalizer._get_date("2024-01-15") == date(2024, 1, 15)
        assert normalizer._get_date("15/01/2024") == date(2024, 1, 15)
        assert normalizer._get_date(None) is None

    def test_quarter_determination(self, sample_config):
        """Test Indian FY quarter determination."""
        normalizer = StockFieldNormalizer(sample_config)

        # Q1: Apr 1 - Jun 15
        assert normalizer._determine_quarter(date(2024, 5, 1)) == "Q1"

        # Q2: Jun 16 - Sep 15
        assert normalizer._determine_quarter(date(2024, 7, 15)) == "Q2"

        # Q3: Sep 16 - Dec 15
        assert normalizer._determine_quarter(date(2024, 10, 1)) == "Q3"

        # Q4: Dec 16 - Mar 15
        assert normalizer._determine_quarter(date(2025, 1, 15)) == "Q4"

        # Q5: Mar 16 - Mar 31
        assert normalizer._determine_quarter(date(2025, 3, 20)) == "Q5"

    def test_fy_determination(self, sample_config):
        """Test financial year determination."""
        normalizer = StockFieldNormalizer(sample_config)

        # After April -> current year FY
        assert normalizer._determine_fy(date(2024, 6, 15)) == "2024-25"

        # Before April -> previous year FY
        assert normalizer._determine_fy(date(2025, 2, 15)) == "2024-25"


# =============================================================================
# XIRR Calculation Tests
# =============================================================================

class TestXIRRCalculator:
    """Tests for XIRR calculation."""

    def test_simple_profit_xirr(self):
        """Test XIRR with simple profit scenario."""
        # Invest 10000, get back 11000 after 1 year
        cashflows = [
            (date(2023, 1, 1), Decimal("-10000")),
            (date(2024, 1, 1), Decimal("11000"))
        ]

        xirr = XIRRCalculator.calculate(cashflows)

        assert xirr is not None
        assert abs(xirr - 0.10) < 0.01  # ~10% return

    def test_simple_loss_xirr(self):
        """Test XIRR with loss scenario."""
        # Invest 10000, get back 9000 after 1 year
        cashflows = [
            (date(2023, 1, 1), Decimal("-10000")),
            (date(2024, 1, 1), Decimal("9000"))
        ]

        xirr = XIRRCalculator.calculate(cashflows)

        assert xirr is not None
        assert xirr < 0  # Negative return

    def test_multiple_investments_xirr(self):
        """Test XIRR with multiple investments."""
        cashflows = [
            (date(2023, 1, 1), Decimal("-5000")),
            (date(2023, 7, 1), Decimal("-5000")),
            (date(2024, 1, 1), Decimal("12000"))
        ]

        xirr = XIRRCalculator.calculate(cashflows)

        assert xirr is not None
        assert xirr > 0

    def test_insufficient_data_xirr(self):
        """Test XIRR with insufficient data."""
        # Only one cashflow
        cashflows = [(date(2023, 1, 1), Decimal("-10000"))]

        xirr = XIRRCalculator.calculate(cashflows)

        assert xirr is None

    def test_all_same_sign_xirr(self):
        """Test XIRR when all cashflows have same sign."""
        # All outflows - invalid
        cashflows = [
            (date(2023, 1, 1), Decimal("-10000")),
            (date(2024, 1, 1), Decimal("-5000"))
        ]

        xirr = XIRRCalculator.calculate(cashflows)

        assert xirr is None

    def test_zero_profit_xirr(self):
        """Test XIRR with zero profit."""
        cashflows = [
            (date(2023, 1, 1), Decimal("-10000")),
            (date(2024, 1, 1), Decimal("10000"))
        ]

        xirr = XIRRCalculator.calculate(cashflows)

        assert xirr is not None
        assert abs(xirr) < 0.01  # ~0% return


# =============================================================================
# Scanner Tests
# =============================================================================

class TestStockStatementScanner:
    """Tests for statement scanning."""

    def test_scan_recursive(self, sample_config, tmp_path):
        """Test recursive folder scanning."""
        scanner = StockStatementScanner(sample_config)

        # Create nested structure
        icici_folder = tmp_path / "ICICIDirect"
        icici_folder.mkdir()
        (icici_folder / "holding_report.xlsx").write_text("icici content")

        zerodha_folder = tmp_path / "Zerodha"
        zerodha_folder.mkdir()
        (zerodha_folder / "taxpnl_2024.xlsx").write_text("zerodha content")

        files = scanner.scan(tmp_path, recursive=True)

        assert len(files) == 2
        brokers = {f.broker for f in files}
        assert BrokerType.ICICIDIRECT in brokers
        assert BrokerType.ZERODHA in brokers

    def test_skip_temp_files(self, sample_config, tmp_path):
        """Test that temp files are skipped."""
        scanner = StockStatementScanner(sample_config)

        # Create valid and temp files
        (tmp_path / "holdings.xlsx").write_text("valid content")
        (tmp_path / "~$holdings.xlsx").write_text("temp content")

        files = scanner.scan(tmp_path, recursive=False)

        assert len(files) == 1
        assert "~$" not in files[0].path.name

    def test_skip_duplicates(self, sample_config, tmp_path):
        """Test duplicate file skipping by hash."""
        scanner = StockStatementScanner(sample_config)

        # Create two files with identical content
        content = "identical content"
        (tmp_path / "file1.xlsx").write_text(content)
        (tmp_path / "file2.xlsx").write_text(content)

        files = scanner.scan(tmp_path, recursive=False)

        # Should only have one (duplicate skipped)
        assert len(files) == 1


# =============================================================================
# Capital Gains Rules Tests
# =============================================================================

class TestCapitalGainsRules:
    """Tests for Indian capital gains tax rules."""

    def test_stcg_classification_under_365_days(self, sample_config):
        """Test STCG classification for holding < 365 days."""
        normalizer = StockFieldNormalizer(sample_config)

        df = pd.DataFrame({
            "Symbol": ["INFY"],
            "ISIN": ["INE009A01021"],
            "Entry Date": ["01-Jun-2024"],
            "Exit Date": ["01-Oct-2024"],
            "Quantity": [10],
            "Buy Value": [15000],
            "Sell Value": [17000],
            "Profit": [2000],
            "Period of Holding": [122]
        })

        transactions = normalizer.normalize_transactions(
            df, BrokerType.ZERODHA, "test.xlsx", "2024-25"
        )

        assert transactions[0].gain_type == GainType.STCG
        assert transactions[0].is_long_term is False

    def test_ltcg_classification_over_365_days(self, sample_config):
        """Test LTCG classification for holding >= 365 days."""
        normalizer = StockFieldNormalizer(sample_config)

        df = pd.DataFrame({
            "Symbol": ["TCS"],
            "ISIN": ["INE467B01029"],
            "Entry Date": ["01-Jan-2023"],
            "Exit Date": ["15-Feb-2024"],
            "Quantity": [5],
            "Buy Value": [15000],
            "Sell Value": [20000],
            "Profit": [5000],
            "Period of Holding": [410]
        })

        transactions = normalizer.normalize_transactions(
            df, BrokerType.ZERODHA, "test.xlsx", "2023-24"
        )

        assert transactions[0].gain_type == GainType.LTCG
        assert transactions[0].is_long_term is True

    def test_ltcg_exactly_365_days(self, sample_config):
        """Test LTCG classification for exactly 365 days."""
        normalizer = StockFieldNormalizer(sample_config)

        df = pd.DataFrame({
            "Symbol": ["HDFC"],
            "ISIN": ["INE001A01036"],
            "Entry Date": ["01-Jan-2024"],
            "Exit Date": ["01-Jan-2025"],
            "Quantity": [3],
            "Buy Value": [5000],
            "Sell Value": [6000],
            "Profit": [1000],
            "Period of Holding": [365]
        })

        transactions = normalizer.normalize_transactions(
            df, BrokerType.ZERODHA, "test.xlsx", "2024-25"
        )

        assert transactions[0].gain_type == GainType.LTCG
        assert transactions[0].is_long_term is True


# =============================================================================
# Data Model Tests
# =============================================================================

class TestDataModels:
    """Tests for data models."""

    def test_normalized_holding_to_dict(self):
        """Test NormalizedHolding serialization."""
        holding = NormalizedHolding(
            symbol="RELIANCE",
            isin="INE002A01018",
            company_name="Reliance Industries",
            sector="Energy",
            quantity_held=100,
            average_buy_price=Decimal("2200.50"),
            current_price=Decimal("2500.00"),
            market_value=Decimal("250000.00"),
            unrealized_pnl=Decimal("29950.00"),
            as_of_date=date(2024, 3, 31)
        )

        d = holding.to_dict()

        assert d["symbol"] == "RELIANCE"
        assert d["isin"] == "INE002A01018"
        assert d["quantity_held"] == 100
        assert d["average_buy_price"] == 2200.50
        assert d["as_of_date"] == "2024-03-31"

    def test_normalized_transaction_to_dict(self):
        """Test NormalizedTransaction serialization."""
        txn = NormalizedTransaction(
            symbol="INFY",
            isin="INE009A01021",
            quantity=10,
            buy_date=date(2024, 1, 15),
            sell_date=date(2024, 7, 15),
            holding_period_days=182,
            buy_value=Decimal("15000.00"),
            sell_value=Decimal("17500.00"),
            profit_loss=Decimal("2500.00"),
            is_long_term=False,
            gain_type=GainType.STCG,
            quarter="Q2",
            financial_year="2024-25"
        )

        d = txn.to_dict()

        assert d["symbol"] == "INFY"
        assert d["quantity"] == 10
        assert d["buy_date"] == "2024-01-15"
        assert d["sell_date"] == "2024-07-15"
        assert d["gain_type"] == "STCG"
        assert d["quarter"] == "Q2"

    def test_scanned_file_hash(self, tmp_path):
        """Test ScannedFile hash computation."""
        file_path = tmp_path / "test.xlsx"
        file_path.write_text("test content for hash")

        scanned = ScannedFile(
            path=file_path,
            broker=BrokerType.ZERODHA,
            statement_type=StatementType.HOLDINGS,
            detection_method="test"
        )

        assert scanned.file_hash != ""
        assert len(scanned.file_hash) == 32  # MD5 hex


# =============================================================================
# Integration-style Tests
# =============================================================================

class TestStockAnalyzerIntegration:
    """Integration-style tests for the analyzer."""

    def test_full_analysis_flow(self, sample_config, tmp_path, mock_path_resolver):
        """Test complete analysis workflow with mock data."""
        # Create mock broker folders and files
        zerodha_folder = tmp_path / "inbox" / "Indian-Stocks" / "Zerodha"
        zerodha_folder.mkdir(parents=True)

        # Create holdings file
        holdings_df = pd.DataFrame({
            "Symbol": ["RELIANCE", "TCS"],
            "ISIN": ["INE002A01018", "INE467B01029"],
            "Quantity Available": [100, 50],
            "Average Price": [2200.00, 3500.00],
            "Previous Closing Price": [2500.00, 4000.00],
            "Unrealized P&L": [30000.00, 25000.00]
        })
        holdings_path = zerodha_folder / "zerodha_holding_mar2024.xlsx"
        holdings_df.to_excel(holdings_path, index=False)

        # Create transactions file
        txn_df = pd.DataFrame({
            "Symbol": ["INFY"],
            "ISIN": ["INE009A01021"],
            "Entry Date": ["01-Jan-2024"],
            "Exit Date": ["15-Jul-2024"],
            "Quantity": [10],
            "Buy Value": [15000.00],
            "Sell Value": [17500.00],
            "Profit": [2500.00],
            "Period of Holding": [196]
        })
        txn_path = zerodha_folder / "zerodha_taxpnl_FY2024-25.xlsx"
        txn_df.to_excel(txn_path, index=False)

        # Test scanner
        scanner = StockStatementScanner(sample_config, mock_path_resolver)
        scanned = scanner.scan(zerodha_folder)

        assert len(scanned) == 2

        holdings_files = [f for f in scanned if f.statement_type == StatementType.HOLDINGS]
        txn_files = [f for f in scanned if f.statement_type == StatementType.TRANSACTIONS]

        assert len(holdings_files) == 1
        assert len(txn_files) == 1
        assert holdings_files[0].broker == BrokerType.ZERODHA


# =============================================================================
# Edge Cases
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_dataframe_normalization(self, sample_config):
        """Test handling of empty DataFrame."""
        normalizer = StockFieldNormalizer(sample_config)

        df = pd.DataFrame()
        holdings = normalizer.normalize_holdings(df, BrokerType.ZERODHA, "test.xlsx")

        assert holdings == []

    def test_missing_required_columns(self, sample_config):
        """Test handling of missing required columns."""
        normalizer = StockFieldNormalizer(sample_config)

        # DataFrame missing key columns
        df = pd.DataFrame({
            "RandomColumn1": [1, 2, 3],
            "RandomColumn2": ["a", "b", "c"]
        })

        holdings = normalizer.normalize_holdings(df, BrokerType.ZERODHA, "test.xlsx")

        # Should return empty list (no valid rows)
        assert holdings == []

    def test_zero_quantity_filtered(self, sample_config):
        """Test that zero quantity holdings are filtered."""
        normalizer = StockFieldNormalizer(sample_config)

        df = pd.DataFrame({
            "Symbol": ["RELIANCE", "TCS"],
            "ISIN": ["INE002A01018", "INE467B01029"],
            "Quantity Available": [0, 50],  # First has zero qty
            "Average Price": [2200.00, 3500.00],
            "Previous Closing Price": [2500.00, 4000.00],
            "Unrealized P&L": [0, 25000.00]
        })

        holdings = normalizer.normalize_holdings(df, BrokerType.ZERODHA, "test.xlsx")

        # Should only have TCS (non-zero quantity)
        assert len(holdings) == 1
        assert holdings[0].symbol == "TCS"

    def test_nan_values_handling(self, sample_config):
        """Test handling of NaN values."""
        normalizer = StockFieldNormalizer(sample_config)

        import numpy as np
        df = pd.DataFrame({
            "Symbol": ["RELIANCE"],
            "ISIN": [np.nan],  # NaN ISIN
            "Quantity Available": [100],
            "Average Price": [np.nan],  # NaN price
            "Previous Closing Price": [2500.00],
            "Unrealized P&L": [np.nan]  # NaN P&L
        })

        holdings = normalizer.normalize_holdings(df, BrokerType.ZERODHA, "test.xlsx")

        assert len(holdings) == 1
        h = holdings[0]
        assert h.symbol == "RELIANCE"
        assert h.isin == ""  # NaN converted to empty string
        assert h.average_buy_price == Decimal("0")  # NaN converted to 0
        assert h.unrealized_pnl == Decimal("0")
