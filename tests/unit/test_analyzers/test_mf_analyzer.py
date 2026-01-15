"""
Unit tests for MF Analyzer module.

Tests:
1. Field normalization (CAMS -> common, Karvy -> common)
2. Duplicate prevention in DB ingestion
3. Classification fallback for unknown schemes
4. Decimal precision handling
5. RTA detection from filenames
6. Date extraction from filenames
"""

import pytest
import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from pfas.analyzers.mf_analyzer import (
    MFStatementScanner,
    MFFieldNormalizer,
    MFDBIngester,
    NormalizedHolding,
    RTA,
    StatementType,
    ScannedFile,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def db_connection():
    """Create in-memory database with schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create necessary tables
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pan_encrypted BLOB NOT NULL,
            pan_salt BLOB NOT NULL,
            name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS mf_holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            folio_id INTEGER,
            amc_name TEXT NOT NULL,
            scheme_name TEXT NOT NULL,
            scheme_type TEXT,
            folio_number TEXT NOT NULL,
            investor_name TEXT,
            units DECIMAL(15,4) NOT NULL,
            nav DECIMAL(15,4),
            nav_date DATE NOT NULL,
            current_value DECIMAL(15,2) NOT NULL,
            cost_value DECIMAL(15,2),
            appreciation DECIMAL(15,2),
            average_holding_days INTEGER,
            annualized_return DECIMAL(8,4),
            dividend_payout DECIMAL(15,2) DEFAULT 0,
            dividend_reinvest DECIMAL(15,2) DEFAULT 0,
            isin TEXT,
            rta TEXT NOT NULL,
            source_file TEXT,
            statement_date DATE,
            UNIQUE(user_id, folio_number, scheme_name, nav_date)
        );

        CREATE TABLE IF NOT EXISTS mf_holdings_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            snapshot_date DATE NOT NULL,
            total_value DECIMAL(15,2) NOT NULL,
            total_cost DECIMAL(15,2),
            total_appreciation DECIMAL(15,2),
            equity_value DECIMAL(15,2) DEFAULT 0,
            debt_value DECIMAL(15,2) DEFAULT 0,
            hybrid_value DECIMAL(15,2) DEFAULT 0,
            weighted_xirr DECIMAL(8,4),
            scheme_count INTEGER,
            folio_count INTEGER,
            source_file TEXT,
            UNIQUE(user_id, snapshot_date)
        );

        INSERT INTO users (pan_encrypted, pan_salt, name) VALUES (x'00', x'00', 'TestUser');
    """)

    return conn


@pytest.fixture
def normalizer():
    """Create field normalizer."""
    return MFFieldNormalizer()


@pytest.fixture
def scanner():
    """Create statement scanner with default config."""
    config = {
        "file_patterns": {
            "cams": {"patterns": ["*CAMS*", "*cams*"]},
            "karvy": {"patterns": ["*Karvy*", "*KFintech*"]}
        }
    }
    return MFStatementScanner(config)


# ============================================================================
# Test 1: CAMS Field Normalization
# ============================================================================

class TestCAMSNormalization:
    """Test CAMS field normalization to common schema."""

    def test_cams_holdings_normalization(self, normalizer):
        """Test normalizing CAMS holdings DataFrame."""
        # Create sample CAMS-style DataFrame
        df = pd.DataFrame({
            "AMCName": ["SBI Mutual Fund"],
            "Scheme": ["SBI Bluechip Fund Direct Growth ISIN : INF200K01234"],
            "Type": ["EQUITY"],
            "Folio": ["12345678/90"],
            "InvestorName": ["Test Investor"],
            "UnitBal": [100.5],
            "NAVDate": ["31-Mar-2024"],
            "CurrentValue": [150000.50],
            "CostValue": [120000.00],
            "Appreciation": [30000.50],
            "WtgAvg": [365],
            "Annualised XIRR": [12.5],
        })

        holdings = normalizer.normalize_holdings(df, RTA.CAMS)

        assert len(holdings) == 1
        h = holdings[0]
        assert h.amc_name == "SBI Mutual Fund"
        assert "SBI Bluechip" in h.scheme_name
        assert h.scheme_type == "EQUITY"
        assert h.folio_number == "12345678/90"
        assert h.units == Decimal("100.5")
        assert h.current_value == Decimal("150000.50")
        assert h.cost_value == Decimal("120000.00")
        assert h.appreciation == Decimal("30000.50")
        assert h.annualized_return == Decimal("12.5")
        assert h.rta == RTA.CAMS
        assert h.isin == "INF200K01234"

    def test_cams_currency_cleaning(self, normalizer):
        """Test that currency symbols are cleaned properly."""
        df = pd.DataFrame({
            "Scheme Name": ["Test Fund"],
            "Folio No": ["123"],
            "Units": [100],
            "NAV Date": ["2024-03-31"],
            "Current Value": ["Rs. 1,50,000.00"],
            "Cost Value": ["Rs.1,20,000"],
        })

        holdings = normalizer.normalize_holdings(df, RTA.CAMS)

        assert len(holdings) == 1
        assert holdings[0].current_value == Decimal("150000.00")
        assert holdings[0].cost_value == Decimal("120000")


# ============================================================================
# Test 2: Karvy Field Normalization
# ============================================================================

class TestKarvyNormalization:
    """Test Karvy/KFintech field normalization to common schema."""

    def test_karvy_holdings_normalization(self, normalizer):
        """Test normalizing Karvy holdings DataFrame."""
        df = pd.DataFrame({
            " Fund Name": ["HDFC Mutual Fund"],
            "Scheme Name": ["HDFC Top 100 Fund Direct ( INF179K01234)"],
            "Folio Number": ["9876543210"],
            "Investor Name": ["Test Investor"],
            "Unit Balance": [200.25],
            "Nav Date": ["31-Mar-2024"],
            "Current Value (Rs.)": ["Rs. 2,50,000.75"],
            "Cost Value (Rs.)": ["Rs. 2,00,000.00"],
            "Appreciation (Rs.)": ["Rs. 50,000.75"],
            "AvgAgeDays": [730],
            "Annualized Yield (%)": ["15.5%"],
            "Dividend Payout": [1000],
            "Dividend Re-Invest": [500],
        })

        holdings = normalizer.normalize_holdings(df, RTA.KFINTECH)

        assert len(holdings) == 1
        h = holdings[0]
        assert h.amc_name == "HDFC Mutual Fund"
        assert "HDFC Top 100" in h.scheme_name
        assert h.folio_number == "9876543210"
        assert h.units == Decimal("200.25")
        assert h.current_value == Decimal("250000.75")
        assert h.cost_value == Decimal("200000.00")
        assert h.appreciation == Decimal("50000.75")
        assert h.annualized_return == Decimal("15.5")
        assert h.dividend_payout == Decimal("1000")
        assert h.dividend_reinvest == Decimal("500")
        assert h.rta == RTA.KFINTECH
        assert h.isin == "INF179K01234"

    def test_karvy_isin_extraction(self, normalizer):
        """Test ISIN extraction from Karvy scheme names."""
        # Karvy format: "Scheme Name ( ISIN)"
        df = pd.DataFrame({
            "Scheme Name": ["Axis Bluechip Fund ( INF846K01234)"],
            "Folio": ["123"],
            "Units": [100],
            "Nav Date": ["2024-03-31"],
            "Current Value": [100000],
        })

        holdings = normalizer.normalize_holdings(df, RTA.KFINTECH)
        assert holdings[0].isin == "INF846K01234"


# ============================================================================
# Test 3: Duplicate Prevention
# ============================================================================

class TestDuplicatePrevention:
    """Test idempotent database ingestion."""

    def test_duplicate_holdings_skipped(self, db_connection):
        """Test that duplicate holdings are skipped on re-ingestion."""
        ingester = MFDBIngester(db_connection)

        holding = NormalizedHolding(
            amc_name="Test AMC",
            scheme_name="Test Scheme",
            scheme_type="EQUITY",
            folio_number="123456",
            investor_name="Test",
            units=Decimal("100"),
            nav_date=date(2024, 3, 31),
            current_value=Decimal("150000"),
            cost_value=Decimal("120000"),
            appreciation=Decimal("30000"),
            average_holding_days=365,
            annualized_return=Decimal("12.5"),
            rta=RTA.CAMS,
        )

        # First ingestion
        inserted, updated, skipped = ingester.ingest_holdings([holding], user_id=1)
        assert inserted == 1
        assert skipped == 0

        # Second ingestion - same data should be skipped
        inserted2, updated2, skipped2 = ingester.ingest_holdings([holding], user_id=1)
        assert inserted2 == 0
        assert skipped2 == 1

    def test_updated_holdings_updated(self, db_connection):
        """Test that holdings with changed values are updated."""
        ingester = MFDBIngester(db_connection)

        holding1 = NormalizedHolding(
            amc_name="Test AMC",
            scheme_name="Test Scheme",
            scheme_type="EQUITY",
            folio_number="123456",
            investor_name="Test",
            units=Decimal("100"),
            nav_date=date(2024, 3, 31),
            current_value=Decimal("150000"),
            cost_value=Decimal("120000"),
            appreciation=Decimal("30000"),
            average_holding_days=365,
            annualized_return=Decimal("12.5"),
            rta=RTA.CAMS,
        )

        # First ingestion
        ingester.ingest_holdings([holding1], user_id=1)

        # Second ingestion with updated value
        holding2 = NormalizedHolding(
            amc_name="Test AMC",
            scheme_name="Test Scheme",
            scheme_type="EQUITY",
            folio_number="123456",
            investor_name="Test",
            units=Decimal("100"),
            nav_date=date(2024, 3, 31),
            current_value=Decimal("160000"),  # Changed
            cost_value=Decimal("120000"),
            appreciation=Decimal("40000"),  # Changed
            average_holding_days=365,
            annualized_return=Decimal("15.0"),  # Changed
            rta=RTA.CAMS,
        )

        inserted, updated, skipped = ingester.ingest_holdings([holding2], user_id=1)
        assert inserted == 0
        assert updated == 1
        assert skipped == 0

        # Verify updated value
        cursor = db_connection.execute("SELECT current_value FROM mf_holdings WHERE id = 1")
        row = cursor.fetchone()
        assert Decimal(str(row[0])) == Decimal("160000")


# ============================================================================
# Test 4: Classification Fallback
# ============================================================================

class TestClassificationFallback:
    """Test scheme classification when type is not provided."""

    def test_equity_classification(self, normalizer):
        """Test equity fund classification from scheme name."""
        df = pd.DataFrame({
            "Scheme Name": ["SBI Bluechip Fund Direct Growth"],
            "Folio": ["123"],
            "Units": [100],
            "Nav Date": ["2024-03-31"],
            "Current Value": [100000],
        })

        holdings = normalizer.normalize_holdings(df, RTA.CAMS)
        assert holdings[0].scheme_type == "EQUITY"

    def test_debt_classification(self, normalizer):
        """Test debt fund classification from scheme name."""
        df = pd.DataFrame({
            "Scheme Name": ["HDFC Corporate Bond Fund Direct Growth"],
            "Folio": ["123"],
            "Units": [100],
            "Nav Date": ["2024-03-31"],
            "Current Value": [100000],
        })

        holdings = normalizer.normalize_holdings(df, RTA.CAMS)
        assert holdings[0].scheme_type == "DEBT"

    def test_hybrid_classification(self, normalizer):
        """Test hybrid fund classification from scheme name."""
        df = pd.DataFrame({
            "Scheme Name": ["ICICI Prudential Balanced Advantage Fund"],
            "Folio": ["123"],
            "Units": [100],
            "Nav Date": ["2024-03-31"],
            "Current Value": [100000],
        })

        holdings = normalizer.normalize_holdings(df, RTA.CAMS)
        assert holdings[0].scheme_type == "HYBRID"

    def test_elss_classification(self, normalizer):
        """Test ELSS (Tax Saver) fund classification."""
        df = pd.DataFrame({
            "Scheme Name": ["Axis Long Term Equity Fund - ELSS"],
            "Folio": ["123"],
            "Units": [100],
            "Nav Date": ["2024-03-31"],
            "Current Value": [100000],
        })

        holdings = normalizer.normalize_holdings(df, RTA.CAMS)
        assert holdings[0].scheme_type == "EQUITY"


# ============================================================================
# Test 5: Decimal Precision
# ============================================================================

class TestDecimalPrecision:
    """Test decimal precision handling."""

    def test_units_precision(self, normalizer):
        """Test that units maintain 4 decimal places."""
        df = pd.DataFrame({
            "Scheme Name": ["Test Fund"],
            "Folio": ["123"],
            "Units": [100.1234],
            "Nav Date": ["2024-03-31"],
            "Current Value": [100000],
        })

        holdings = normalizer.normalize_holdings(df, RTA.CAMS)
        assert holdings[0].units == Decimal("100.1234")

    def test_currency_precision(self, normalizer):
        """Test that currency values maintain 2 decimal places."""
        df = pd.DataFrame({
            "Scheme Name": ["Test Fund"],
            "Folio": ["123"],
            "Units": [100],
            "Nav Date": ["2024-03-31"],
            "Current Value": ["150000.99"],
            "Cost Value": ["120000.50"],
        })

        holdings = normalizer.normalize_holdings(df, RTA.CAMS)
        assert holdings[0].current_value == Decimal("150000.99")
        assert holdings[0].cost_value == Decimal("120000.50")

    def test_xirr_precision(self, normalizer):
        """Test that XIRR/return maintains precision."""
        df = pd.DataFrame({
            "Scheme Name": ["Test Fund"],
            "Folio": ["123"],
            "Units": [100],
            "Nav Date": ["2024-03-31"],
            "Current Value": [100000],
            "Annualised XIRR": [12.345],
        })

        holdings = normalizer.normalize_holdings(df, RTA.CAMS)
        assert holdings[0].annualized_return == Decimal("12.345")


# ============================================================================
# Test 6: RTA Detection
# ============================================================================

class TestRTADetection:
    """Test RTA detection from filenames."""

    def test_cams_detection(self, scanner):
        """Test CAMS file detection."""
        assert scanner._detect_rta(Path("Sanjay_CAMS_CG_FY2024-25.xlsx")) == RTA.CAMS
        assert scanner._detect_rta(Path("cams_holdings_2024.xlsx")) == RTA.CAMS
        assert scanner._detect_rta(Path("CAMS_Statement.pdf")) == RTA.CAMS

    def test_karvy_detection(self, scanner):
        """Test Karvy/KFintech file detection."""
        assert scanner._detect_rta(Path("MF_Karvy_CG_FY24-25.xlsx")) == RTA.KFINTECH
        assert scanner._detect_rta(Path("KFintech_Holdings.xlsx")) == RTA.KFINTECH
        assert scanner._detect_rta(Path("Karvy_PAN_Statement.pdf")) == RTA.KFINTECH

    def test_unknown_rta(self, scanner):
        """Test unknown RTA returns None."""
        assert scanner._detect_rta(Path("random_statement.xlsx")) is None
        assert scanner._detect_rta(Path("MF_Statement_2024.pdf")) is None


# ============================================================================
# Test 7: Date Extraction from Filename
# ============================================================================

class TestDateExtraction:
    """Test date extraction from filenames."""

    def test_fy_date_extraction(self, scanner):
        """Test FY date extraction."""
        assert scanner._extract_date_from_filename("CAMS_CG_FY24-25.xlsx") == date(2025, 3, 31)
        assert scanner._extract_date_from_filename("Karvy_FY2425.xlsx") == date(2025, 3, 31)

    def test_iso_date_extraction(self, scanner):
        """Test ISO date extraction."""
        assert scanner._extract_date_from_filename("Statement_2024-03-31.xlsx") == date(2024, 3, 31)

    def test_dmy_date_extraction(self, scanner):
        """Test DMY date extraction."""
        assert scanner._extract_date_from_filename("Statement_31-03-2024.xlsx") == date(2024, 3, 31)

    def test_no_date(self, scanner):
        """Test filename without date."""
        assert scanner._extract_date_from_filename("Statement.xlsx") is None


# ============================================================================
# Test 8: Statement Type Detection
# ============================================================================

class TestStatementTypeDetection:
    """Test statement type detection from filenames."""

    def test_capital_gains_detection(self, scanner):
        """Test capital gains statement detection."""
        assert scanner._detect_statement_type(Path("CAMS_CG_FY2425.xlsx")) == StatementType.CAPITAL_GAINS
        assert scanner._detect_statement_type(Path("Capital_Gains_2024.xlsx")) == StatementType.CAPITAL_GAINS

    def test_holdings_detection(self, scanner):
        """Test holdings statement detection."""
        assert scanner._detect_statement_type(Path("Holdings_2024.xlsx")) == StatementType.HOLDINGS
        assert scanner._detect_statement_type(Path("CAMS_Consolidated_Summary.xlsx")) == StatementType.HOLDINGS

    def test_transactions_default(self, scanner):
        """Test default to transactions."""
        assert scanner._detect_statement_type(Path("Statement_2024.xlsx")) == StatementType.TRANSACTIONS
