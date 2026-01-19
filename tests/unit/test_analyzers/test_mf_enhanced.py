"""
Unit tests for enhanced MF Analyzer components.

Tests:
- Field normalization (CAMS -> common, Karvy -> common)
- Duplicate prevention
- Asset class classification fallback
- Decimal precision
- Reconciliation logic
- FY summary calculations
"""

import json
import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from pfas.analyzers.mf_analyzer import (
    MFFieldNormalizer,
    NormalizedHolding,
    RTA,
)
from pfas.analyzers.mf_reconciler import MFReconciler, ReconciliationResult
from pfas.analyzers.mf_fy_analyzer import MFFYAnalyzer, FYSummary, HoldingsSnapshot
from pfas.parsers.mf.classifier import classify_scheme
from pfas.parsers.mf.models import AssetClass


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def in_memory_db():
    """Create in-memory SQLite database with schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create minimal schema for tests
    conn.executescript("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            pan_encrypted BLOB,
            pan_salt BLOB,
            name TEXT
        );

        CREATE TABLE mf_amcs (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        );

        CREATE TABLE mf_schemes (
            id INTEGER PRIMARY KEY,
            amc_id INTEGER,
            name TEXT,
            asset_class TEXT,
            isin TEXT
        );

        CREATE TABLE mf_folios (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            scheme_id INTEGER,
            folio_number TEXT,
            UNIQUE(user_id, scheme_id, folio_number)
        );

        CREATE TABLE mf_transactions (
            id INTEGER PRIMARY KEY,
            folio_id INTEGER,
            user_id INTEGER,
            transaction_type TEXT,
            date DATE,
            units DECIMAL,
            nav DECIMAL,
            amount DECIMAL,
            short_term_gain DECIMAL,
            long_term_gain DECIMAL,
            purchase_date DATE
        );

        CREATE TABLE mf_holdings (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            amc_name TEXT,
            scheme_name TEXT,
            scheme_type TEXT,
            folio_number TEXT,
            investor_name TEXT,
            units DECIMAL,
            nav DECIMAL,
            nav_date DATE,
            current_value DECIMAL,
            cost_value DECIMAL,
            appreciation DECIMAL,
            average_holding_days INTEGER,
            annualized_return DECIMAL,
            dividend_payout DECIMAL,
            dividend_reinvest DECIMAL,
            isin TEXT,
            rta TEXT,
            source_file TEXT,
            statement_date DATE,
            UNIQUE(user_id, folio_number, scheme_name, nav_date)
        );

        CREATE TABLE mf_capital_gains (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            financial_year TEXT,
            asset_class TEXT,
            stcg_amount DECIMAL,
            ltcg_amount DECIMAL,
            UNIQUE(user_id, financial_year, asset_class)
        );

        CREATE TABLE mf_cg_reconciliation (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            financial_year TEXT,
            rta TEXT,
            asset_class TEXT,
            calc_stcg DECIMAL,
            calc_ltcg DECIMAL,
            calc_total_gain DECIMAL,
            reported_stcg DECIMAL,
            reported_ltcg DECIMAL,
            reported_total_gain DECIMAL,
            stcg_difference DECIMAL,
            ltcg_difference DECIMAL,
            total_difference DECIMAL,
            is_reconciled BOOLEAN,
            tolerance_used DECIMAL,
            reconciliation_notes TEXT,
            source_file TEXT,
            reconciled_at TIMESTAMP,
            UNIQUE(user_id, financial_year, rta, asset_class)
        );

        CREATE TABLE mf_cg_reconciliation_items (
            id INTEGER PRIMARY KEY,
            reconciliation_id INTEGER,
            scheme_name TEXT,
            folio_number TEXT,
            calc_stcg DECIMAL,
            calc_ltcg DECIMAL,
            reported_stcg DECIMAL,
            reported_ltcg DECIMAL,
            difference DECIMAL,
            match_status TEXT,
            notes TEXT
        );

        CREATE TABLE mf_fy_summary (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            financial_year TEXT,
            scheme_type TEXT,
            amc_name TEXT,
            rta TEXT,
            opening_units DECIMAL,
            opening_value DECIMAL,
            opening_cost DECIMAL,
            purchase_units DECIMAL,
            purchase_amount DECIMAL,
            purchase_count INTEGER,
            redemption_units DECIMAL,
            redemption_amount DECIMAL,
            redemption_count INTEGER,
            switch_in_units DECIMAL,
            switch_in_amount DECIMAL,
            switch_out_units DECIMAL,
            switch_out_amount DECIMAL,
            dividend_payout DECIMAL,
            dividend_reinvest DECIMAL,
            stcg_realized DECIMAL,
            ltcg_realized DECIMAL,
            closing_units DECIMAL,
            closing_value DECIMAL,
            closing_cost DECIMAL,
            absolute_return DECIMAL,
            xirr DECIMAL,
            UNIQUE(user_id, financial_year, scheme_type, amc_name, rta)
        );

        CREATE TABLE mf_holdings_snapshot (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            snapshot_date DATE,
            snapshot_type TEXT,
            financial_year TEXT,
            holdings_json TEXT,
            total_schemes INTEGER,
            total_folios INTEGER,
            total_units DECIMAL,
            total_value DECIMAL,
            total_cost DECIMAL,
            total_appreciation DECIMAL,
            equity_value DECIMAL,
            equity_schemes INTEGER,
            debt_value DECIMAL,
            debt_schemes INTEGER,
            hybrid_value DECIMAL,
            hybrid_schemes INTEGER,
            weighted_xirr DECIMAL,
            UNIQUE(user_id, snapshot_date, snapshot_type)
        );

        CREATE TABLE mf_yoy_growth (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            base_year TEXT,
            compare_year TEXT,
            base_value DECIMAL,
            compare_value DECIMAL,
            value_change DECIMAL,
            value_change_pct DECIMAL,
            base_cost DECIMAL,
            compare_cost DECIMAL,
            net_investment DECIMAL,
            base_appreciation DECIMAL,
            compare_appreciation DECIMAL,
            appreciation_change DECIMAL,
            equity_growth_pct DECIMAL,
            debt_growth_pct DECIMAL,
            hybrid_growth_pct DECIMAL,
            schemes_added INTEGER,
            schemes_removed INTEGER,
            schemes_unchanged INTEGER,
            UNIQUE(user_id, base_year, compare_year)
        );

        INSERT INTO users (id, pan_encrypted, pan_salt, name) VALUES (1, X'00', X'00', 'TestUser');
    """)

    yield conn
    conn.close()


@pytest.fixture
def sample_cams_df():
    """Sample CAMS holdings DataFrame."""
    return pd.DataFrame({
        "AMCName": ["HDFC Mutual Fund", "ICICI Prudential Mutual Fund"],
        "Scheme": [
            "HDFC Equity Fund - Direct Plan - Growth",
            "ICICI Prudential Bluechip Fund - Direct Plan"
        ],
        "Type": ["Equity", "Index Fund"],
        "Folio": ["12345678", "87654321"],
        "InvestorName": ["Test User", "Test User"],
        "UnitBal": ["100.5000", "200.2500"],
        "NAVDate": ["31-Mar-2024", "31-Mar-2024"],
        "CurrentValue": ["Rs. 1,50,000.00", "Rs. 2,25,000.00"],
        "CostValue": ["Rs. 1,00,000.00", "Rs. 1,75,000.00"],
        "Appreciation": ["Rs. 50,000.00", "Rs. 50,000.00"],
        "WtgAvg": ["365", "180"],
        "Annualised XIRR": ["15.5%", "12.3%"],
        "ISIN": ["INF179K01234", "INF109K01567"],
    })


@pytest.fixture
def sample_karvy_df():
    """Sample Karvy/KFintech holdings DataFrame."""
    return pd.DataFrame({
        "Scheme Name": [
            "Axis Bluechip Fund - Direct Plan - Growth",
            "SBI Liquid Fund - Direct Plan"
        ],
        "Folio": ["11111111", "22222222"],
        "Investor Name": ["Test User", "Test User"],
        "Unit Balance": ["50.0000", "1000.0000"],
        "Nav Date": ["31-Mar-2024", "31-Mar-2024"],
        "Current Value (Rs.)": ["75,000.00", "1,00,000.00"],
        "Cost Value (Rs.)": ["60,000.00", "99,500.00"],
        "Appreciation (Rs.)": ["15,000.00", "500.00"],
        "AvgAgeDays": ["730", "30"],
        "Annualized Yield (%)": ["11.2", "5.5"],
        "Dividend Payout": ["0", "0"],
        "Dividend Re-Invest": ["0", "0"],
    })


# ============================================================================
# Test: Field Normalization
# ============================================================================

class TestMFFieldNormalizer:
    """Tests for MFFieldNormalizer."""

    def test_normalize_cams_holdings(self, sample_cams_df):
        """Test CAMS to common schema normalization."""
        normalizer = MFFieldNormalizer()
        holdings = normalizer.normalize_holdings(sample_cams_df, RTA.CAMS, "test.xlsx")

        assert len(holdings) == 2

        # Verify first holding
        h1 = holdings[0]
        assert h1.amc_name == "HDFC Mutual Fund"
        assert h1.scheme_name == "HDFC Equity Fund - Direct Plan - Growth"
        assert h1.scheme_type == "EQUITY"
        assert h1.folio_number == "12345678"
        assert h1.units == Decimal("100.5000")
        assert h1.current_value == Decimal("150000.00")
        assert h1.cost_value == Decimal("100000.00")
        assert h1.appreciation == Decimal("50000.00")
        assert h1.average_holding_days == 365
        assert h1.annualized_return == Decimal("15.5")
        assert h1.isin == "INF179K01234"
        assert h1.rta == RTA.CAMS

    def test_normalize_karvy_holdings(self, sample_karvy_df):
        """Test Karvy to common schema normalization."""
        normalizer = MFFieldNormalizer()
        holdings = normalizer.normalize_holdings(sample_karvy_df, RTA.KFINTECH, "test.xlsx")

        assert len(holdings) == 2

        # Verify liquid fund classification
        liquid_fund = next(h for h in holdings if "Liquid" in h.scheme_name)
        assert liquid_fund.scheme_type == "DEBT"
        assert liquid_fund.units == Decimal("1000.0000")
        assert liquid_fund.current_value == Decimal("100000.00")
        assert liquid_fund.rta == RTA.KFINTECH

    def test_currency_cleaning(self):
        """Test currency symbol and comma removal."""
        normalizer = MFFieldNormalizer()

        # Test various formats
        assert normalizer._parse_decimal("Rs. 1,50,000.00") == Decimal("150000.00")
        assert normalizer._parse_decimal("1,50,000.00") == Decimal("150000.00")
        assert normalizer._parse_decimal("150000.00") == Decimal("150000.00")
        assert normalizer._parse_decimal("Rs.150000") == Decimal("150000")
        assert normalizer._parse_decimal("") == Decimal("0")
        assert normalizer._parse_decimal(None) == Decimal("0")

    def test_date_parsing(self):
        """Test date parsing from various formats."""
        normalizer = MFFieldNormalizer()

        assert normalizer._parse_date("31-Mar-2024") == date(2024, 3, 31)
        assert normalizer._parse_date("31/03/2024") == date(2024, 3, 31)
        assert normalizer._parse_date("2024-03-31") == date(2024, 3, 31)
        assert normalizer._parse_date("31-03-2024") == date(2024, 3, 31)
        assert normalizer._parse_date(None) is None
        assert normalizer._parse_date("") is None

    def test_decimal_precision(self):
        """Test Decimal precision is maintained."""
        normalizer = MFFieldNormalizer()

        # Units should have 4 decimal places
        assert normalizer._parse_decimal("100.1234") == Decimal("100.1234")
        assert normalizer._parse_decimal("100.12345678") == Decimal("100.12345678")

        # Currency values
        assert normalizer._parse_decimal("1,23,456.78") == Decimal("123456.78")


# ============================================================================
# Test: Asset Class Classification
# ============================================================================

class TestSchemeClassification:
    """Tests for scheme classification fallback."""

    def test_equity_classification(self):
        """Test equity scheme classification."""
        assert classify_scheme("HDFC Equity Fund - Direct").value == "EQUITY"
        assert classify_scheme("ICICI Prudential Bluechip Fund").value == "EQUITY"
        assert classify_scheme("Axis ELSS Tax Saver Fund").value == "EQUITY"
        assert classify_scheme("SBI Nifty Index Fund").value == "EQUITY"
        assert classify_scheme("Mirae Asset Large Cap Fund").value == "EQUITY"

    def test_debt_classification(self):
        """Test debt scheme classification."""
        assert classify_scheme("HDFC Liquid Fund").value == "DEBT"
        assert classify_scheme("ICICI Prudential Corporate Bond Fund").value == "DEBT"
        assert classify_scheme("SBI Overnight Fund").value == "DEBT"
        assert classify_scheme("Axis Money Market Fund").value == "DEBT"
        assert classify_scheme("UTI Treasury Advantage Fund").value == "DEBT"

    def test_hybrid_classification(self):
        """Test hybrid scheme classification."""
        assert classify_scheme("HDFC Balanced Advantage Fund").value == "HYBRID"
        assert classify_scheme("ICICI Prudential Equity Savings Fund").value == "HYBRID"
        assert classify_scheme("SBI Arbitrage Opportunities Fund").value == "HYBRID"

    def test_fallback_to_other(self):
        """Test fallback to OTHER for unknown schemes."""
        # Should default to OTHER or EQUITY based on implementation
        result = classify_scheme("Some Random Fund XYZ 123")
        assert result.value in ["EQUITY", "OTHER"]


# ============================================================================
# Test: Duplicate Prevention
# ============================================================================

class TestDuplicatePrevention:
    """Tests for duplicate detection and prevention."""

    def test_holdings_unique_constraint(self, in_memory_db):
        """Test that duplicate holdings are prevented by unique constraint."""
        # Insert first holding
        in_memory_db.execute("""
            INSERT INTO mf_holdings
            (user_id, amc_name, scheme_name, folio_number, nav_date, current_value, scheme_type, rta, units)
            VALUES (1, 'HDFC MF', 'HDFC Equity Fund', '12345', '2024-03-31', '100000', 'EQUITY', 'CAMS', '100')
        """)
        in_memory_db.commit()

        # Try to insert duplicate - should fail
        with pytest.raises(sqlite3.IntegrityError):
            in_memory_db.execute("""
                INSERT INTO mf_holdings
                (user_id, amc_name, scheme_name, folio_number, nav_date, current_value, scheme_type, rta, units)
                VALUES (1, 'HDFC MF', 'HDFC Equity Fund', '12345', '2024-03-31', '110000', 'EQUITY', 'CAMS', '105')
            """)

    def test_different_nav_date_allowed(self, in_memory_db):
        """Test that same scheme with different nav_date is allowed."""
        # Insert first holding
        in_memory_db.execute("""
            INSERT INTO mf_holdings
            (user_id, amc_name, scheme_name, folio_number, nav_date, current_value, scheme_type, rta, units)
            VALUES (1, 'HDFC MF', 'HDFC Equity Fund', '12345', '2024-03-31', '100000', 'EQUITY', 'CAMS', '100')
        """)

        # Insert same scheme with different date - should succeed
        in_memory_db.execute("""
            INSERT INTO mf_holdings
            (user_id, amc_name, scheme_name, folio_number, nav_date, current_value, scheme_type, rta, units)
            VALUES (1, 'HDFC MF', 'HDFC Equity Fund', '12345', '2024-04-30', '110000', 'EQUITY', 'CAMS', '100')
        """)
        in_memory_db.commit()

        cursor = in_memory_db.execute("SELECT COUNT(*) FROM mf_holdings")
        assert cursor.fetchone()[0] == 2


# ============================================================================
# Test: Reconciliation Logic
# ============================================================================

class TestMFReconciler:
    """Tests for capital gains reconciliation."""

    def test_reconciliation_within_tolerance(self, in_memory_db):
        """Test reconciliation passes when difference is within tolerance."""
        # Insert calculated capital gains
        in_memory_db.execute("""
            INSERT INTO mf_capital_gains (user_id, financial_year, asset_class, stcg_amount, ltcg_amount)
            VALUES (1, '2024-25', 'EQUITY', '10000', '50000')
        """)
        in_memory_db.commit()

        reconciler = MFReconciler(in_memory_db, {"reconciliation": {"tolerance_amount": 1.00}})

        result = ReconciliationResult(
            user_id=1,
            financial_year="2024-25",
            rta="CAMS",
            asset_class="EQUITY",
            calc_stcg=Decimal("10000"),
            calc_ltcg=Decimal("50000"),
            reported_stcg=Decimal("10000.50"),
            reported_ltcg=Decimal("50000.25"),
            tolerance_used=Decimal("1.00")
        )

        result.calculate_differences()

        # Difference is 0.75, within 1.00 tolerance
        assert result.is_reconciled is True
        assert result.total_difference == Decimal("-0.75")

    def test_reconciliation_outside_tolerance(self, in_memory_db):
        """Test reconciliation fails when difference exceeds tolerance."""
        result = ReconciliationResult(
            user_id=1,
            financial_year="2024-25",
            rta="CAMS",
            asset_class="EQUITY",
            calc_stcg=Decimal("10000"),
            calc_ltcg=Decimal("50000"),
            reported_stcg=Decimal("10500"),
            reported_ltcg=Decimal("50000"),
            tolerance_used=Decimal("1.00")
        )

        result.calculate_differences()

        # Difference is 500, exceeds 1.00 tolerance
        assert result.is_reconciled is False
        assert result.stcg_difference == Decimal("-500")

    def test_save_reconciliation_result(self, in_memory_db):
        """Test saving reconciliation result to database."""
        reconciler = MFReconciler(in_memory_db)

        result = ReconciliationResult(
            user_id=1,
            financial_year="2024-25",
            rta="CAMS",
            asset_class="ALL",
            calc_stcg=Decimal("10000"),
            calc_ltcg=Decimal("50000"),
            reported_stcg=Decimal("10000"),
            reported_ltcg=Decimal("50000"),
            is_reconciled=True,
            tolerance_used=Decimal("1.00")
        )
        result.calculate_differences()

        recon_id = reconciler.save_result(result)
        assert recon_id > 0

        # Verify saved
        cursor = in_memory_db.execute(
            "SELECT is_reconciled FROM mf_cg_reconciliation WHERE id = ?",
            (recon_id,)
        )
        assert cursor.fetchone()[0] == 1


# ============================================================================
# Test: FY Summary Calculations
# ============================================================================

class TestMFFYAnalyzer:
    """Tests for FY analysis and snapshots."""

    def test_holdings_snapshot(self, in_memory_db):
        """Test taking holdings snapshot."""
        # Insert sample holdings
        in_memory_db.execute("""
            INSERT INTO mf_holdings
            (user_id, amc_name, scheme_name, scheme_type, folio_number, units, nav_date,
             current_value, cost_value, appreciation, annualized_return, rta)
            VALUES
            (1, 'HDFC MF', 'HDFC Equity Fund', 'EQUITY', '12345', '100', '2024-03-31',
             '150000', '100000', '50000', '15.5', 'CAMS'),
            (1, 'SBI MF', 'SBI Liquid Fund', 'DEBT', '67890', '1000', '2024-03-31',
             '100000', '99000', '1000', '5.5', 'CAMS')
        """)
        in_memory_db.commit()

        analyzer = MFFYAnalyzer(in_memory_db)
        snapshot = analyzer.take_holdings_snapshot(
            user_id=1,
            snapshot_date=date(2024, 3, 31),
            snapshot_type="FY_END",
            financial_year="2023-24"
        )

        assert snapshot.total_schemes == 2
        assert snapshot.total_value == Decimal("250000")
        assert snapshot.total_cost == Decimal("199000")
        assert snapshot.total_appreciation == Decimal("51000")
        assert snapshot.equity_value == Decimal("150000")
        assert snapshot.debt_value == Decimal("100000")
        assert snapshot.equity_schemes == 1
        assert snapshot.debt_schemes == 1

    def test_save_and_retrieve_snapshot(self, in_memory_db):
        """Test saving and retrieving holdings snapshot."""
        analyzer = MFFYAnalyzer(in_memory_db)

        snapshot = HoldingsSnapshot(
            user_id=1,
            snapshot_date=date(2024, 3, 31),
            snapshot_type="FY_END",
            financial_year="2023-24",
            holdings=[
                {"scheme_name": "Test Fund", "current_value": "100000"}
            ],
            total_schemes=1,
            total_folios=1,
            total_value=Decimal("100000"),
            total_cost=Decimal("80000"),
            total_appreciation=Decimal("20000"),
            equity_value=Decimal("100000"),
            equity_schemes=1
        )

        snapshot_id = analyzer.save_holdings_snapshot(snapshot)
        assert snapshot_id > 0

        # Verify saved
        cursor = in_memory_db.execute(
            "SELECT total_value, financial_year FROM mf_holdings_snapshot WHERE id = ?",
            (snapshot_id,)
        )
        row = cursor.fetchone()
        assert Decimal(row[0]) == Decimal("100000")
        assert row[1] == "2023-24"

    def test_yoy_growth_calculation(self, in_memory_db):
        """Test year-over-year growth calculation."""
        # Insert snapshots for two years
        in_memory_db.execute("""
            INSERT INTO mf_holdings_snapshot
            (user_id, snapshot_date, snapshot_type, financial_year, holdings_json,
             total_value, total_cost, total_appreciation, equity_value, debt_value, hybrid_value)
            VALUES
            (1, '2023-03-31', 'FY_END', '2022-23', '[]', '100000', '80000', '20000', '70000', '30000', '0'),
            (1, '2024-03-31', 'FY_END', '2023-24', '[]', '150000', '100000', '50000', '100000', '50000', '0')
        """)
        in_memory_db.commit()

        analyzer = MFFYAnalyzer(in_memory_db)
        growth = analyzer.calculate_yoy_growth(
            user_id=1,
            base_year="2022-23",
            compare_year="2023-24"
        )

        assert growth.base_value == Decimal("100000")
        assert growth.compare_value == Decimal("150000")
        assert growth.value_change == Decimal("50000")
        assert growth.value_change_pct == Decimal("50")  # 50% growth


# ============================================================================
# Test: FY Date Utilities
# ============================================================================

class TestFYDateUtilities:
    """Tests for financial year date utilities."""

    def test_get_fy_dates(self, in_memory_db):
        """Test FY start/end date calculation."""
        analyzer = MFFYAnalyzer(in_memory_db)

        start, end = analyzer._get_fy_dates("2024-25")
        assert start == "2024-04-01"
        assert end == "2025-03-31"

        start, end = analyzer._get_fy_dates("2023-24")
        assert start == "2023-04-01"
        assert end == "2024-03-31"
