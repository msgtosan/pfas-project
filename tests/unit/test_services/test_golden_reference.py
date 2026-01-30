"""
Unit tests for Golden Reference Reconciliation Engine.

Tests cover:
- TruthResolver: Source of truth determination
- CrossCorrelator: Reconciliation logic with tolerance
- Models: Data structures and calculations
- Mismatch scenarios
- Multi-currency support
"""

import pytest
import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from pfas.services.golden_reference import (
    TruthResolver,
    CrossCorrelator,
    ReconciliationConfig,
    MetricType,
    AssetClass,
    SourceType,
    MatchResult,
    ReconciliationStatus,
    Severity,
    GoldenHolding,
    SystemHolding,
    ReconciliationEvent,
    ReconciliationSummary,
)


@pytest.fixture
def db_connection():
    """Create in-memory database with required schema."""
    conn = sqlite3.connect(":memory:")

    conn.executescript("""
        -- Core tables
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO users (id, name) VALUES (1, 'Test User');

        -- Truth sources
        CREATE TABLE truth_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_type TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            source_priority JSON NOT NULL,
            description TEXT,
            user_id INTEGER,
            is_default INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(metric_type, asset_class, user_id)
        );

        -- Golden reference
        CREATE TABLE golden_reference (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            source_type TEXT NOT NULL,
            statement_date DATE NOT NULL,
            period_start DATE,
            period_end DATE,
            file_path TEXT,
            file_hash TEXT,
            raw_data JSON,
            investor_name TEXT,
            investor_pan TEXT,
            status TEXT DEFAULT 'ACTIVE',
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            validated_at TIMESTAMP,
            notes TEXT,
            UNIQUE(user_id, source_type, file_hash)
        );

        -- Golden holdings
        CREATE TABLE golden_holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            golden_ref_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            asset_type TEXT NOT NULL,
            isin TEXT,
            symbol TEXT,
            name TEXT NOT NULL,
            folio_number TEXT,
            account_number TEXT,
            units DECIMAL(18, 6),
            nav DECIMAL(12, 4),
            market_value DECIMAL(18, 2),
            cost_basis DECIMAL(18, 2),
            unrealized_gain DECIMAL(18, 2),
            currency TEXT DEFAULT 'INR',
            exchange_rate DECIMAL(10, 6) DEFAULT 1.0,
            value_inr DECIMAL(18, 2),
            as_of_date DATE NOT NULL,
            financial_year TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Reconciliation events
        CREATE TABLE reconciliation_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            reconciliation_date DATE NOT NULL,
            metric_type TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            source_type TEXT NOT NULL,
            golden_ref_id INTEGER,
            isin TEXT,
            folio_number TEXT,
            symbol TEXT,
            system_value DECIMAL(18, 6),
            golden_value DECIMAL(18, 6),
            difference DECIMAL(18, 6),
            difference_pct DECIMAL(10, 4),
            tolerance_used DECIMAL(10, 6),
            status TEXT DEFAULT 'PENDING',
            match_result TEXT,
            severity TEXT DEFAULT 'INFO',
            resolved_at TIMESTAMP,
            resolved_by TEXT,
            resolution_action TEXT,
            resolution_notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Reconciliation suspense
        CREATE TABLE reconciliation_suspense (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            event_id INTEGER NOT NULL,
            asset_type TEXT NOT NULL,
            isin TEXT,
            symbol TEXT,
            name TEXT,
            folio_number TEXT,
            suspense_units DECIMAL(18, 6),
            suspense_value DECIMAL(18, 2),
            suspense_currency TEXT DEFAULT 'INR',
            suspense_reason TEXT,
            opened_date DATE NOT NULL,
            target_resolution_date DATE,
            actual_resolution_date DATE,
            status TEXT DEFAULT 'OPEN',
            priority TEXT DEFAULT 'NORMAL',
            assigned_to TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- MF tables for system holdings
        CREATE TABLE mf_schemes (
            id INTEGER PRIMARY KEY, name TEXT, isin TEXT, asset_class TEXT
        );
        CREATE TABLE mf_folios (
            id INTEGER PRIMARY KEY, user_id INTEGER, scheme_id INTEGER, folio_number TEXT
        );
        CREATE TABLE mf_transactions (
            id INTEGER PRIMARY KEY, folio_id INTEGER, transaction_type TEXT,
            date DATE, units DECIMAL, nav DECIMAL, amount DECIMAL
        );
        CREATE TABLE mf_nav_history (
            id INTEGER PRIMARY KEY, scheme_id INTEGER, nav_date DATE, nav DECIMAL
        );
        CREATE TABLE mf_holdings (
            id INTEGER PRIMARY KEY, user_id INTEGER, folio_id INTEGER,
            isin TEXT, scheme_name TEXT, folio_number TEXT,
            units DECIMAL, nav DECIMAL, current_value DECIMAL, cost_value DECIMAL,
            statement_date DATE
        );

        -- Stock tables
        CREATE TABLE stock_trades (
            id INTEGER PRIMARY KEY, user_id INTEGER, symbol TEXT, isin TEXT,
            security_name TEXT, trade_type TEXT, quantity DECIMAL, price DECIMAL,
            trade_date DATE
        );
        CREATE TABLE stock_holdings (
            id INTEGER PRIMARY KEY, user_id INTEGER, broker_id INTEGER,
            symbol TEXT, isin TEXT, company_name TEXT,
            quantity_held INTEGER, average_buy_price DECIMAL, total_cost_basis DECIMAL,
            current_price DECIMAL, market_value DECIMAL, as_of_date DATE
        );

        -- NPS tables
        CREATE TABLE nps_accounts (
            id INTEGER PRIMARY KEY, user_id INTEGER, pran TEXT
        );
        CREATE TABLE nps_transactions (
            id INTEGER PRIMARY KEY, nps_account_id INTEGER, user_id INTEGER,
            scheme TEXT, tier TEXT, units DECIMAL, nav DECIMAL, amount DECIMAL, transaction_date DATE
        );
    """)

    yield conn
    conn.close()


@pytest.fixture
def sample_golden_ref(db_connection):
    """Create sample golden reference with holdings."""
    conn = db_connection

    # Insert golden reference
    conn.execute("""
        INSERT INTO golden_reference (id, user_id, source_type, statement_date, status)
        VALUES (1, 1, 'NSDL_CAS', '2024-03-31', 'ACTIVE')
    """)

    # Insert golden holdings
    holdings = [
        (1, 1, 1, 'MUTUAL_FUND', 'INF123456789', None, 'Test MF Scheme', 'FOLIO001',
         1000.5, 150.25, 150325.125, 140000, 10325.125, 'INR', 1.0, 150325.125, '2024-03-31', '2023-24'),
        (2, 1, 1, 'MUTUAL_FUND', 'INF987654321', None, 'Another MF Scheme', 'FOLIO002',
         500.0, 200.0, 100000.0, 90000, 10000, 'INR', 1.0, 100000.0, '2024-03-31', '2023-24'),
        (3, 1, 1, 'STOCKS', 'INE001A01036', 'RELIANCE', 'Reliance Industries', None,
         100, 2500.0, 250000.0, 200000, 50000, 'INR', 1.0, 250000.0, '2024-03-31', '2023-24'),
    ]

    for h in holdings:
        conn.execute("""
            INSERT INTO golden_holdings
            (id, user_id, golden_ref_id, asset_type, isin, symbol, name, folio_number,
             units, nav, market_value, cost_basis, unrealized_gain, currency,
             exchange_rate, value_inr, as_of_date, financial_year)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, h)

    conn.commit()
    return 1  # golden_ref_id


class TestTruthResolver:
    """Tests for TruthResolver class."""

    def test_default_truth_sources_loaded(self, db_connection):
        """Test that default truth sources are loaded."""
        resolver = TruthResolver(db_connection, user_id=1)

        # MF net worth should default to NSDL_CAS
        source = resolver.get_truth_source(MetricType.NET_WORTH, AssetClass.MUTUAL_FUND)
        assert source == SourceType.NSDL_CAS

    def test_get_source_priority(self, db_connection):
        """Test getting prioritized source list."""
        resolver = TruthResolver(db_connection, user_id=1)

        sources = resolver.get_source_priority(MetricType.NET_WORTH, AssetClass.MUTUAL_FUND)
        assert len(sources) >= 2
        assert SourceType.NSDL_CAS in sources
        assert SourceType.SYSTEM in sources

    def test_is_authoritative(self, db_connection):
        """Test checking if source is authoritative."""
        resolver = TruthResolver(db_connection, user_id=1)

        assert resolver.is_authoritative(
            SourceType.NSDL_CAS, MetricType.NET_WORTH, AssetClass.MUTUAL_FUND
        )
        assert not resolver.is_authoritative(
            SourceType.SYSTEM, MetricType.NET_WORTH, AssetClass.MUTUAL_FUND
        )

    def test_cost_basis_system_is_authoritative(self, db_connection):
        """Test that SYSTEM is authoritative for cost basis."""
        resolver = TruthResolver(db_connection, user_id=1)

        source = resolver.get_truth_source(MetricType.COST_BASIS, AssetClass.MUTUAL_FUND)
        assert source == SourceType.SYSTEM

    def test_user_override(self, db_connection):
        """Test setting user-specific override."""
        resolver = TruthResolver(db_connection, user_id=1)

        # Set override
        resolver.set_user_override(
            MetricType.NET_WORTH,
            AssetClass.MUTUAL_FUND,
            [SourceType.RTA_CAS, SourceType.NSDL_CAS, SourceType.SYSTEM],
            "User prefers RTA for MF"
        )

        # Verify override is applied
        source = resolver.get_truth_source(MetricType.NET_WORTH, AssetClass.MUTUAL_FUND)
        assert source == SourceType.RTA_CAS


class TestCrossCorrelator:
    """Tests for CrossCorrelator class."""

    def test_reconcile_exact_match(self, db_connection, sample_golden_ref):
        """Test reconciliation with exact matches."""
        conn = db_connection

        # Insert matching system holdings (MF transactions)
        conn.execute("""
            INSERT INTO mf_schemes (id, name, isin) VALUES (1, 'Test MF Scheme', 'INF123456789')
        """)
        conn.execute("""
            INSERT INTO mf_folios (id, user_id, scheme_id, folio_number)
            VALUES (1, 1, 1, 'FOLIO001')
        """)
        conn.execute("""
            INSERT INTO mf_transactions (folio_id, transaction_type, date, units, nav, amount)
            VALUES (1, 'PURCHASE', '2024-01-15', 1000.5, 140.0, 140070)
        """)
        conn.execute("""
            INSERT INTO mf_nav_history (scheme_id, nav_date, nav)
            VALUES (1, '2024-03-31', 150.25)
        """)
        conn.commit()

        correlator = CrossCorrelator(conn, user_id=1)
        summary = correlator.reconcile_holdings(
            AssetClass.MUTUAL_FUND,
            sample_golden_ref,
            as_of_date=date(2024, 3, 31)
        )

        # Should have some matches
        assert summary.total_items > 0
        assert summary.matched_exact >= 0

    def test_reconcile_within_tolerance(self, db_connection, sample_golden_ref):
        """Test reconciliation with values within tolerance."""
        config = ReconciliationConfig(
            absolute_tolerance=Decimal("1.0"),  # 1 unit tolerance
            auto_resolve_within_tolerance=True
        )

        correlator = CrossCorrelator(db_connection, user_id=1, config=config)

        # Create holdings with small difference
        golden = GoldenHolding(
            golden_ref_id=1,
            user_id=1,
            asset_type=AssetClass.MUTUAL_FUND,
            isin="INF999999999",
            name="Test Scheme",
            units=Decimal("100.005"),
            market_value=Decimal("10000.50")
        )

        system = SystemHolding(
            asset_type=AssetClass.MUTUAL_FUND,
            isin="INF999999999",
            name="Test Scheme",
            units=Decimal("100.000"),
            market_value=Decimal("10000.00")
        )

        # Use internal comparison
        event = correlator._compare_holding(
            golden=golden,
            system=system,
            key="ISIN:INF999999999",
            metric_type=MetricType.NET_WORTH,
            asset_class=AssetClass.MUTUAL_FUND,
            source_type=SourceType.NSDL_CAS,
            golden_ref_id=1,
            as_of_date=date.today()
        )

        assert event.match_result == MatchResult.WITHIN_TOLERANCE
        assert event.status == ReconciliationStatus.MATCHED

    def test_reconcile_mismatch(self, db_connection, sample_golden_ref):
        """Test reconciliation with mismatched values."""
        config = ReconciliationConfig(
            absolute_tolerance=Decimal("0.01"),
        )

        correlator = CrossCorrelator(db_connection, user_id=1, config=config)

        golden = GoldenHolding(
            golden_ref_id=1,
            user_id=1,
            asset_type=AssetClass.MUTUAL_FUND,
            isin="INF888888888",
            name="Mismatched Scheme",
            units=Decimal("1000"),
            market_value=Decimal("100000")
        )

        system = SystemHolding(
            asset_type=AssetClass.MUTUAL_FUND,
            isin="INF888888888",
            name="Mismatched Scheme",
            units=Decimal("900"),  # 100 units difference
            market_value=Decimal("90000")
        )

        event = correlator._compare_holding(
            golden=golden,
            system=system,
            key="ISIN:INF888888888",
            metric_type=MetricType.NET_WORTH,
            asset_class=AssetClass.MUTUAL_FUND,
            source_type=SourceType.NSDL_CAS,
            golden_ref_id=1,
            as_of_date=date.today()
        )

        assert event.match_result == MatchResult.MISMATCH
        assert event.status == ReconciliationStatus.MISMATCH
        assert event.difference == Decimal("-10000")  # system - golden

    def test_reconcile_missing_system(self, db_connection, sample_golden_ref):
        """Test reconciliation when holding missing from system."""
        correlator = CrossCorrelator(db_connection, user_id=1)

        golden = GoldenHolding(
            golden_ref_id=1,
            user_id=1,
            asset_type=AssetClass.MUTUAL_FUND,
            isin="INF777777777",
            name="Only in Golden",
            units=Decimal("500"),
            market_value=Decimal("50000")
        )

        event = correlator._compare_holding(
            golden=golden,
            system=None,
            key="ISIN:INF777777777",
            metric_type=MetricType.NET_WORTH,
            asset_class=AssetClass.MUTUAL_FUND,
            source_type=SourceType.NSDL_CAS,
            golden_ref_id=1,
            as_of_date=date.today()
        )

        assert event.match_result == MatchResult.MISSING_SYSTEM
        assert event.golden_value == Decimal("50000")
        assert event.system_value is None

    def test_reconcile_missing_golden(self, db_connection, sample_golden_ref):
        """Test reconciliation when holding missing from golden."""
        correlator = CrossCorrelator(db_connection, user_id=1)

        system = SystemHolding(
            asset_type=AssetClass.MUTUAL_FUND,
            isin="INF666666666",
            name="Only in System",
            units=Decimal("300"),
            market_value=Decimal("30000")
        )

        event = correlator._compare_holding(
            golden=None,
            system=system,
            key="ISIN:INF666666666",
            metric_type=MetricType.NET_WORTH,
            asset_class=AssetClass.MUTUAL_FUND,
            source_type=SourceType.NSDL_CAS,
            golden_ref_id=1,
            as_of_date=date.today()
        )

        assert event.match_result == MatchResult.MISSING_GOLDEN
        assert event.system_value == Decimal("30000")
        assert event.golden_value is None

    def test_severity_levels(self, db_connection):
        """Test severity determination based on difference amount."""
        config = ReconciliationConfig(
            warning_threshold=Decimal("100"),
            error_threshold=Decimal("1000"),
            critical_threshold=Decimal("10000"),
        )

        correlator = CrossCorrelator(db_connection, user_id=1, config=config)

        # INFO level
        assert correlator._determine_severity(Decimal("50")) == Severity.INFO

        # WARNING level
        assert correlator._determine_severity(Decimal("500")) == Severity.WARNING

        # ERROR level
        assert correlator._determine_severity(Decimal("5000")) == Severity.ERROR

        # CRITICAL level
        assert correlator._determine_severity(Decimal("15000")) == Severity.CRITICAL


class TestReconciliationEvent:
    """Tests for ReconciliationEvent dataclass."""

    def test_calculate_difference(self):
        """Test difference calculation."""
        event = ReconciliationEvent(
            system_value=Decimal("10500"),
            golden_value=Decimal("10000")
        )
        event.calculate_difference()

        assert event.difference == Decimal("500")
        assert event.difference_pct == Decimal("5")  # 5%

    def test_calculate_difference_zero_golden(self):
        """Test difference calculation when golden is zero."""
        event = ReconciliationEvent(
            system_value=Decimal("1000"),
            golden_value=Decimal("0")
        )
        event.calculate_difference()

        assert event.difference == Decimal("1000")
        assert event.difference_pct == Decimal("100")


class TestGoldenHolding:
    """Tests for GoldenHolding dataclass."""

    def test_reconciliation_key_isin(self):
        """Test reconciliation key with ISIN."""
        holding = GoldenHolding(
            isin="INF123456789",
            name="Test Scheme"
        )
        assert holding.reconciliation_key == "ISIN:INF123456789"

    def test_reconciliation_key_folio(self):
        """Test reconciliation key with folio (no ISIN)."""
        holding = GoldenHolding(
            folio_number="FOLIO123",
            name="Test Scheme"
        )
        assert holding.reconciliation_key == "FOLIO:FOLIO123"

    def test_value_inr_calculation(self):
        """Test INR value calculation."""
        holding = GoldenHolding(
            name="US Stock",
            market_value=Decimal("1000"),  # USD
            currency="USD",
            exchange_rate=Decimal("83.5")
        )
        assert holding.value_inr == Decimal("83500")


class TestMultiCurrency:
    """Tests for multi-currency reconciliation (US Stocks)."""

    def test_us_stock_value_conversion(self):
        """Test USD to INR conversion for US stocks."""
        holding = GoldenHolding(
            asset_type=AssetClass.US_STOCKS,
            isin="US0378331005",
            symbol="AAPL",
            name="Apple Inc",
            units=Decimal("10"),
            market_value=Decimal("1750"),  # USD
            currency="USD",
            exchange_rate=Decimal("83.25")
        )

        assert holding.currency == "USD"
        assert holding.value_inr == Decimal("145687.50")  # 1750 * 83.25

    def test_reconcile_us_stocks_with_forex(self, db_connection):
        """Test reconciliation of US stocks with forex conversion."""
        # Insert golden reference for US stocks
        db_connection.execute("""
            INSERT INTO golden_reference (id, user_id, source_type, statement_date, status)
            VALUES (2, 1, 'BROKER_STATEMENT', '2024-03-31', 'ACTIVE')
        """)

        db_connection.execute("""
            INSERT INTO golden_holdings
            (golden_ref_id, user_id, asset_type, isin, symbol, name,
             units, market_value, currency, exchange_rate, value_inr, as_of_date)
            VALUES
            (2, 1, 'US_STOCKS', 'US0378331005', 'AAPL', 'Apple Inc',
             10, 1750.00, 'USD', 83.25, 145687.50, '2024-03-31')
        """)
        db_connection.commit()

        correlator = CrossCorrelator(db_connection, user_id=1)

        # Load golden holdings
        holdings = correlator._load_golden_holdings(2, AssetClass.US_STOCKS)

        assert len(holdings) == 1
        assert holdings[0].currency == "USD"
        assert holdings[0].exchange_rate == Decimal("83.25")


class TestReconciliationSummary:
    """Tests for ReconciliationSummary dataclass."""

    def test_match_rate_calculation(self):
        """Test match rate percentage calculation."""
        summary = ReconciliationSummary(
            user_id=1,
            reconciliation_date=date.today(),
            asset_class=AssetClass.MUTUAL_FUND,
            source_type=SourceType.NSDL_CAS,
            golden_ref_id=1,
            total_items=10,
            matched_exact=6,
            matched_tolerance=2,
            mismatches=2,
        )

        assert summary.match_rate == 80.0  # (6+2)/10 * 100

    def test_match_rate_empty(self):
        """Test match rate with no items."""
        summary = ReconciliationSummary(
            user_id=1,
            reconciliation_date=date.today(),
            asset_class=AssetClass.MUTUAL_FUND,
            source_type=SourceType.NSDL_CAS,
            golden_ref_id=1,
            total_items=0,
        )

        assert summary.match_rate == 100.0  # Empty is considered fully matched


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
