"""
Unit tests for Asset Parsers - Rental, SGB, REIT, Dividends.

Tests New Tax Regime calculations only.
"""

import pytest
import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path
import tempfile

from src.pfas.parsers.assets import (
    # Models
    Property,
    PropertyType,
    RentalIncome,
    RentalIncomeCalculation,
    SGBHolding,
    SGBInterest,
    REITHolding,
    REITDistribution,
    DistributionType,
    DividendRecord,
    DividendSummary,
    AssetIncomeSummary,
    # Managers
    RentalIncomeCalculator,
    RentalIncomeManager,
    SGBParser,
    SGBTracker,
    REITTracker,
    DividendTracker,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def temp_db():
    """Create temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn

    conn.close()
    Path(db_path).unlink(missing_ok=True)


# ============================================================================
# Rental Income Tests
# ============================================================================

class TestRentalIncomeCalculator:
    """Tests for RentalIncomeCalculator."""

    def test_let_out_property_calculation(self):
        """Test rental income calculation for let-out property."""
        calc = RentalIncomeCalculator()

        result = calc.calculate(
            gross_rent=Decimal("360000"),  # 30K/month
            municipal_tax=Decimal("10000"),
            home_loan_interest=Decimal("50000"),
            property_type=PropertyType.LET_OUT
        )

        assert result.gross_rent == Decimal("360000")
        assert result.municipal_tax == Decimal("10000")
        assert result.net_annual_value == Decimal("350000")  # 360K - 10K
        assert result.standard_deduction == Decimal("105000")  # 30% of 350K
        assert result.home_loan_interest == Decimal("50000")  # No cap for let-out
        assert result.income_from_hp == Decimal("195000")  # 350K - 105K - 50K

    def test_self_occupied_property_calculation(self):
        """Test rental income calculation for self-occupied property."""
        calc = RentalIncomeCalculator()

        result = calc.calculate(
            gross_rent=Decimal("0"),  # No rent for self-occupied
            municipal_tax=Decimal("0"),
            home_loan_interest=Decimal("300000"),  # More than 2L limit
            property_type=PropertyType.SELF_OCCUPIED
        )

        assert result.gross_rent == Decimal("0")
        assert result.net_annual_value == Decimal("0")
        assert result.standard_deduction == Decimal("0")
        assert result.home_loan_interest == Decimal("200000")  # Capped at 2L
        assert result.income_from_hp == Decimal("-200000")  # Loss

    def test_loss_setoff_calculation(self):
        """Test HP loss set-off calculation (max 2L)."""
        calc = RentalIncomeCalculator()

        # Loss within limit
        assert calc.calculate_loss_setoff(Decimal("-150000")) == Decimal("150000")

        # Loss exceeding limit
        assert calc.calculate_loss_setoff(Decimal("-300000")) == Decimal("200000")

        # No loss
        assert calc.calculate_loss_setoff(Decimal("50000")) == Decimal("0")


class TestRentalIncomeManager:
    """Tests for RentalIncomeManager."""

    def test_add_property(self, temp_db):
        """Test adding a property."""
        manager = RentalIncomeManager(temp_db)

        prop = Property(
            user_id=1,
            property_type=PropertyType.LET_OUT,
            address="123 Test Street",
            city="Hyderabad",
            tenant_name="Ramakrishnan",
        )

        prop_id = manager.add_property(prop)
        assert prop_id > 0

        retrieved = manager.get_property(prop_id)
        assert retrieved.address == "123 Test Street"
        assert retrieved.property_type == PropertyType.LET_OUT

    def test_add_rental_income(self, temp_db):
        """Test adding rental income."""
        manager = RentalIncomeManager(temp_db)

        # Add property first
        prop = Property(user_id=1, property_type=PropertyType.LET_OUT, address="Test")
        prop_id = manager.add_property(prop)

        # Add rental income
        income = RentalIncome(
            property_id=prop_id,
            financial_year="FY 2024-25",
            month="Apr-2024",
            gross_rent=Decimal("30000"),
        )
        manager.add_rental_income(income)

        # Verify
        total = manager.get_annual_rental_income(prop_id, "FY 2024-25")
        assert total == Decimal("30000")

    def test_hp_income_calculation(self, temp_db):
        """Test complete HP income calculation."""
        manager = RentalIncomeManager(temp_db)

        # Add property
        prop = Property(user_id=1, property_type=PropertyType.LET_OUT, address="Test")
        prop_id = manager.add_property(prop)

        # Add 12 months of rent
        for month in ["Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]:
            income = RentalIncome(
                property_id=prop_id,
                financial_year="FY 2024-25",
                month=f"{month}-2024",
                gross_rent=Decimal("30000"),
            )
            manager.add_rental_income(income)

        # Calculate HP income
        result = manager.calculate_hp_income(prop_id, "FY 2024-25")

        assert result.gross_rent == Decimal("270000")  # 9 months
        assert result.standard_deduction == Decimal("81000")  # 30%


# ============================================================================
# SGB Tests
# ============================================================================

class TestSGBParser:
    """Tests for SGBParser."""

    def test_extract_maturity_date(self):
        """Test maturity date extraction from series name."""
        parser = SGBParser()

        # Test various formats
        date1 = parser._extract_maturity_date("2.50% Sov. Gold Bond 8 Sep 28")
        assert date1 == date(2028, 9, 8)

        date2 = parser._extract_maturity_date("2.50% Sov. Gold Bond 5 Jan 29")
        assert date2 == date(2029, 1, 5)

        date3 = parser._extract_maturity_date("2.50% Sov. Gold Bond 14 Jul 28")
        assert date3 == date(2028, 7, 14)


class TestSGBTracker:
    """Tests for SGBTracker."""

    def test_save_and_get_holding(self, temp_db):
        """Test saving and retrieving SGB holding."""
        tracker = SGBTracker(temp_db)

        holding = SGBHolding(
            user_id=1,
            series="2.50% Sov. Gold Bond 8 Sep 28",
            quantity=125,
            issue_price=Decimal("5067"),
            interest_earned=Decimal("79887.39"),
        )

        tracker.save_holding(holding)
        holdings = tracker.get_holdings(user_id=1)

        assert len(holdings) == 1
        assert holdings[0].quantity == 125
        assert holdings[0].issue_price == Decimal("5067")

    def test_semi_annual_interest_calculation(self, temp_db):
        """Test SGB semi-annual interest calculation."""
        tracker = SGBTracker(temp_db)

        holding = SGBHolding(
            series="Test SGB",
            quantity=100,
            issue_price=Decimal("5000"),
        )

        interest = tracker.calculate_semi_annual_interest(holding)

        # Interest = 5000 * 100 * 2.5% / 2 = 6250
        assert interest == Decimal("6250")

    def test_add_interest_from_bank(self, temp_db):
        """Test adding SGB interest from bank statement."""
        tracker = SGBTracker(temp_db)

        tracker.add_interest_from_bank(
            amount=Decimal("6250"),
            payment_date=date(2024, 9, 8),
            financial_year="FY 2024-25",
        )

        total = tracker.get_total_interest_for_fy("FY 2024-25")
        assert total == Decimal("6250")

    def test_maturity_cg_exempt(self, temp_db):
        """Test that SGB CG at maturity is exempt."""
        tracker = SGBTracker(temp_db)

        holding = SGBHolding(
            series="Test SGB",
            quantity=100,
            issue_price=Decimal("5000"),
            issue_date=date(2016, 1, 1),  # 8+ years ago
        )

        gain, is_exempt = tracker.calculate_maturity_cg(holding, Decimal("7000"))

        # Gain = 7000 * 100 - 5000 * 100 = 200000
        assert gain == Decimal("200000")
        assert is_exempt is True  # Held till maturity


# ============================================================================
# REIT Tests
# ============================================================================

class TestREITTracker:
    """Tests for REITTracker."""

    def test_add_holding(self, temp_db):
        """Test adding REIT holding."""
        tracker = REITTracker(temp_db)

        holding = REITHolding(
            user_id=1,
            symbol="EMBASSY",
            name="Embassy Office Parks REIT",
            units=Decimal("100"),
            purchase_price=Decimal("350"),
            cost_basis=Decimal("35000"),
        )

        tracker.add_holding(holding)
        holdings = tracker.get_holdings(user_id=1)

        assert len(holdings) == 1
        assert holdings[0].symbol == "EMBASSY"

    def test_distribution_types(self, temp_db):
        """Test different REIT distribution types."""
        tracker = REITTracker(temp_db)

        # Add dividend distribution (exempt)
        tracker.add_distribution_from_bank(
            symbol="EMBASSY",
            amount=Decimal("500"),
            payment_date=date(2024, 6, 15),
            financial_year="FY 2024-25",
            distribution_type=DistributionType.DIVIDEND,
        )

        # Add interest distribution (taxable)
        tracker.add_distribution_from_bank(
            symbol="EMBASSY",
            amount=Decimal("300"),
            payment_date=date(2024, 9, 15),
            financial_year="FY 2024-25",
            distribution_type=DistributionType.INTEREST,
            tds=Decimal("30"),
        )

        summary = tracker.get_distribution_summary("FY 2024-25")

        assert summary["dividend"] == Decimal("500")  # Exempt
        assert summary["interest"] == Decimal("300")  # Taxable
        assert summary["tds"] == Decimal("30")

    def test_cost_reduction(self, temp_db):
        """Test cost basis reduction on OTHER distribution."""
        tracker = REITTracker(temp_db)

        # Add holding with cost basis
        holding = REITHolding(
            user_id=1,
            symbol="MINDSPACE",
            units=Decimal("100"),
            cost_basis=Decimal("30000"),
        )
        tracker.add_holding(holding)

        # Add capital reduction
        tracker.add_distribution_from_bank(
            symbol="MINDSPACE",
            amount=Decimal("5000"),
            payment_date=date(2024, 12, 15),
            financial_year="FY 2024-25",
            distribution_type=DistributionType.OTHER,
        )

        # Verify cost basis reduced
        holdings = tracker.get_holdings(user_id=1)
        mindspace = [h for h in holdings if h.symbol == "MINDSPACE"][0]
        assert mindspace.cost_basis == Decimal("25000")  # 30000 - 5000


# ============================================================================
# Dividend Tests
# ============================================================================

class TestDividendTracker:
    """Tests for DividendTracker."""

    def test_add_dividend(self, temp_db):
        """Test adding dividend record."""
        tracker = DividendTracker(temp_db)

        dividend = DividendRecord(
            user_id=1,
            symbol="TCS",
            payment_date=date(2024, 6, 15),
            financial_year="FY 2024-25",
            gross_amount=Decimal("1000"),
            tds_deducted=Decimal("100"),
            net_amount=Decimal("900"),
        )

        tracker.add_dividend(dividend)
        dividends = tracker.get_dividends_for_fy("FY 2024-25", user_id=1)

        assert len(dividends) == 1
        assert dividends[0].symbol == "TCS"

    def test_dividend_summary(self, temp_db):
        """Test dividend summary calculation."""
        tracker = DividendTracker(temp_db)

        # Add multiple dividends
        for symbol, amount in [("TCS", 1000), ("INFY", 500), ("HDFC", 750)]:
            tracker.add_dividend_from_bank(
                amount=Decimal(str(amount)),
                payment_date=date(2024, 6, 15),
                financial_year="FY 2024-25",
                symbol=symbol,
                tds=Decimal(str(amount * 0.1)),
                user_id=1,
            )

        summary = tracker.get_summary_for_fy("FY 2024-25", user_id=1)

        assert summary.total_dividend_income == Decimal("2250")
        assert summary.total_tds_deducted == Decimal("225")
        assert summary.dividend_count == 3

    def test_symbol_extraction(self, temp_db):
        """Test symbol extraction from bank description."""
        tracker = DividendTracker(temp_db)

        # Test known symbols
        assert tracker._extract_symbol("NEFT-DIV-RELIANCE INDUSTRIES") == "RELIANCE"
        assert tracker._extract_symbol("ACH-TCS DIVIDEND") == "TCS"

    def test_symbol_summary(self, temp_db):
        """Test dividend summary by symbol."""
        tracker = DividendTracker(temp_db)

        # Add multiple dividends from same company
        for i in range(3):
            tracker.add_dividend_from_bank(
                amount=Decimal("1000"),
                payment_date=date(2024, 6 + i, 15),
                financial_year="FY 2024-25",
                symbol="TCS",
                user_id=1,
            )

        summary = tracker.get_symbol_summary("FY 2024-25", user_id=1)

        assert "TCS" in summary
        assert summary["TCS"]["total_amount"] == Decimal("3000")
        assert summary["TCS"]["count"] == 3


# ============================================================================
# Asset Income Summary Tests
# ============================================================================

class TestAssetIncomeSummary:
    """Tests for AssetIncomeSummary model."""

    def test_total_taxable_income(self):
        """Test total taxable income calculation."""
        summary = AssetIncomeSummary(
            financial_year="FY 2024-25",
            sgb_interest=Decimal("100000"),
            reit_interest=Decimal("50000"),
            dividend_income=Decimal("200000"),
            reit_dividend=Decimal("30000"),  # Exempt
        )

        # Only SGB interest, REIT interest, and dividends are taxable
        assert summary.total_taxable_income == Decimal("350000")
        assert summary.total_exempt_income == Decimal("30000")

    def test_house_property_loss(self):
        """Test HP loss calculation."""
        summary = AssetIncomeSummary(
            financial_year="FY 2024-25",
            rental_net_income=Decimal("-150000"),  # Loss
        )

        assert summary.house_property_loss == Decimal("150000")

    def test_total_tds_credit(self):
        """Test total TDS credit calculation."""
        summary = AssetIncomeSummary(
            financial_year="FY 2024-25",
            tds_on_rent=Decimal("5000"),
            tds_on_sgb=Decimal("0"),
            tds_on_reit=Decimal("3000"),
            tds_on_dividend=Decimal("20000"),
        )

        assert summary.total_tds_credit == Decimal("28000")


# ============================================================================
# Integration Test
# ============================================================================

class TestAssetIntegration:
    """Integration tests for asset parsers."""

    def test_full_workflow(self, temp_db):
        """Test complete workflow with all asset types."""
        # Initialize all trackers
        rental_mgr = RentalIncomeManager(temp_db)
        sgb_tracker = SGBTracker(temp_db)
        reit_tracker = REITTracker(temp_db)
        dividend_tracker = DividendTracker(temp_db)

        # Add property and rental income
        prop = Property(user_id=1, property_type=PropertyType.LET_OUT, address="Test")
        prop_id = rental_mgr.add_property(prop)

        for i in range(12):
            rental_mgr.add_rental_income(RentalIncome(
                property_id=prop_id,
                financial_year="FY 2024-25",
                month=f"Month-{i+1}",
                gross_rent=Decimal("30000"),
            ))

        # Add SGB interest
        sgb_tracker.add_interest_from_bank(
            amount=Decimal("106153"),
            payment_date=date(2024, 9, 8),
            financial_year="FY 2024-25",
        )

        # Add dividends
        dividend_tracker.add_dividend_from_bank(
            amount=Decimal("50000"),
            payment_date=date(2024, 6, 15),
            financial_year="FY 2024-25",
            symbol="RELIANCE",
            user_id=1,
        )

        # Verify totals
        hp_calc = rental_mgr.calculate_hp_income(prop_id, "FY 2024-25")
        assert hp_calc.gross_rent == Decimal("360000")

        sgb_total = sgb_tracker.get_total_interest_for_fy("FY 2024-25")
        assert sgb_total == Decimal("106153")

        div_summary = dividend_tracker.get_summary_for_fy("FY 2024-25", user_id=1)
        assert div_summary.total_dividend_income == Decimal("50000")
