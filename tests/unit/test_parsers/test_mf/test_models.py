"""Tests for mutual fund data models."""

import pytest
from datetime import date
from decimal import Decimal

from pfas.parsers.mf.models import (
    MFScheme, MFTransaction, AssetClass, TransactionType, ParseResult
)


class TestAssetClass:
    """Tests for AssetClass enum."""

    def test_asset_class_values(self):
        """Test that all asset classes exist."""
        assert AssetClass.EQUITY.value == "EQUITY"
        assert AssetClass.DEBT.value == "DEBT"
        assert AssetClass.HYBRID.value == "HYBRID"
        assert AssetClass.OTHER.value == "OTHER"


class TestTransactionType:
    """Tests for TransactionType enum."""

    def test_transaction_type_values(self):
        """Test that all transaction types exist."""
        assert TransactionType.PURCHASE.value == "PURCHASE"
        assert TransactionType.REDEMPTION.value == "REDEMPTION"
        assert TransactionType.SWITCH_IN.value == "SWITCH_IN"
        assert TransactionType.SWITCH_OUT.value == "SWITCH_OUT"
        assert TransactionType.DIVIDEND.value == "DIVIDEND"
        assert TransactionType.DIVIDEND_REINVEST.value == "DIVIDEND_REINVEST"


class TestMFScheme:
    """Tests for MFScheme dataclass."""

    def test_scheme_creation(self):
        """Test basic scheme creation."""
        scheme = MFScheme(
            name="SBI Bluechip Fund Direct Growth",
            amc_name="SBI Mutual Fund",
            isin="INF200K01123",
            asset_class=AssetClass.EQUITY
        )

        assert scheme.name == "SBI Bluechip Fund Direct Growth"
        assert scheme.amc_name == "SBI Mutual Fund"
        assert scheme.isin == "INF200K01123"
        assert scheme.asset_class == AssetClass.EQUITY

    def test_scheme_auto_classification(self):
        """Test that schemes auto-classify based on name."""
        # Equity fund
        equity_scheme = MFScheme(
            name="HDFC Top 100 Fund",
            amc_name="HDFC Mutual Fund"
        )
        assert equity_scheme.asset_class == AssetClass.EQUITY

        # Debt fund
        debt_scheme = MFScheme(
            name="Kotak Corporate Bond Fund",
            amc_name="Kotak Mutual Fund"
        )
        assert debt_scheme.asset_class == AssetClass.DEBT


class TestMFTransaction:
    """Tests for MFTransaction dataclass."""

    def test_purchase_transaction(self):
        """Test creating a purchase transaction."""
        scheme = MFScheme(
            name="Test Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.EQUITY
        )

        txn = MFTransaction(
            folio_number="12345/01",
            scheme=scheme,
            transaction_type=TransactionType.PURCHASE,
            date=date(2024, 6, 15),
            units=Decimal("100"),
            nav=Decimal("50"),
            amount=Decimal("5000")
        )

        assert txn.folio_number == "12345/01"
        assert txn.transaction_type == TransactionType.PURCHASE
        assert txn.units == Decimal("100")
        assert txn.nav == Decimal("50")
        assert txn.amount == Decimal("5000")

    def test_holding_period_days_redemption(self):
        """Test holding period calculation for redemption."""
        scheme = MFScheme(
            name="Test Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.EQUITY
        )

        txn = MFTransaction(
            folio_number="12345/01",
            scheme=scheme,
            transaction_type=TransactionType.REDEMPTION,
            date=date(2024, 7, 15),
            units=Decimal("100"),
            nav=Decimal("60"),
            amount=Decimal("6000"),
            purchase_date=date(2024, 1, 15)
        )

        # Should be ~182 days (Jan 15 to Jul 15)
        assert txn.holding_period_days == 182

    def test_holding_period_days_purchase(self):
        """Test that purchase transactions have no holding period."""
        scheme = MFScheme(
            name="Test Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.EQUITY
        )

        txn = MFTransaction(
            folio_number="12345/01",
            scheme=scheme,
            transaction_type=TransactionType.PURCHASE,
            date=date(2024, 6, 15),
            units=Decimal("100"),
            nav=Decimal("50"),
            amount=Decimal("5000")
        )

        assert txn.holding_period_days is None

    def test_is_long_term_equity_short(self):
        """Test STCG for equity <12 months."""
        scheme = MFScheme(
            name="Test Equity Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.EQUITY
        )

        txn = MFTransaction(
            folio_number="12345/01",
            scheme=scheme,
            transaction_type=TransactionType.REDEMPTION,
            date=date(2024, 7, 15),
            units=Decimal("100"),
            nav=Decimal("60"),
            amount=Decimal("6000"),
            purchase_date=date(2024, 3, 1)  # ~136 days
        )

        assert txn.is_long_term is False

    def test_is_long_term_equity_long(self):
        """Test LTCG for equity >12 months."""
        scheme = MFScheme(
            name="Test Equity Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.EQUITY
        )

        txn = MFTransaction(
            folio_number="12345/01",
            scheme=scheme,
            transaction_type=TransactionType.REDEMPTION,
            date=date(2024, 7, 15),
            units=Decimal("100"),
            nav=Decimal("60"),
            amount=Decimal("6000"),
            purchase_date=date(2023, 1, 1)  # >12 months
        )

        assert txn.is_long_term is True

    def test_is_long_term_debt(self):
        """Test LTCG for debt >24 months."""
        scheme = MFScheme(
            name="Test Debt Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.DEBT
        )

        # Short-term (<24 months)
        txn_short = MFTransaction(
            folio_number="12345/01",
            scheme=scheme,
            transaction_type=TransactionType.REDEMPTION,
            date=date(2024, 7, 15),
            units=Decimal("100"),
            nav=Decimal("60"),
            amount=Decimal("6000"),
            purchase_date=date(2023, 1, 1)  # ~18 months
        )
        assert txn_short.is_long_term is False

        # Long-term (>24 months)
        txn_long = MFTransaction(
            folio_number="12345/01",
            scheme=scheme,
            transaction_type=TransactionType.REDEMPTION,
            date=date(2024, 7, 15),
            units=Decimal("100"),
            nav=Decimal("60"),
            amount=Decimal("6000"),
            purchase_date=date(2022, 1, 1)  # >24 months
        )
        assert txn_long.is_long_term is True

    def test_is_grandfathered_true(self):
        """Test grandfathering detection for pre-31-Jan-2018."""
        scheme = MFScheme(
            name="Test Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.EQUITY
        )

        txn = MFTransaction(
            folio_number="12345/01",
            scheme=scheme,
            transaction_type=TransactionType.REDEMPTION,
            date=date(2024, 7, 15),
            units=Decimal("100"),
            nav=Decimal("60"),
            amount=Decimal("6000"),
            purchase_date=date(2017, 6, 1),  # Before 31-Jan-2018
            grandfathered_value=Decimal("5500")
        )

        assert txn.is_grandfathered is True

    def test_is_grandfathered_false_after_date(self):
        """Test no grandfathering for post-31-Jan-2018."""
        scheme = MFScheme(
            name="Test Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.EQUITY
        )

        txn = MFTransaction(
            folio_number="12345/01",
            scheme=scheme,
            transaction_type=TransactionType.REDEMPTION,
            date=date(2024, 7, 15),
            units=Decimal("100"),
            nav=Decimal("60"),
            amount=Decimal("6000"),
            purchase_date=date(2018, 6, 1),  # After 31-Jan-2018
            grandfathered_value=Decimal("5500")
        )

        assert txn.is_grandfathered is False


class TestParseResult:
    """Tests for ParseResult."""

    def test_successful_result(self):
        """Test creating a successful parse result."""
        result = ParseResult(success=True, source_file="test.xlsx")

        assert result.success is True
        assert result.source_file == "test.xlsx"
        assert len(result.transactions) == 0
        assert len(result.errors) == 0
        assert len(result.warnings) == 0

    def test_add_error(self):
        """Test adding errors."""
        result = ParseResult(success=True)
        result.add_error("Test error")

        assert result.success is False
        assert len(result.errors) == 1
        assert result.errors[0] == "Test error"

    def test_add_warning(self):
        """Test adding warnings."""
        result = ParseResult(success=True)
        result.add_warning("Test warning")

        assert result.success is True  # Warnings don't change success
        assert len(result.warnings) == 1
        assert result.warnings[0] == "Test warning"
