"""Tests for stock trade models."""

import pytest
from datetime import date
from decimal import Decimal

from pfas.parsers.stock.models import (
    StockTrade,
    TradeType,
    TradeCategory,
    CapitalGainsSummary,
    ParseResult
)


class TestStockTrade:
    """Tests for StockTrade model."""

    def test_trade_creation(self):
        """Test creating a stock trade."""
        trade = StockTrade(
            symbol="RELIANCE",
            trade_date=date(2024, 7, 15),
            trade_type=TradeType.BUY,
            quantity=10,
            price=Decimal("2500.50"),
            amount=Decimal("25005.00")
        )

        assert trade.symbol == "RELIANCE"
        assert trade.quantity == 10
        assert trade.price == Decimal("2500.50")
        assert trade.amount == Decimal("25005.00")

    def test_net_amount_calculation_buy(self):
        """Test net amount calculation for buy trade."""
        trade = StockTrade(
            symbol="TCS",
            trade_date=date(2024, 7, 15),
            trade_type=TradeType.BUY,
            quantity=5,
            price=Decimal("3500.00"),
            amount=Decimal("17500.00"),
            brokerage=Decimal("10.00"),
            stt=Decimal("5.00")
        )

        # Net amount = amount + charges for BUY
        assert trade.net_amount == Decimal("17515.00")

    def test_net_amount_calculation_sell(self):
        """Test net amount calculation for sell trade."""
        trade = StockTrade(
            symbol="TCS",
            trade_date=date(2024, 7, 15),
            trade_type=TradeType.SELL,
            quantity=5,
            price=Decimal("3600.00"),
            amount=Decimal("18000.00"),
            brokerage=Decimal("10.00"),
            stt=Decimal("5.00")
        )

        # Net amount = amount - charges for SELL
        assert trade.net_amount == Decimal("17985.00")

    def test_holding_period_calculation(self):
        """Test holding period calculation."""
        trade = StockTrade(
            symbol="INFY",
            trade_date=date(2024, 7, 15),
            trade_type=TradeType.SELL,
            quantity=10,
            price=Decimal("1500.00"),
            amount=Decimal("15000.00"),
            buy_date=date(2024, 1, 1)
        )

        holding_days = trade.calculate_holding_period()
        assert holding_days == (date(2024, 7, 15) - date(2024, 1, 1)).days

    def test_is_ltcg_true(self):
        """Test LTCG check for >12 months."""
        trade = StockTrade(
            symbol="HDFC",
            trade_date=date(2024, 7, 15),
            trade_type=TradeType.SELL,
            quantity=10,
            price=Decimal("2800.00"),
            amount=Decimal("28000.00"),
            buy_date=date(2023, 1, 1),  # > 12 months
            holding_period_days=561
        )

        assert trade.is_ltcg() is True

    def test_is_ltcg_false(self):
        """Test LTCG check for <12 months."""
        trade = StockTrade(
            symbol="HDFC",
            trade_date=date(2024, 7, 15),
            trade_type=TradeType.SELL,
            quantity=10,
            price=Decimal("2800.00"),
            amount=Decimal("28000.00"),
            buy_date=date(2024, 3, 1),  # < 12 months
            holding_period_days=136
        )

        assert trade.is_ltcg() is False

    def test_intraday_trade(self):
        """Test intraday trade properties."""
        trade = StockTrade(
            symbol="WIPRO",
            trade_date=date(2024, 7, 15),
            trade_type=TradeType.SELL,
            quantity=50,
            price=Decimal("450.00"),
            amount=Decimal("22500.00"),
            trade_category=TradeCategory.INTRADAY
        )

        assert trade.is_intraday is True
        assert trade.is_delivery is False
        assert trade.is_fno is False


class TestCapitalGainsSummary:
    """Tests for CapitalGainsSummary."""

    def test_taxable_amounts_calculation(self):
        """Test calculation of taxable STCG and LTCG."""
        summary = CapitalGainsSummary(
            financial_year="2024-25",
            trade_category=TradeCategory.DELIVERY,
            stcg_amount=Decimal("50000"),
            ltcg_amount=Decimal("300000")
        )

        summary.calculate_taxable_amounts()

        # STCG is fully taxable
        assert summary.taxable_stcg == Decimal("50000")

        # LTCG exemption of ₹1.25L
        assert summary.taxable_ltcg == Decimal("175000")  # 300000 - 125000

    def test_ltcg_below_exemption(self):
        """Test LTCG below exemption limit."""
        summary = CapitalGainsSummary(
            financial_year="2024-25",
            trade_category=TradeCategory.DELIVERY,
            ltcg_amount=Decimal("100000")
        )

        summary.calculate_taxable_amounts()

        # LTCG < ₹1.25L, fully exempt
        assert summary.taxable_ltcg == Decimal("0")

    def test_tax_calculation(self):
        """Test total tax calculation."""
        summary = CapitalGainsSummary(
            financial_year="2024-25",
            trade_category=TradeCategory.DELIVERY,
            stcg_amount=Decimal("100000"),
            ltcg_amount=Decimal("200000"),
            stcg_tax_rate=Decimal("20"),
            ltcg_tax_rate=Decimal("12.5")
        )

        summary.calculate_taxable_amounts()
        tax = summary.calculate_tax()

        # STCG tax: 100000 * 20% = 20000
        # LTCG taxable: 200000 - 125000 = 75000
        # LTCG tax: 75000 * 12.5% = 9375
        # Total: 29375
        assert tax == Decimal("29375.00")


class TestParseResult:
    """Tests for ParseResult."""

    def test_parse_result_success(self):
        """Test successful parse result."""
        result = ParseResult(success=True, source_file="test.xlsx")

        assert result.success is True
        assert result.source_file == "test.xlsx"
        assert len(result.trades) == 0
        assert len(result.errors) == 0

    def test_add_error(self):
        """Test adding error marks result as failed."""
        result = ParseResult(success=True)
        result.add_error("Test error")

        assert result.success is False
        assert len(result.errors) == 1
        assert result.errors[0] == "Test error"

    def test_add_warning(self):
        """Test adding warning."""
        result = ParseResult(success=True)
        result.add_warning("Test warning")

        assert result.success is True  # Warnings don't fail
        assert len(result.warnings) == 1
        assert result.warnings[0] == "Test warning"
