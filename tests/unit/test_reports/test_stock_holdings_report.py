"""Tests for Stock Holdings Report Generator."""

import pytest
from datetime import date
from decimal import Decimal
from pathlib import Path
import tempfile

from pfas.reports.stock_holdings_report import StockHoldingsReport, HoldingsReportData
from pfas.parsers.stock.models import StockTrade, StockHolding, TradeType, TradeCategory


class TestHoldingsReportData:
    """Tests for HoldingsReportData dataclass."""

    def test_calculate_totals(self):
        """Test total cost calculation."""
        report = HoldingsReportData(
            as_of_date=date(2024, 12, 31),
            holdings=[
                StockHolding(
                    symbol="INFY",
                    isin="INE009A01021",
                    quantity=100,
                    average_cost=Decimal("1500"),
                    total_cost=Decimal("150000"),
                ),
                StockHolding(
                    symbol="TCS",
                    isin="INE467B01029",
                    quantity=50,
                    average_cost=Decimal("3000"),
                    total_cost=Decimal("150000"),
                ),
            ]
        )

        report.calculate_totals()

        assert report.total_cost == Decimal("300000")

    def test_empty_holdings(self):
        """Test report with no holdings."""
        report = HoldingsReportData(as_of_date=date(2024, 12, 31))
        report.calculate_totals()

        assert report.total_cost == Decimal("0")
        assert len(report.holdings) == 0


class TestStockHoldingsReport:
    """Tests for StockHoldingsReport class."""

    def test_initialization(self, db_connection):
        """Test report generator can be initialized."""
        report = StockHoldingsReport(db_connection)
        assert report.conn is not None

    def test_generate_empty(self, db_connection, sample_user):
        """Test generate with no trades returns empty report."""
        report = StockHoldingsReport(db_connection)
        result = report.generate(sample_user["id"])

        assert result.as_of_date is not None
        assert len(result.holdings) == 0
        assert result.total_cost == Decimal("0")


class TestStockHoldingsReportFromTrades:
    """Tests for generating holdings from parsed trades."""

    def test_single_buy(self, db_connection):
        """Test holdings with single buy trade."""
        report = StockHoldingsReport(db_connection)

        trades = [
            StockTrade(
                symbol="INFY",
                isin="INE009A01021",
                trade_date=date(2024, 1, 15),
                trade_type=TradeType.BUY,
                quantity=100,
                price=Decimal("1500"),
                amount=Decimal("150000"),
                net_amount=Decimal("150000"),
                trade_category=TradeCategory.DELIVERY,
            )
        ]

        result = report.generate_from_trades(trades)

        assert len(result.holdings) == 1
        assert result.holdings[0].symbol == "INFY"
        assert result.holdings[0].quantity == 100
        assert result.holdings[0].average_cost == Decimal("1500")
        assert result.holdings[0].total_cost == Decimal("150000")

    def test_multiple_buys_same_stock(self, db_connection):
        """Test holdings with multiple buys of same stock (averaging)."""
        report = StockHoldingsReport(db_connection)

        trades = [
            StockTrade(
                symbol="INFY",
                isin="INE009A01021",
                trade_date=date(2024, 1, 15),
                trade_type=TradeType.BUY,
                quantity=100,
                price=Decimal("1500"),
                amount=Decimal("150000"),
                net_amount=Decimal("150000"),
                trade_category=TradeCategory.DELIVERY,
            ),
            StockTrade(
                symbol="INFY",
                isin="INE009A01021",
                trade_date=date(2024, 2, 15),
                trade_type=TradeType.BUY,
                quantity=100,
                price=Decimal("1600"),
                amount=Decimal("160000"),
                net_amount=Decimal("160000"),
                trade_category=TradeCategory.DELIVERY,
            ),
        ]

        result = report.generate_from_trades(trades)

        assert len(result.holdings) == 1
        assert result.holdings[0].quantity == 200
        assert result.holdings[0].total_cost == Decimal("310000")
        assert result.holdings[0].average_cost == Decimal("1550")

    def test_buy_and_partial_sell_fifo(self, db_connection):
        """Test FIFO cost basis with partial sell."""
        report = StockHoldingsReport(db_connection)

        trades = [
            StockTrade(
                symbol="INFY",
                isin="INE009A01021",
                trade_date=date(2024, 1, 15),
                trade_type=TradeType.BUY,
                quantity=100,
                price=Decimal("1500"),
                amount=Decimal("150000"),
                net_amount=Decimal("150000"),
                trade_category=TradeCategory.DELIVERY,
            ),
            StockTrade(
                symbol="INFY",
                isin="INE009A01021",
                trade_date=date(2024, 2, 15),
                trade_type=TradeType.BUY,
                quantity=100,
                price=Decimal("1600"),
                amount=Decimal("160000"),
                net_amount=Decimal("160000"),
                trade_category=TradeCategory.DELIVERY,
            ),
            StockTrade(
                symbol="INFY",
                isin="INE009A01021",
                trade_date=date(2024, 6, 15),
                trade_type=TradeType.SELL,
                quantity=50,
                price=Decimal("1700"),
                amount=Decimal("85000"),
                net_amount=Decimal("85000"),
                trade_category=TradeCategory.DELIVERY,
            ),
        ]

        result = report.generate_from_trades(trades)

        assert len(result.holdings) == 1
        assert result.holdings[0].quantity == 150
        # FIFO: 50 sold from first lot at 1500
        # Remaining: 50 @ 1500 + 100 @ 1600 = 75000 + 160000 = 235000
        assert result.holdings[0].total_cost == Decimal("235000")

    def test_complete_sell(self, db_connection):
        """Test holdings after selling all shares."""
        report = StockHoldingsReport(db_connection)

        trades = [
            StockTrade(
                symbol="INFY",
                isin="INE009A01021",
                trade_date=date(2024, 1, 15),
                trade_type=TradeType.BUY,
                quantity=100,
                price=Decimal("1500"),
                amount=Decimal("150000"),
                net_amount=Decimal("150000"),
                trade_category=TradeCategory.DELIVERY,
            ),
            StockTrade(
                symbol="INFY",
                isin="INE009A01021",
                trade_date=date(2024, 6, 15),
                trade_type=TradeType.SELL,
                quantity=100,
                price=Decimal("1700"),
                amount=Decimal("170000"),
                net_amount=Decimal("170000"),
                trade_category=TradeCategory.DELIVERY,
            ),
        ]

        result = report.generate_from_trades(trades)

        # No holdings after selling all
        assert len(result.holdings) == 0

    def test_multiple_stocks(self, db_connection):
        """Test holdings with multiple different stocks."""
        report = StockHoldingsReport(db_connection)

        trades = [
            StockTrade(
                symbol="INFY",
                isin="INE009A01021",
                trade_date=date(2024, 1, 15),
                trade_type=TradeType.BUY,
                quantity=100,
                price=Decimal("1500"),
                amount=Decimal("150000"),
                net_amount=Decimal("150000"),
                trade_category=TradeCategory.DELIVERY,
            ),
            StockTrade(
                symbol="TCS",
                isin="INE467B01029",
                trade_date=date(2024, 2, 15),
                trade_type=TradeType.BUY,
                quantity=50,
                price=Decimal("3500"),
                amount=Decimal("175000"),
                net_amount=Decimal("175000"),
                trade_category=TradeCategory.DELIVERY,
            ),
        ]

        result = report.generate_from_trades(trades)

        assert len(result.holdings) == 2
        assert result.total_cost == Decimal("325000")

        # Holdings are sorted by symbol
        assert result.holdings[0].symbol == "INFY"
        assert result.holdings[1].symbol == "TCS"

    def test_as_of_date_filter(self, db_connection):
        """Test holdings as of a specific date."""
        report = StockHoldingsReport(db_connection)

        trades = [
            StockTrade(
                symbol="INFY",
                isin="INE009A01021",
                trade_date=date(2024, 1, 15),
                trade_type=TradeType.BUY,
                quantity=100,
                price=Decimal("1500"),
                amount=Decimal("150000"),
                net_amount=Decimal("150000"),
                trade_category=TradeCategory.DELIVERY,
            ),
            StockTrade(
                symbol="INFY",
                isin="INE009A01021",
                trade_date=date(2024, 6, 15),
                trade_type=TradeType.BUY,
                quantity=100,
                price=Decimal("1600"),
                amount=Decimal("160000"),
                net_amount=Decimal("160000"),
                trade_category=TradeCategory.DELIVERY,
            ),
        ]

        # Holdings as of Feb 2024 (only first buy)
        result = report.generate_from_trades(trades, as_of_date=date(2024, 2, 28))

        assert len(result.holdings) == 1
        assert result.holdings[0].quantity == 100
        assert result.holdings[0].total_cost == Decimal("150000")

    def test_intraday_trades_excluded(self, db_connection):
        """Test that intraday trades are excluded from holdings."""
        report = StockHoldingsReport(db_connection)

        trades = [
            StockTrade(
                symbol="INFY",
                isin="INE009A01021",
                trade_date=date(2024, 1, 15),
                trade_type=TradeType.BUY,
                quantity=100,
                price=Decimal("1500"),
                amount=Decimal("150000"),
                net_amount=Decimal("150000"),
                trade_category=TradeCategory.DELIVERY,
            ),
            StockTrade(
                symbol="SBIN",
                isin="INE062A01020",
                trade_date=date(2024, 2, 15),
                trade_type=TradeType.BUY,
                quantity=500,
                price=Decimal("800"),
                amount=Decimal("400000"),
                net_amount=Decimal("400000"),
                trade_category=TradeCategory.INTRADAY,  # Should be excluded
            ),
        ]

        result = report.generate_from_trades(trades)

        assert len(result.holdings) == 1
        assert result.holdings[0].symbol == "INFY"


class TestStockHoldingsReportExcel:
    """Tests for Excel export functionality."""

    def test_export_excel_creates_file(self, db_connection):
        """Test Excel export creates a file."""
        report_gen = StockHoldingsReport(db_connection)

        report = HoldingsReportData(
            as_of_date=date(2024, 12, 31),
            holdings=[
                StockHolding(
                    symbol="INFY",
                    isin="INE009A01021",
                    quantity=100,
                    average_cost=Decimal("1500"),
                    total_cost=Decimal("150000"),
                    first_purchase_date=date(2024, 1, 15),
                ),
            ]
        )
        report.calculate_totals()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "holdings.xlsx"
            result = report_gen.export_excel(report, output_path)

            assert result.exists()
            assert result.suffix == ".xlsx"

    def test_export_excel_empty_holdings(self, db_connection):
        """Test Excel export with no holdings."""
        report_gen = StockHoldingsReport(db_connection)

        report = HoldingsReportData(as_of_date=date(2024, 12, 31))
        report.calculate_totals()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "holdings.xlsx"
            result = report_gen.export_excel(report, output_path)

            assert result.exists()
