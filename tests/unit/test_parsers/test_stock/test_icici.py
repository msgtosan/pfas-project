"""Tests for ICICI Direct Capital Gains parser."""

import pytest
from datetime import date
from decimal import Decimal
from pathlib import Path
import tempfile

from pfas.parsers.stock.icici import ICICIDirectParser
from pfas.parsers.stock.models import TradeType, TradeCategory, ParseResult


class TestICICIDirectParser:
    """Tests for ICICI Direct parser."""

    def test_parser_initialization(self, db_connection):
        """Test parser can be initialized."""
        parser = ICICIDirectParser(db_connection)
        assert parser.conn is not None

    def test_parse_nonexistent_file(self, db_connection):
        """Test parsing nonexistent file returns error."""
        parser = ICICIDirectParser(db_connection)
        result = parser.parse(Path("/nonexistent/file.csv"))

        assert result.success is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0].lower()

    def test_parse_unsupported_format(self, db_connection, tmp_path):
        """Test parsing unsupported file format."""
        parser = ICICIDirectParser(db_connection)

        # Create a dummy file with unsupported extension
        test_file = tmp_path / "test.xlsx"
        test_file.write_text("dummy content")

        result = parser.parse(test_file)

        assert result.success is False
        assert len(result.errors) > 0
        assert "Unsupported file format" in result.errors[0]

    def test_parse_date_icici_format(self, db_connection):
        """Test date parsing from ICICI format (DD-MMM-YY)."""
        parser = ICICIDirectParser(db_connection)

        # ICICI format
        date1 = parser._parse_date("21-May-24")
        assert date1 == date(2024, 5, 21)

        date2 = parser._parse_date("15-Jan-23")
        assert date2 == date(2023, 1, 15)

    def test_parse_date_iso_format(self, db_connection):
        """Test date parsing from ISO format."""
        parser = ICICIDirectParser(db_connection)

        date1 = parser._parse_date("2024-07-15")
        assert date1 == date(2024, 7, 15)

    def test_parse_date_object(self, db_connection):
        """Test date parsing from date object."""
        parser = ICICIDirectParser(db_connection)

        date1 = parser._parse_date(date(2024, 6, 15))
        assert date1 == date(2024, 6, 15)

    def test_parse_date_invalid(self, db_connection):
        """Test date parsing for invalid values."""
        parser = ICICIDirectParser(db_connection)

        assert parser._parse_date(None) is None
        assert parser._parse_date("") is None
        assert parser._parse_date("invalid-date") is None

    def test_to_decimal_valid(self, db_connection):
        """Test Decimal conversion for valid values."""
        parser = ICICIDirectParser(db_connection)

        assert parser._to_decimal(123.45) == Decimal("123.45")
        assert parser._to_decimal("678.90") == Decimal("678.90")
        assert parser._to_decimal(100) == Decimal("100")

    def test_to_decimal_with_commas(self, db_connection):
        """Test Decimal conversion handles Indian number format with commas."""
        parser = ICICIDirectParser(db_connection)

        assert parser._to_decimal("1,23,456.78") == Decimal("123456.78")
        assert parser._to_decimal("50,000") == Decimal("50000")

    def test_to_decimal_invalid(self, db_connection):
        """Test Decimal conversion for invalid values."""
        parser = ICICIDirectParser(db_connection)

        assert parser._to_decimal(None) == Decimal("0")
        assert parser._to_decimal("") == Decimal("0")

    def test_get_or_create_broker_new(self, db_connection):
        """Test creating a new broker."""
        parser = ICICIDirectParser(db_connection)

        broker_id = parser._get_or_create_broker("ICICI Direct")
        assert broker_id > 0

        # Verify it was created
        cursor = db_connection.execute(
            "SELECT name FROM stock_brokers WHERE id = ?", (broker_id,)
        )
        row = cursor.fetchone()
        assert row["name"] == "ICICI Direct"

    def test_get_or_create_broker_existing(self, db_connection):
        """Test getting existing broker."""
        parser = ICICIDirectParser(db_connection)

        # Create first time
        broker_id1 = parser._get_or_create_broker("ICICI Direct")

        # Get same broker again
        broker_id2 = parser._get_or_create_broker("ICICI Direct")

        # Should return same ID
        assert broker_id1 == broker_id2

    def test_section_markers(self, db_connection):
        """Test section marker constants."""
        parser = ICICIDirectParser(db_connection)

        assert parser.STCG_MARKER == "Short Term Capital Gain (STT paid)"
        assert parser.LTCG_MARKER == "Long Term Capital Gain (STT paid)"
        assert "Total" in parser.SKIP_MARKERS
        assert "Grand Total" in parser.SKIP_MARKERS


class TestICICIDirectParserCapitalGains:
    """Tests for ICICI Direct parser capital gains calculation."""

    def test_calculate_capital_gains_empty(self, db_connection):
        """Test capital gains calculation with empty trades."""
        parser = ICICIDirectParser(db_connection)

        from pfas.parsers.stock.models import CapitalGainsSummary
        summary = parser.calculate_capital_gains([], "2024-25")

        assert summary.stcg_amount == Decimal("0")
        assert summary.ltcg_amount == Decimal("0")

    def test_calculate_capital_gains_stcg_only(self, db_connection):
        """Test capital gains calculation with STCG trades only."""
        parser = ICICIDirectParser(db_connection)

        from pfas.parsers.stock.models import StockTrade
        trades = [
            StockTrade(
                symbol="TEST",
                trade_date=date(2024, 5, 21),
                trade_type=TradeType.SELL,
                quantity=10,
                price=Decimal("100"),
                amount=Decimal("1000"),
                is_long_term=False,
                capital_gain=Decimal("500")
            ),
            StockTrade(
                symbol="TEST2",
                trade_date=date(2024, 5, 22),
                trade_type=TradeType.SELL,
                quantity=5,
                price=Decimal("200"),
                amount=Decimal("1000"),
                is_long_term=False,
                capital_gain=Decimal("300")
            ),
        ]

        summary = parser.calculate_capital_gains(trades, "2024-25")

        assert summary.stcg_amount == Decimal("800")
        assert summary.ltcg_amount == Decimal("0")
        assert summary.taxable_stcg == Decimal("800")

    def test_calculate_capital_gains_ltcg_only(self, db_connection):
        """Test capital gains calculation with LTCG trades only."""
        parser = ICICIDirectParser(db_connection)

        from pfas.parsers.stock.models import StockTrade
        trades = [
            StockTrade(
                symbol="TEST",
                trade_date=date(2024, 5, 21),
                trade_type=TradeType.SELL,
                quantity=10,
                price=Decimal("100"),
                amount=Decimal("1000"),
                is_long_term=True,
                capital_gain=Decimal("200000")
            ),
        ]

        summary = parser.calculate_capital_gains(trades, "2024-25")

        assert summary.stcg_amount == Decimal("0")
        assert summary.ltcg_amount == Decimal("200000")
        # Taxable LTCG = 200000 - 125000 exemption = 75000
        assert summary.taxable_ltcg == Decimal("75000")

    def test_calculate_capital_gains_ltcg_below_exemption(self, db_connection):
        """Test LTCG below exemption limit."""
        parser = ICICIDirectParser(db_connection)

        from pfas.parsers.stock.models import StockTrade
        trades = [
            StockTrade(
                symbol="TEST",
                trade_date=date(2024, 5, 21),
                trade_type=TradeType.SELL,
                quantity=10,
                price=Decimal("100"),
                amount=Decimal("1000"),
                is_long_term=True,
                capital_gain=Decimal("50000")
            ),
        ]

        summary = parser.calculate_capital_gains(trades, "2024-25")

        assert summary.ltcg_amount == Decimal("50000")
        # Below ₹1.25L exemption, so no taxable LTCG
        assert summary.taxable_ltcg == Decimal("0")


class TestICICIDirectParserDatabaseOperations:
    """Tests for ICICI Direct parser database operations."""

    def test_save_to_db_empty_result(self, db_connection):
        """Test saving empty result returns 0."""
        parser = ICICIDirectParser(db_connection)

        result = ParseResult(success=True)
        count = parser.save_to_db(result, user_id=1)
        assert count == 0

    def test_save_to_db_failed_result(self, db_connection):
        """Test saving failed result returns 0."""
        parser = ICICIDirectParser(db_connection)

        result = ParseResult(success=False)
        count = parser.save_to_db(result, user_id=1)
        assert count == 0


class TestICICIDirectParserCSVParsing:
    """Tests for ICICI Direct CSV parsing logic."""

    def test_parse_valid_csv(self, db_connection, tmp_path):
        """Test parsing a valid ICICI Direct CSV file."""
        parser = ICICIDirectParser(db_connection)

        # Create a minimal valid CSV
        csv_content = """Account,8500480693
Name,Test User
Capital Gain,2024-2025
Stock Symbol,ISIN,Qty,Sale Date,Sale Rate,Sale Value,Sale Expenses,Purchase Date,Purchase Rate,Price as on 31st Jan 2018,Purchase Price Considered,Purchase Value,Purchase Expenses,Profit/Loss(-)
Short Term Capital Gain (STT paid),,,,,,,,,,,,
TESTSTOCK,INE123456789,10,21-May-24,100,1000,10,15-Apr-24,80,,,800,5,185
Total,,,,,,,,,,,,,185
"""
        test_file = tmp_path / "test_icici.csv"
        test_file.write_text(csv_content)

        result = parser.parse(test_file)

        assert result.success is True
        assert len(result.trades) == 1

        trade = result.trades[0]
        assert trade.symbol == "TESTSTOCK"
        assert trade.isin == "INE123456789"
        assert trade.quantity == 10
        assert trade.trade_type == TradeType.SELL
        assert trade.is_long_term is False
        assert trade.capital_gain == Decimal("185")

    def test_parse_csv_with_ltcg_section(self, db_connection, tmp_path):
        """Test parsing CSV with LTCG section."""
        parser = ICICIDirectParser(db_connection)

        csv_content = """Account,8500480693
Name,Test User
Capital Gain,2024-2025
Stock Symbol,ISIN,Qty,Sale Date,Sale Rate,Sale Value,Sale Expenses,Purchase Date,Purchase Rate,Price as on 31st Jan 2018,Purchase Price Considered,Purchase Value,Purchase Expenses,Profit/Loss(-)
Short Term Capital Gain (STT paid),,,,,,,,,,,,
STCG_STOCK,INE111111111,5,21-May-24,200,1000,10,15-Apr-24,150,,,750,5,235
Total,,,,,,,,,,,,,235
Long Term Capital Gain (STT paid),,,,,,,,,,,,
LTCG_STOCK,INE222222222,10,16-May-24,300,3000,20,15-Jan-22,100,,,1000,10,1970
Total,,,,,,,,,,,,,1970
Grand Total,,,,,,,,,,,,,2205
"""
        test_file = tmp_path / "test_icici_ltcg.csv"
        test_file.write_text(csv_content)

        result = parser.parse(test_file)

        assert result.success is True
        assert len(result.trades) == 2

        # First trade should be STCG
        stcg_trade = result.trades[0]
        assert stcg_trade.symbol == "STCG_STOCK"
        assert stcg_trade.is_long_term is False

        # Second trade should be LTCG
        ltcg_trade = result.trades[1]
        assert ltcg_trade.symbol == "LTCG_STOCK"
        assert ltcg_trade.is_long_term is True

    def test_parse_empty_csv(self, db_connection, tmp_path):
        """Test parsing CSV with no trades."""
        parser = ICICIDirectParser(db_connection)

        csv_content = """Account,8500480693
Name,Test User
Capital Gain,2024-2025
Stock Symbol,ISIN,Qty,Sale Date,Sale Rate,Sale Value,Sale Expenses,Purchase Date,Purchase Rate,Price as on 31st Jan 2018,Purchase Price Considered,Purchase Value,Purchase Expenses,Profit/Loss(-)
Short Term Capital Gain (STT paid),,,,,,,,,,,,
Total,,,,,,,,,,,,,0
"""
        test_file = tmp_path / "test_empty.csv"
        test_file.write_text(csv_content)

        result = parser.parse(test_file)

        assert result.success is True
        assert len(result.trades) == 0
        assert len(result.warnings) > 0  # Should warn about no trades
