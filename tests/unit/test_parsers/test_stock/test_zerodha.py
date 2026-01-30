"""Tests for Zerodha Tax P&L parser."""

import pytest
from datetime import date
from decimal import Decimal
from pathlib import Path

from pfas.parsers.stock.zerodha import ZerodhaParser
from pfas.parsers.stock.models import TradeType, TradeCategory


class TestZerodhaParser:
    """Tests for Zerodha parser."""

    def test_parser_initialization(self, db_connection):
        """Test parser can be initialized."""
        parser = ZerodhaParser(db_connection)
        assert parser.conn is not None

    def test_parse_nonexistent_file(self, db_connection):
        """Test parsing nonexistent file returns error."""
        parser = ZerodhaParser(db_connection)
        result = parser.parse(Path("/nonexistent/file.xlsx"))

        assert result.success is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0].lower()

    def test_parse_date_valid(self, db_connection):
        """Test date parsing from various formats."""
        parser = ZerodhaParser(db_connection)

        # String date
        date1 = parser._parse_date("2024-07-15")
        assert date1 == date(2024, 7, 15)

        # Date object
        date2 = parser._parse_date(date(2024, 6, 15))
        assert date2 == date(2024, 6, 15)

    def test_parse_date_invalid(self, db_connection):
        """Test date parsing for invalid values."""
        parser = ZerodhaParser(db_connection)

        assert parser._parse_date(None) is None
        assert parser._parse_date("") is None

    def test_to_decimal_valid(self, db_connection):
        """Test Decimal conversion for valid values."""
        parser = ZerodhaParser(db_connection)

        assert parser._to_decimal(123.45) == Decimal("123.45")
        assert parser._to_decimal("678.90") == Decimal("678.90")
        assert parser._to_decimal(100) == Decimal("100")

    def test_to_decimal_invalid(self, db_connection):
        """Test Decimal conversion for invalid values."""
        parser = ZerodhaParser(db_connection)

        assert parser._to_decimal(None) == Decimal("0")
        assert parser._to_decimal("") == Decimal("0")

    def test_get_or_create_broker_new(self, db_connection, sample_user):
        """Test creating a new broker."""
        from pfas.core.transaction_service import TransactionService
        parser = ZerodhaParser(db_connection)
        txn_service = TransactionService(db_connection)

        broker_id = parser._get_or_create_broker_via_service(txn_service, "Zerodha", user_id=sample_user["id"])
        assert broker_id > 0

        # Verify it was created
        cursor = db_connection.execute(
            "SELECT name FROM stock_brokers WHERE id = ?", (broker_id,)
        )
        row = cursor.fetchone()
        assert row["name"] == "Zerodha"

    def test_get_or_create_broker_existing(self, db_connection, sample_user):
        """Test getting existing broker."""
        from pfas.core.transaction_service import TransactionService
        parser = ZerodhaParser(db_connection)
        txn_service = TransactionService(db_connection)

        # Create first time
        broker_id1 = parser._get_or_create_broker_via_service(txn_service, "Zerodha", user_id=sample_user["id"])

        # Get same broker again
        broker_id2 = parser._get_or_create_broker_via_service(txn_service, "Zerodha", user_id=sample_user["id"])

        # Should return same ID
        assert broker_id1 == broker_id2


class TestZerodhaParserDatabaseOperations:
    """Tests for Zerodha parser database operations."""

    def test_save_to_db_empty_result(self, db_connection):
        """Test saving empty result returns 0."""
        parser = ZerodhaParser(db_connection)

        from pfas.parsers.stock.models import ParseResult
        result = ParseResult(success=True)

        count = parser.save_to_db(result, user_id=1)
        assert count == 0

    def test_save_to_db_failed_result(self, db_connection):
        """Test saving failed result returns 0."""
        parser = ZerodhaParser(db_connection)

        from pfas.parsers.stock.models import ParseResult
        result = ParseResult(success=False)

        count = parser.save_to_db(result, user_id=1)
        assert count == 0


class TestZerodhaParserDividends:
    """Tests for Zerodha dividend parsing (REQ-STK-003)."""

    def test_dividend_summary_empty(self, db_connection):
        """Test dividend summary with no dividends."""
        parser = ZerodhaParser(db_connection)

        from pfas.parsers.stock.models import DividendSummary
        summary = parser.calculate_dividend_summary([], "2024-25")

        assert summary.financial_year == "2024-25"
        assert summary.total_dividend == Decimal("0")
        assert summary.total_tds == Decimal("0")
        assert summary.dividend_count == 0

    def test_dividend_summary_with_dividends(self, db_connection):
        """Test dividend summary calculation."""
        parser = ZerodhaParser(db_connection)

        from pfas.parsers.stock.models import StockDividend
        dividends = [
            StockDividend(
                symbol="INFY",
                isin="INE009A01021",
                dividend_date=date(2024, 5, 15),
                quantity=100,
                dividend_per_share=Decimal("10"),
                gross_amount=Decimal("1000"),
                tds_amount=Decimal("0"),
                net_amount=Decimal("1000"),
            ),
            StockDividend(
                symbol="TCS",
                isin="INE467B01029",
                dividend_date=date(2024, 7, 20),
                quantity=50,
                dividend_per_share=Decimal("20"),
                gross_amount=Decimal("1000"),
                tds_amount=Decimal("100"),  # TDS applied
                net_amount=Decimal("900"),
            ),
        ]

        summary = parser.calculate_dividend_summary(dividends, "2024-25")

        assert summary.total_dividend == Decimal("2000")
        assert summary.total_tds == Decimal("100")
        assert summary.net_dividend == Decimal("1900")
        assert summary.dividend_count == 2

    def test_save_dividends_empty(self, db_connection):
        """Test saving empty dividends returns 0."""
        parser = ZerodhaParser(db_connection)

        from pfas.parsers.stock.models import ParseResult
        result = ParseResult(success=True)

        count = parser.save_dividends_to_db(result, user_id=1)
        assert count == 0


class TestZerodhaParserSTT:
    """Tests for Zerodha STT tracking (REQ-STK-006)."""

    def test_stt_summary_empty(self, db_connection):
        """Test STT summary with no entries."""
        parser = ZerodhaParser(db_connection)

        from pfas.parsers.stock.models import STTSummary
        summary = parser.calculate_stt_summary([], "2024-25")

        assert summary.financial_year == "2024-25"
        assert summary.total_stt == Decimal("0")
        assert summary.delivery_stt == Decimal("0")
        assert summary.intraday_stt == Decimal("0")

    def test_stt_summary_with_entries(self, db_connection):
        """Test STT summary calculation."""
        parser = ZerodhaParser(db_connection)

        from pfas.parsers.stock.models import STTEntry
        entries = [
            STTEntry(
                trade_date=date(2024, 5, 15),
                symbol="INFY",
                trade_type=TradeType.SELL,
                trade_category=TradeCategory.DELIVERY,
                trade_value=Decimal("100000"),
                stt_amount=Decimal("100"),
            ),
            STTEntry(
                trade_date=date(2024, 6, 20),
                symbol="SBIN",
                trade_type=TradeType.SELL,
                trade_category=TradeCategory.INTRADAY,
                trade_value=Decimal("50000"),
                stt_amount=Decimal("12.5"),
            ),
        ]

        summary = parser.calculate_stt_summary(entries, "2024-25")

        assert summary.total_stt == Decimal("112.5")
        assert summary.delivery_stt == Decimal("100")
        assert summary.intraday_stt == Decimal("12.5")

    def test_save_stt_empty(self, db_connection):
        """Test saving empty STT entries returns 0."""
        parser = ZerodhaParser(db_connection)

        from pfas.parsers.stock.models import ParseResult
        result = ParseResult(success=True)

        count = parser.save_stt_to_db(result, user_id=1)
        assert count == 0


class TestZerodhaParserSaveAll:
    """Tests for save_all_to_db method."""

    def test_save_all_empty(self, db_connection):
        """Test save_all with empty result."""
        parser = ZerodhaParser(db_connection)

        from pfas.parsers.stock.models import ParseResult
        result = ParseResult(success=True)

        counts = parser.save_all_to_db(result, user_id=1)

        assert counts["trades"] == 0
        assert counts["dividends"] == 0
        assert counts["stt"] == 0
