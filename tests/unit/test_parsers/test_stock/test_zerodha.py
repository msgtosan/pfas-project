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

    def test_get_or_create_broker_new(self, db_connection):
        """Test creating a new broker."""
        parser = ZerodhaParser(db_connection)

        broker_id = parser._get_or_create_broker("Zerodha")
        assert broker_id > 0

        # Verify it was created
        cursor = db_connection.execute(
            "SELECT name FROM stock_brokers WHERE id = ?", (broker_id,)
        )
        row = cursor.fetchone()
        assert row["name"] == "Zerodha"

    def test_get_or_create_broker_existing(self, db_connection):
        """Test getting existing broker."""
        parser = ZerodhaParser(db_connection)

        # Create first time
        broker_id1 = parser._get_or_create_broker("Zerodha")

        # Get same broker again
        broker_id2 = parser._get_or_create_broker("Zerodha")

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
