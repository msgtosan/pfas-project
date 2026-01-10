"""Tests for CAMS parser."""

import pytest
from datetime import date
from decimal import Decimal
from pathlib import Path

from pfas.parsers.mf.cams import CAMSParser
from pfas.parsers.mf.models import TransactionType, AssetClass


class TestCAMSParser:
    """Tests for CAMS CAS parser."""

    def test_parser_initialization(self, db_connection):
        """Test parser can be initialized."""
        parser = CAMSParser(db_connection)
        assert parser.conn is not None

    def test_extract_isin(self, db_connection):
        """Test ISIN extraction from scheme name."""
        parser = CAMSParser(db_connection)

        # Standard format
        scheme1 = "SBI Bluechip Fund Direct Growth, ISIN : INF200K01123"
        assert parser._extract_isin(scheme1) == "INF200K01123"

        # With different spacing
        scheme2 = "HDFC Fund ISIN: INF123456789"
        assert parser._extract_isin(scheme2) == "INF123456789"

        # No ISIN
        scheme3 = "Some Fund Without ISIN"
        assert parser._extract_isin(scheme3) is None

    def test_determine_transaction_type_redemption(self, db_connection):
        """Test transaction type detection for redemptions."""
        parser = CAMSParser(db_connection)

        assert parser._determine_transaction_type("Redemption") == TransactionType.REDEMPTION
        assert parser._determine_transaction_type("REDEMPTION") == TransactionType.REDEMPTION

    def test_determine_transaction_type_purchase(self, db_connection):
        """Test transaction type detection for purchases."""
        parser = CAMSParser(db_connection)

        assert parser._determine_transaction_type("Purchase") == TransactionType.PURCHASE
        assert parser._determine_transaction_type("Purchase - Systematic") == TransactionType.PURCHASE
        assert parser._determine_transaction_type("Additional Purchase") == TransactionType.PURCHASE

    def test_determine_transaction_type_switch(self, db_connection):
        """Test transaction type detection for switches."""
        parser = CAMSParser(db_connection)

        assert parser._determine_transaction_type("Switch Out") == TransactionType.SWITCH_OUT
        assert parser._determine_transaction_type("SWITCH-OUT") == TransactionType.SWITCH_OUT
        assert parser._determine_transaction_type("Switch In") == TransactionType.SWITCH_IN
        assert parser._determine_transaction_type("SWITCH-IN") == TransactionType.SWITCH_IN

    def test_determine_transaction_type_dividend(self, db_connection):
        """Test transaction type detection for dividends."""
        parser = CAMSParser(db_connection)

        assert parser._determine_transaction_type("Dividend Payout") == TransactionType.DIVIDEND
        assert parser._determine_transaction_type("Dividend Reinvestment") == TransactionType.DIVIDEND_REINVEST

    def test_parse_date_valid(self, db_connection):
        """Test date parsing from various formats."""
        parser = CAMSParser(db_connection)

        # String date
        date1 = parser._parse_date("2024-07-15")
        assert date1 == date(2024, 7, 15)

        # Date object
        date2 = parser._parse_date(date(2024, 6, 15))
        assert date2 == date(2024, 6, 15)

    def test_parse_date_invalid(self, db_connection):
        """Test date parsing for invalid values."""
        parser = CAMSParser(db_connection)

        assert parser._parse_date(None) is None
        assert parser._parse_date("") is None

    def test_to_decimal_valid(self, db_connection):
        """Test Decimal conversion for valid values."""
        parser = CAMSParser(db_connection)

        assert parser._to_decimal(123.45) == Decimal("123.45")
        assert parser._to_decimal("678.90") == Decimal("678.90")
        assert parser._to_decimal(100) == Decimal("100")

    def test_to_decimal_invalid(self, db_connection):
        """Test Decimal conversion for invalid values."""
        parser = CAMSParser(db_connection)

        assert parser._to_decimal(None) == Decimal("0")
        assert parser._to_decimal("") == Decimal("0")

    def test_parse_nonexistent_file(self, db_connection):
        """Test parsing nonexistent file returns error."""
        parser = CAMSParser(db_connection)
        result = parser.parse(Path("/nonexistent/file.xlsx"))

        assert result.success is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0].lower()

    def test_parse_unsupported_format(self, db_connection, tmp_path):
        """Test parsing unsupported file format."""
        parser = CAMSParser(db_connection)

        # Create a dummy file with unsupported extension
        test_file = tmp_path / "test.txt"
        test_file.write_text("dummy content")

        result = parser.parse(test_file)

        assert result.success is False
        assert len(result.errors) > 0
        assert "Unsupported file format" in result.errors[0]

    def test_get_or_create_amc_new(self, db_connection):
        """Test creating a new AMC."""
        parser = CAMSParser(db_connection)

        amc_id = parser._get_or_create_amc("SBI Mutual Fund")
        assert amc_id > 0

        # Verify it was created
        cursor = db_connection.execute(
            "SELECT name FROM mf_amcs WHERE id = ?", (amc_id,)
        )
        row = cursor.fetchone()
        assert row["name"] == "SBI Mutual Fund"

    def test_get_or_create_amc_existing(self, db_connection):
        """Test getting existing AMC."""
        parser = CAMSParser(db_connection)

        # Create first time
        amc_id1 = parser._get_or_create_amc("HDFC Mutual Fund")

        # Get same AMC again
        amc_id2 = parser._get_or_create_amc("HDFC Mutual Fund")

        # Should return same ID
        assert amc_id1 == amc_id2


class TestCAMSParserDatabaseOperations:
    """Tests for CAMS parser database operations."""

    def test_save_to_db_empty_result(self, db_connection):
        """Test saving empty result returns 0."""
        parser = CAMSParser(db_connection)

        from pfas.parsers.mf.models import ParseResult
        result = ParseResult(success=True)

        count = parser.save_to_db(result, user_id=1)
        assert count == 0

    def test_save_to_db_failed_result(self, db_connection):
        """Test saving failed result returns 0."""
        parser = CAMSParser(db_connection)

        from pfas.parsers.mf.models import ParseResult
        result = ParseResult(success=False)

        count = parser.save_to_db(result, user_id=1)
        assert count == 0
