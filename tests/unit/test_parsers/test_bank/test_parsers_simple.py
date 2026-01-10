"""
Simple unit tests for bank parsers.

Tests helper methods and basic functionality.
"""

import pytest
from decimal import Decimal
from datetime import date

from pfas.parsers.bank.icici import ICICIParser
from pfas.parsers.bank.sbi import SBIParser
from pfas.parsers.bank.hdfc import HDFCParser


class TestICICIParser:
    """Tests for ICICI Bank parser helper methods."""

    @pytest.fixture
    def parser(self, db_with_accounts, master_key):
        """Create ICICI parser instance."""
        return ICICIParser(db_with_accounts, master_key)

    def test_parser_name(self, parser):
        """Test parser has correct bank name."""
        assert parser.BANK_NAME == "ICICI Bank"

    def test_parse_amount(self, parser):
        """Test parsing amounts with Indian comma formatting."""
        assert parser._parse_amount("1,234,567.89") == Decimal("1234567.89")
        assert parser._parse_amount("12,345.00") == Decimal("12345.00")
        assert parser._parse_amount("5,000.00") == Decimal("5000.00")
        assert parser._parse_amount("100.00") == Decimal("100.00")

    def test_parse_amount_empty(self, parser):
        """Test parsing empty amounts."""
        assert parser._parse_amount("") == Decimal("0")
        assert parser._parse_amount("   ") == Decimal("0")
        assert parser._parse_amount("-") == Decimal("0")
        assert parser._parse_amount(None) == Decimal("0")

    def test_parse_date(self, parser):
        """Test parsing dates in various formats."""
        test_cases = [
            ("01-04-2024", date(2024, 4, 1)),
            ("15-07-2024", date(2024, 7, 15)),
            ("01/04/2024", date(2024, 4, 1)),
        ]

        for date_str, expected_date in test_cases:
            parsed = parser._parse_date(date_str)
            assert parsed == expected_date, f"Failed for: {date_str}"

    def test_parse_date_invalid(self, parser):
        """Test parsing invalid dates."""
        assert parser._parse_date("") is None
        assert parser._parse_date(None) is None
        assert parser._parse_date("invalid") is None


class TestSBIParser:
    """Tests for SBI Bank parser helper methods."""

    @pytest.fixture
    def parser(self, db_with_accounts, master_key):
        """Create SBI parser instance."""
        return SBIParser(db_with_accounts, master_key)

    def test_parser_name(self, parser):
        """Test parser has correct bank name."""
        assert parser.BANK_NAME == "State Bank of India"

    def test_map_columns(self, parser):
        """Test column mapping for various formats."""
        # Test exact match
        columns = ["Txn Date", "Description", "Debit", "Credit", "Balance"]
        mapping = parser._map_columns(columns)

        assert "date" in mapping
        assert "description" in mapping
        assert "debit" in mapping
        assert "credit" in mapping
        assert "balance" in mapping

    def test_map_columns_case_insensitive(self, parser):
        """Test column mapping is case insensitive."""
        columns = ["TXN DATE", "DESCRIPTION", "DEBIT", "CREDIT", "BALANCE"]
        mapping = parser._map_columns(columns)

        assert "date" in mapping
        assert "description" in mapping

    def test_map_columns_variants(self, parser):
        """Test column mapping handles variants."""
        columns = ["Date", "Narration", "Withdrawal", "Deposit", "Closing Balance"]
        mapping = parser._map_columns(columns)

        assert "date" in mapping
        assert "description" in mapping
        assert "debit" in mapping
        assert "credit" in mapping
        assert "balance" in mapping


class TestHDFCParser:
    """Tests for HDFC Bank parser helper methods."""

    @pytest.fixture
    def parser(self, db_with_accounts, master_key):
        """Create HDFC parser instance."""
        return HDFCParser(db_with_accounts, master_key)

    def test_parser_name(self, parser):
        """Test parser has correct bank name."""
        assert parser.BANK_NAME == "HDFC Bank"

    def test_parse_amount(self, parser):
        """Test parsing amounts with Indian comma formatting."""
        assert parser._parse_amount("1,567,890.12") == Decimal("1567890.12")
        assert parser._parse_amount("630,338.38") == Decimal("630338.38")
        assert parser._parse_amount("15,234.56") == Decimal("15234.56")

    def test_parse_amount_empty(self, parser):
        """Test parsing empty amounts."""
        assert parser._parse_amount("") == Decimal("0")
        assert parser._parse_amount("   ") == Decimal("0")

    def test_parse_date(self, parser):
        """Test parsing HDFC date format (DD/MM/YYYY)."""
        test_cases = [
            ("01/04/2024", date(2024, 4, 1)),
            ("15/07/2024", date(2024, 7, 15)),
            ("30/09/2024", date(2024, 9, 30)),
        ]

        for date_str, expected_date in test_cases:
            parsed = parser._parse_date(date_str)
            assert parsed == expected_date, f"Failed for: {date_str}"
