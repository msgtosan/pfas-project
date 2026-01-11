"""Tests for Karvy/KFintech parser."""

import pytest
from datetime import date
from decimal import Decimal
from pathlib import Path

from pfas.parsers.mf.karvy import KarvyParser
from pfas.parsers.mf.models import TransactionType, AssetClass, ParseResult


class TestKarvyParser:
    """Tests for Karvy CAS parser."""

    def test_parser_initialization(self, db_connection):
        """Test parser can be initialized."""
        parser = KarvyParser(db_connection)
        assert parser.conn is not None
        assert parser._duplicate_count == 0

    def test_extract_isin_parenthetical_format(self, db_connection):
        """Test ISIN extraction from Karvy's parenthetical format."""
        parser = KarvyParser(db_connection)

        # Karvy format: scheme name ( ISIN )
        scheme1 = "Mirae Asset Large and Midcap Fund Direct Growth ( INF769K01BI1)"
        assert parser._extract_isin(scheme1) == "INF769K01BI1"

        # With extra spaces
        scheme2 = "HDFC Liquid Fund (  INF123456789  )"
        assert parser._extract_isin(scheme2) == "INF123456789"

    def test_extract_isin_cams_format(self, db_connection):
        """Test ISIN extraction also works with CAMS format."""
        parser = KarvyParser(db_connection)

        # CAMS format: ISIN : code
        scheme = "SBI Fund ISIN : INF200K01123"
        assert parser._extract_isin(scheme) == "INF200K01123"

    def test_extract_isin_none(self, db_connection):
        """Test ISIN extraction returns None when not found."""
        parser = KarvyParser(db_connection)

        scheme = "Some Fund Without ISIN Code"
        assert parser._extract_isin(scheme) is None

    def test_determine_transaction_type_redemption(self, db_connection):
        """Test transaction type detection for redemptions."""
        parser = KarvyParser(db_connection)

        assert parser._determine_transaction_type("Redemption") == TransactionType.REDEMPTION
        assert parser._determine_transaction_type("REDEMPTION") == TransactionType.REDEMPTION
        assert parser._determine_transaction_type("redemption") == TransactionType.REDEMPTION

    def test_determine_transaction_type_purchase(self, db_connection):
        """Test transaction type detection for purchases."""
        parser = KarvyParser(db_connection)

        assert parser._determine_transaction_type("Purchase") == TransactionType.PURCHASE
        assert parser._determine_transaction_type("New Purchase") == TransactionType.PURCHASE
        assert parser._determine_transaction_type("Additional Purchase") == TransactionType.PURCHASE
        assert parser._determine_transaction_type("Systematic Investment") == TransactionType.PURCHASE

    def test_determine_transaction_type_switch(self, db_connection):
        """Test transaction type detection for switches."""
        parser = KarvyParser(db_connection)

        # Standard switch
        assert parser._determine_transaction_type("Switch Out") == TransactionType.SWITCH_OUT
        assert parser._determine_transaction_type("Switch In") == TransactionType.SWITCH_IN

        # Karvy-specific: Lateral Shift
        assert parser._determine_transaction_type("Lateral Shift Out") == TransactionType.SWITCH_OUT
        assert parser._determine_transaction_type("Lateral Shift In") == TransactionType.SWITCH_IN

        # STP (Systematic Transfer Plan)
        assert parser._determine_transaction_type("STP Out") == TransactionType.SWITCH_OUT
        assert parser._determine_transaction_type("STP In") == TransactionType.SWITCH_IN

    def test_determine_transaction_type_dividend(self, db_connection):
        """Test transaction type detection for dividends."""
        parser = KarvyParser(db_connection)

        assert parser._determine_transaction_type("Dividend Payout") == TransactionType.DIVIDEND
        assert parser._determine_transaction_type("Dividend Reinvestment") == TransactionType.DIVIDEND_REINVEST

    def test_determine_transaction_type_empty(self, db_connection):
        """Test transaction type detection for empty/None input."""
        parser = KarvyParser(db_connection)

        assert parser._determine_transaction_type("") == TransactionType.PURCHASE
        assert parser._determine_transaction_type(None) == TransactionType.PURCHASE

    def test_parse_date_valid_formats(self, db_connection):
        """Test date parsing from various formats."""
        parser = KarvyParser(db_connection)

        # ISO format
        assert parser._parse_date("2024-07-15") == date(2024, 7, 15)

        # Indian format (DD/MM/YYYY)
        assert parser._parse_date("15/07/2024") == date(2024, 7, 15)

        # Date object
        assert parser._parse_date(date(2024, 6, 15)) == date(2024, 6, 15)

    def test_parse_date_invalid(self, db_connection):
        """Test date parsing for invalid values."""
        parser = KarvyParser(db_connection)

        assert parser._parse_date(None) is None
        assert parser._parse_date("") is None
        assert parser._parse_date("invalid") is None

    def test_to_decimal_valid(self, db_connection):
        """Test Decimal conversion for valid values."""
        parser = KarvyParser(db_connection)

        assert parser._to_decimal(123.45) == Decimal("123.45")
        assert parser._to_decimal("678.90") == Decimal("678.90")
        assert parser._to_decimal(100) == Decimal("100")
        assert parser._to_decimal(0) == Decimal("0")

    def test_to_decimal_invalid(self, db_connection):
        """Test Decimal conversion for invalid values."""
        parser = KarvyParser(db_connection)

        assert parser._to_decimal(None) == Decimal("0")
        assert parser._to_decimal("") == Decimal("0")
        assert parser._to_decimal("invalid") == Decimal("0")

    def test_parse_nonexistent_file(self, db_connection):
        """Test parsing nonexistent file returns error."""
        parser = KarvyParser(db_connection)
        result = parser.parse(Path("/nonexistent/karvy_file.xlsx"))

        assert result.success is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0].lower()

    def test_parse_unsupported_format(self, db_connection, tmp_path):
        """Test parsing unsupported file format."""
        parser = KarvyParser(db_connection)

        # Create a dummy file with unsupported extension
        test_file = tmp_path / "test.txt"
        test_file.write_text("dummy content")

        result = parser.parse(test_file)

        assert result.success is False
        assert len(result.errors) > 0
        assert "Unsupported file format" in result.errors[0]

    def test_parse_pdf_invalid_content(self, db_connection, tmp_path):
        """Test PDF parsing handles invalid PDF content."""
        parser = KarvyParser(db_connection)

        # Create a dummy file with invalid PDF content
        test_file = tmp_path / "test.pdf"
        test_file.write_bytes(b"%PDF-1.4 fake content - not a real PDF")

        result = parser.parse(test_file)

        # Should fail because it's not a valid PDF
        assert result.success is False
        assert len(result.errors) > 0

    def test_get_or_create_amc_new(self, db_connection):
        """Test creating a new AMC."""
        parser = KarvyParser(db_connection)

        amc_id = parser._get_or_create_amc("Mirae Asset Mutual Fund")
        assert amc_id > 0

        # Verify it was created
        cursor = db_connection.execute(
            "SELECT name FROM mf_amcs WHERE id = ?", (amc_id,)
        )
        row = cursor.fetchone()
        assert row["name"] == "Mirae Asset Mutual Fund"

    def test_get_or_create_amc_existing(self, db_connection):
        """Test getting existing AMC."""
        parser = KarvyParser(db_connection)

        # Create first time
        amc_id1 = parser._get_or_create_amc("PPFAS Mutual Fund")

        # Get same AMC again
        amc_id2 = parser._get_or_create_amc("PPFAS Mutual Fund")

        # Should return same ID
        assert amc_id1 == amc_id2

    def test_duplicate_count_tracking(self, db_connection):
        """Test duplicate transaction count tracking."""
        parser = KarvyParser(db_connection)

        # Initial count should be 0
        assert parser.get_duplicate_count() == 0

        # Reset should keep it at 0
        parser.reset_duplicate_count()
        assert parser.get_duplicate_count() == 0


class TestKarvyParserDatabaseOperations:
    """Tests for Karvy parser database operations."""

    def test_save_to_db_empty_result(self, db_connection):
        """Test saving empty result returns 0."""
        parser = KarvyParser(db_connection)

        result = ParseResult(success=True)
        count = parser.save_to_db(result, user_id=1)
        assert count == 0

    def test_save_to_db_failed_result(self, db_connection):
        """Test saving failed result returns 0."""
        parser = KarvyParser(db_connection)

        result = ParseResult(success=False)
        result.add_error("Test error")

        count = parser.save_to_db(result, user_id=1)
        assert count == 0

    def test_get_column_value_variations(self, db_connection):
        """Test column value extraction with variations."""
        import pandas as pd

        parser = KarvyParser(db_connection)

        # Create a mock row with various column names
        row = pd.Series({
            'Scheme Name': 'Test Scheme',
            ' Fund Name': '  Mirae Asset  ',
            'Folio Number': '12345',
            'Empty Col': '',
            'None Col': None,
        })

        # Should find value with different possible names
        assert parser._get_column_value(row, ['Scheme Name', 'scheme_name']) == 'Test Scheme'
        assert parser._get_column_value(row, [' Fund Name', 'Fund Name']) == 'Mirae Asset'
        assert parser._get_column_value(row, ['Folio Number']) == '12345'

        # Should return None for empty/missing columns
        assert parser._get_column_value(row, ['Empty Col']) is None
        assert parser._get_column_value(row, ['None Col']) is None
        assert parser._get_column_value(row, ['NonExistent']) is None


class TestKarvyParserValidation:
    """Tests for Karvy parser dataframe validation."""

    def test_validate_karvy_dataframe_valid(self, db_connection):
        """Test validation passes for valid Karvy dataframe."""
        import pandas as pd

        parser = KarvyParser(db_connection)

        df = pd.DataFrame({
            'Scheme Name': ['Test Fund'],
            'Amount': [10000],
            'Date': ['2024-07-15']
        })

        assert parser._validate_karvy_dataframe(df) is True

    def test_validate_karvy_dataframe_missing_columns(self, db_connection):
        """Test validation fails for missing required columns."""
        import pandas as pd

        parser = KarvyParser(db_connection)

        # Missing 'Amount' column
        df = pd.DataFrame({
            'Scheme Name': ['Test Fund'],
            'Date': ['2024-07-15']
        })

        assert parser._validate_karvy_dataframe(df) is False

    def test_validate_karvy_dataframe_empty(self, db_connection):
        """Test validation fails for empty dataframe."""
        import pandas as pd

        parser = KarvyParser(db_connection)

        df = pd.DataFrame()
        assert parser._validate_karvy_dataframe(df) is False

        df = None
        assert parser._validate_karvy_dataframe(df) is False

    def test_validate_karvy_dataframe_case_insensitive(self, db_connection):
        """Test validation is case-insensitive for column names."""
        import pandas as pd

        parser = KarvyParser(db_connection)

        # Lowercase column names
        df = pd.DataFrame({
            'scheme name': ['Test Fund'],
            'amount': [10000]
        })

        assert parser._validate_karvy_dataframe(df) is True

        # Uppercase column names
        df = pd.DataFrame({
            'SCHEME NAME': ['Test Fund'],
            'AMOUNT': [10000]
        })

        assert parser._validate_karvy_dataframe(df) is True
