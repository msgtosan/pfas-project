"""Tests for Morgan Stanley statement parser."""

import pytest
from datetime import date
from decimal import Decimal

from pfas.parsers.foreign import MorganStanleyParser
from pfas.parsers.foreign.models import ActivityType, GrantType


class TestMorganStanleyParser:
    """Tests for MorganStanleyParser class."""

    def test_parser_initialization(self):
        """Test parser can be initialized."""
        parser = MorganStanleyParser()
        assert parser is not None

    def test_parse_empty_content(self):
        """Test parsing empty content."""
        parser = MorganStanleyParser()
        result = parser.parse("", "test.txt")

        assert result.success is True
        assert len(result.activities) == 0


class TestActivityTypeMapping:
    """Tests for activity type keyword mapping."""

    def test_vest_keywords(self):
        """Test vest activity detection."""
        parser = MorganStanleyParser()

        assert parser._determine_activity_type("Vest of RSU shares") == ActivityType.VEST
        assert parser._determine_activity_type("Vesting release") == ActivityType.VEST
        assert parser._determine_activity_type("Stock release event") == ActivityType.VEST

    def test_sale_keywords(self):
        """Test sale activity detection."""
        parser = MorganStanleyParser()

        assert parser._determine_activity_type("Sale of shares") == ActivityType.SALE
        assert parser._determine_activity_type("Sold 100 shares") == ActivityType.SALE
        assert parser._determine_activity_type("Sell order executed") == ActivityType.SALE

    def test_dividend_keywords(self):
        """Test dividend activity detection."""
        parser = MorganStanleyParser()

        assert parser._determine_activity_type("Dividend payment") == ActivityType.DIVIDEND
        assert parser._determine_activity_type("Cash div received") == ActivityType.DIVIDEND

    def test_dividend_reinvest_keywords(self):
        """Test dividend reinvestment detection."""
        parser = MorganStanleyParser()

        assert parser._determine_activity_type("DRIP reinvestment") == ActivityType.DIVIDEND_REINVEST
        assert parser._determine_activity_type("Dividend reinvest") == ActivityType.DIVIDEND_REINVEST

    def test_purchase_keywords(self):
        """Test purchase activity detection."""
        parser = MorganStanleyParser()

        assert parser._determine_activity_type("ESPP purchase") == ActivityType.PURCHASE
        assert parser._determine_activity_type("Buy order") == ActivityType.PURCHASE

    def test_tax_withhold_keywords(self):
        """Test tax withholding detection."""
        parser = MorganStanleyParser()

        assert parser._determine_activity_type("Tax withholding") == ActivityType.TAX_WITHHOLD
        assert parser._determine_activity_type("Federal W/H") == ActivityType.TAX_WITHHOLD

    def test_fee_keywords(self):
        """Test fee detection."""
        parser = MorganStanleyParser()

        assert parser._determine_activity_type("Commission fee") == ActivityType.FEE
        assert parser._determine_activity_type("Service charge") == ActivityType.FEE

    def test_transfer_default(self):
        """Test default transfer type."""
        parser = MorganStanleyParser()

        # Unknown activity should default to TRANSFER
        assert parser._determine_activity_type("Unknown activity") == ActivityType.TRANSFER


class TestDateParsing:
    """Tests for date parsing."""

    def test_parse_date_us_format(self):
        """Test parsing US date format."""
        parser = MorganStanleyParser()

        assert parser._parse_date("06/15/2024") == date(2024, 6, 15)
        assert parser._parse_date("12/31/2024") == date(2024, 12, 31)

    def test_parse_date_iso_format(self):
        """Test parsing ISO date format."""
        parser = MorganStanleyParser()

        assert parser._parse_date("2024-06-15") == date(2024, 6, 15)

    def test_parse_date_short_year(self):
        """Test parsing short year format."""
        parser = MorganStanleyParser()

        assert parser._parse_date("06/15/24") == date(2024, 6, 15)

    def test_parse_date_text_format(self):
        """Test parsing text date format."""
        parser = MorganStanleyParser()

        assert parser._parse_date("Jun 15, 2024") == date(2024, 6, 15)
        assert parser._parse_date("December 31, 2024") == date(2024, 12, 31)

    def test_parse_date_invalid(self):
        """Test invalid date raises error."""
        parser = MorganStanleyParser()

        with pytest.raises(ValueError):
            parser._parse_date("invalid date")


class TestDecimalConversion:
    """Tests for decimal conversion."""

    def test_to_decimal_simple(self):
        """Test simple decimal conversion."""
        parser = MorganStanleyParser()

        assert parser._to_decimal("100") == Decimal("100")
        assert parser._to_decimal("100.50") == Decimal("100.50")

    def test_to_decimal_with_commas(self):
        """Test decimal with thousand separators."""
        parser = MorganStanleyParser()

        assert parser._to_decimal("1,000") == Decimal("1000")
        assert parser._to_decimal("1,234,567.89") == Decimal("1234567.89")

    def test_to_decimal_with_currency(self):
        """Test decimal with currency symbol."""
        parser = MorganStanleyParser()

        assert parser._to_decimal("$100") == Decimal("100")
        assert parser._to_decimal("$1,234.56") == Decimal("1234.56")

    def test_to_decimal_negative_parentheses(self):
        """Test negative numbers in parentheses."""
        parser = MorganStanleyParser()

        assert parser._to_decimal("(100)") == Decimal("-100")
        assert parser._to_decimal("($1,234.56)") == Decimal("-1234.56")

    def test_to_decimal_empty(self):
        """Test empty value returns zero."""
        parser = MorganStanleyParser()

        assert parser._to_decimal("") == Decimal("0")
        assert parser._to_decimal(None) == Decimal("0")


class TestCSVParsing:
    """Tests for CSV parsing."""

    def test_parse_csv_empty(self):
        """Test parsing empty CSV."""
        parser = MorganStanleyParser()
        result = parser.parse_csv("Date,Description,Amount\n", "test.csv")

        assert result.success is True
        assert len(result.activities) == 0

    def test_parse_csv_basic(self):
        """Test parsing basic CSV."""
        parser = MorganStanleyParser()

        csv_content = """Date,Description,Amount,Symbol,Quantity
06/15/2024,Vest of RSU,5000.00,AAPL,50
06/16/2024,Dividend,25.00,AAPL,
06/17/2024,Sale,2500.00,AAPL,25
"""

        result = parser.parse_csv(csv_content, "test.csv")

        assert result.success is True
        assert len(result.activities) == 3

    def test_parse_csv_with_different_columns(self):
        """Test CSV with different column names."""
        parser = MorganStanleyParser()

        csv_content = """Trade Date,Transaction Type,Net Amount,Ticker,Shares
06/15/2024,Vest,5000.00,MSFT,100
"""

        result = parser.parse_csv(csv_content, "test.csv")

        assert result.success is True
        assert len(result.activities) == 1
        assert result.activities[0].activity_date == date(2024, 6, 15)


class TestParseResult:
    """Tests for parse result structure."""

    def test_parse_result_fields(self):
        """Test parse result has all expected fields."""
        parser = MorganStanleyParser()
        result = parser.parse("", "test.txt")

        assert hasattr(result, 'success')
        assert hasattr(result, 'statement_period')
        assert hasattr(result, 'account_number')
        assert hasattr(result, 'stock_plan_details')
        assert hasattr(result, 'rsu_vests')
        assert hasattr(result, 'rsu_sales')
        assert hasattr(result, 'espp_purchases')
        assert hasattr(result, 'espp_sales')
        assert hasattr(result, 'dividends')
        assert hasattr(result, 'activities')
        assert hasattr(result, 'errors')
        assert hasattr(result, 'warnings')

    def test_parse_result_lists_empty(self):
        """Test parse result lists are empty by default."""
        parser = MorganStanleyParser()
        result = parser.parse("", "test.txt")

        assert len(result.stock_plan_details) == 0
        assert len(result.rsu_vests) == 0
        assert len(result.rsu_sales) == 0
        assert len(result.espp_purchases) == 0
        assert len(result.espp_sales) == 0
        assert len(result.dividends) == 0
        assert len(result.activities) == 0
        assert len(result.errors) == 0
        assert len(result.warnings) == 0


class TestGrantType:
    """Tests for GrantType enum."""

    def test_grant_types(self):
        """Test all grant types exist."""
        assert GrantType.RSU.value == "RSU"
        assert GrantType.ESPP.value == "ESPP"
        assert GrantType.ESOP.value == "ESOP"
        assert GrantType.DRIP.value == "DRIP"


class TestActivityType:
    """Tests for ActivityType enum."""

    def test_activity_types(self):
        """Test all activity types exist."""
        assert ActivityType.VEST.value == "VEST"
        assert ActivityType.SALE.value == "SALE"
        assert ActivityType.DIVIDEND.value == "DIVIDEND"
        assert ActivityType.DIVIDEND_REINVEST.value == "DIVIDEND_REINVEST"
        assert ActivityType.PURCHASE.value == "PURCHASE"
        assert ActivityType.INTEREST.value == "INTEREST"
        assert ActivityType.FEE.value == "FEE"
        assert ActivityType.TAX_WITHHOLD.value == "TAX_WITHHOLD"
        assert ActivityType.TRANSFER.value == "TRANSFER"
