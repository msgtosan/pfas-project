"""Tests for DTAA Credit Calculator."""

import pytest
from datetime import date
from decimal import Decimal

from pfas.services.foreign.dtaa_calculator import DTAACalculator, DTAACredit, DTAASummary, Form67Data


class TestDTAACalculator:
    """Tests for DTAACalculator class."""

    def test_calculator_initialization(self, db_connection):
        """Test calculator can be initialized."""
        calculator = DTAACalculator(db_connection)
        assert calculator.conn is not None
        assert calculator.rate_provider is not None

    def test_treaty_rates_us(self, db_connection):
        """Test US DTAA treaty rates."""
        calculator = DTAACalculator(db_connection)

        assert calculator.TREATY_RATES['US']['DIVIDEND'] == Decimal("0.25")
        assert calculator.TREATY_RATES['US']['INTEREST'] == Decimal("0.15")
        assert calculator.TREATY_RATES['US']['ROYALTY'] == Decimal("0.15")

    def test_treaty_rates_uk(self, db_connection):
        """Test UK DTAA treaty rates."""
        calculator = DTAACalculator(db_connection)

        assert calculator.TREATY_RATES['UK']['DIVIDEND'] == Decimal("0.15")
        assert calculator.TREATY_RATES['UK']['INTEREST'] == Decimal("0.15")

    def test_dtaa_articles_us(self, db_connection):
        """Test US DTAA article references."""
        calculator = DTAACalculator(db_connection)

        assert calculator.DTAA_ARTICLES['US']['DIVIDEND'] == 'Article 10(2)(b)'
        assert calculator.DTAA_ARTICLES['US']['INTEREST'] == 'Article 11(2)'
        assert calculator.DTAA_ARTICLES['US']['CAPITAL_GAINS'] == 'Article 13'


class TestDTAACredit:
    """Tests for DTAACredit dataclass."""

    def test_credit_creation(self):
        """Test creating DTAA credit."""
        credit = DTAACredit(
            income_type='DIVIDEND',
            income_country='US',
            income_date=date(2024, 6, 15),
            gross_income_usd=Decimal("1000.00"),
            tax_withheld_usd=Decimal("250.00"),
            gross_income_inr=Decimal("83500.00"),
            tax_withheld_inr=Decimal("20875.00"),
            dtaa_article='Article 10(2)(b)',
            max_dtaa_rate=Decimal("0.25"),
            indian_tax_on_income=Decimal("25050.00"),
            credit_allowed=Decimal("20875.00")
        )

        assert credit.income_type == 'DIVIDEND'
        assert credit.income_country == 'US'
        assert credit.gross_income_usd == Decimal("1000.00")
        assert credit.tax_withheld_usd == Decimal("250.00")
        assert credit.credit_allowed == Decimal("20875.00")


class TestDTAASummary:
    """Tests for DTAASummary dataclass."""

    def test_summary_creation(self):
        """Test creating DTAA summary."""
        summary = DTAASummary(
            financial_year="2024-25",
            country="US"
        )

        assert summary.financial_year == "2024-25"
        assert summary.country == "US"
        assert summary.dividend_income_inr == Decimal("0")
        assert summary.total_income_inr == Decimal("0")
        assert summary.dtaa_credit_allowed == Decimal("0")

    def test_summary_with_values(self):
        """Test summary with income values."""
        summary = DTAASummary(
            financial_year="2024-25",
            country="US",
            dividend_income_inr=Decimal("100000.00"),
            dividend_tax_withheld_inr=Decimal("25000.00"),
            total_income_inr=Decimal("100000.00"),
            total_tax_withheld_inr=Decimal("25000.00"),
            dtaa_credit_allowed=Decimal("25000.00")
        )

        assert summary.dividend_income_inr == Decimal("100000.00")
        assert summary.dtaa_credit_allowed == Decimal("25000.00")


class TestForm67Data:
    """Tests for Form67Data dataclass."""

    def test_form67_creation(self):
        """Test creating Form 67 data."""
        form_data = Form67Data(
            financial_year="2024-25",
            name="Test User",
            pan="ABCDE1234F",
            assessment_year="2025-26"
        )

        assert form_data.financial_year == "2024-25"
        assert form_data.name == "Test User"
        assert form_data.pan == "ABCDE1234F"
        assert form_data.assessment_year == "2025-26"
        assert len(form_data.countries) == 0
        assert len(form_data.income_details) == 0

    def test_form67_relief_defaults(self):
        """Test Form 67 relief defaults."""
        form_data = Form67Data(
            financial_year="2024-25",
            name="Test User",
            pan="ABCDE1234F",
            assessment_year="2025-26"
        )

        assert form_data.section_90_relief == Decimal("0")
        assert form_data.section_91_relief == Decimal("0")
        assert form_data.total_relief_claimed == Decimal("0")


class TestCalculateDividendCredit:
    """Tests for dividend credit calculation."""

    def test_dividend_credit_basic(self, db_connection):
        """Test basic dividend credit calculation."""
        calculator = DTAACalculator(db_connection)

        # Add exchange rate
        calculator.rate_provider.add_rate(
            date(2024, 6, 15), "USD", "INR", Decimal("83.50"), "SBI"
        )

        credit = calculator.calculate_dividend_credit(
            dividend_date=date(2024, 6, 15),
            gross_dividend_usd=Decimal("1000.00"),
            tax_withheld_usd=Decimal("250.00"),
            country="US",
            indian_tax_rate=Decimal("0.30")
        )

        assert credit.income_type == 'DIVIDEND'
        assert credit.income_country == 'US'
        assert credit.gross_income_usd == Decimal("1000.00")
        assert credit.tax_withheld_usd == Decimal("250.00")
        assert credit.gross_income_inr == Decimal("83500.00")
        assert credit.tax_withheld_inr == Decimal("20875.00")

    def test_dividend_credit_limited_by_indian_tax(self, db_connection):
        """Test credit limited by Indian tax on income."""
        calculator = DTAACalculator(db_connection)

        calculator.rate_provider.add_rate(
            date(2024, 6, 20), "USD", "INR", Decimal("83.50"), "SBI"
        )

        credit = calculator.calculate_dividend_credit(
            dividend_date=date(2024, 6, 20),
            gross_dividend_usd=Decimal("1000.00"),
            tax_withheld_usd=Decimal("250.00"),  # 25% US withholding
            country="US",
            indian_tax_rate=Decimal("0.20")  # 20% Indian tax rate
        )

        # Indian tax = 83500 × 0.20 = 16700
        # US tax withheld = 20875
        # Credit allowed = min(20875, 16700) = 16700
        assert credit.indian_tax_on_income == Decimal("16700.00")
        assert credit.credit_allowed == Decimal("16700.00")

    def test_dividend_credit_article_reference(self, db_connection):
        """Test DTAA article reference."""
        calculator = DTAACalculator(db_connection)

        calculator.rate_provider.add_rate(
            date(2024, 6, 25), "USD", "INR", Decimal("83.50"), "SBI"
        )

        credit = calculator.calculate_dividend_credit(
            dividend_date=date(2024, 6, 25),
            gross_dividend_usd=Decimal("500.00"),
            tax_withheld_usd=Decimal("125.00"),
            country="US"
        )

        assert credit.dtaa_article == 'Article 10(2)(b)'
        assert credit.max_dtaa_rate == Decimal("0.25")


class TestCalculateInterestCredit:
    """Tests for interest credit calculation."""

    def test_interest_credit_basic(self, db_connection):
        """Test basic interest credit calculation."""
        calculator = DTAACalculator(db_connection)

        calculator.rate_provider.add_rate(
            date(2024, 7, 1), "USD", "INR", Decimal("83.60"), "SBI"
        )

        credit = calculator.calculate_interest_credit(
            interest_date=date(2024, 7, 1),
            gross_interest_usd=Decimal("500.00"),
            tax_withheld_usd=Decimal("75.00"),  # 15% withholding
            country="US"
        )

        assert credit.income_type == 'INTEREST'
        assert credit.income_country == 'US'
        assert credit.max_dtaa_rate == Decimal("0.15")
        assert credit.dtaa_article == 'Article 11(2)'


class TestGetCreditsForYear:
    """Tests for getting credits by year."""

    def test_get_credits_empty(self, db_connection, sample_user):
        """Test getting credits when none exist."""
        calculator = DTAACalculator(db_connection)

        credits = calculator.get_credits_for_year(sample_user["id"], "2024-25")

        assert len(credits) == 0


class TestGenerateForm67Data:
    """Tests for Form 67 data generation."""

    def test_generate_form67_empty(self, db_connection, sample_user):
        """Test generating Form 67 with no foreign income."""
        calculator = DTAACalculator(db_connection)

        form_data = calculator.generate_form_67_data(
            user_id=sample_user["id"],
            financial_year="2024-25",
            pan="ABCDE1234F",
            name="Test User"
        )

        assert form_data.financial_year == "2024-25"
        assert form_data.pan == "ABCDE1234F"
        assert form_data.assessment_year == "2025-26"
        assert form_data.total_relief_claimed == Decimal("0")


class TestCountryNames:
    """Tests for country name lookup."""

    def test_get_country_name_us(self, db_connection):
        """Test US country name."""
        calculator = DTAACalculator(db_connection)
        assert calculator._get_country_name('US') == 'United States of America'

    def test_get_country_name_uk(self, db_connection):
        """Test UK country name."""
        calculator = DTAACalculator(db_connection)
        assert calculator._get_country_name('UK') == 'United Kingdom'

    def test_get_country_name_unknown(self, db_connection):
        """Test unknown country code returns code."""
        calculator = DTAACalculator(db_connection)
        assert calculator._get_country_name('XY') == 'XY'
