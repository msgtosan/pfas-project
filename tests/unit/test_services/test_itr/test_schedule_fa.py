"""Tests for Schedule FA Generator."""

import pytest
from datetime import date
from decimal import Decimal

from pfas.services.itr.schedule_fa import (
    ScheduleFAGenerator,
    ScheduleFAData,
    ForeignAccount,
    ForeignCustodialAccount,
    ForeignEquityHolding
)


class TestScheduleFAGenerator:
    """Tests for ScheduleFAGenerator class."""

    def test_generator_initialization(self, db_connection):
        """Test generator can be initialized."""
        generator = ScheduleFAGenerator(db_connection)
        assert generator.conn is not None
        assert generator.rate_provider is not None

    def test_country_codes(self, db_connection):
        """Test country codes mapping."""
        generator = ScheduleFAGenerator(db_connection)

        assert generator.COUNTRY_CODES['US'] == '1'
        assert generator.COUNTRY_CODES['USA'] == '1'
        assert generator.COUNTRY_CODES['UK'] == '2'
        assert generator.COUNTRY_CODES['Singapore'] == '3'


class TestForeignAccount:
    """Tests for ForeignAccount dataclass."""

    def test_account_creation(self):
        """Test creating foreign account."""
        account = ForeignAccount(
            country_code='1',
            country_name='United States',
            account_name='Morgan Stanley',
            account_address='1585 Broadway, New York',
            zip_code='10036',
            account_number='12345678',
            account_status='O',
            account_opening_date=date(2020, 1, 15)
        )

        assert account.country_code == '1'
        assert account.country_name == 'United States'
        assert account.account_number == '12345678'
        assert account.account_status == 'O'

    def test_account_default_values(self):
        """Test account default values."""
        account = ForeignAccount(
            country_code='1',
            country_name='United States',
            account_name='Test Bank',
            account_address='Test Address',
            zip_code='12345',
            account_number='123',
            account_status='O'
        )

        assert account.peak_balance_foreign == Decimal("0")
        assert account.peak_balance_inr == Decimal("0")
        assert account.closing_balance_foreign == Decimal("0")
        assert account.closing_balance_inr == Decimal("0")
        assert account.gross_interest_credited == Decimal("0")
        assert account.nature_of_account == "Savings"


class TestForeignCustodialAccount:
    """Tests for ForeignCustodialAccount dataclass."""

    def test_custodial_account_creation(self):
        """Test creating custodial account."""
        account = ForeignCustodialAccount(
            country_code='1',
            country_name='United States',
            institution_name='Morgan Stanley',
            institution_address='New York',
            zip_code='10036',
            account_number='MS-12345',
            account_status='O',
            peak_value_foreign=Decimal("50000.00"),
            peak_value_inr=Decimal("4175000.00"),
            closing_value_foreign=Decimal("45000.00"),
            closing_value_inr=Decimal("3757500.00")
        )

        assert account.institution_name == 'Morgan Stanley'
        assert account.peak_value_foreign == Decimal("50000.00")
        assert account.closing_value_foreign == Decimal("45000.00")


class TestForeignEquityHolding:
    """Tests for ForeignEquityHolding dataclass."""

    def test_equity_holding_creation(self):
        """Test creating equity holding."""
        holding = ForeignEquityHolding(
            country_code='1',
            country_name='United States',
            entity_name='Apple Inc.',
            entity_address='Cupertino, CA',
            zip_code='95014',
            nature_of_entity='Company',
            date_of_acquisition=date(2023, 6, 15),
            initial_investment_foreign=Decimal("15000.00"),
            initial_investment_inr=Decimal("1252500.00"),
            closing_value_foreign=Decimal("18000.00"),
            closing_value_inr=Decimal("1503000.00"),
            symbol='AAPL',
            shares_held=Decimal("100")
        )

        assert holding.entity_name == 'Apple Inc.'
        assert holding.symbol == 'AAPL'
        assert holding.shares_held == Decimal("100")
        assert holding.nature_of_entity == 'Company'

    def test_equity_holding_defaults(self):
        """Test equity holding default values."""
        holding = ForeignEquityHolding(
            country_code='1',
            country_name='United States',
            entity_name='Test Corp',
            entity_address='USA',
            zip_code='',
            nature_of_entity='Company'
        )

        assert holding.initial_investment_foreign == Decimal("0")
        assert holding.peak_value_foreign == Decimal("0")
        assert holding.closing_value_foreign == Decimal("0")
        assert holding.income_accrued_foreign == Decimal("0")
        assert holding.symbol == ""
        assert holding.shares_held == Decimal("0")


class TestScheduleFAData:
    """Tests for ScheduleFAData dataclass."""

    def test_schedule_fa_creation(self):
        """Test creating Schedule FA data."""
        schedule_fa = ScheduleFAData(
            financial_year="2024-25",
            assessment_year="2025-26"
        )

        assert schedule_fa.financial_year == "2024-25"
        assert schedule_fa.assessment_year == "2025-26"
        assert len(schedule_fa.depository_accounts) == 0
        assert len(schedule_fa.custodial_accounts) == 0
        assert len(schedule_fa.equity_holdings) == 0

    def test_schedule_fa_calculate_totals_empty(self):
        """Test totals calculation with no data."""
        schedule_fa = ScheduleFAData(
            financial_year="2024-25",
            assessment_year="2025-26"
        )

        schedule_fa.calculate_totals()

        assert schedule_fa.total_peak_value_inr == Decimal("0")
        assert schedule_fa.total_closing_value_inr == Decimal("0")
        assert schedule_fa.total_income_inr == Decimal("0")

    def test_schedule_fa_calculate_totals_with_data(self):
        """Test totals calculation with data."""
        schedule_fa = ScheduleFAData(
            financial_year="2024-25",
            assessment_year="2025-26"
        )

        # Add custodial account
        schedule_fa.custodial_accounts.append(ForeignCustodialAccount(
            country_code='1',
            country_name='US',
            institution_name='MS',
            institution_address='NY',
            zip_code='10036',
            account_number='123',
            account_status='O',
            peak_value_inr=Decimal("1000000"),
            closing_value_inr=Decimal("900000"),
            gross_income_inr=Decimal("50000")
        ))

        # Add equity holding
        schedule_fa.equity_holdings.append(ForeignEquityHolding(
            country_code='1',
            country_name='US',
            entity_name='Test',
            entity_address='USA',
            zip_code='',
            nature_of_entity='Company',
            peak_value_inr=Decimal("500000"),
            closing_value_inr=Decimal("550000"),
            income_accrued_inr=Decimal("25000")
        ))

        schedule_fa.calculate_totals()

        assert schedule_fa.total_peak_value_inr == Decimal("1500000")
        assert schedule_fa.total_closing_value_inr == Decimal("1450000")
        assert schedule_fa.total_income_inr == Decimal("75000")


class TestGenerateScheduleFA:
    """Tests for Schedule FA generation."""

    def test_generate_empty(self, db_connection, sample_user):
        """Test generating Schedule FA with no foreign assets."""
        from pfas.services.currency import SBITTRateProvider

        # Add required exchange rate for FY end
        rate_provider = SBITTRateProvider(db_connection)
        rate_provider.add_rate(date(2025, 3, 31), "USD", "INR", Decimal("84.00"), "SBI")

        generator = ScheduleFAGenerator(db_connection)

        schedule_fa = generator.generate(sample_user["id"], "2024-25")

        assert schedule_fa.financial_year == "2024-25"
        assert schedule_fa.assessment_year == "2025-26"
        assert len(schedule_fa.custodial_accounts) == 0
        assert len(schedule_fa.equity_holdings) == 0

    def test_assessment_year_calculation(self, db_connection, sample_user):
        """Test assessment year calculation."""
        from pfas.services.currency import SBITTRateProvider

        # Add required exchange rates for FY ends
        rate_provider = SBITTRateProvider(db_connection)
        rate_provider.add_rate(date(2024, 3, 31), "USD", "INR", Decimal("83.50"), "SBI")
        rate_provider.add_rate(date(2025, 3, 31), "USD", "INR", Decimal("84.00"), "SBI")

        generator = ScheduleFAGenerator(db_connection)

        schedule_fa = generator.generate(sample_user["id"], "2023-24")
        assert schedule_fa.assessment_year == "2024-25"

        schedule_fa = generator.generate(sample_user["id"], "2024-25")
        assert schedule_fa.assessment_year == "2025-26"


class TestSaveScheduleFA:
    """Tests for saving Schedule FA."""

    def test_to_json(self, db_connection):
        """Test JSON serialization."""
        generator = ScheduleFAGenerator(db_connection)

        schedule_fa = ScheduleFAData(
            financial_year="2024-25",
            assessment_year="2025-26",
            total_peak_value_inr=Decimal("1000000"),
            total_closing_value_inr=Decimal("950000")
        )

        json_str = generator._to_json(schedule_fa)

        assert "2024-25" in json_str
        assert "1000000" in json_str
