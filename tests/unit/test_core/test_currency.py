"""
Unit tests for currency module.

Tests multi-currency conversion and exchange rate management.
"""

import pytest
from decimal import Decimal
from datetime import date, timedelta

from pfas.core.currency import CurrencyConverter, ExchangeRate, get_sbi_tt_buying_rate
from pfas.core.exceptions import ExchangeRateNotFoundError


@pytest.fixture
def converter(db_connection):
    """Provide a CurrencyConverter instance."""
    return CurrencyConverter(db_connection)


class TestExchangeRateManagement:
    """Tests for exchange rate management."""

    def test_add_rate(self, converter):
        """Test adding an exchange rate."""
        rate_id = converter.add_rate(
            rate_date=date(2024, 6, 15),
            from_currency="USD",
            rate=Decimal("83.50"),
        )

        assert rate_id > 0

    def test_add_rate_with_source(self, converter):
        """Test adding a rate with custom source."""
        converter.add_rate(
            rate_date=date(2024, 6, 15),
            from_currency="USD",
            rate=Decimal("83.50"),
            source="MANUAL",
        )

        rate = converter.get_rate("USD", date(2024, 6, 15))
        assert rate.source == "MANUAL"

    def test_add_rate_upsert(self, converter):
        """Test that adding rate for same date/currency updates existing."""
        converter.add_rate(date(2024, 6, 15), "USD", Decimal("83.00"))
        converter.add_rate(date(2024, 6, 15), "USD", Decimal("84.00"))

        rate = converter.get_rate("USD", date(2024, 6, 15))
        assert rate.rate == Decimal("84.00")

    def test_get_rate(self, converter):
        """Test getting an exchange rate."""
        converter.add_rate(date(2024, 6, 15), "USD", Decimal("83.50"))

        rate = converter.get_rate("USD", date(2024, 6, 15))

        assert rate is not None
        assert isinstance(rate, ExchangeRate)
        assert rate.from_currency == "USD"
        assert rate.to_currency == "INR"
        assert rate.rate == Decimal("83.50")

    def test_get_rate_not_found(self, converter):
        """Test getting rate for non-existent date returns None."""
        rate = converter.get_rate("USD", date(2024, 1, 1))
        assert rate is None

    def test_get_rate_case_insensitive(self, converter):
        """Test that currency codes are case insensitive."""
        converter.add_rate(date(2024, 6, 15), "usd", Decimal("83.50"))

        rate = converter.get_rate("USD", date(2024, 6, 15))
        assert rate is not None


class TestCurrencyConversion:
    """Tests for currency conversion."""

    def test_usd_to_inr_conversion(self, converter, sample_exchange_rates):
        """Test USD to INR conversion using exchange rate (TC-CORE-004)."""
        inr_amount = converter.convert(
            amount=Decimal("100"),
            from_currency="USD",
            as_of_date=date(2024, 6, 15),
        )

        # Rate on 2024-06-15 is 83.75
        assert inr_amount == Decimal("8375.00")

    def test_convert_same_currency(self, converter):
        """Test conversion with same currency returns original amount."""
        amount = Decimal("1000.50")
        result = converter.convert(amount, "INR", date.today(), "INR")

        assert result == Decimal("1000.50")

    def test_convert_with_rounding(self, converter):
        """Test conversion rounds to 2 decimal places."""
        converter.add_rate(date(2024, 6, 15), "USD", Decimal("83.333"))

        result = converter.convert(
            Decimal("100"),
            "USD",
            date(2024, 6, 15),
        )

        # 100 * 83.333 = 8333.3 -> rounds to 8333.30
        assert result == Decimal("8333.30")

    def test_convert_rate_not_found(self, converter):
        """Test conversion raises error when rate not found."""
        with pytest.raises(ExchangeRateNotFoundError):
            converter.convert(
                Decimal("100"),
                "USD",
                date(2024, 1, 1),
                use_nearest=False,
            )


class TestNearestRate:
    """Tests for nearest rate lookup."""

    def test_get_rate_or_nearest_exact(self, converter, sample_exchange_rates):
        """Test nearest rate returns exact match if available."""
        rate = converter.get_rate_or_nearest("USD", date(2024, 6, 15))

        assert rate is not None
        assert rate.date == date(2024, 6, 15)
        assert rate.rate == Decimal("83.75")

    def test_get_rate_or_nearest_fallback(self, converter, sample_exchange_rates):
        """Test nearest rate falls back to previous date."""
        # Date 2024-06-14 doesn't exist, should get 2024-06-12
        rate = converter.get_rate_or_nearest("USD", date(2024, 6, 14))

        assert rate is not None
        assert rate.date == date(2024, 6, 12)
        assert rate.rate == Decimal("83.50")

    def test_get_rate_or_nearest_beyond_max_days(self, converter, sample_exchange_rates):
        """Test nearest rate returns None if beyond max days."""
        # Last USD rate is 2024-06-15, looking for 2024-06-25 with 7 day max
        rate = converter.get_rate_or_nearest(
            "USD",
            date(2024, 6, 25),
            max_days_back=7,
        )

        assert rate is None

    def test_convert_uses_nearest_by_default(self, converter, sample_exchange_rates):
        """Test that convert uses nearest rate by default."""
        # Date 2024-06-13 doesn't exist, should use 2024-06-12 (83.50)
        result = converter.convert(Decimal("100"), "USD", date(2024, 6, 13))

        assert result == Decimal("8350.00")


class TestRatePeriod:
    """Tests for rate period queries."""

    def test_get_rates_for_period(self, converter, sample_exchange_rates):
        """Test getting rates within a date range."""
        rates = converter.get_rates_for_period(
            "USD",
            date(2024, 6, 10),
            date(2024, 6, 15),
        )

        assert len(rates) == 4  # 10, 11, 12, 15

        # Should be in chronological order
        assert rates[0].date == date(2024, 6, 10)
        assert rates[-1].date == date(2024, 6, 15)

    def test_get_rates_for_period_empty(self, converter):
        """Test getting rates for period with no data."""
        rates = converter.get_rates_for_period(
            "GBP",
            date(2024, 6, 10),
            date(2024, 6, 15),
        )

        assert len(rates) == 0


class TestBulkRates:
    """Tests for bulk rate operations."""

    def test_bulk_add_rates(self, converter):
        """Test adding multiple rates at once."""
        rates = [
            (date(2024, 7, 1), "USD", Decimal("84.00")),
            (date(2024, 7, 2), "USD", Decimal("84.25")),
            (date(2024, 7, 3), "USD", Decimal("84.50")),
        ]

        count = converter.bulk_add_rates(rates)
        assert count == 3

        # Verify rates were added
        for rate_date, currency, expected_rate in rates:
            rate = converter.get_rate(currency, rate_date)
            assert rate is not None
            assert rate.rate == expected_rate


class TestExchangeRateDataclass:
    """Tests for ExchangeRate dataclass."""

    def test_from_row(self, db_connection, converter):
        """Test creating ExchangeRate from database row."""
        converter.add_rate(date(2024, 6, 15), "USD", Decimal("83.50"))

        cursor = db_connection.execute(
            "SELECT * FROM exchange_rates WHERE from_currency = 'USD'"
        )
        row = cursor.fetchone()

        rate = ExchangeRate.from_row(row)

        assert rate.from_currency == "USD"
        assert rate.to_currency == "INR"
        assert rate.rate == Decimal("83.50")


class TestSbiRatePlaceholder:
    """Tests for SBI rate lookup placeholder."""

    def test_get_sbi_tt_buying_rate_placeholder(self):
        """Test that SBI rate lookup returns None (Phase 1 placeholder)."""
        rate = get_sbi_tt_buying_rate("USD", date.today())
        assert rate is None
