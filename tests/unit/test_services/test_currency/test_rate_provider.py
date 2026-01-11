"""Tests for SBI TT Rate Provider."""

import pytest
from datetime import date
from decimal import Decimal

from pfas.services.currency import SBITTRateProvider, ExchangeRate


class TestSBITTRateProvider:
    """Tests for SBITTRateProvider class."""

    def test_provider_initialization(self, db_connection):
        """Test provider can be initialized."""
        provider = SBITTRateProvider(db_connection)
        assert provider.conn is not None

    def test_max_lookback_constant(self, db_connection):
        """Test max lookback days constant."""
        provider = SBITTRateProvider(db_connection)
        assert provider.MAX_LOOKBACK_DAYS == 7


class TestAddRate:
    """Tests for adding exchange rates."""

    def test_add_rate(self, db_connection):
        """Test adding a single rate."""
        provider = SBITTRateProvider(db_connection)

        provider.add_rate(
            rate_date=date(2024, 6, 15),
            from_currency="USD",
            to_currency="INR",
            rate=Decimal("83.50"),
            source="SBI"
        )

        # Verify rate was added
        rate = provider.get_rate(date(2024, 6, 15))
        assert rate == Decimal("83.50")

    def test_add_manual_rate(self, db_connection):
        """Test adding manual rate."""
        provider = SBITTRateProvider(db_connection)

        provider.add_manual_rate(
            rate_date=date(2024, 6, 16),
            rate=Decimal("83.75")
        )

        rate = provider.get_rate(date(2024, 6, 16))
        assert rate == Decimal("83.75")

    def test_add_rate_replaces_existing(self, db_connection):
        """Test that adding rate replaces existing."""
        provider = SBITTRateProvider(db_connection)

        provider.add_rate(date(2024, 6, 17), "USD", "INR", Decimal("83.00"), "SBI")
        provider.add_rate(date(2024, 6, 17), "USD", "INR", Decimal("84.00"), "MANUAL")

        rate = provider.get_rate(date(2024, 6, 17))
        assert rate == Decimal("84.00")


class TestGetRate:
    """Tests for getting exchange rates."""

    def test_get_exact_rate(self, db_connection):
        """Test getting rate for exact date."""
        provider = SBITTRateProvider(db_connection)
        provider.add_rate(date(2024, 6, 20), "USD", "INR", Decimal("83.25"), "SBI")

        rate = provider.get_rate(date(2024, 6, 20))
        assert rate == Decimal("83.25")

    def test_get_nearest_rate(self, db_connection):
        """Test getting nearest rate when exact not available."""
        provider = SBITTRateProvider(db_connection)
        # Add rate for Friday
        provider.add_rate(date(2024, 6, 21), "USD", "INR", Decimal("83.30"), "SBI")

        # Request rate for Saturday (should get Friday's rate)
        rate = provider.get_rate(date(2024, 6, 22))
        assert rate == Decimal("83.30")

    def test_get_rate_no_rate_available(self, db_connection):
        """Test error when no rate available."""
        provider = SBITTRateProvider(db_connection)

        with pytest.raises(ValueError, match="No exchange rate available"):
            provider.get_rate(date(2020, 1, 1))

    def test_get_rate_different_currency(self, db_connection):
        """Test getting rate for different currency pair."""
        provider = SBITTRateProvider(db_connection)
        provider.add_rate(date(2024, 6, 25), "GBP", "INR", Decimal("106.50"), "SBI")

        rate = provider.get_rate(date(2024, 6, 25), "GBP", "INR")
        assert rate == Decimal("106.50")


class TestGetRateRecord:
    """Tests for getting full rate records."""

    def test_get_rate_record(self, db_connection):
        """Test getting full rate record."""
        provider = SBITTRateProvider(db_connection)
        provider.add_rate(date(2024, 6, 26), "USD", "INR", Decimal("83.40"), "SBI")

        record = provider.get_rate_record(date(2024, 6, 26))

        assert record is not None
        assert record.rate_date == date(2024, 6, 26)
        assert record.from_currency == "USD"
        assert record.to_currency == "INR"
        assert record.rate == Decimal("83.40")
        assert record.source == "SBI"

    def test_get_rate_record_none(self, db_connection):
        """Test getting rate record when not available."""
        provider = SBITTRateProvider(db_connection)

        record = provider.get_rate_record(date(2020, 1, 1))
        assert record is None


class TestBulkAddRates:
    """Tests for bulk adding rates."""

    def test_bulk_add_rates(self, db_connection):
        """Test bulk adding multiple rates."""
        provider = SBITTRateProvider(db_connection)

        rates = [
            ExchangeRate(date(2024, 7, 1), "USD", "INR", Decimal("83.10"), "SBI"),
            ExchangeRate(date(2024, 7, 2), "USD", "INR", Decimal("83.15"), "SBI"),
            ExchangeRate(date(2024, 7, 3), "USD", "INR", Decimal("83.20"), "SBI"),
        ]

        count = provider.bulk_add_rates(rates)
        assert count == 3

        # Verify all rates were added
        assert provider.get_rate(date(2024, 7, 1)) == Decimal("83.10")
        assert provider.get_rate(date(2024, 7, 2)) == Decimal("83.15")
        assert provider.get_rate(date(2024, 7, 3)) == Decimal("83.20")


class TestGetRatesForPeriod:
    """Tests for getting rates in a date range."""

    def test_get_rates_for_period(self, db_connection):
        """Test getting rates for a period."""
        provider = SBITTRateProvider(db_connection)

        # Add rates
        provider.add_rate(date(2024, 7, 10), "USD", "INR", Decimal("83.50"), "SBI")
        provider.add_rate(date(2024, 7, 11), "USD", "INR", Decimal("83.55"), "SBI")
        provider.add_rate(date(2024, 7, 12), "USD", "INR", Decimal("83.60"), "SBI")

        rates = provider.get_rates_for_period(
            start_date=date(2024, 7, 10),
            end_date=date(2024, 7, 12)
        )

        assert len(rates) == 3
        assert rates[0].rate == Decimal("83.50")
        assert rates[2].rate == Decimal("83.60")

    def test_get_rates_empty_period(self, db_connection):
        """Test getting rates for period with no data."""
        provider = SBITTRateProvider(db_connection)

        rates = provider.get_rates_for_period(
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 31)
        )

        assert len(rates) == 0


class TestGetFYEndRate:
    """Tests for FY end rate."""

    def test_get_fy_end_rate(self, db_connection):
        """Test getting FY end rate."""
        provider = SBITTRateProvider(db_connection)
        # Add rate for March 31, 2025
        provider.add_rate(date(2025, 3, 31), "USD", "INR", Decimal("84.00"), "SBI")

        rate = provider.get_fy_end_rate("2024-25")
        assert rate == Decimal("84.00")

    def test_get_fy_end_rate_nearest(self, db_connection):
        """Test getting nearest FY end rate when exact not available."""
        provider = SBITTRateProvider(db_connection)
        # Add rate for March 28 (Friday before March 31 weekend)
        provider.add_rate(date(2025, 3, 28), "USD", "INR", Decimal("83.90"), "SBI")

        rate = provider.get_fy_end_rate("2024-25")
        assert rate == Decimal("83.90")


class TestConvert:
    """Tests for currency conversion."""

    def test_convert(self, db_connection):
        """Test currency conversion."""
        provider = SBITTRateProvider(db_connection)
        provider.add_rate(date(2024, 8, 1), "USD", "INR", Decimal("83.50"), "SBI")

        inr_amount = provider.convert(
            amount=Decimal("100.00"),
            rate_date=date(2024, 8, 1)
        )

        assert inr_amount == Decimal("8350.00")

    def test_convert_with_decimals(self, db_connection):
        """Test conversion with decimal amounts."""
        provider = SBITTRateProvider(db_connection)
        provider.add_rate(date(2024, 8, 2), "USD", "INR", Decimal("83.45"), "SBI")

        inr_amount = provider.convert(
            amount=Decimal("156.78"),
            rate_date=date(2024, 8, 2)
        )

        expected = Decimal("156.78") * Decimal("83.45")
        assert inr_amount == expected


class TestExchangeRateModel:
    """Tests for ExchangeRate dataclass."""

    def test_exchange_rate_creation(self):
        """Test creating ExchangeRate."""
        rate = ExchangeRate(
            rate_date=date(2024, 6, 1),
            from_currency="USD",
            to_currency="INR",
            rate=Decimal("83.50"),
            source="SBI"
        )

        assert rate.rate_date == date(2024, 6, 1)
        assert rate.from_currency == "USD"
        assert rate.to_currency == "INR"
        assert rate.rate == Decimal("83.50")
        assert rate.source == "SBI"
