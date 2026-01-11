"""Tests for HRA Exemption Calculator."""

import pytest
from decimal import Decimal

from pfas.parsers.salary.hra_calculator import (
    HRACalculator,
    HRACalculationInput,
    HRACalculationResult,
    CityType,
)


class TestHRACalculator:
    """Tests for HRACalculator class."""

    def test_calculator_creation(self):
        """Test calculator can be created."""
        calc = HRACalculator()
        assert calc is not None

    def test_metro_percentage(self):
        """Test metro percentage is 50%."""
        calc = HRACalculator()
        assert calc.METRO_PERCENTAGE == Decimal("0.50")

    def test_non_metro_percentage(self):
        """Test non-metro percentage is 40%."""
        calc = HRACalculator()
        assert calc.NON_METRO_PERCENTAGE == Decimal("0.40")


class TestHRACalculation:
    """Tests for HRA exemption calculation."""

    def test_exemption_limited_by_actual_hra(self):
        """Test exemption when limited by actual HRA received."""
        calc = HRACalculator()

        input_data = HRACalculationInput(
            basic_salary=Decimal("1200000"),  # 12L annual
            hra_received=Decimal("100000"),  # 1L HRA (very low)
            rent_paid=Decimal("360000"),  # 3L rent
            city_type=CityType.METRO
        )

        result = calc.calculate(input_data)

        # Actual HRA (1L) < Rent-10% (3L - 1.2L = 1.8L) < 50% Basic (6L)
        assert result.exemption_allowed == Decimal("100000")
        assert result.taxable_hra == Decimal("0")

    def test_exemption_limited_by_rent_minus_10_percent(self):
        """Test exemption when limited by Rent - 10% of Basic."""
        calc = HRACalculator()

        input_data = HRACalculationInput(
            basic_salary=Decimal("1200000"),  # 12L annual
            hra_received=Decimal("480000"),  # 4.8L HRA
            rent_paid=Decimal("180000"),  # 1.8L rent (low rent)
            city_type=CityType.METRO
        )

        result = calc.calculate(input_data)

        # Rent - 10% Basic = 1.8L - 1.2L = 0.6L
        # This is less than 4.8L HRA and 6L (50% of 12L)
        assert result.rent_minus_10_percent == Decimal("60000")
        assert result.exemption_allowed == Decimal("60000")

    def test_exemption_limited_by_percentage_of_basic_metro(self):
        """Test exemption when limited by 50% of Basic (Metro)."""
        calc = HRACalculator()

        input_data = HRACalculationInput(
            basic_salary=Decimal("600000"),  # 6L annual
            hra_received=Decimal("480000"),  # 4.8L HRA
            rent_paid=Decimal("600000"),  # 6L rent
            city_type=CityType.METRO
        )

        result = calc.calculate(input_data)

        # 50% of Basic = 3L, which is less than HRA (4.8L) and Rent-10% (5.4L)
        assert result.percentage_of_basic == Decimal("300000")
        assert result.exemption_allowed == Decimal("300000")

    def test_exemption_limited_by_percentage_of_basic_non_metro(self):
        """Test exemption when limited by 40% of Basic (Non-Metro)."""
        calc = HRACalculator()

        input_data = HRACalculationInput(
            basic_salary=Decimal("600000"),  # 6L annual
            hra_received=Decimal("480000"),  # 4.8L HRA
            rent_paid=Decimal("600000"),  # 6L rent
            city_type=CityType.NON_METRO
        )

        result = calc.calculate(input_data)

        # 40% of Basic = 2.4L, which is less than HRA (4.8L) and Rent-10% (5.4L)
        assert result.percentage_of_basic == Decimal("240000")
        assert result.exemption_allowed == Decimal("240000")

    def test_zero_rent_paid(self):
        """Test HRA calculation when no rent is paid."""
        calc = HRACalculator()

        input_data = HRACalculationInput(
            basic_salary=Decimal("600000"),
            hra_received=Decimal("240000"),
            rent_paid=Decimal("0"),
            city_type=CityType.METRO
        )

        result = calc.calculate(input_data)

        # Rent - 10% Basic = 0 - 60000 = negative, capped at 0
        assert result.rent_minus_10_percent == Decimal("0")
        assert result.exemption_allowed == Decimal("0")
        assert result.taxable_hra == Decimal("240000")

    def test_taxable_hra_calculation(self):
        """Test taxable HRA is correctly calculated."""
        calc = HRACalculator()

        input_data = HRACalculationInput(
            basic_salary=Decimal("600000"),
            hra_received=Decimal("300000"),
            rent_paid=Decimal("180000"),
            city_type=CityType.NON_METRO
        )

        result = calc.calculate(input_data)

        # Exemption = min(3L, 1.8L-0.6L=1.2L, 2.4L) = 1.2L
        assert result.exemption_allowed == Decimal("120000")
        assert result.taxable_hra == Decimal("180000")  # 3L - 1.2L


class TestHRAMonthlyCalculation:
    """Tests for monthly HRA calculation."""

    def test_calculate_monthly(self):
        """Test monthly HRA calculation."""
        calc = HRACalculator()

        result = calc.calculate_monthly(
            monthly_basic=Decimal("50000"),
            monthly_hra=Decimal("20000"),
            monthly_rent=Decimal("25000"),
            city_type=CityType.METRO
        )

        # 10% of Basic = 5000
        # Rent - 10% = 25000 - 5000 = 20000
        # 50% of Basic = 25000
        # Actual HRA = 20000
        # Min = 20000
        assert result.exemption_allowed == Decimal("20000")


class TestHRAAnnualFromMonthly:
    """Tests for annual HRA calculation from monthly records."""

    def test_calculate_annual_from_monthly(self):
        """Test annual calculation from monthly records."""
        calc = HRACalculator()

        monthly_records = [
            {'basic_salary': 50000, 'hra': 20000, 'rent_paid': 25000}
            for _ in range(12)
        ]

        result = calc.calculate_annual_from_monthly(
            monthly_records,
            city_type=CityType.METRO
        )

        # Annual values
        assert result.actual_hra == Decimal("240000")  # 20k * 12


class TestMetroCityDetection:
    """Tests for metro city detection."""

    def test_is_metro_city_delhi(self):
        """Test Delhi is detected as metro."""
        assert HRACalculator.is_metro_city("Delhi") is True
        assert HRACalculator.is_metro_city("NEW DELHI") is True

    def test_is_metro_city_mumbai(self):
        """Test Mumbai is detected as metro."""
        assert HRACalculator.is_metro_city("Mumbai") is True

    def test_is_metro_city_chennai(self):
        """Test Chennai is detected as metro."""
        assert HRACalculator.is_metro_city("Chennai") is True

    def test_is_metro_city_kolkata(self):
        """Test Kolkata is detected as metro."""
        assert HRACalculator.is_metro_city("Kolkata") is True

    def test_is_metro_city_bangalore(self):
        """Test Bangalore is detected as metro (extended list)."""
        assert HRACalculator.is_metro_city("Bangalore") is True
        assert HRACalculator.is_metro_city("Bengaluru") is True

    def test_is_metro_city_hyderabad(self):
        """Test Hyderabad is detected as metro (extended list)."""
        assert HRACalculator.is_metro_city("Hyderabad") is True

    def test_is_non_metro_city(self):
        """Test non-metro cities are not detected as metro."""
        assert HRACalculator.is_metro_city("Pune") is False
        assert HRACalculator.is_metro_city("Jaipur") is False
        assert HRACalculator.is_metro_city("Ahmedabad") is False

    def test_is_metro_city_empty(self):
        """Test empty city name returns False."""
        assert HRACalculator.is_metro_city("") is False
        assert HRACalculator.is_metro_city(None) is False


class TestNewRegimeHRA:
    """Tests for HRA under new tax regime."""

    def test_new_regime_no_exemption(self):
        """Test HRA exemption is not available under new regime."""
        calc = HRACalculator()
        result = calc.calculate_for_new_regime()

        assert result['exemption_allowed'] == Decimal("0")
        assert "NOT available" in result['note']
        assert "new tax regime" in result['note'].lower()
