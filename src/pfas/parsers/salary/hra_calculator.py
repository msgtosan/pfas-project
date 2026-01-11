"""HRA Exemption Calculator.

Calculates House Rent Allowance exemption under section 10(13A).

HRA exemption is the MINIMUM of:
1. Actual HRA received
2. Rent paid - 10% of Basic Salary
3. 50% of Basic (Metro) or 40% of Basic (Non-Metro)

Note: Under new tax regime, HRA exemption is NOT available.
This calculator is for old regime only.
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import List


class CityType(Enum):
    """Type of city for HRA calculation."""
    METRO = "METRO"  # Delhi, Mumbai, Chennai, Kolkata
    NON_METRO = "NON_METRO"


@dataclass
class HRACalculationInput:
    """Input for HRA exemption calculation."""
    basic_salary: Decimal  # Annual basic salary
    hra_received: Decimal  # Annual HRA received
    rent_paid: Decimal  # Annual rent paid
    city_type: CityType = CityType.NON_METRO


@dataclass
class HRACalculationResult:
    """Result of HRA exemption calculation."""
    actual_hra: Decimal
    rent_minus_10_percent: Decimal
    percentage_of_basic: Decimal
    exemption_allowed: Decimal
    taxable_hra: Decimal
    calculation_notes: str


class HRACalculator:
    """
    Calculate HRA exemption under section 10(13A).

    Metro cities: Delhi, Mumbai, Chennai, Kolkata
    - 50% of Basic Salary

    Non-Metro cities:
    - 40% of Basic Salary
    """

    METRO_PERCENTAGE = Decimal("0.50")  # 50% for metro
    NON_METRO_PERCENTAGE = Decimal("0.40")  # 40% for non-metro
    BASIC_DEDUCTION_PERCENTAGE = Decimal("0.10")  # 10% of basic

    METRO_CITIES = [
        'DELHI', 'NEW DELHI', 'MUMBAI', 'CHENNAI', 'KOLKATA',
        'BANGALORE', 'BENGALURU', 'HYDERABAD'  # Extended list (some interpretations)
    ]

    def calculate(self, input_data: HRACalculationInput) -> HRACalculationResult:
        """
        Calculate HRA exemption.

        HRA exemption = Minimum of:
        1. Actual HRA received
        2. Rent paid - 10% of Basic Salary
        3. 50% of Basic (Metro) or 40% of Basic (Non-Metro)

        Args:
            input_data: HRA calculation inputs

        Returns:
            HRACalculationResult with exemption details
        """
        # 1. Actual HRA received
        actual_hra = input_data.hra_received

        # 2. Rent paid - 10% of Basic
        ten_percent_basic = input_data.basic_salary * self.BASIC_DEDUCTION_PERCENTAGE
        rent_minus_10_percent = max(
            Decimal("0"),
            input_data.rent_paid - ten_percent_basic
        )

        # 3. Percentage of Basic based on city
        if input_data.city_type == CityType.METRO:
            percentage_of_basic = input_data.basic_salary * self.METRO_PERCENTAGE
            city_note = "Metro (50% of Basic)"
        else:
            percentage_of_basic = input_data.basic_salary * self.NON_METRO_PERCENTAGE
            city_note = "Non-Metro (40% of Basic)"

        # Exemption is minimum of the three
        exemption_allowed = min(
            actual_hra,
            rent_minus_10_percent,
            percentage_of_basic
        )

        # Taxable HRA = Actual HRA - Exemption
        taxable_hra = actual_hra - exemption_allowed

        # Determine which limit applied
        if exemption_allowed == actual_hra:
            limit_note = "Limited by Actual HRA"
        elif exemption_allowed == rent_minus_10_percent:
            limit_note = "Limited by Rent - 10% Basic"
        else:
            limit_note = f"Limited by {city_note}"

        notes = (
            f"1. Actual HRA: ₹{actual_hra:,.2f}\n"
            f"2. Rent - 10% Basic: ₹{rent_minus_10_percent:,.2f} "
            f"(₹{input_data.rent_paid:,.2f} - ₹{ten_percent_basic:,.2f})\n"
            f"3. {city_note}: ₹{percentage_of_basic:,.2f}\n"
            f"Exemption: ₹{exemption_allowed:,.2f} ({limit_note})"
        )

        return HRACalculationResult(
            actual_hra=actual_hra,
            rent_minus_10_percent=rent_minus_10_percent,
            percentage_of_basic=percentage_of_basic,
            exemption_allowed=exemption_allowed,
            taxable_hra=taxable_hra,
            calculation_notes=notes
        )

    def calculate_monthly(
        self,
        monthly_basic: Decimal,
        monthly_hra: Decimal,
        monthly_rent: Decimal,
        city_type: CityType = CityType.NON_METRO
    ) -> HRACalculationResult:
        """
        Calculate HRA exemption for a single month.

        Args:
            monthly_basic: Monthly basic salary
            monthly_hra: Monthly HRA received
            monthly_rent: Monthly rent paid
            city_type: Metro or Non-Metro

        Returns:
            HRACalculationResult for the month
        """
        input_data = HRACalculationInput(
            basic_salary=monthly_basic,
            hra_received=monthly_hra,
            rent_paid=monthly_rent,
            city_type=city_type
        )
        return self.calculate(input_data)

    def calculate_annual_from_monthly(
        self,
        monthly_records: List[dict],
        city_type: CityType = CityType.NON_METRO
    ) -> HRACalculationResult:
        """
        Calculate annual HRA exemption from monthly salary records.

        Args:
            monthly_records: List of monthly salary dicts with
                            'basic_salary', 'hra', and 'rent_paid' keys
            city_type: Metro or Non-Metro

        Returns:
            HRACalculationResult for the year
        """
        total_basic = sum(
            Decimal(str(r.get('basic_salary', 0)))
            for r in monthly_records
        )
        total_hra = sum(
            Decimal(str(r.get('hra', 0)))
            for r in monthly_records
        )
        total_rent = sum(
            Decimal(str(r.get('rent_paid', 0)))
            for r in monthly_records
        )

        input_data = HRACalculationInput(
            basic_salary=total_basic,
            hra_received=total_hra,
            rent_paid=total_rent,
            city_type=city_type
        )

        return self.calculate(input_data)

    @staticmethod
    def is_metro_city(city_name: str) -> bool:
        """
        Check if city qualifies as Metro for HRA purposes.

        Note: Strict interpretation includes only Delhi, Mumbai,
        Chennai, Kolkata. Extended interpretation may include
        Bangalore and Hyderabad.

        Args:
            city_name: City name

        Returns:
            True if Metro city
        """
        if not city_name:
            return False

        city_upper = city_name.upper().strip()

        # Check against known metro cities
        for metro in HRACalculator.METRO_CITIES:
            if metro in city_upper or city_upper in metro:
                return True

        return False

    def calculate_for_new_regime(self) -> dict:
        """
        Return info about HRA under new tax regime.

        Returns:
            Dictionary with new regime info
        """
        return {
            'exemption_allowed': Decimal("0"),
            'note': (
                "HRA exemption under section 10(13A) is NOT available "
                "under the new tax regime (section 115BAC). "
                "The entire HRA received is taxable."
            )
        }
