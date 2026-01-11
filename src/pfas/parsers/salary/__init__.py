"""Salary and Form 16 parsers module.

Provides parsers for:
- Monthly payslips (Qualcomm format)
- Form 16 Part A & B
- Form 12BA (Perquisites)

Key features:
- RSU Tax Credit handling (negative deduction = credit)
- ESPP deduction tracking
- TCS on ESPP (Section 206CQ)
- HRA exemption calculation
- RSU tax credit correlation with vest events
"""

from .models import (
    SalaryRecord,
    RSUTaxCredit,
    Form16Record,
    Form12BARecord,
    Perquisite,
    PerquisiteType,
    CorrelationStatus,
    SalaryParseResult,
    AnnualSalarySummary,
)
from .payslip import PayslipParser
from .form16 import Form16Parser
from .form12ba import Form12BAParser
from .rsu_correlation import RSUTaxCreditCorrelator
from .hra_calculator import HRACalculator, HRACalculationInput, HRACalculationResult, CityType

__all__ = [
    # Models
    "SalaryRecord",
    "RSUTaxCredit",
    "Form16Record",
    "Form12BARecord",
    "Perquisite",
    "PerquisiteType",
    "CorrelationStatus",
    "SalaryParseResult",
    "AnnualSalarySummary",
    # Parsers
    "PayslipParser",
    "Form16Parser",
    "Form12BAParser",
    # Utilities
    "RSUTaxCreditCorrelator",
    "HRACalculator",
    "HRACalculationInput",
    "HRACalculationResult",
    "CityType",
]
