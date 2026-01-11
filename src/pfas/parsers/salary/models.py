"""Salary and Form 16 data models."""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Optional


class PerquisiteType(Enum):
    """Types of perquisites from Form 12BA."""
    RSU = "RSU"
    ESPP_DISCOUNT = "ESPP_DISCOUNT"
    EMPLOYER_PF = "EMPLOYER_PF"
    EMPLOYER_NPS = "EMPLOYER_NPS"
    INTEREST_ACCRETION = "INTEREST_ACCRETION"
    OTHER = "OTHER"


class CorrelationStatus(Enum):
    """Status of RSU tax credit correlation."""
    PENDING = "PENDING"
    MATCHED = "MATCHED"
    UNMATCHED = "UNMATCHED"


@dataclass
class SalaryRecord:
    """
    Monthly salary record from payslip.

    Key Tax Concepts:
    - RSU Tax Credit: When RSUs vest, company pays tax on your behalf.
      Appears as NEGATIVE deduction in payslip = money added back.
    - ESPP Deduction: Investment in US stock through payroll.
    - TCS on ESPP: 20% TCS on LRS remittance (Section 206CQ), claimable as credit.
    - Professional Tax: State tax, deductible under section 16(iii).
    """

    pay_period: str  # 'June 2024'
    pay_date: Optional[date] = None

    # Employee Info
    employee_id: Optional[str] = None
    employee_name: Optional[str] = None
    pan: Optional[str] = None
    pf_number: Optional[str] = None
    uan: Optional[str] = None

    # Earnings
    basic_salary: Decimal = Decimal("0")
    hra: Decimal = Decimal("0")
    special_allowance: Decimal = Decimal("0")
    lta: Decimal = Decimal("0")
    other_allowances: Decimal = Decimal("0")
    gross_salary: Decimal = Decimal("0")

    # Deductions
    pf_employee: Decimal = Decimal("0")
    pf_employer: Decimal = Decimal("0")
    nps_employee: Decimal = Decimal("0")
    nps_employer: Decimal = Decimal("0")
    professional_tax: Decimal = Decimal("0")
    income_tax_deducted: Decimal = Decimal("0")
    espp_deduction: Decimal = Decimal("0")
    tcs_on_espp: Decimal = Decimal("0")
    other_deductions: Decimal = Decimal("0")

    # RSU Tax Credit - CRITICAL
    # This is a TAX CREDIT (money added back) when RSUs vest
    # Appears as NEGATIVE number in payslip deductions
    # Store as POSITIVE amount here
    rsu_tax_credit: Decimal = Decimal("0")

    # Totals
    total_deductions: Decimal = Decimal("0")
    net_pay: Decimal = Decimal("0")

    def calculate_totals(self):
        """Calculate gross and total deductions if not provided."""
        if self.gross_salary == Decimal("0"):
            self.gross_salary = (
                self.basic_salary + self.hra + self.special_allowance +
                self.lta + self.other_allowances
            )

        # Note: RSU tax credit is NOT a deduction, it's a credit
        # So it should NOT be included in total_deductions calculation


@dataclass
class RSUTaxCredit:
    """
    RSU tax credit record.

    When RSUs vest, the company withholds shares to cover tax liability.
    The payslip shows this as a NEGATIVE deduction (money added back).
    This should be correlated with the RSU vest event for accurate reporting.
    """

    salary_record_id: int
    credit_amount: Decimal  # Stored as positive
    credit_date: date
    vest_id: Optional[int] = None  # Link to RSU vest event (Phase 2)
    correlation_status: CorrelationStatus = CorrelationStatus.PENDING


@dataclass
class Form16Record:
    """
    Form 16 record with Part A (TDS) and Part B (Salary details).

    Part A: Quarterly TDS certificate
    Part B: Salary breakup and deductions
    """

    assessment_year: str  # '2025-26'
    employer_name: Optional[str] = None
    employer_tan: Optional[str] = None
    employee_pan: Optional[str] = None

    # Part A - Quarterly TDS
    q1_tds: Decimal = Decimal("0")
    q2_tds: Decimal = Decimal("0")
    q3_tds: Decimal = Decimal("0")
    q4_tds: Decimal = Decimal("0")
    total_tds: Decimal = Decimal("0")

    # Part B - Income under Section 17
    salary_17_1: Decimal = Decimal("0")  # Salary as per section 17(1)
    perquisites_17_2: Decimal = Decimal("0")  # Value of perquisites u/s 17(2)
    profits_17_3: Decimal = Decimal("0")  # Profits in lieu of salary u/s 17(3)
    gross_salary: Decimal = Decimal("0")  # Total of above

    # Exemptions under Section 10
    hra_exemption: Decimal = Decimal("0")
    lta_exemption: Decimal = Decimal("0")
    other_exemptions: Decimal = Decimal("0")

    # Deductions under Section 16
    standard_deduction: Decimal = Decimal("0")  # Rs 75,000 for FY 2024-25
    professional_tax: Decimal = Decimal("0")  # Max Rs 2,500

    # Chapter VI-A Deductions
    section_80c: Decimal = Decimal("0")
    section_80ccc: Decimal = Decimal("0")
    section_80ccd_1: Decimal = Decimal("0")
    section_80ccd_1b: Decimal = Decimal("0")  # Additional NPS
    section_80ccd_2: Decimal = Decimal("0")  # Employer NPS contribution
    section_80d: Decimal = Decimal("0")  # Medical insurance
    section_80e: Decimal = Decimal("0")  # Education loan
    section_80g: Decimal = Decimal("0")  # Donations

    # Taxable Income and Tax
    taxable_income: Decimal = Decimal("0")
    tax_on_income: Decimal = Decimal("0")
    surcharge: Decimal = Decimal("0")
    education_cess: Decimal = Decimal("0")
    total_tax_payable: Decimal = Decimal("0")
    relief_87a: Decimal = Decimal("0")
    net_tax_payable: Decimal = Decimal("0")

    def calculate_total_tds(self):
        """Calculate total TDS from quarterly values."""
        self.total_tds = self.q1_tds + self.q2_tds + self.q3_tds + self.q4_tds

    def calculate_gross_salary(self):
        """Calculate gross salary from components."""
        self.gross_salary = self.salary_17_1 + self.perquisites_17_2 + self.profits_17_3


@dataclass
class Perquisite:
    """
    Perquisite record from Form 12BA.

    Common perquisites:
    - RSU: Stock options (non-qualified)
    - ESPP Discount: Discount on ESPP purchase
    - Employer PF: Contribution above Rs 7.5L limit
    - Interest Accretion: Interest on taxable employer contributions
    """

    perquisite_type: PerquisiteType
    description: str
    gross_value: Decimal
    recovered_from_employee: Decimal = Decimal("0")
    taxable_value: Decimal = Decimal("0")

    def calculate_taxable_value(self):
        """Calculate taxable value after recovery."""
        self.taxable_value = self.gross_value - self.recovered_from_employee


@dataclass
class Form12BARecord:
    """
    Form 12BA - Statement of Perquisites.

    Contains details of perquisites provided by employer that are
    taxable as part of salary income.
    """

    assessment_year: str
    employer_tan: Optional[str] = None
    employee_pan: Optional[str] = None
    perquisites: list[Perquisite] = field(default_factory=list)
    total_perquisites: Decimal = Decimal("0")

    def calculate_total(self):
        """Calculate total perquisites."""
        self.total_perquisites = sum(p.taxable_value for p in self.perquisites)


@dataclass
class SalaryParseResult:
    """Result of parsing salary/Form 16 files."""

    success: bool
    salary_records: list[SalaryRecord] = field(default_factory=list)
    form16_record: Optional[Form16Record] = None
    form12ba_record: Optional[Form12BARecord] = None
    rsu_credits: list[RSUTaxCredit] = field(default_factory=list)
    source_file: str = ""
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def add_error(self, error: str):
        """Add an error message."""
        self.errors.append(error)
        self.success = False

    def add_warning(self, warning: str):
        """Add a warning message."""
        self.warnings.append(warning)


@dataclass
class AnnualSalarySummary:
    """
    Annual salary summary aggregated from monthly records.

    Used for:
    - Verifying against Form 16
    - ITR filing
    - Tax planning
    """

    financial_year: str
    user_id: Optional[int] = None

    # Earnings
    total_basic: Decimal = Decimal("0")
    total_hra: Decimal = Decimal("0")
    total_special_allowance: Decimal = Decimal("0")
    total_lta: Decimal = Decimal("0")
    total_other_allowances: Decimal = Decimal("0")
    total_gross_salary: Decimal = Decimal("0")

    # Deductions
    total_pf_employee: Decimal = Decimal("0")
    total_pf_employer: Decimal = Decimal("0")
    total_nps_employee: Decimal = Decimal("0")
    total_nps_employer: Decimal = Decimal("0")
    total_professional_tax: Decimal = Decimal("0")
    total_income_tax: Decimal = Decimal("0")
    total_espp: Decimal = Decimal("0")
    total_tcs_espp: Decimal = Decimal("0")

    # RSU Credits
    total_rsu_credits: Decimal = Decimal("0")

    # Net
    total_net_pay: Decimal = Decimal("0")
    months_processed: int = 0

    def add_monthly_record(self, record: SalaryRecord):
        """Add a monthly salary record to the summary."""
        self.total_basic += record.basic_salary
        self.total_hra += record.hra
        self.total_special_allowance += record.special_allowance
        self.total_lta += record.lta
        self.total_other_allowances += record.other_allowances
        self.total_gross_salary += record.gross_salary

        self.total_pf_employee += record.pf_employee
        self.total_pf_employer += record.pf_employer
        self.total_nps_employee += record.nps_employee
        self.total_nps_employer += record.nps_employer
        self.total_professional_tax += record.professional_tax
        self.total_income_tax += record.income_tax_deducted
        self.total_espp += record.espp_deduction
        self.total_tcs_espp += record.tcs_on_espp

        self.total_rsu_credits += record.rsu_tax_credit
        self.total_net_pay += record.net_pay
        self.months_processed += 1
