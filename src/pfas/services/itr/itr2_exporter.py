"""ITR-2 JSON Exporter.

Generates ITR-2 JSON file compatible with Income Tax Department's
JSON utility for e-filing.

ITR-2 is used by individuals with:
- Salary income
- Capital gains
- Foreign assets
- Income from other sources
"""

import json
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import date
from decimal import Decimal
from typing import Optional, List, Dict, Any

from pfas.services.currency import SBITTRateProvider
from .schedule_fa import ScheduleFAGenerator, ScheduleFAData


@dataclass
class PersonalInfo:
    """Personal information for ITR."""

    pan: str
    name: str
    father_name: str = ""
    dob: Optional[date] = None
    aadhaar: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    pincode: str = ""
    mobile: str = ""
    email: str = ""
    employer_category: str = "OTH"  # OTH, GOV, PSU, PE, NA
    residential_status: str = "RES"  # RES, RNOR, NR


@dataclass
class SalaryIncome:
    """Schedule S - Salary Income."""

    employer_name: str = ""
    employer_tan: str = ""
    gross_salary: Decimal = Decimal("0")
    perquisites: Decimal = Decimal("0")  # Section 17(2)
    profits_in_lieu: Decimal = Decimal("0")  # Section 17(3)
    total_salary: Decimal = Decimal("0")
    standard_deduction: Decimal = Decimal("50000")  # Section 16(ia)
    professional_tax: Decimal = Decimal("0")
    entertainment_allowance: Decimal = Decimal("0")
    net_salary: Decimal = Decimal("0")

    def calculate(self) -> None:
        """Calculate net salary."""
        self.total_salary = self.gross_salary + self.perquisites + self.profits_in_lieu
        deductions = self.standard_deduction + self.professional_tax + self.entertainment_allowance
        self.net_salary = max(Decimal("0"), self.total_salary - deductions)


@dataclass
class CapitalGains:
    """Schedule CG - Capital Gains."""

    # Short term gains (Section 111A - STT paid)
    stcg_111a: Decimal = Decimal("0")

    # Short term gains (other)
    stcg_other: Decimal = Decimal("0")

    # Long term gains (Section 112A - equity with STT)
    ltcg_112a_full: Decimal = Decimal("0")
    ltcg_112a_exempt: Decimal = Decimal("125000")  # ₹1.25L exemption
    ltcg_112a_taxable: Decimal = Decimal("0")

    # Long term gains (Section 112 - without indexation)
    ltcg_112_without_indexation: Decimal = Decimal("0")

    # Long term gains (Section 112 - with indexation)
    ltcg_112_with_indexation: Decimal = Decimal("0")

    # Foreign capital gains (no STT)
    foreign_ltcg: Decimal = Decimal("0")
    foreign_stcg: Decimal = Decimal("0")

    total_stcg: Decimal = Decimal("0")
    total_ltcg: Decimal = Decimal("0")

    def calculate(self) -> None:
        """Calculate totals."""
        self.ltcg_112a_taxable = max(Decimal("0"), self.ltcg_112a_full - self.ltcg_112a_exempt)
        self.total_stcg = self.stcg_111a + self.stcg_other + self.foreign_stcg
        self.total_ltcg = self.ltcg_112a_taxable + self.ltcg_112_without_indexation + self.ltcg_112_with_indexation + self.foreign_ltcg


@dataclass
class OtherIncome:
    """Schedule OS - Other Sources Income."""

    dividend_income: Decimal = Decimal("0")  # 10(34) onwards taxable
    interest_savings: Decimal = Decimal("0")
    interest_deposits: Decimal = Decimal("0")
    interest_bonds: Decimal = Decimal("0")
    interest_others: Decimal = Decimal("0")
    rental_income: Decimal = Decimal("0")
    foreign_dividend: Decimal = Decimal("0")  # From foreign stocks
    other_income: Decimal = Decimal("0")

    total_other_income: Decimal = Decimal("0")

    def calculate(self) -> None:
        """Calculate total."""
        self.total_other_income = (
            self.dividend_income +
            self.interest_savings +
            self.interest_deposits +
            self.interest_bonds +
            self.interest_others +
            self.rental_income +
            self.foreign_dividend +
            self.other_income
        )


@dataclass
class Chapter6ADeductions:
    """Chapter VI-A Deductions."""

    # Section 80C (max ₹1.5L)
    section_80c: Decimal = Decimal("0")
    section_80ccc: Decimal = Decimal("0")
    section_80ccd_1: Decimal = Decimal("0")
    section_80ccd_1b: Decimal = Decimal("0")  # Additional NPS ₹50K
    section_80ccd_2: Decimal = Decimal("0")  # Employer NPS

    # Health insurance (80D)
    section_80d: Decimal = Decimal("0")

    # Donations (80G)
    section_80g: Decimal = Decimal("0")

    # Interest on education loan (80E)
    section_80e: Decimal = Decimal("0")

    # Housing loan interest (80EE/80EEA)
    section_80ee: Decimal = Decimal("0")

    # Rent paid (80GG)
    section_80gg: Decimal = Decimal("0")

    # Other deductions
    section_80tta: Decimal = Decimal("0")  # Savings interest ₹10K
    section_80ttb: Decimal = Decimal("0")  # Senior citizen ₹50K
    section_80u: Decimal = Decimal("0")  # Disability

    total_deductions: Decimal = Decimal("0")

    def calculate(self) -> None:
        """Calculate total deductions."""
        # 80C limit
        sec_80c_total = min(
            self.section_80c + self.section_80ccc + self.section_80ccd_1,
            Decimal("150000")
        )

        self.total_deductions = (
            sec_80c_total +
            self.section_80ccd_1b +
            self.section_80ccd_2 +
            self.section_80d +
            self.section_80g +
            self.section_80e +
            self.section_80ee +
            self.section_80gg +
            self.section_80tta +
            self.section_80ttb +
            self.section_80u
        )


@dataclass
class TaxRelief:
    """Tax relief under various sections."""

    # Section 89 relief (arrears/advance salary)
    section_89: Decimal = Decimal("0")

    # Section 90/91 (DTAA/Foreign tax credit)
    section_90: Decimal = Decimal("0")  # With DTAA
    section_91: Decimal = Decimal("0")  # Without DTAA

    total_relief: Decimal = Decimal("0")

    def calculate(self) -> None:
        """Calculate total relief."""
        self.total_relief = self.section_89 + self.section_90 + self.section_91


@dataclass
class TDSDetails:
    """TDS details."""

    tan: str
    deductor_name: str
    income_type: str  # 'Salary', 'Other', 'CG'
    income_amount: Decimal = Decimal("0")
    tds_deducted: Decimal = Decimal("0")


@dataclass
class ITR2Data:
    """Complete ITR-2 data structure."""

    financial_year: str
    assessment_year: str
    form_name: str = "ITR2"

    personal_info: PersonalInfo = field(default_factory=PersonalInfo)
    salary_income: SalaryIncome = field(default_factory=SalaryIncome)
    capital_gains: CapitalGains = field(default_factory=CapitalGains)
    other_income: OtherIncome = field(default_factory=OtherIncome)
    deductions: Chapter6ADeductions = field(default_factory=Chapter6ADeductions)
    tax_relief: TaxRelief = field(default_factory=TaxRelief)

    # Schedule FA
    schedule_fa: Optional[ScheduleFAData] = None

    # TDS details
    tds_on_salary: List[TDSDetails] = field(default_factory=list)
    tds_other: List[TDSDetails] = field(default_factory=list)

    # Calculated fields
    gross_total_income: Decimal = Decimal("0")
    total_income: Decimal = Decimal("0")  # After deductions
    tax_payable: Decimal = Decimal("0")
    total_tds: Decimal = Decimal("0")
    advance_tax: Decimal = Decimal("0")
    self_assessment_tax: Decimal = Decimal("0")
    tax_refund: Decimal = Decimal("0")
    tax_due: Decimal = Decimal("0")


class ITR2Exporter:
    """
    Exports ITR-2 JSON for e-filing.

    Generates JSON format compatible with Income Tax Department's
    JSON utility (available at https://www.incometax.gov.in).
    """

    # Tax slabs for AY 2025-26 (Old Regime)
    OLD_REGIME_SLABS = [
        (Decimal("250000"), Decimal("0")),      # 0-2.5L: 0%
        (Decimal("500000"), Decimal("0.05")),   # 2.5-5L: 5%
        (Decimal("1000000"), Decimal("0.20")),  # 5-10L: 20%
        (Decimal("999999999"), Decimal("0.30")), # >10L: 30%
    ]

    # Tax slabs for AY 2025-26 (New Regime)
    NEW_REGIME_SLABS = [
        (Decimal("300000"), Decimal("0")),      # 0-3L: 0%
        (Decimal("600000"), Decimal("0.05")),   # 3-6L: 5%
        (Decimal("900000"), Decimal("0.10")),   # 6-9L: 10%
        (Decimal("1200000"), Decimal("0.15")),  # 9-12L: 15%
        (Decimal("1500000"), Decimal("0.20")),  # 12-15L: 20%
        (Decimal("999999999"), Decimal("0.30")), # >15L: 30%
    ]

    SURCHARGE_THRESHOLDS = [
        (Decimal("5000000"), Decimal("0")),     # 0-50L: 0%
        (Decimal("10000000"), Decimal("0.10")), # 50L-1Cr: 10%
        (Decimal("20000000"), Decimal("0.15")), # 1-2Cr: 15%
        (Decimal("50000000"), Decimal("0.25")), # 2-5Cr: 25%
        (Decimal("999999999"), Decimal("0.37")), # >5Cr: 37%
    ]

    CESS_RATE = Decimal("0.04")  # 4% Health and Education Cess

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize ITR-2 exporter.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection
        self.rate_provider = SBITTRateProvider(db_connection)
        self.schedule_fa_gen = ScheduleFAGenerator(db_connection)

    def generate(
        self,
        user_id: int,
        financial_year: str,
        personal_info: PersonalInfo,
        use_new_regime: bool = True
    ) -> ITR2Data:
        """
        Generate ITR-2 data for a financial year.

        Args:
            user_id: User ID
            financial_year: FY in format '2024-25'
            personal_info: Personal information
            use_new_regime: Use new tax regime

        Returns:
            ITR2Data for export
        """
        start_year = int(financial_year.split('-')[0])
        assessment_year = f"{start_year + 1}-{str(start_year + 2)[2:]}"

        itr_data = ITR2Data(
            financial_year=financial_year,
            assessment_year=assessment_year,
            personal_info=personal_info,
        )

        # Populate salary income
        self._populate_salary(itr_data, user_id, financial_year)

        # Populate capital gains
        self._populate_capital_gains(itr_data, user_id, financial_year)

        # Populate other income
        self._populate_other_income(itr_data, user_id, financial_year)

        # Populate deductions (only for old regime)
        if not use_new_regime:
            self._populate_deductions(itr_data, user_id, financial_year)

        # Generate Schedule FA
        itr_data.schedule_fa = self.schedule_fa_gen.generate(user_id, financial_year)

        # Populate TDS
        self._populate_tds(itr_data, user_id, financial_year)

        # Populate tax relief (DTAA)
        self._populate_tax_relief(itr_data, user_id, financial_year)

        # Calculate tax
        self._calculate_tax(itr_data, use_new_regime)

        return itr_data

    def export_json(self, itr_data: ITR2Data, output_path: str) -> str:
        """
        Export ITR-2 data to JSON file.

        Args:
            itr_data: ITR-2 data
            output_path: Output file path

        Returns:
            Path to exported file
        """
        # Convert to JSON-compatible dict
        json_data = self._to_json_dict(itr_data)

        with open(output_path, 'w') as f:
            json.dump(json_data, f, indent=2, default=str)

        return output_path

    def _populate_salary(self, itr_data: ITR2Data, user_id: int, financial_year: str) -> None:
        """Populate salary income from database."""
        start_year = int(financial_year.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)

        cursor = self.conn.execute(
            """SELECT SUM(gross_salary) as gross, SUM(0) as perq,
                      SUM(professional_tax) as pt, e.name as employer_name, e.tan as employer_tan
            FROM salary_records sr
            JOIN employers e ON sr.employer_id = e.id
            WHERE sr.user_id = ?
                AND pay_period >= ?
                AND pay_period <= ?
            GROUP BY employer_id""",
            (user_id, fy_start.isoformat(), fy_end.isoformat())
        )

        row = cursor.fetchone()
        if row:
            itr_data.salary_income.gross_salary = Decimal(str(row['gross'])) if row['gross'] else Decimal("0")
            itr_data.salary_income.perquisites = Decimal(str(row['perq'])) if row['perq'] else Decimal("0")
            itr_data.salary_income.professional_tax = Decimal(str(row['pt'])) if row['pt'] else Decimal("0")
            itr_data.salary_income.employer_name = row['employer_name'] or ""
            itr_data.salary_income.employer_tan = row['employer_tan'] or ""

        itr_data.salary_income.calculate()

    def _populate_capital_gains(self, itr_data: ITR2Data, user_id: int, financial_year: str) -> None:
        """Populate capital gains from database."""
        start_year = int(financial_year.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)

        # Indian equity (from stock_capital_gains table)
        cursor = self.conn.execute(
            """SELECT SUM(ltcg_amount) as ltcg, SUM(stcg_amount) as stcg
            FROM stock_capital_gains
            WHERE user_id = ?
                AND financial_year = ?
                AND trade_category = 'DELIVERY'""",
            (user_id, financial_year)
        )

        row = cursor.fetchone()
        if row:
            itr_data.capital_gains.ltcg_112a_full = Decimal(str(row['ltcg'])) if row['ltcg'] else Decimal("0")
            itr_data.capital_gains.stcg_111a = Decimal(str(row['stcg'])) if row['stcg'] else Decimal("0")

        # Foreign equity (RSU/ESPP - no STT)
        cursor = self.conn.execute(
            """SELECT SUM(CASE WHEN is_ltcg = 1 THEN gain_inr ELSE 0 END) as foreign_ltcg,
                      SUM(CASE WHEN is_ltcg = 0 THEN gain_inr ELSE 0 END) as foreign_stcg
            FROM rsu_sales
            WHERE user_id = ?
                AND sale_date >= ?
                AND sale_date <= ?""",
            (user_id, fy_start.isoformat(), fy_end.isoformat())
        )

        row = cursor.fetchone()
        if row:
            itr_data.capital_gains.foreign_ltcg = Decimal(str(row['foreign_ltcg'])) if row['foreign_ltcg'] else Decimal("0")
            itr_data.capital_gains.foreign_stcg = Decimal(str(row['foreign_stcg'])) if row['foreign_stcg'] else Decimal("0")

        # Add ESPP sales
        cursor = self.conn.execute(
            """SELECT SUM(CASE WHEN is_ltcg = 1 THEN gain_inr ELSE 0 END) as espp_ltcg,
                      SUM(CASE WHEN is_ltcg = 0 THEN gain_inr ELSE 0 END) as espp_stcg
            FROM espp_sales
            WHERE user_id = ?
                AND sale_date >= ?
                AND sale_date <= ?""",
            (user_id, fy_start.isoformat(), fy_end.isoformat())
        )

        row = cursor.fetchone()
        if row:
            itr_data.capital_gains.foreign_ltcg += Decimal(str(row['espp_ltcg'])) if row['espp_ltcg'] else Decimal("0")
            itr_data.capital_gains.foreign_stcg += Decimal(str(row['espp_stcg'])) if row['espp_stcg'] else Decimal("0")

        # Mutual funds (uses financial_year and has ltcg_amount/stcg_amount columns)
        cursor = self.conn.execute(
            """SELECT SUM(ltcg_amount) as mf_ltcg, SUM(stcg_amount) as mf_stcg
            FROM mf_capital_gains
            WHERE user_id = ?
                AND financial_year = ?""",
            (user_id, financial_year)
        )

        row = cursor.fetchone()
        if row:
            # Equity MF goes to 112A, Debt MF to 112
            itr_data.capital_gains.ltcg_112a_full += Decimal(str(row['mf_ltcg'])) if row['mf_ltcg'] else Decimal("0")
            itr_data.capital_gains.stcg_111a += Decimal(str(row['mf_stcg'])) if row['mf_stcg'] else Decimal("0")

        itr_data.capital_gains.calculate()

    def _populate_other_income(self, itr_data: ITR2Data, user_id: int, financial_year: str) -> None:
        """Populate other income from database."""
        start_year = int(financial_year.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)

        # Foreign dividends
        cursor = self.conn.execute(
            """SELECT SUM(gross_dividend_inr) as foreign_div
            FROM foreign_dividends
            WHERE user_id = ?
                AND dividend_date >= ?
                AND dividend_date <= ?""",
            (user_id, fy_start.isoformat(), fy_end.isoformat())
        )

        row = cursor.fetchone()
        if row and row['foreign_div']:
            itr_data.other_income.foreign_dividend = Decimal(str(row['foreign_div']))

        # Indian dividends
        cursor = self.conn.execute(
            """SELECT SUM(gross_amount) as indian_div
            FROM stock_dividends
            WHERE user_id = ?
                AND dividend_date >= ?
                AND dividend_date <= ?""",
            (user_id, fy_start.isoformat(), fy_end.isoformat())
        )

        row = cursor.fetchone()
        if row and row['indian_div']:
            itr_data.other_income.dividend_income = Decimal(str(row['indian_div']))

        itr_data.other_income.calculate()

    def _populate_deductions(self, itr_data: ITR2Data, user_id: int, financial_year: str) -> None:
        """Populate deductions from database."""
        # Get from Form 16 or manual entries
        cursor = self.conn.execute(
            """SELECT section_80c, section_80d, section_80g, section_80e,
                      section_80ccd_1b, section_80ccd_2
            FROM deductions
            WHERE user_id = ? AND financial_year = ?""",
            (user_id, financial_year)
        )

        row = cursor.fetchone()
        if row:
            itr_data.deductions.section_80c = Decimal(str(row['section_80c'])) if row['section_80c'] else Decimal("0")
            itr_data.deductions.section_80d = Decimal(str(row['section_80d'])) if row['section_80d'] else Decimal("0")
            itr_data.deductions.section_80g = Decimal(str(row['section_80g'])) if row['section_80g'] else Decimal("0")
            itr_data.deductions.section_80e = Decimal(str(row['section_80e'])) if row['section_80e'] else Decimal("0")
            itr_data.deductions.section_80ccd_1b = Decimal(str(row['section_80ccd_1b'])) if row['section_80ccd_1b'] else Decimal("0")
            itr_data.deductions.section_80ccd_2 = Decimal(str(row['section_80ccd_2'])) if row['section_80ccd_2'] else Decimal("0")

        itr_data.deductions.calculate()

    def _populate_tds(self, itr_data: ITR2Data, user_id: int, financial_year: str) -> None:
        """Populate TDS details from Form 16."""
        # Form16 uses assessment_year (e.g., "2025-26" for FY 2024-25)
        start_year = int(financial_year.split('-')[0])
        assessment_year = f"{start_year + 1}-{str(start_year + 2)[2:]}"

        cursor = self.conn.execute(
            """SELECT e.tan as employer_tan, e.name as employer_name,
                      f.gross_salary as total_salary, f.total_tds
            FROM form16_records f
            JOIN employers e ON f.employer_id = e.id
            WHERE f.user_id = ? AND f.assessment_year = ?""",
            (user_id, assessment_year)
        )

        for row in cursor.fetchall():
            tds = TDSDetails(
                tan=row['employer_tan'] or "",
                deductor_name=row['employer_name'] or "",
                income_type='Salary',
                income_amount=Decimal(str(row['total_salary'])) if row['total_salary'] else Decimal("0"),
                tds_deducted=Decimal(str(row['total_tds'])) if row['total_tds'] else Decimal("0"),
            )
            itr_data.tds_on_salary.append(tds)
            itr_data.total_tds += tds.tds_deducted

    def _populate_tax_relief(self, itr_data: ITR2Data, user_id: int, financial_year: str) -> None:
        """Populate DTAA tax relief."""
        cursor = self.conn.execute(
            """SELECT SUM(credit_allowed) as dtaa_credit
            FROM dtaa_credits
            WHERE user_id = ?
                AND income_date >= ?
                AND income_date <= ?""",
            (user_id,
             f"{int(financial_year.split('-')[0])}-04-01",
             f"{int(financial_year.split('-')[0]) + 1}-03-31")
        )

        row = cursor.fetchone()
        if row and row['dtaa_credit']:
            itr_data.tax_relief.section_90 = Decimal(str(row['dtaa_credit']))

        itr_data.tax_relief.calculate()

    def _calculate_tax(self, itr_data: ITR2Data, use_new_regime: bool) -> None:
        """Calculate tax liability."""
        # Gross total income
        itr_data.gross_total_income = (
            itr_data.salary_income.net_salary +
            itr_data.capital_gains.total_stcg +
            itr_data.capital_gains.total_ltcg +
            itr_data.other_income.total_other_income
        )

        # Total income after deductions
        if use_new_regime:
            itr_data.total_income = itr_data.gross_total_income
        else:
            itr_data.total_income = max(
                Decimal("0"),
                itr_data.gross_total_income - itr_data.deductions.total_deductions
            )

        # Calculate tax
        slabs = self.NEW_REGIME_SLABS if use_new_regime else self.OLD_REGIME_SLABS

        # Normal income (excluding special rate income)
        normal_income = itr_data.total_income - itr_data.capital_gains.total_stcg - itr_data.capital_gains.total_ltcg

        # Tax on normal income
        normal_tax = self._calculate_slab_tax(normal_income, slabs)

        # Tax on STCG 111A (15%)
        stcg_tax = itr_data.capital_gains.stcg_111a * Decimal("0.15")

        # Tax on LTCG 112A (12.5% for gains above ₹1.25L)
        ltcg_tax = itr_data.capital_gains.ltcg_112a_taxable * Decimal("0.125")

        # Tax on foreign CG (at slab rate for STCG, 20% for LTCG)
        foreign_stcg_tax = self._calculate_slab_tax(itr_data.capital_gains.foreign_stcg, slabs)
        foreign_ltcg_tax = itr_data.capital_gains.foreign_ltcg * Decimal("0.20")

        # Total base tax
        base_tax = normal_tax + stcg_tax + ltcg_tax + foreign_stcg_tax + foreign_ltcg_tax

        # Surcharge
        surcharge = self._calculate_surcharge(base_tax, itr_data.total_income)

        # Cess
        cess = (base_tax + surcharge) * self.CESS_RATE

        # Total tax
        itr_data.tax_payable = base_tax + surcharge + cess

        # Relief
        itr_data.tax_payable = max(Decimal("0"), itr_data.tax_payable - itr_data.tax_relief.total_relief)

        # Tax due/refund
        total_paid = itr_data.total_tds + itr_data.advance_tax + itr_data.self_assessment_tax
        if itr_data.tax_payable > total_paid:
            itr_data.tax_due = itr_data.tax_payable - total_paid
            itr_data.tax_refund = Decimal("0")
        else:
            itr_data.tax_refund = total_paid - itr_data.tax_payable
            itr_data.tax_due = Decimal("0")

    def _calculate_slab_tax(self, income: Decimal, slabs: List[tuple]) -> Decimal:
        """Calculate tax using slab rates."""
        if income <= 0:
            return Decimal("0")

        tax = Decimal("0")
        remaining = income
        prev_limit = Decimal("0")

        for limit, rate in slabs:
            if remaining <= 0:
                break

            taxable_in_slab = min(remaining, limit - prev_limit)
            tax += taxable_in_slab * rate
            remaining -= taxable_in_slab
            prev_limit = limit

        return tax

    def _calculate_surcharge(self, tax: Decimal, total_income: Decimal) -> Decimal:
        """Calculate surcharge on tax."""
        for threshold, rate in self.SURCHARGE_THRESHOLDS:
            if total_income <= threshold:
                return tax * rate

        return tax * Decimal("0.37")  # Max surcharge

    def _to_json_dict(self, itr_data: ITR2Data) -> Dict[str, Any]:
        """Convert ITR data to JSON-compatible dictionary."""

        def convert_value(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            if isinstance(obj, date):
                return obj.isoformat()
            if hasattr(obj, '__dict__'):
                return {k: convert_value(v) for k, v in obj.__dict__.items()
                        if not k.startswith('_')}
            if isinstance(obj, list):
                return [convert_value(item) for item in obj]
            return obj

        return convert_value(itr_data)
