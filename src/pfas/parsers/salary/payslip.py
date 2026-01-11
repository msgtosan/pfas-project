"""Monthly payslip PDF parser.

Parses Qualcomm-format payslips and extracts all salary components.

Key features:
- RSU Tax Credit handling (negative deduction = credit)
- ESPP deduction tracking
- TCS on ESPP extraction
- Professional tax tracking
"""

import re
import pdfplumber
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal
from typing import Optional
import sqlite3

from .models import SalaryRecord, SalaryParseResult, RSUTaxCredit, CorrelationStatus


class PayslipParser:
    """
    Parser for Qualcomm-format monthly payslips.

    Payslip structure:
    - Header: Employee details, PAN, PF number, UAN
    - Earnings: Basic, HRA, Special Allowance, LTA
    - Deductions: PF, NPS, Prof Tax, Income Tax, ESPP, RSU Tax
    - Summary: Gross, Total Deductions, Net Pay

    CRITICAL: RSU Tax appears as NEGATIVE in deductions when RSUs vest.
    This is a TAX CREDIT (money added back), not a deduction.
    """

    # Regular expression patterns for extracting components
    PATTERNS = {
        # Header patterns
        'pay_period': r'Pay\s*Slip\s*for\s*(?:the\s*)?Month\s*of\s+(\w+\s*\d{4})',
        'employee_id': r'Employee\s*ID[:\s]+(\d+)',
        'employee_name': r'Employee\s*Name[:\s]+([A-Za-z\s]+?)(?:\s{2,}|$)',
        'pan': r'PAN[:\s]+([A-Z]{5}\d{4}[A-Z])',
        'pf_number': r'PF\s*Number[:\s]+(\S+)',
        'uan': r'UAN[:\s]+(\d+)',

        # Earnings patterns
        'basic_salary': r'Basic\s*Salary\s+(?:[\d,]+\.?\d*)?\s*([\d,]+\.\d{2})',
        'hra': r'House\s*Rent\s*Allowance\s+([\d,]+\.\d{2})',
        'special_allowance': r'Special\s*Allowance\s+([\d,]+\.\d{2})',
        'lta': r'(?:Leave\s*Travel\s*Allowance|LTA)\s+([\d,]+\.\d{2})',

        # Deduction patterns - order matters for regex matching
        'rsu_tax': r'\*?RSUs?\s*Tax\s+(-?[\d,]+\.\d{2})',  # Can be negative!
        'espp_deduction': r'ESPP\s*Deduction\s+([\d,]+\.\d{2})',
        'tcs_espp': r'TCS\s*(?:on\s*)?ESPP\s+([\d,]+\.\d{2})',
        'pf_employee': r'EE\s*PF\s*(?:contribution)?\s+([\d,]+\.\d{2})',
        'professional_tax': r'Prof(?:essional)?\s*Tax\s+([\d,]+\.\d{2})',
        'income_tax': r'Income\s*Tax\s+([\d,]+\.\d{2})',
        'nps_contribution': r'NPS\s*Contribution\s+([\d,]+\.\d{2})',
        'trust_fund': r'(?:QCOM\s*)?Trust\s*Fund\s+([\d,]+\.\d{2})',

        # Summary patterns
        'gross_salary': r'Total\s*Gross\s+([\d,]+\.\d{2})',
        'total_deductions': r'(?:Total\s*Dedns?|Less:\s*Total\s*Dedns?)\s+([\d,]+\.\d{2})',
        'net_pay': r'NET\s*PAY\s+([\d,]+\.\d{2})',
    }

    def __init__(self, db_connection: Optional[sqlite3.Connection] = None):
        """
        Initialize payslip parser.

        Args:
            db_connection: Optional database connection for saving records
        """
        self.conn = db_connection

    def parse(self, file_path: Path) -> SalaryParseResult:
        """
        Parse a monthly payslip PDF.

        Args:
            file_path: Path to payslip PDF

        Returns:
            SalaryParseResult with extracted salary record
        """
        file_path = Path(file_path)
        result = SalaryParseResult(success=True, source_file=str(file_path))

        if not file_path.exists():
            result.add_error(f"File not found: {file_path}")
            return result

        if not file_path.suffix.lower() == '.pdf':
            result.add_error(f"Unsupported file format: {file_path.suffix}")
            return result

        try:
            # Extract text from PDF
            text = self._extract_text(file_path)

            if not text:
                result.add_error("Could not extract text from PDF")
                return result

            # Parse salary record
            record = self._extract_salary_record(text)
            result.salary_records.append(record)

            # Extract RSU tax credit if present
            if record.rsu_tax_credit > Decimal("0"):
                rsu_credit = RSUTaxCredit(
                    salary_record_id=0,  # Will be set when saved to DB
                    credit_amount=record.rsu_tax_credit,
                    credit_date=record.pay_date or date.today(),
                    correlation_status=CorrelationStatus.PENDING
                )
                result.rsu_credits.append(rsu_credit)

            # Validation warnings
            if record.gross_salary == Decimal("0"):
                result.add_warning("Gross salary not found or is zero")

            if record.net_pay == Decimal("0"):
                result.add_warning("Net pay not found or is zero")

        except Exception as e:
            result.add_error(f"Failed to parse payslip: {str(e)}")

        return result

    def _extract_text(self, file_path: Path) -> str:
        """Extract text from PDF file."""
        text = ""
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        return text

    def _extract_salary_record(self, text: str) -> SalaryRecord:
        """
        Extract salary components from payslip text.

        Args:
            text: Extracted text from payslip PDF

        Returns:
            SalaryRecord with all extracted components
        """
        record = SalaryRecord(pay_period="")

        # Extract pay period
        match = re.search(self.PATTERNS['pay_period'], text, re.IGNORECASE)
        if match:
            record.pay_period = match.group(1).strip()
            # Try to parse pay date
            record.pay_date = self._parse_pay_period(record.pay_period)

        # Extract employee info
        for field in ['employee_id', 'employee_name', 'pan', 'pf_number', 'uan']:
            match = re.search(self.PATTERNS[field], text, re.IGNORECASE)
            if match:
                setattr(record, field, match.group(1).strip())

        # Extract earnings
        for field in ['basic_salary', 'hra', 'special_allowance', 'lta']:
            match = re.search(self.PATTERNS[field], text, re.IGNORECASE)
            if match:
                value = self._to_decimal(match.group(1))
                setattr(record, field, value)

        # Extract deductions
        # RSU Tax - CRITICAL: Can be negative (credit) or positive (deduction)
        match = re.search(self.PATTERNS['rsu_tax'], text, re.IGNORECASE)
        if match:
            value = self._to_decimal(match.group(1))
            if value < Decimal("0"):
                # Negative = Tax credit when RSUs vest
                record.rsu_tax_credit = abs(value)
            else:
                # Positive = Actual tax deduction (rare)
                # Add to other deductions if needed
                pass

        # ESPP deduction
        match = re.search(self.PATTERNS['espp_deduction'], text, re.IGNORECASE)
        if match:
            record.espp_deduction = self._to_decimal(match.group(1))

        # TCS on ESPP
        match = re.search(self.PATTERNS['tcs_espp'], text, re.IGNORECASE)
        if match:
            record.tcs_on_espp = self._to_decimal(match.group(1))

        # PF Employee contribution
        match = re.search(self.PATTERNS['pf_employee'], text, re.IGNORECASE)
        if match:
            record.pf_employee = self._to_decimal(match.group(1))

        # Professional Tax
        match = re.search(self.PATTERNS['professional_tax'], text, re.IGNORECASE)
        if match:
            record.professional_tax = self._to_decimal(match.group(1))

        # Income Tax
        match = re.search(self.PATTERNS['income_tax'], text, re.IGNORECASE)
        if match:
            record.income_tax_deducted = self._to_decimal(match.group(1))

        # NPS Contribution
        match = re.search(self.PATTERNS['nps_contribution'], text, re.IGNORECASE)
        if match:
            record.nps_employee = self._to_decimal(match.group(1))

        # Trust Fund (other deductions)
        match = re.search(self.PATTERNS['trust_fund'], text, re.IGNORECASE)
        if match:
            record.other_deductions += self._to_decimal(match.group(1))

        # Summary
        match = re.search(self.PATTERNS['gross_salary'], text, re.IGNORECASE)
        if match:
            record.gross_salary = self._to_decimal(match.group(1))

        match = re.search(self.PATTERNS['total_deductions'], text, re.IGNORECASE)
        if match:
            record.total_deductions = self._to_decimal(match.group(1))

        match = re.search(self.PATTERNS['net_pay'], text, re.IGNORECASE)
        if match:
            record.net_pay = self._to_decimal(match.group(1))

        # Calculate totals if not found
        record.calculate_totals()

        return record

    def _to_decimal(self, value: str) -> Decimal:
        """
        Convert string to Decimal, handling negatives and commas.

        Args:
            value: String value (may have commas or negative sign)

        Returns:
            Decimal value
        """
        if not value:
            return Decimal("0")

        # Remove commas
        clean = value.replace(",", "").strip()

        try:
            return Decimal(clean)
        except:
            return Decimal("0")

    def _parse_pay_period(self, pay_period: str) -> Optional[date]:
        """
        Parse pay period string to date (last day of month).

        Args:
            pay_period: String like 'June 2024'

        Returns:
            Last day of the pay period month
        """
        try:
            # Try parsing 'Month Year' format
            dt = datetime.strptime(pay_period.strip(), "%B %Y")
            # Return last day of month
            if dt.month == 12:
                return date(dt.year + 1, 1, 1) - datetime.timedelta(days=1)
            else:
                return date(dt.year, dt.month + 1, 1) - datetime.timedelta(days=1)
        except:
            try:
                # Try 'Mon Year' format
                dt = datetime.strptime(pay_period.strip(), "%b %Y")
                if dt.month == 12:
                    return date(dt.year + 1, 1, 1) - datetime.timedelta(days=1)
                else:
                    return date(dt.year, dt.month + 1, 1) - datetime.timedelta(days=1)
            except:
                return None

    def save_to_db(
        self,
        result: SalaryParseResult,
        user_id: int,
        employer_id: Optional[int] = None
    ) -> int:
        """
        Save parsed salary records to database.

        Args:
            result: SalaryParseResult from parsing
            user_id: User ID
            employer_id: Employer ID (optional)

        Returns:
            Number of records saved
        """
        if not self.conn or not result.success:
            return 0

        count = 0
        cursor = self.conn.cursor()

        try:
            for record in result.salary_records:
                cursor.execute(
                    """INSERT INTO salary_records
                    (user_id, employer_id, pay_period, pay_date,
                     basic_salary, hra, special_allowance, lta, other_allowances,
                     gross_salary, pf_employee, pf_employer, nps_employee, nps_employer,
                     professional_tax, income_tax_deducted, espp_deduction, tcs_on_espp,
                     other_deductions, rsu_tax_credit, total_deductions, net_pay, source_file)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        user_id,
                        employer_id,
                        record.pay_period,
                        record.pay_date.isoformat() if record.pay_date else None,
                        str(record.basic_salary),
                        str(record.hra),
                        str(record.special_allowance),
                        str(record.lta),
                        str(record.other_allowances),
                        str(record.gross_salary),
                        str(record.pf_employee),
                        str(record.pf_employer),
                        str(record.nps_employee),
                        str(record.nps_employer),
                        str(record.professional_tax),
                        str(record.income_tax_deducted),
                        str(record.espp_deduction),
                        str(record.tcs_on_espp),
                        str(record.other_deductions),
                        str(record.rsu_tax_credit),
                        str(record.total_deductions),
                        str(record.net_pay),
                        result.source_file
                    )
                )
                salary_record_id = cursor.lastrowid
                count += 1

                # Save RSU tax credit if present
                if record.rsu_tax_credit > Decimal("0"):
                    cursor.execute(
                        """INSERT INTO rsu_tax_credits
                        (salary_record_id, credit_amount, credit_date, correlation_status)
                        VALUES (?, ?, ?, ?)""",
                        (
                            salary_record_id,
                            str(record.rsu_tax_credit),
                            record.pay_date.isoformat() if record.pay_date else date.today().isoformat(),
                            CorrelationStatus.PENDING.value
                        )
                    )

            self.conn.commit()
            return count

        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to save salary records: {e}") from e
