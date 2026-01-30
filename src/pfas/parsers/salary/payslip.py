"""Monthly payslip PDF parser.

Parses Qualcomm-format payslips and extracts all salary components.

Key features:
- RSU Tax Credit handling (negative deduction = credit)
- ESPP deduction tracking
- TCS on ESPP extraction
- Professional tax tracking
"""

import re
import hashlib
import pdfplumber
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List
import sqlite3

from pfas.core.transaction_service import (
    TransactionService,
    TransactionSource,
    AssetRecord,
)
from pfas.core.journal import JournalEntry
from pfas.core.accounts import get_account_by_code

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
        Save parsed salary records to database via TransactionService.

        All inserts flow through TransactionService.record() ensuring:
        - Idempotency (duplicate prevention)
        - Audit logging
        - Double-entry accounting (Dr Bank + Dr TDS | Cr Salary Income)
        - Atomic transactions

        Args:
            result: SalaryParseResult from parsing
            user_id: User ID
            employer_id: Employer ID (optional)

        Returns:
            Number of records saved
        """
        if not self.conn or not result.success:
            return 0

        txn_service = TransactionService(self.conn)
        file_hash = hashlib.sha256(result.source_file.encode()).hexdigest()[:8]

        count = 0
        for idx, record in enumerate(result.salary_records):
            if self._record_salary(txn_service, user_id, employer_id, record, result.source_file, file_hash, idx):
                count += 1

        return count

    def _record_salary(
        self,
        txn_service: TransactionService,
        user_id: int,
        employer_id: Optional[int],
        record: SalaryRecord,
        source_file: str,
        file_hash: str,
        row_idx: int
    ) -> bool:
        """
        Record salary via TransactionService with journal entry.

        Journal Entry:
            Dr Bank Account (1101)       | Net Pay
            Dr TDS Receivable (1601)     | Income Tax Deducted
            Dr EPF Asset (1301)          | PF Employee Contribution
            Cr Gross Salary Income (4109)| Gross Salary
        """
        # Generate idempotency key
        idempotency_key = f"salary:{file_hash}:{row_idx}:{record.pay_period}:{record.net_pay}"

        # Create journal entries
        entries = self._create_salary_journal_entries(record, user_id)

        # Create asset record for salary_records table
        asset_record = AssetRecord(
            table_name="salary_records",
            data={
                "user_id": user_id,
                "employer_id": employer_id,
                "pay_period": record.pay_period,
                "pay_date": record.pay_date.isoformat() if record.pay_date else None,
                "basic_salary": str(record.basic_salary),
                "hra": str(record.hra),
                "special_allowance": str(record.special_allowance),
                "lta": str(record.lta),
                "other_allowances": str(record.other_allowances),
                "gross_salary": str(record.gross_salary),
                "pf_employee": str(record.pf_employee),
                "pf_employer": str(record.pf_employer),
                "nps_employee": str(record.nps_employee),
                "nps_employer": str(record.nps_employer),
                "professional_tax": str(record.professional_tax),
                "income_tax_deducted": str(record.income_tax_deducted),
                "espp_deduction": str(record.espp_deduction),
                "tcs_on_espp": str(record.tcs_on_espp),
                "other_deductions": str(record.other_deductions),
                "rsu_tax_credit": str(record.rsu_tax_credit),
                "total_deductions": str(record.total_deductions),
                "net_pay": str(record.net_pay),
                "source_file": source_file,
            },
            on_conflict="IGNORE"
        )

        # Build asset records list
        asset_records = [asset_record]

        # Add RSU tax credit record if present
        # Note: This will be linked via a separate record after we get the salary_record_id

        # Parse pay_date for transaction date
        txn_date = record.pay_date or date.today()

        result = txn_service.record(
            user_id=user_id,
            entries=entries,
            description=f"Salary: {record.pay_period} - Net {record.net_pay}",
            source=TransactionSource.MANUAL,  # Could add PARSER_SALARY
            idempotency_key=idempotency_key,
            txn_date=txn_date,
            reference_type="SALARY",
            asset_records=asset_records,
        )

        if result.result.value != "success":
            return False

        # Save RSU tax credit if present (needs salary_record_id)
        salary_record_id = result.asset_record_ids.get("salary_records")
        if salary_record_id and record.rsu_tax_credit > Decimal("0"):
            rsu_idempotency_key = f"rsu_credit:{file_hash}:{row_idx}:{record.pay_period}"

            rsu_asset_record = AssetRecord(
                table_name="rsu_tax_credits",
                data={
                    "salary_record_id": salary_record_id,
                    "credit_amount": str(record.rsu_tax_credit),
                    "credit_date": record.pay_date.isoformat() if record.pay_date else date.today().isoformat(),
                    "correlation_status": CorrelationStatus.PENDING.value,
                },
                on_conflict="IGNORE"
            )

            txn_service.record_asset_only(
                user_id=user_id,
                asset_records=[rsu_asset_record],
                idempotency_key=rsu_idempotency_key,
                source=TransactionSource.MANUAL,
                description=f"RSU Tax Credit: {record.pay_period}",
            )

        return True

    def _create_salary_journal_entries(self, record: SalaryRecord, user_id: int) -> List[JournalEntry]:
        """Create journal entries for salary record."""
        entries = []

        # Get account IDs
        bank_account = get_account_by_code(self.conn, "1101")  # Bank - Savings
        tds_account = get_account_by_code(self.conn, "1601")   # TDS Receivable
        epf_account = get_account_by_code(self.conn, "1301")   # EPF - Employee
        salary_income = get_account_by_code(self.conn, "4109") # Gross Salary - Composite

        if not bank_account or not salary_income:
            return entries

        # Dr Bank for net pay
        if record.net_pay > Decimal("0"):
            entries.append(JournalEntry(
                account_id=bank_account.id,
                debit=record.net_pay,
                narration=f"Salary credit: {record.pay_period}"
            ))

        # Dr TDS Receivable for income tax deducted
        if tds_account and record.income_tax_deducted > Decimal("0"):
            entries.append(JournalEntry(
                account_id=tds_account.id,
                debit=record.income_tax_deducted,
                narration=f"TDS on salary: {record.pay_period}"
            ))

        # Dr EPF Asset for employee contribution
        if epf_account and record.pf_employee > Decimal("0"):
            entries.append(JournalEntry(
                account_id=epf_account.id,
                debit=record.pf_employee,
                narration=f"EPF contribution: {record.pay_period}"
            ))

        # Cr Gross Salary Income
        # Total credits should equal total debits (net + deductions)
        total_debits = record.net_pay + record.income_tax_deducted + record.pf_employee
        entries.append(JournalEntry(
            account_id=salary_income.id,
            credit=total_debits,  # This ensures balanced entries
            narration=f"Gross salary: {record.pay_period}"
        ))

        return entries
