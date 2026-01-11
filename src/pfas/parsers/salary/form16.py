"""Form 16 Part A & B parser.

Form 16 is the TDS certificate issued by employer containing:
- Part A: Quarterly TDS details with BSR codes
- Part B: Salary breakup, exemptions, and deductions
"""

import re
import zipfile
import tempfile
import pdfplumber
from pathlib import Path
from decimal import Decimal
from typing import Optional
import sqlite3

from .models import Form16Record, SalaryParseResult


class Form16Parser:
    """
    Parser for Form 16 (TDS Certificate).

    Form 16 is typically provided as a ZIP containing:
    - Part A PDF: TDS certificate with quarterly breakdown
    - Part B PDF: Salary details and deductions

    Alternatively, can parse standalone Part A or Part B PDFs.
    """

    # Part A patterns
    PART_A_PATTERNS = {
        'assessment_year': r'Assessment\s*Year\s*[:\-]?\s*(\d{4}\s*[-–]\s*\d{2,4})',
        'employer_tan': r'TAN\s*(?:of\s*(?:the\s*)?(?:Deductor|Employer))?[:\s]+([A-Z]{4}\d{5}[A-Z])',
        'employee_pan': r'PAN\s*(?:of\s*(?:the\s*)?(?:Employee|Deductee))?[:\s]+([A-Z]{5}\d{4}[A-Z])',
        'q1_tds': r'Q1.*?(?:Amount|Tax)\s*(?:Deposited)?\s*[:\s]*([\d,]+(?:\.\d{2})?)',
        'q2_tds': r'Q2.*?(?:Amount|Tax)\s*(?:Deposited)?\s*[:\s]*([\d,]+(?:\.\d{2})?)',
        'q3_tds': r'Q3.*?(?:Amount|Tax)\s*(?:Deposited)?\s*[:\s]*([\d,]+(?:\.\d{2})?)',
        'q4_tds': r'Q4.*?(?:Amount|Tax)\s*(?:Deposited)?\s*[:\s]*([\d,]+(?:\.\d{2})?)',
        'total_tds': r'Total.*?(?:Tax|Amount)\s*(?:Deposited|Deducted)?\s*[:\s]*([\d,]+(?:\.\d{2})?)',
    }

    # Part B patterns
    PART_B_PATTERNS = {
        'salary_17_1': r'(?:Salary\s*(?:as\s*per|u/s)\s*)?(?:section\s*)?17\s*\(?1\)?[^\d]*([\d,]+(?:\.\d{2})?)',
        'perquisites_17_2': r'(?:Value\s*of\s*)?(?:perquisites\s*)?(?:u/s\s*)?17\s*\(?2\)?[^\d]*([\d,]+(?:\.\d{2})?)',
        'profits_17_3': r'(?:Profits\s*)?(?:u/s\s*)?17\s*\(?3\)?[^\d]*([\d,]+(?:\.\d{2})?)',
        'gross_salary': r'(?:Gross|Total)\s*[Ss]alary[^\d]*([\d,]+(?:\.\d{2})?)',
        'standard_deduction': r'(?:Standard\s*[Dd]eduction|16\s*\(?ia\)?)[^\d]*([\d,]+(?:\.\d{2})?)',
        'professional_tax': r'(?:Professional\s*[Tt]ax|16\s*\(?iii\)?)[^\d]*([\d,]+(?:\.\d{2})?)',
        'section_80c': r'80\s*C[^\d]*([\d,]+(?:\.\d{2})?)',
        'section_80ccd_1b': r'80\s*CCD\s*\(?1B\)?[^\d]*([\d,]+(?:\.\d{2})?)',
        'section_80ccd_2': r'80\s*CCD\s*\(?2\)?[^\d]*([\d,]+(?:\.\d{2})?)',
        'section_80d': r'80\s*D[^\d]*([\d,]+(?:\.\d{2})?)',
        'taxable_income': r'(?:Total\s*)?[Tt]axable\s*[Ii]ncome[^\d]*([\d,]+(?:\.\d{2})?)',
        'tax_on_income': r'[Tt]ax\s*(?:on\s*)?(?:total\s*)?[Ii]ncome[^\d]*([\d,]+(?:\.\d{2})?)',
        'education_cess': r'(?:Education|Health)\s*[Cc]ess[^\d]*([\d,]+(?:\.\d{2})?)',
        'total_tax_payable': r'[Tt]otal\s*[Tt]ax\s*[Pp]ayable[^\d]*([\d,]+(?:\.\d{2})?)',
        'relief_87a': r'(?:Relief|Rebate)\s*(?:u/s\s*)?87\s*A?[^\d]*([\d,]+(?:\.\d{2})?)',
    }

    def __init__(self, db_connection: Optional[sqlite3.Connection] = None):
        """
        Initialize Form 16 parser.

        Args:
            db_connection: Optional database connection
        """
        self.conn = db_connection

    def parse(self, file_path: Path) -> SalaryParseResult:
        """
        Parse Form 16 from ZIP archive or PDF.

        Args:
            file_path: Path to Form 16 ZIP or PDF

        Returns:
            SalaryParseResult with Form16Record
        """
        file_path = Path(file_path)
        result = SalaryParseResult(success=True, source_file=str(file_path))

        if not file_path.exists():
            result.add_error(f"File not found: {file_path}")
            return result

        try:
            if file_path.suffix.lower() == '.zip':
                record = self._parse_zip(file_path, result)
            elif file_path.suffix.lower() == '.pdf':
                record = self._parse_pdf(file_path, result)
            else:
                result.add_error(f"Unsupported file format: {file_path.suffix}")
                return result

            result.form16_record = record
            record.calculate_total_tds()
            record.calculate_gross_salary()

        except Exception as e:
            result.add_error(f"Failed to parse Form 16: {str(e)}")

        return result

    def _parse_zip(self, zip_path: Path, result: SalaryParseResult) -> Form16Record:
        """Parse Form 16 ZIP archive containing Part A and Part B."""
        record = Form16Record(assessment_year="")

        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            for filename in zip_file.namelist():
                if not filename.lower().endswith('.pdf'):
                    continue

                # Extract PDF to temp file
                with zip_file.open(filename) as pdf_file:
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
                        tmp.write(pdf_file.read())
                        tmp_path = Path(tmp.name)

                try:
                    filename_lower = filename.lower()
                    if 'part_a' in filename_lower or 'parta' in filename_lower or 'form16a' in filename_lower:
                        self._parse_part_a(tmp_path, record, result)
                    elif 'part_b' in filename_lower or 'partb' in filename_lower or 'form16b' in filename_lower:
                        self._parse_part_b(tmp_path, record, result)
                    else:
                        # Try to determine from content
                        text = self._extract_text(tmp_path)
                        if 'Part A' in text or 'PART A' in text:
                            self._parse_part_a(tmp_path, record, result)
                        if 'Part B' in text or 'PART B' in text:
                            self._parse_part_b(tmp_path, record, result)
                finally:
                    tmp_path.unlink(missing_ok=True)

        return record

    def _parse_pdf(self, pdf_path: Path, result: SalaryParseResult) -> Form16Record:
        """Parse standalone Form 16 PDF (may contain both parts)."""
        record = Form16Record(assessment_year="")

        text = self._extract_text(pdf_path)

        # Parse Part A if present
        if re.search(r'Part\s*A|PART\s*A|Certificate.*TDS', text, re.IGNORECASE):
            self._parse_part_a_text(text, record, result)

        # Parse Part B if present
        if re.search(r'Part\s*B|PART\s*B|Annexure.*Salary', text, re.IGNORECASE):
            self._parse_part_b_text(text, record, result)

        return record

    def _extract_text(self, pdf_path: Path) -> str:
        """Extract text from PDF file."""
        text = ""
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        return text

    def _parse_part_a(self, pdf_path: Path, record: Form16Record, result: SalaryParseResult):
        """Parse Part A from PDF file."""
        text = self._extract_text(pdf_path)
        self._parse_part_a_text(text, record, result)

    def _parse_part_a_text(self, text: str, record: Form16Record, result: SalaryParseResult):
        """Parse Part A content (TDS certificate)."""
        # Extract header info
        for field in ['assessment_year', 'employer_tan', 'employee_pan']:
            match = re.search(self.PART_A_PATTERNS[field], text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                if field == 'assessment_year':
                    # Normalize format
                    value = value.replace(' ', '').replace('–', '-')
                setattr(record, field, value)

        # Extract quarterly TDS using table parsing
        # Look for quarterly amounts in a table format
        tds_pattern = r'Q([1-4]).*?([\d,]+(?:\.\d{2})?)'
        matches = re.findall(tds_pattern, text, re.IGNORECASE | re.DOTALL)

        for quarter, amount in matches:
            value = self._to_decimal(amount)
            if quarter == '1':
                record.q1_tds = value
            elif quarter == '2':
                record.q2_tds = value
            elif quarter == '3':
                record.q3_tds = value
            elif quarter == '4':
                record.q4_tds = value

        # Try to extract total TDS
        match = re.search(self.PART_A_PATTERNS['total_tds'], text, re.IGNORECASE)
        if match:
            record.total_tds = self._to_decimal(match.group(1))

    def _parse_part_b(self, pdf_path: Path, record: Form16Record, result: SalaryParseResult):
        """Parse Part B from PDF file."""
        text = self._extract_text(pdf_path)
        self._parse_part_b_text(text, record, result)

    def _parse_part_b_text(self, text: str, record: Form16Record, result: SalaryParseResult):
        """Parse Part B content (salary details)."""
        for field, pattern in self.PART_B_PATTERNS.items():
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                value = self._to_decimal(match.group(1))
                setattr(record, field, value)

    def _to_decimal(self, value: str) -> Decimal:
        """Convert string to Decimal, handling commas."""
        if not value:
            return Decimal("0")

        clean = value.replace(",", "").strip()

        try:
            return Decimal(clean)
        except:
            return Decimal("0")

    def save_to_db(
        self,
        result: SalaryParseResult,
        user_id: int,
        employer_id: Optional[int] = None
    ) -> bool:
        """
        Save Form 16 record to database.

        Args:
            result: SalaryParseResult from parsing
            user_id: User ID
            employer_id: Employer ID

        Returns:
            True if saved successfully
        """
        if not self.conn or not result.success or not result.form16_record:
            return False

        record = result.form16_record

        try:
            self.conn.execute(
                """INSERT INTO form16_records
                (user_id, employer_id, assessment_year,
                 q1_tds, q2_tds, q3_tds, q4_tds, total_tds,
                 salary_17_1, perquisites_17_2, profits_17_3, gross_salary,
                 hra_exemption, lta_exemption, other_exemptions,
                 standard_deduction, professional_tax,
                 section_80c, section_80ccd_1b, section_80ccd_2, section_80d,
                 taxable_income, tax_payable, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, employer_id, assessment_year)
                DO UPDATE SET
                    q1_tds = excluded.q1_tds,
                    q2_tds = excluded.q2_tds,
                    q3_tds = excluded.q3_tds,
                    q4_tds = excluded.q4_tds,
                    total_tds = excluded.total_tds,
                    salary_17_1 = excluded.salary_17_1,
                    perquisites_17_2 = excluded.perquisites_17_2,
                    gross_salary = excluded.gross_salary,
                    standard_deduction = excluded.standard_deduction,
                    section_80ccd_2 = excluded.section_80ccd_2,
                    taxable_income = excluded.taxable_income,
                    source_file = excluded.source_file""",
                (
                    user_id,
                    employer_id,
                    record.assessment_year,
                    str(record.q1_tds),
                    str(record.q2_tds),
                    str(record.q3_tds),
                    str(record.q4_tds),
                    str(record.total_tds),
                    str(record.salary_17_1),
                    str(record.perquisites_17_2),
                    str(record.profits_17_3),
                    str(record.gross_salary),
                    str(record.hra_exemption),
                    str(record.lta_exemption),
                    str(record.other_exemptions),
                    str(record.standard_deduction),
                    str(record.professional_tax),
                    str(record.section_80c),
                    str(record.section_80ccd_1b),
                    str(record.section_80ccd_2),
                    str(record.section_80d),
                    str(record.taxable_income),
                    str(record.total_tax_payable),
                    result.source_file
                )
            )
            self.conn.commit()
            return True

        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to save Form 16 record: {e}") from e
