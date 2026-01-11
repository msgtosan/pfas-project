"""Form 12BA - Statement of Perquisites parser.

Form 12BA details perquisites provided by employer that are
taxable as part of salary income under section 17(2).

Common perquisites:
- RSU/ESOP: Stock options taxed at FMV on vest
- ESPP Discount: Discount on employee stock purchase
- Employer PF: Contribution above Rs 7.5L limit
- Interest Accretion: Interest on taxable employer contributions
"""

import re
import pdfplumber
from pathlib import Path
from decimal import Decimal
from typing import Optional
import sqlite3

from .models import Form12BARecord, Perquisite, PerquisiteType, SalaryParseResult


class Form12BAParser:
    """
    Parser for Form 12BA (Statement of Perquisites).

    Form 12BA is an annexure to Form 16 Part B containing:
    - Nature of perquisite
    - Gross value
    - Amount recovered from employee
    - Taxable value
    """

    # Perquisite type patterns
    PERQUISITE_PATTERNS = {
        PerquisiteType.RSU: [
            r'(?:Stock\s*[Oo]ptions?|ESOP|RSU|Restricted\s*Stock)',
            r'(?:Sweat\s*[Ee]quity|Non[-\s]?[Qq]ualified)',
        ],
        PerquisiteType.ESPP_DISCOUNT: [
            r'ESPP',
            r'Employee\s*Stock\s*Purchase',
        ],
        PerquisiteType.EMPLOYER_PF: [
            r'(?:Employer|ER)\s*(?:contribution\s*to\s*)?(?:PF|Provident\s*Fund)',
            r'Contribution.*(?:fund|PF).*excess',
        ],
        PerquisiteType.EMPLOYER_NPS: [
            r'(?:Employer|ER)\s*(?:contribution\s*to\s*)?NPS',
            r'National\s*Pension',
        ],
        PerquisiteType.INTEREST_ACCRETION: [
            r'Interest\s*(?:accretion|accrued)',
            r'Accretion\s*of\s*interest',
        ],
    }

    # Table extraction patterns
    TABLE_PATTERNS = {
        'assessment_year': r'Assessment\s*Year\s*[:\-]?\s*(\d{4}\s*[-–]\s*\d{2,4})',
        'employer_tan': r'TAN\s*[:\s]+([A-Z]{4}\d{5}[A-Z])',
        'employee_pan': r'PAN\s*[:\s]+([A-Z]{5}\d{4}[A-Z])',
        'total_perquisites': r'(?:Total|Grand\s*Total)\s*(?:Perquisites?)?[:\s]*([\d,]+(?:\.\d{2})?)',
    }

    def __init__(self, db_connection: Optional[sqlite3.Connection] = None):
        """
        Initialize Form 12BA parser.

        Args:
            db_connection: Optional database connection
        """
        self.conn = db_connection

    def parse(self, file_path: Path) -> SalaryParseResult:
        """
        Parse Form 12BA PDF.

        Args:
            file_path: Path to Form 12BA PDF

        Returns:
            SalaryParseResult with Form12BARecord
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
            text = self._extract_text(file_path)
            tables = self._extract_tables(file_path)

            record = self._parse_content(text, tables, result)
            record.calculate_total()
            result.form12ba_record = record

        except Exception as e:
            result.add_error(f"Failed to parse Form 12BA: {str(e)}")

        return result

    def _extract_text(self, pdf_path: Path) -> str:
        """Extract text from PDF file."""
        text = ""
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        return text

    def _extract_tables(self, pdf_path: Path) -> list:
        """Extract tables from PDF file."""
        tables = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_tables = page.extract_tables()
                if page_tables:
                    tables.extend(page_tables)
        return tables

    def _parse_content(
        self,
        text: str,
        tables: list,
        result: SalaryParseResult
    ) -> Form12BARecord:
        """Parse Form 12BA content."""
        record = Form12BARecord(assessment_year="")

        # Extract header info
        match = re.search(self.TABLE_PATTERNS['assessment_year'], text, re.IGNORECASE)
        if match:
            record.assessment_year = match.group(1).replace(' ', '').replace('–', '-')

        match = re.search(self.TABLE_PATTERNS['employer_tan'], text, re.IGNORECASE)
        if match:
            record.employer_tan = match.group(1)

        match = re.search(self.TABLE_PATTERNS['employee_pan'], text, re.IGNORECASE)
        if match:
            record.employee_pan = match.group(1)

        # Extract perquisites from tables
        for table in tables:
            perquisites = self._parse_perquisite_table(table)
            record.perquisites.extend(perquisites)

        # Also try to extract from text patterns
        text_perquisites = self._parse_perquisites_from_text(text)
        for p in text_perquisites:
            # Avoid duplicates
            if not any(existing.perquisite_type == p.perquisite_type and
                      existing.gross_value == p.gross_value
                      for existing in record.perquisites):
                record.perquisites.append(p)

        # Get total from text if not calculated
        match = re.search(self.TABLE_PATTERNS['total_perquisites'], text, re.IGNORECASE)
        if match:
            expected_total = self._to_decimal(match.group(1))
            if record.total_perquisites == Decimal("0"):
                record.total_perquisites = expected_total
            elif abs(record.total_perquisites - expected_total) > Decimal("1"):
                result.add_warning(
                    f"Calculated total ({record.total_perquisites}) differs from "
                    f"document total ({expected_total})"
                )

        return record

    def _parse_perquisite_table(self, table: list) -> list[Perquisite]:
        """Parse perquisites from a table structure."""
        perquisites = []

        if not table or len(table) < 2:
            return perquisites

        # Find header row
        header_row = None
        for i, row in enumerate(table):
            row_text = ' '.join(str(cell or '') for cell in row).lower()
            if 'nature' in row_text or 'perquisite' in row_text or 'value' in row_text:
                header_row = i
                break

        if header_row is None:
            return perquisites

        # Parse data rows
        for row in table[header_row + 1:]:
            if not row or len(row) < 2:
                continue

            # Try to extract perquisite
            description = str(row[0] or '').strip() if row[0] else ''
            if not description:
                continue

            # Determine perquisite type
            p_type = self._identify_perquisite_type(description)

            # Extract values
            gross_value = Decimal("0")
            recovered = Decimal("0")
            taxable = Decimal("0")

            for i, cell in enumerate(row[1:], 1):
                value = self._to_decimal(str(cell or ''))
                if value > Decimal("0"):
                    if i == 1 or 'gross' in str(table[header_row][i] or '').lower():
                        gross_value = value
                    elif 'recover' in str(table[header_row][i] or '').lower():
                        recovered = value
                    elif 'taxable' in str(table[header_row][i] or '').lower():
                        taxable = value
                    elif gross_value == Decimal("0"):
                        gross_value = value

            if gross_value > Decimal("0") or taxable > Decimal("0"):
                perquisite = Perquisite(
                    perquisite_type=p_type,
                    description=description,
                    gross_value=gross_value,
                    recovered_from_employee=recovered,
                    taxable_value=taxable if taxable > 0 else gross_value - recovered
                )
                perquisites.append(perquisite)

        return perquisites

    def _parse_perquisites_from_text(self, text: str) -> list[Perquisite]:
        """Parse perquisites from text patterns."""
        perquisites = []

        # Pattern: perquisite description followed by amount
        patterns = [
            # RSU pattern
            (PerquisiteType.RSU, r'(?:Stock\s*[Oo]ptions?|RSU|ESOP)[^\d]*([\d,]+(?:\.\d{2})?)'),
            # Employer contribution pattern
            (PerquisiteType.EMPLOYER_PF, r'(?:Employer|ER)\s*contribution[^\d]*([\d,]+(?:\.\d{2})?)'),
            # Interest pattern
            (PerquisiteType.INTEREST_ACCRETION, r'Interest\s*(?:accretion|accrued)[^\d]*([\d,]+(?:\.\d{2})?)'),
        ]

        for p_type, pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for amount in matches:
                value = self._to_decimal(amount)
                if value > Decimal("0"):
                    perquisite = Perquisite(
                        perquisite_type=p_type,
                        description=f"{p_type.value} from text",
                        gross_value=value,
                        taxable_value=value
                    )
                    perquisites.append(perquisite)

        return perquisites

    def _identify_perquisite_type(self, description: str) -> PerquisiteType:
        """Identify perquisite type from description."""
        desc_lower = description.lower()

        for p_type, patterns in self.PERQUISITE_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, description, re.IGNORECASE):
                    return p_type

        return PerquisiteType.OTHER

    def _to_decimal(self, value: str) -> Decimal:
        """Convert string to Decimal."""
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
        form16_id: int
    ) -> int:
        """
        Save perquisites to database.

        Args:
            result: SalaryParseResult from parsing
            form16_id: Associated Form 16 record ID

        Returns:
            Number of perquisites saved
        """
        if not self.conn or not result.success or not result.form12ba_record:
            return 0

        record = result.form12ba_record
        count = 0

        try:
            for perquisite in record.perquisites:
                self.conn.execute(
                    """INSERT INTO perquisites
                    (form16_id, perquisite_type, description,
                     gross_value, recovered_from_employee, taxable_value)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        form16_id,
                        perquisite.perquisite_type.value,
                        perquisite.description,
                        str(perquisite.gross_value),
                        str(perquisite.recovered_from_employee),
                        str(perquisite.taxable_value)
                    )
                )
                count += 1

            self.conn.commit()
            return count

        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to save perquisites: {e}") from e
