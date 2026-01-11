"""PDF extraction utilities for Mutual Fund statements.

This module provides PDF parsing capabilities for CAMS and Karvy
Consolidated Account Statements using pdfplumber.

Supported formats:
- CAMS CAS PDF (password-protected)
- Karvy/KFintech CAS PDF (password-protected)
- Capital Gains Statement PDF (when available)
"""

import re
import logging
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass

try:
    import pdfplumber
    PDF_SUPPORT_AVAILABLE = True
except ImportError:
    PDF_SUPPORT_AVAILABLE = False
    pdfplumber = None

from .models import MFTransaction, MFScheme, AssetClass, TransactionType, ParseResult
from .classifier import classify_scheme

logger = logging.getLogger(__name__)


@dataclass
class PDFTransactionRow:
    """Raw transaction data extracted from PDF."""
    date: str
    description: str
    amount: str
    nav: str
    units: str
    balance: Optional[str] = None
    scheme_name: Optional[str] = None
    folio_number: Optional[str] = None


class PDFExtractor:
    """
    Base PDF extractor for Mutual Fund statements.

    This class provides common PDF extraction functionality that can be
    used by both CAMS and Karvy parsers.
    """

    # Common transaction type patterns
    TRANSACTION_PATTERNS = {
        'REDEMPTION': [
            r'redemption',
            r'red(?:emp)?',
            r'switch\s*out',
            r'switch-out',
        ],
        'PURCHASE': [
            r'purchase',
            r'sip\s*purchase',
            r'additional\s*purchase',
            r'new\s*purchase',
            r'systematic\s*investment',
        ],
        'SWITCH_IN': [
            r'switch\s*in',
            r'switch-in',
            r'lateral\s*shift\s*in',
            r'stp\s*in',
        ],
        'SWITCH_OUT': [
            r'switch\s*out',
            r'switch-out',
            r'lateral\s*shift\s*out',
            r'stp\s*out',
        ],
        'DIVIDEND': [
            r'dividend\s*payout',
            r'dividend(?!\s*reinvest)',
        ],
        'DIVIDEND_REINVEST': [
            r'dividend\s*reinvest',
            r'reinvestment',
        ],
    }

    def __init__(self):
        """Initialize PDF extractor."""
        if not PDF_SUPPORT_AVAILABLE:
            raise ImportError(
                "pdfplumber is required for PDF support. "
                "Install it with: pip install pdfplumber"
            )

    def extract_from_pdf(
        self,
        file_path: Path,
        password: Optional[str] = None
    ) -> ParseResult:
        """
        Extract transactions from a PDF file.

        Args:
            file_path: Path to PDF file
            password: PDF password (if encrypted)

        Returns:
            ParseResult with extracted transactions
        """
        result = ParseResult(success=True, source_file=str(file_path))

        if not file_path.exists():
            result.add_error(f"File not found: {file_path}")
            return result

        try:
            with pdfplumber.open(str(file_path), password=password or "") as pdf:
                all_transactions = []
                current_scheme = None
                current_folio = None

                for page_num, page in enumerate(pdf.pages, 1):
                    try:
                        page_txns, current_scheme, current_folio = self._extract_page(
                            page, current_scheme, current_folio
                        )
                        all_transactions.extend(page_txns)
                    except Exception as e:
                        result.add_warning(f"Page {page_num}: {str(e)}")

                # Convert raw transactions to MFTransaction objects
                for raw_txn in all_transactions:
                    try:
                        txn = self._convert_to_transaction(raw_txn)
                        if txn:
                            result.transactions.append(txn)
                    except Exception as e:
                        result.add_warning(f"Transaction conversion error: {str(e)}")

                if not result.transactions:
                    result.add_warning("No transactions found in PDF")

        except Exception as e:
            error_msg = str(e)
            if "password" in error_msg.lower() or "encrypted" in error_msg.lower():
                result.add_error("PDF is password-protected. Please provide the password.")
            else:
                result.add_error(f"Failed to read PDF: {error_msg}")
            result.success = False

        return result

    def _extract_page(
        self,
        page,
        current_scheme: Optional[str],
        current_folio: Optional[str]
    ) -> Tuple[List[PDFTransactionRow], Optional[str], Optional[str]]:
        """
        Extract transactions from a single PDF page.

        Args:
            page: pdfplumber page object
            current_scheme: Current scheme name (carried from previous page)
            current_folio: Current folio number (carried from previous page)

        Returns:
            Tuple of (transactions, updated_scheme, updated_folio)
        """
        transactions = []

        # Try table extraction first
        tables = page.extract_tables()
        if tables:
            for table in tables:
                txns, current_scheme, current_folio = self._parse_table(
                    table, current_scheme, current_folio
                )
                transactions.extend(txns)

        # If no tables, try text extraction
        if not transactions:
            text = page.extract_text()
            if text:
                txns, current_scheme, current_folio = self._parse_text(
                    text, current_scheme, current_folio
                )
                transactions.extend(txns)

        return transactions, current_scheme, current_folio

    def _parse_table(
        self,
        table: List[List[str]],
        current_scheme: Optional[str],
        current_folio: Optional[str]
    ) -> Tuple[List[PDFTransactionRow], Optional[str], Optional[str]]:
        """
        Parse a table extracted from PDF.

        Args:
            table: 2D list of table cells
            current_scheme: Current scheme name
            current_folio: Current folio number

        Returns:
            Tuple of (transactions, updated_scheme, updated_folio)
        """
        transactions = []

        for row in table:
            if not row or not row[0]:
                continue

            # Check if this is a scheme header row
            row_text = str(row[0]).strip()
            if self._is_scheme_header(row_text):
                scheme_info = self._extract_scheme_info(row_text)
                if scheme_info:
                    current_scheme = scheme_info.get('name')
                continue

            # Check if this is a folio row
            if 'folio' in row_text.lower():
                folio_match = re.search(r'folio[:\s]*(\d+)', row_text, re.IGNORECASE)
                if folio_match:
                    current_folio = folio_match.group(1)
                continue

            # Try to parse as transaction row
            txn = self._parse_transaction_row(row_text, current_scheme, current_folio)
            if txn:
                transactions.append(txn)

        return transactions, current_scheme, current_folio

    def _parse_text(
        self,
        text: str,
        current_scheme: Optional[str],
        current_folio: Optional[str]
    ) -> Tuple[List[PDFTransactionRow], Optional[str], Optional[str]]:
        """
        Parse raw text extracted from PDF.

        Args:
            text: Page text content
            current_scheme: Current scheme name
            current_folio: Current folio number

        Returns:
            Tuple of (transactions, updated_scheme, updated_folio)
        """
        transactions = []
        lines = text.split('\n')

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check for scheme name
            if self._is_scheme_header(line):
                scheme_info = self._extract_scheme_info(line)
                if scheme_info:
                    current_scheme = scheme_info.get('name')
                continue

            # Check for folio number
            folio_match = re.search(r'folio[:\s]*(\d+)', line, re.IGNORECASE)
            if folio_match:
                current_folio = folio_match.group(1)
                continue

            # Try to parse as transaction
            txn = self._parse_transaction_row(line, current_scheme, current_folio)
            if txn:
                transactions.append(txn)

        return transactions, current_scheme, current_folio

    def _parse_transaction_row(
        self,
        text: str,
        scheme_name: Optional[str],
        folio_number: Optional[str]
    ) -> Optional[PDFTransactionRow]:
        """
        Parse a single transaction row from text.

        Expected format:
        DD-Mon-YYYY Description Amount NAV Units [Balance]

        Args:
            text: Row text
            scheme_name: Current scheme name
            folio_number: Current folio number

        Returns:
            PDFTransactionRow or None
        """
        # Match date at start: DD-Mon-YYYY or DD/MM/YYYY
        date_pattern = r'^(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{4})'
        date_match = re.match(date_pattern, text)

        if not date_match:
            return None

        date_str = date_match.group(1)
        remaining = text[date_match.end():].strip()

        # Skip stamp duty entries
        if 'stamp duty' in remaining.lower():
            return None

        # Extract numbers from the end (amount, nav, units, balance)
        # Pattern: numbers separated by spaces, with possible commas and decimals
        numbers_pattern = r'([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s*([\d,]+\.?\d*)?$'
        numbers_match = re.search(numbers_pattern, remaining)

        if not numbers_match:
            return None

        # Description is everything before the numbers
        description = remaining[:numbers_match.start()].strip()

        return PDFTransactionRow(
            date=date_str,
            description=description,
            amount=numbers_match.group(1),
            nav=numbers_match.group(2),
            units=numbers_match.group(3),
            balance=numbers_match.group(4) if numbers_match.group(4) else None,
            scheme_name=scheme_name,
            folio_number=folio_number
        )

    def _is_scheme_header(self, text: str) -> bool:
        """Check if text is a scheme header."""
        # Scheme headers typically contain fund names
        scheme_indicators = [
            'fund', 'growth', 'dividend', 'direct', 'regular',
            'idcw', 'plan', 'option', 'mutual'
        ]
        text_lower = text.lower()
        return any(ind in text_lower for ind in scheme_indicators) and len(text) > 20

    def _extract_scheme_info(self, text: str) -> Optional[Dict[str, str]]:
        """Extract scheme information from header text."""
        # Extract ISIN if present
        isin_match = re.search(r'ISIN[:\s]*([A-Z0-9]{12})', text, re.IGNORECASE)
        isin = isin_match.group(1) if isin_match else None

        # Clean up scheme name
        name = re.sub(r'ISIN[:\s]*[A-Z0-9]{12}', '', text, flags=re.IGNORECASE)
        name = re.sub(r'Advisor:.*$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'Registrar:.*$', '', name, flags=re.IGNORECASE)
        name = name.strip()

        if name:
            return {'name': name, 'isin': isin}
        return None

    def _convert_to_transaction(
        self,
        raw: PDFTransactionRow
    ) -> Optional[MFTransaction]:
        """
        Convert raw PDF transaction to MFTransaction.

        Args:
            raw: PDFTransactionRow from PDF extraction

        Returns:
            MFTransaction or None
        """
        # Parse date
        txn_date = self._parse_date(raw.date)
        if not txn_date:
            return None

        # Determine transaction type
        txn_type = self._determine_transaction_type(raw.description)

        # Parse numeric values
        amount = self._parse_decimal(raw.amount)
        nav = self._parse_decimal(raw.nav)
        units = self._parse_decimal(raw.units)

        # Classify scheme
        asset_class = classify_scheme(raw.scheme_name or '')

        # Create scheme
        scheme = MFScheme(
            name=raw.scheme_name or 'Unknown Scheme',
            amc_name='',  # Not available in CAS PDF
            isin=None,
            asset_class=asset_class
        )

        return MFTransaction(
            folio_number=raw.folio_number or '',
            scheme=scheme,
            transaction_type=txn_type,
            date=txn_date,
            units=units,
            nav=nav,
            amount=amount,
            stt=Decimal("0"),
        )

    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string to date object."""
        formats = [
            '%d-%b-%Y',  # 15-Jul-2024
            '%d/%m/%Y',  # 15/07/2024
            '%d-%m-%Y',  # 15-07-2024
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        return None

    def _parse_decimal(self, value: str) -> Decimal:
        """Parse string to Decimal."""
        if not value:
            return Decimal("0")

        try:
            # Remove commas
            clean = value.replace(',', '')
            return Decimal(clean)
        except (InvalidOperation, ValueError):
            return Decimal("0")

    def _determine_transaction_type(self, description: str) -> TransactionType:
        """Determine transaction type from description."""
        if not description:
            return TransactionType.PURCHASE

        desc_lower = description.lower()

        for txn_type, patterns in self.TRANSACTION_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, desc_lower):
                    return TransactionType[txn_type]

        return TransactionType.PURCHASE


def check_pdf_support() -> bool:
    """Check if PDF support is available."""
    return PDF_SUPPORT_AVAILABLE


def get_pdf_extractor() -> PDFExtractor:
    """
    Get a PDF extractor instance.

    Returns:
        PDFExtractor instance

    Raises:
        ImportError: If pdfplumber is not installed
    """
    return PDFExtractor()
