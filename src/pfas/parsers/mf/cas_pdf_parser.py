"""
CAS PDF Parser - Parse CAMS/KFintech CAS PDF statements.

This module provides comprehensive CAS (Consolidated Account Statement)
PDF parsing using PyMuPDF (primary) with pdfplumber fallback.

Features:
- CAMS and KFintech CAS format support
- Statement period and investor info extraction
- Transaction extraction with full detail
- Balance reconciliation
- Support for both SUMMARY and DETAILED CAS formats
- Proper exception handling
"""

import re
import logging
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Dict, Tuple, Any
from dataclasses import dataclass, field

# Try PyMuPDF first (faster and more accurate)
try:
    import fitz  # PyMuPDF
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False
    fitz = None

# Fallback to pdfplumber
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    pdfplumber = None

from .models import (
    CASFileType, CASSource, InvestorInfo, StatementPeriod,
    CASTransaction, SchemeValuation, CASScheme, CASFolio, CASData,
    TransactionType, AssetClass
)
from .exceptions import (
    CASParseError, HeaderParseError, IncorrectPasswordError,
    UnsupportedFormatError, BalanceMismatchError
)
from .classifier import classify_scheme

logger = logging.getLogger(__name__)


@dataclass
class FolioConsolidationEntry:
    """Record of a single folio consolidation event."""
    folio: str
    amc: str
    original_count: int
    merged_count: int
    schemes_merged: List[str]


@dataclass
class ConsolidationResult:
    """Result of folio consolidation process."""
    original_folio_count: int
    consolidated_folio_count: int
    consolidation_entries: List[FolioConsolidationEntry] = field(default_factory=list)
    schemes_renamed: Dict[str, str] = field(default_factory=dict)  # old_name -> new_name

    @property
    def consolidation_summary(self) -> str:
        """Generate a summary of consolidation actions."""
        lines = []
        lines.append(f"Folio Consolidation: {self.original_folio_count} -> {self.consolidated_folio_count}")

        if self.consolidation_entries:
            lines.append("\nConsolidated Folios:")
            for entry in self.consolidation_entries:
                if entry.merged_count > 0:
                    lines.append(f"  {entry.folio} ({entry.amc}): Merged {entry.merged_count} duplicate entries")
                    for scheme in entry.schemes_merged:
                        lines.append(f"    - {scheme[:60]}...")

        if self.schemes_renamed:
            lines.append(f"\nScheme Names Cleaned: {len(self.schemes_renamed)}")
            # Show first few examples
            for old, new in list(self.schemes_renamed.items())[:5]:
                lines.append(f"  {old[:40]}... -> {new[:40]}...")
            if len(self.schemes_renamed) > 5:
                lines.append(f"  ... and {len(self.schemes_renamed) - 5} more")

        return "\n".join(lines)


# Scheme name prefix pattern (e.g., "B92Z-", "D860-", "P8017-", "PPCHFGZ-", "RMFLFAGG-")
# Matches 3-10 character alphanumeric codes followed by hyphen
SCHEME_PREFIX_PATTERN = re.compile(r'^[A-Z][A-Z0-9]{2,9}-')


# =============================================================================
# Regex Patterns for CAMS CAS
# =============================================================================

# Statement period pattern (matches "01-Jan-1990 To 15-Jan-2026" format)
PERIOD_PATTERN = re.compile(
    r'(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4})\s+(?:To|to|-)\s+(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4})',
    re.IGNORECASE
)

# Alternate period pattern (numeric month: DD-MM-YYYY)
PERIOD_PATTERN_ALT = re.compile(
    r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})\s*(?:to|-)\s*(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
    re.IGNORECASE
)

# Period pattern with "Statement period" prefix
PERIOD_PATTERN_PREFIX = re.compile(
    r'(?:Statement\s+(?:for\s+the\s+)?period|Period)\s*:?\s*'
    r'(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4})\s*(?:to|-)\s*(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4})',
    re.IGNORECASE
)

# Investor name pattern
INVESTOR_PATTERN = re.compile(
    r'(?:Dear|Name)\s*:?\s*([A-Z][A-Za-z\s]+?)(?:\n|,|PAN)',
    re.IGNORECASE
)

# Email pattern
EMAIL_PATTERN = re.compile(
    r'Email\s*:?\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
    re.IGNORECASE
)

# Mobile pattern
MOBILE_PATTERN = re.compile(
    r'Mobile\s*:?\s*(\+?91[-\s]?)?(\d{10})',
    re.IGNORECASE
)

# PAN pattern
PAN_PATTERN = re.compile(
    r'PAN\s*:?\s*([A-Z]{5}\d{4}[A-Z])',
    re.IGNORECASE
)

# Folio pattern
FOLIO_PATTERN = re.compile(
    r'Folio\s*(?:No\.?|Number)?\s*:?\s*(\d+(?:/\d+)?)',
    re.IGNORECASE
)

# AMC pattern
AMC_PATTERN = re.compile(
    r'^([A-Za-z\s]+(?:Mutual\s+Fund|Asset\s+Management|AMC))',
    re.IGNORECASE | re.MULTILINE
)

# Scheme pattern with ISIN
SCHEME_PATTERN = re.compile(
    r'([A-Z][A-Za-z0-9\s\-()]+(?:Fund|Growth|Dividend|IDCW|Direct|Regular|Plan|Option)[A-Za-z0-9\s\-()]*)'
    r'(?:\s*[-–]\s*ISIN:\s*([A-Z]{2}[A-Z0-9]{10}))?',
    re.IGNORECASE
)

# Number pattern that handles both -123.45 and (123.45) formats
# Used in transaction patterns below
_NUM = r'(?:-?[\d,]+\.?\d*|\([\d,]+\.?\d*\))'

# Transaction line pattern: Date | Description | Amount | Units | NAV | Balance
# Example: 02-Mar-2015 Purchase-SIP - Instalment 1/83 - via Internet 10,000.00 57.428 174.13 57.428
# Example: 02-Apr-2024 Switch Out - To Parag Parikh Flexi Cap -25,000.00 -18.621 1342.5792 1456.045
# Example: 04-Aug-2024 Switch Out - To HDFC Small Cap Fund (100,000.00) (20.566) 4,862.3209 325.100
TXN_PATTERN = re.compile(
    r'^(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4})\s+'  # Date
    r'(.+?)\s+'                                # Description (non-greedy)
    rf'({_NUM})\s+'                            # Amount (can be negative or in parens)
    rf'({_NUM})\s+'                            # Units (can be negative or in parens)
    r'([\d,]+\.?\d*)\s+'                       # NAV/Price
    rf'({_NUM})$',                             # Balance
    re.IGNORECASE
)

# Alternative transaction pattern (without balance)
TXN_PATTERN_ALT = re.compile(
    r'^(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4})\s+'  # Date
    r'(.+?)\s+'                                # Description (non-greedy)
    rf'({_NUM})\s+'                            # Amount
    rf'({_NUM})\s+'                            # Units
    r'([\d,]+\.?\d*)',                         # NAV
    re.IGNORECASE
)

# More flexible transaction pattern - handles varied spacing and formats
TXN_PATTERN_FLEX = re.compile(
    r'^(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4})\s+'  # Date
    r'(.+?)\s{2,}'                             # Description followed by 2+ spaces
    rf'({_NUM})\s+'                            # Amount
    rf'({_NUM})\s+'                            # Units
    r'([\d,]+\.?\d*)'                          # NAV (balance optional)
    rf'(?:\s+({_NUM}))?',                      # Optional Balance
    re.IGNORECASE
)

# Opening/Closing balance pattern
BALANCE_PATTERN = re.compile(
    r'(?:Opening|Closing)\s+(?:Unit\s+)?Balance\s*:?\s*([\d,]+\.?\d*)',
    re.IGNORECASE
)

# Valuation pattern
VALUATION_PATTERN = re.compile(
    r'Valuation\s+on\s+(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4})\s*:?\s*'
    r'NAV\s*:?\s*([\d,]+\.?\d*)\s*.*?'
    r'Value\s*:?\s*Rs\.?\s*([\d,]+\.?\d*)',
    re.IGNORECASE | re.DOTALL
)

# ISIN pattern
ISIN_PATTERN = re.compile(r'ISIN:\s*([A-Z]{2}[A-Z0-9]{10})', re.IGNORECASE)

# Stamp duty pattern - can appear in two formats:
# 1. "*** Stamp Duty ***" on a separate line
# 2. "14-Jan-2025 *** Stamp Duty *** 2.50" (with date and amount)
STAMP_DUTY_PATTERN = re.compile(r'^\*{3}\s*Stamp\s*Duty\s*\*{3}', re.IGNORECASE)
STAMP_DUTY_TXN_PATTERN = re.compile(
    r'^(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4})\s+'  # Date
    r'\*{3}\s*Stamp\s*Duty\s*\*{3}\s*'         # *** Stamp Duty ***
    r'([\d,]+\.?\d*)?',                        # Optional amount
    re.IGNORECASE
)

# STT pattern (appears as "*** STT Paid ***")
STT_PATTERN = re.compile(r'^\*{3}\s*STT\s*(?:Paid)?\s*\*{3}', re.IGNORECASE)
STT_TXN_PATTERN = re.compile(
    r'^(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4})\s+'  # Date
    r'\*{3}\s*STT\s*(?:Paid)?\s*\*{3}\s*'     # *** STT ***
    r'([\d,]+\.?\d*)?',                        # Optional amount
    re.IGNORECASE
)

# Registrar (RTA) pattern
RTA_PATTERN = re.compile(
    r'Registrar\s*:?\s*(CAMS|KFintech|Karvy)',
    re.IGNORECASE
)


class CASPDFParser:
    """
    Comprehensive CAS PDF parser.

    Supports CAMS and KFintech CAS formats with:
    - PyMuPDF primary extraction (fast, accurate)
    - pdfplumber fallback
    - Statement period parsing
    - Investor info extraction
    - Full transaction history
    - Balance reconciliation
    - Folio consolidation (optional)
    - Scheme name cleaning (optional)
    """

    def __init__(
        self,
        consolidate_folios: bool = True,
        clean_scheme_names: bool = True,
        parse_stamp_duty: bool = True,
        parse_valuation: bool = True,
        balance_tolerance: float = 0.01
    ):
        """
        Initialize parser with configuration options.

        Args:
            consolidate_folios: Merge schemes under same folio number (default: True)
            clean_scheme_names: Remove prefix codes like "B92Z-" (default: True)
            parse_stamp_duty: Parse stamp duty as separate transactions (default: True)
            parse_valuation: Parse NAV, Cost, Value from valuation lines (default: True)
            balance_tolerance: Tolerance for balance mismatch detection (default: 0.01)
        """
        if not HAS_PYMUPDF and not HAS_PDFPLUMBER:
            raise ImportError(
                "Either PyMuPDF (fitz) or pdfplumber is required. "
                "Install with: pip install PyMuPDF or pip install pdfplumber"
            )

        self.consolidate_folios_enabled = consolidate_folios
        self.clean_scheme_names_enabled = clean_scheme_names
        self.parse_stamp_duty_enabled = parse_stamp_duty
        self.parse_valuation_enabled = parse_valuation
        self.balance_tolerance = balance_tolerance

        # Track consolidation for reporting
        self.consolidation_result: Optional[ConsolidationResult] = None

    def parse(
        self,
        pdf_path: Path,
        password: Optional[str] = None
    ) -> CASData:
        """
        Parse a CAS PDF file.

        Args:
            pdf_path: Path to CAS PDF file
            password: PDF password (if encrypted)

        Returns:
            CASData with all parsed information

        Raises:
            CASParseError: If parsing fails
            IncorrectPasswordError: If password is wrong
            UnsupportedFormatError: If format is not supported
        """
        pdf_path = Path(pdf_path)

        if not pdf_path.exists():
            raise CASParseError(f"File not found: {pdf_path}")

        # Extract text from PDF
        text = self._extract_text(pdf_path, password)

        if not text or len(text) < 100:
            raise CASParseError(
                f"Could not extract text from PDF: {pdf_path}",
                details={"file": str(pdf_path)}
            )

        # Detect CAS type (DETAILED vs SUMMARY)
        cas_type = self._detect_cas_type(text)

        # Detect CAS source (CAMS, KFintech, NSDL)
        cas_source = self._detect_cas_source(text)

        # Parse header (period and investor info)
        statement_period = self._parse_statement_period(text)
        investor_info = self._parse_investor_info(text)

        # Parse folios and transactions
        if cas_type == CASFileType.DETAILED:
            folios = self._parse_detailed_cas(text)
        else:
            folios = self._parse_summary_cas(text)

        # Apply consolidation if enabled
        if self.consolidate_folios_enabled:
            folios, self.consolidation_result = self._consolidate_folios(folios)
        else:
            self.consolidation_result = ConsolidationResult(
                original_folio_count=len(folios),
                consolidated_folio_count=len(folios)
            )

        # Clean scheme names if enabled
        if self.clean_scheme_names_enabled:
            self._clean_all_scheme_names(folios)

        return CASData(
            statement_period=statement_period,
            investor_info=investor_info,
            folios=folios,
            cas_type=cas_type,
            cas_source=cas_source
        )

    def _extract_text(
        self,
        pdf_path: Path,
        password: Optional[str] = None
    ) -> str:
        """
        Extract text from PDF using PyMuPDF or pdfplumber.

        Args:
            pdf_path: Path to PDF
            password: Password for encrypted PDF

        Returns:
            Extracted text content
        """
        if HAS_PYMUPDF:
            return self._extract_with_pymupdf(pdf_path, password)
        else:
            return self._extract_with_pdfplumber(pdf_path, password)

    def _extract_with_pymupdf(
        self,
        pdf_path: Path,
        password: Optional[str] = None
    ) -> str:
        """Extract text using PyMuPDF (fitz)."""
        try:
            doc = fitz.open(str(pdf_path))

            # Handle password-protected PDF
            if doc.is_encrypted:
                if not password:
                    doc.close()
                    raise IncorrectPasswordError(str(pdf_path))

                if not doc.authenticate(password):
                    doc.close()
                    raise IncorrectPasswordError(str(pdf_path))

            # Extract text from all pages
            text_parts = []
            for page_num in range(len(doc)):
                page = doc[page_num]
                text_parts.append(page.get_text())

            doc.close()
            return "\n".join(text_parts)

        except fitz.FileDataError as e:
            raise CASParseError(
                f"Invalid PDF file: {pdf_path}",
                details={"error": str(e)}
            )
        except Exception as e:
            if "password" in str(e).lower():
                raise IncorrectPasswordError(str(pdf_path))
            raise CASParseError(
                f"Failed to read PDF: {pdf_path}",
                details={"error": str(e)}
            )

    def _extract_with_pdfplumber(
        self,
        pdf_path: Path,
        password: Optional[str] = None
    ) -> str:
        """Extract text using pdfplumber as fallback."""
        try:
            with pdfplumber.open(str(pdf_path), password=password or "") as pdf:
                text_parts = []
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                return "\n".join(text_parts)

        except Exception as e:
            error_msg = str(e).lower()
            if "password" in error_msg or "encrypted" in error_msg:
                raise IncorrectPasswordError(str(pdf_path))
            raise CASParseError(
                f"Failed to read PDF: {pdf_path}",
                details={"error": str(e)}
            )

    def _detect_cas_type(self, text: str) -> CASFileType:
        """Detect if CAS is DETAILED (with transactions) or SUMMARY."""
        # Primary check: If we find actual transaction lines, it's DETAILED
        # Transaction pattern match is the most reliable indicator
        if TXN_PATTERN.search(text) or TXN_PATTERN_ALT.search(text):
            return CASFileType.DETAILED

        # Secondary check: Look for transaction-related keywords
        detailed_keywords = [
            "Opening Unit Balance",
            "Closing Unit Balance",
            "Transaction Date",
        ]

        for kw in detailed_keywords:
            if kw.lower() in text.lower():
                return CASFileType.DETAILED

        # If no transaction indicators found, it's a SUMMARY CAS
        return CASFileType.SUMMARY

    def _detect_cas_source(self, text: str) -> CASSource:
        """Detect CAS source (CAMS, KFintech, NSDL)."""
        text_lower = text.lower()

        if "cams" in text_lower or "computer age management" in text_lower:
            return CASSource.CAMS
        elif "kfintech" in text_lower or "karvy" in text_lower:
            return CASSource.KFINTECH
        elif "nsdl" in text_lower or "national securities" in text_lower:
            return CASSource.NSDL
        elif "cdsl" in text_lower or "central depository" in text_lower:
            return CASSource.CDSL

        return CASSource.UNKNOWN

    def _parse_statement_period(self, text: str) -> StatementPeriod:
        """Extract statement period from header."""
        # Try patterns in order of specificity
        match = PERIOD_PATTERN_PREFIX.search(text)  # "Statement period: ..."
        if not match:
            match = PERIOD_PATTERN.search(text)      # "DD-Mon-YYYY To DD-Mon-YYYY"
        if not match:
            match = PERIOD_PATTERN_ALT.search(text)  # "DD-MM-YYYY to DD-MM-YYYY"

        if not match:
            raise HeaderParseError(
                "Could not extract statement period from CAS",
                details={"hint": "Check if PDF is a valid CAS statement"}
            )

        from_str = match.group(1)
        to_str = match.group(2)

        from_date = self._parse_date(from_str)
        to_date = self._parse_date(to_str)

        if not from_date or not to_date:
            raise HeaderParseError(
                f"Invalid date format in period: {from_str} to {to_str}"
            )

        return StatementPeriod(from_date=from_date, to_date=to_date)

    def _parse_investor_info(self, text: str) -> InvestorInfo:
        """Extract investor information from header."""
        # Extract email first (reliable anchor point)
        email = ""
        email_match = EMAIL_PATTERN.search(text)
        if email_match:
            email = email_match.group(1)

        # Extract mobile
        mobile = ""
        mobile_match = MOBILE_PATTERN.search(text)
        if mobile_match:
            mobile = mobile_match.group(2)

        # Extract PAN
        pan = ""
        pan_match = PAN_PATTERN.search(text)
        if pan_match:
            pan = pan_match.group(1)

        # Extract name - try multiple patterns
        name = ""

        # Pattern 1: "Dear Name" or "Name:"
        name_match = INVESTOR_PATTERN.search(text)
        if name_match:
            name = name_match.group(1).strip()

        # Pattern 2: Line after email (common in CAMS CAS)
        if not name and email:
            # Find line after email line that looks like a name
            lines = text.split('\n')
            found_email = False
            for line in lines:
                if found_email:
                    line = line.strip()
                    # Check if line looks like a name (words starting with caps)
                    if re.match(r'^[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*$', line):
                        name = line
                        break
                if email.lower() in line.lower():
                    found_email = True

        # Pattern 3: Look for name near "C/O:" address
        if not name:
            co_match = re.search(
                r'^([A-Z][a-zA-Z\s]+)\s*\n.*?C/O',
                text, re.MULTILINE
            )
            if co_match:
                name = co_match.group(1).strip()

        return InvestorInfo(
            name=name or "Unknown",
            email=email,
            mobile=mobile,
            pan=pan
        )

    def _parse_detailed_cas(self, text: str) -> List[CASFolio]:
        """
        Parse DETAILED CAS with full transaction history.

        Returns:
            List of CASFolio with schemes and transactions
        """
        folios = []
        current_folio = None
        current_scheme = None
        current_amc = ""

        lines = text.split('\n')
        i = 0

        while i < len(lines):
            line = lines[i].strip()

            if not line:
                i += 1
                continue

            # Check for AMC header
            amc_match = AMC_PATTERN.match(line)
            if amc_match:
                current_amc = amc_match.group(1).strip()
                i += 1
                continue

            # Check for folio line
            folio_match = FOLIO_PATTERN.search(line)
            if folio_match:
                folio_number = folio_match.group(1)

                # Extract PAN/KYC if on same line
                pan = ""
                pan_match = PAN_PATTERN.search(line)
                if pan_match:
                    pan = pan_match.group(1)

                # Save previous folio
                if current_folio:
                    if current_scheme:
                        self._finalize_scheme(current_scheme)
                        current_folio.schemes.append(current_scheme)
                    folios.append(current_folio)

                current_folio = CASFolio(
                    folio=folio_number,
                    amc=current_amc,
                    pan=pan
                )
                current_scheme = None
                i += 1
                continue

            # Check for scheme header (contains Fund, Growth, etc.)
            if self._is_scheme_line(line):
                # Finalize previous scheme
                if current_scheme and current_folio:
                    self._finalize_scheme(current_scheme)
                    current_folio.schemes.append(current_scheme)

                # Parse scheme info
                scheme_info = self._parse_scheme_line(line, lines, i)
                current_scheme = CASScheme(
                    scheme=scheme_info.get('name', 'Unknown'),
                    isin=scheme_info.get('isin'),
                    rta=scheme_info.get('rta', ''),
                    rta_code=scheme_info.get('rta_code', ''),
                    advisor=scheme_info.get('advisor'),
                )

                # Look for opening balance
                i += 1
                while i < len(lines):
                    next_line = lines[i].strip()
                    open_match = re.search(
                        r'Opening\s+(?:Unit\s+)?Balance\s*:?\s*([\d,]+\.?\d*)',
                        next_line, re.IGNORECASE
                    )
                    if open_match:
                        current_scheme.open = self._parse_decimal(open_match.group(1))
                        i += 1
                        break
                    elif self._is_transaction_line(next_line):
                        # No opening balance line, start processing transactions
                        break
                    i += 1
                continue

            # Check for stamp duty transaction line (date + *** Stamp Duty *** + amount)
            stamp_txn_match = STAMP_DUTY_TXN_PATTERN.match(line)
            if current_scheme and stamp_txn_match:
                txn_date = self._parse_date(stamp_txn_match.group(1))
                amount = self._parse_decimal(stamp_txn_match.group(2)) if stamp_txn_match.group(2) else Decimal("0")
                stamp_txn = CASTransaction(
                    date=txn_date or date.today(),
                    description="*** Stamp Duty ***",
                    amount=amount,
                    units=Decimal("0"),
                    nav=Decimal("0"),
                    balance=None,
                    transaction_type=TransactionType.STAMP_DUTY_TAX
                )
                current_scheme.transactions.append(stamp_txn)
                i += 1
                continue

            # Check for stamp duty line without date (*** Stamp Duty ***)
            if current_scheme and STAMP_DUTY_PATTERN.match(line):
                # Create a stamp duty transaction linked to previous transaction's date
                prev_date = None
                if current_scheme.transactions:
                    prev_date = current_scheme.transactions[-1].date
                stamp_txn = CASTransaction(
                    date=prev_date or date.today(),
                    description="*** Stamp Duty ***",
                    amount=Decimal("0"),
                    units=Decimal("0"),
                    nav=Decimal("0"),
                    balance=None,
                    transaction_type=TransactionType.STAMP_DUTY_TAX
                )
                current_scheme.transactions.append(stamp_txn)
                i += 1
                continue

            # Check for STT transaction line (date + *** STT *** + amount)
            stt_txn_match = STT_TXN_PATTERN.match(line)
            if current_scheme and stt_txn_match:
                txn_date = self._parse_date(stt_txn_match.group(1))
                amount = self._parse_decimal(stt_txn_match.group(2)) if stt_txn_match.group(2) else Decimal("0")
                stt_txn = CASTransaction(
                    date=txn_date or date.today(),
                    description="*** STT Paid ***",
                    amount=amount,
                    units=Decimal("0"),
                    nav=Decimal("0"),
                    balance=None,
                    transaction_type=TransactionType.STT_TAX
                )
                current_scheme.transactions.append(stt_txn)
                i += 1
                continue

            # Check for STT line without date (*** STT Paid ***)
            if current_scheme and STT_PATTERN.match(line):
                prev_date = None
                if current_scheme.transactions:
                    prev_date = current_scheme.transactions[-1].date
                stt_txn = CASTransaction(
                    date=prev_date or date.today(),
                    description="*** STT Paid ***",
                    amount=Decimal("0"),
                    units=Decimal("0"),
                    nav=Decimal("0"),
                    balance=None,
                    transaction_type=TransactionType.STT_TAX
                )
                current_scheme.transactions.append(stt_txn)
                i += 1
                continue

            # Check for transaction line
            if current_scheme and self._is_transaction_line(line):
                txn = self._parse_transaction_line(line)
                if txn:
                    current_scheme.transactions.append(txn)
                i += 1
                continue

            # Check for closing balance
            close_match = re.search(
                r'Closing\s+(?:Unit\s+)?Balance\s*:?\s*([\d,]+\.?\d*)',
                line, re.IGNORECASE
            )
            if close_match and current_scheme:
                current_scheme.close = self._parse_decimal(close_match.group(1))
                i += 1
                continue

            # Check for valuation line
            val_match = VALUATION_PATTERN.search(line)
            if val_match and current_scheme:
                val_date = self._parse_date(val_match.group(1))
                val_nav = self._parse_decimal(val_match.group(2))
                val_value = self._parse_decimal(val_match.group(3))

                if val_date:
                    current_scheme.valuation = SchemeValuation(
                        date=val_date,
                        nav=val_nav,
                        value=val_value
                    )
                i += 1
                continue

            i += 1

        # Don't forget last folio/scheme
        if current_scheme and current_folio:
            self._finalize_scheme(current_scheme)
            current_folio.schemes.append(current_scheme)

        if current_folio:
            folios.append(current_folio)

        return folios

    def _parse_summary_cas(self, text: str) -> List[CASFolio]:
        """
        Parse SUMMARY CAS (holdings only, no transactions).

        Returns:
            List of CASFolio with schemes but no transactions
        """
        folios = []
        current_folio = None
        current_amc = ""

        lines = text.split('\n')

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check for AMC
            amc_match = AMC_PATTERN.match(line)
            if amc_match:
                current_amc = amc_match.group(1).strip()
                continue

            # Check for folio
            folio_match = FOLIO_PATTERN.search(line)
            if folio_match:
                if current_folio:
                    folios.append(current_folio)

                current_folio = CASFolio(
                    folio=folio_match.group(1),
                    amc=current_amc
                )
                continue

            # Check for scheme with holdings
            if current_folio and self._is_scheme_line(line):
                scheme_info = self._parse_scheme_line(line, [], 0)

                # Try to extract units and value from same or next lines
                units_match = re.search(r'Units?\s*:?\s*([\d,]+\.?\d*)', line)
                value_match = re.search(r'Value\s*:?\s*Rs\.?\s*([\d,]+\.?\d*)', line)

                scheme = CASScheme(
                    scheme=scheme_info.get('name', 'Unknown'),
                    isin=scheme_info.get('isin'),
                    close=self._parse_decimal(units_match.group(1)) if units_match else Decimal("0"),
                )

                if value_match:
                    scheme.valuation = SchemeValuation(
                        date=date.today(),
                        nav=Decimal("0"),
                        value=self._parse_decimal(value_match.group(1))
                    )

                current_folio.schemes.append(scheme)

        if current_folio:
            folios.append(current_folio)

        return folios

    def _is_scheme_line(self, line: str) -> bool:
        """
        Check if line contains a mutual fund scheme header.

        In CAMS/KFintech CAS PDFs, scheme headers have very specific patterns:
        1. Start with RTA scheme code (e.g., B92Z-, RMFLFAGG-, HSTOGT-, etc.)
        2. Or contain ISIN information

        This function is intentionally strict to avoid matching:
        - Transaction description continuations
        - Balance lines
        - Fund type labels without scheme codes
        """
        line_stripped = line.strip()

        # Skip empty or very short lines
        if len(line_stripped) < 15:
            return False

        # Skip if line starts with a date (transaction line)
        if re.match(r'^\d{1,2}[-/][A-Za-z]{3}[-/]\d{4}', line_stripped):
            return False

        # Skip if line is just AMC name with numbers (portfolio summary format)
        if re.match(r'^[A-Za-z\s]+(?:Mutual\s+Fund|MF)\s+[\d,\.]+\s+[\d,\.]+$', line_stripped, re.IGNORECASE):
            return False

        # Skip lines that are just AMC names
        if re.match(r'^[A-Za-z\s]+(?:Mutual\s+Fund|Asset\s+Management)$', line_stripped, re.IGNORECASE):
            return False

        # Skip balance lines
        if re.match(r'^(?:Opening|Closing)\s+(?:Unit\s+)?Balance', line_stripped, re.IGNORECASE):
            return False

        # Skip valuation lines
        if re.match(r'^Valuation\s+on', line_stripped, re.IGNORECASE):
            return False

        # Skip lines that are just numbers (continuation of previous line)
        if re.match(r'^[\d,\.\s\-]+$', line_stripped):
            return False

        # Skip lines that start with common non-scheme prefixes
        if re.match(r'^(?:PAN|KYC|PANKYC|Registrar|Advisor|Folio|F\.No|Email|Mobile)[\s:]+', line_stripped, re.IGNORECASE):
            return False

        # Skip stamp duty and STT lines
        if re.match(r'^\*{3}\s*(?:Stamp\s*Duty|STT)', line_stripped, re.IGNORECASE):
            return False

        # ONLY match lines with STRONG scheme indicators
        # These are the definitive markers of a scheme header line

        # 1. Scheme code prefix pattern (most reliable)
        # Matches: B92Z-Aditya, RMFLFAGG-NIPPON, HSTOGT-HDFC, PP001ZG-Parag, etc.
        # Pattern: 3-10 alphanumeric chars starting with letter, followed by hyphen and letter
        if re.match(r'^[A-Z][A-Z0-9]{2,9}-[A-Za-z]', line_stripped):
            return True

        # 2. ISIN line pattern
        # Matches: ISIN: INF209K01YY7(Advisor: DIRECT)
        if re.search(r'ISIN:\s*[A-Z]{2}[A-Z0-9]{10}', line_stripped, re.IGNORECASE):
            return True

        # 3. Scheme with Advisor notation
        # Matches: (Advisor: DIRECT), (Advisor: IFA Name)
        if re.search(r'\(Advisor:\s*(?:DIRECT|[A-Z][A-Za-z\s]+)\)', line_stripped, re.IGNORECASE):
            # But only if it also looks like a scheme name (has Fund or Plan in it)
            if re.search(r'(?:Fund|Plan|Growth|IDCW)', line_stripped, re.IGNORECASE):
                return True

        # Do NOT use moderate patterns - they cause too many false positives
        # with transaction description lines and continuation lines
        return False

    def _is_transaction_line(self, line: str) -> bool:
        """Check if line is a transaction line (starts with date)."""
        return bool(re.match(r'^\d{1,2}[-/][A-Za-z]{3}[-/]\d{4}', line))

    def _parse_scheme_line(
        self,
        line: str,
        all_lines: List[str],
        current_idx: int
    ) -> Dict[str, Any]:
        """Parse scheme name and metadata from line."""
        result = {'name': '', 'isin': None, 'rta': '', 'rta_code': '', 'advisor': None}

        # Extract ISIN
        isin_match = ISIN_PATTERN.search(line)
        if isin_match:
            result['isin'] = isin_match.group(1)
            line = ISIN_PATTERN.sub('', line)

        # Extract RTA
        rta_match = RTA_PATTERN.search(line)
        if rta_match:
            result['rta'] = rta_match.group(1).upper()
            line = RTA_PATTERN.sub('', line)

        # Extract advisor
        advisor_match = re.search(r'Advisor\s*:?\s*([A-Za-z\s]+)', line, re.IGNORECASE)
        if advisor_match:
            result['advisor'] = advisor_match.group(1).strip()
            line = re.sub(r'Advisor\s*:?\s*[A-Za-z\s]+', '', line, flags=re.IGNORECASE)

        # Clean up scheme name
        name = re.sub(r'Registrar\s*:?\s*\w+', '', line, flags=re.IGNORECASE)
        name = re.sub(r'Folio.*', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s+', ' ', name).strip()

        result['name'] = name

        return result

    def _parse_transaction_line(self, line: str) -> Optional[CASTransaction]:
        """Parse a single transaction line."""
        # Pattern groups: Date, Description, Amount, Units, NAV, Balance
        # Try primary pattern first (strict 6 groups with balance)
        match = TXN_PATTERN.match(line)
        if match:
            return self._create_transaction(
                date_str=match.group(1),
                description=match.group(2).strip(),
                amount=match.group(3),
                units=match.group(4),
                nav=match.group(5),
                balance=match.group(6)
            )

        # Try flexible pattern (handles varied spacing, optional balance)
        match = TXN_PATTERN_FLEX.match(line)
        if match:
            return self._create_transaction(
                date_str=match.group(1),
                description=match.group(2).strip(),
                amount=match.group(3),
                units=match.group(4),
                nav=match.group(5),
                balance=match.group(6) if match.lastindex >= 6 else None
            )

        # Try alternate pattern (without balance)
        match = TXN_PATTERN_ALT.match(line)
        if match:
            return self._create_transaction(
                date_str=match.group(1),
                description=match.group(2).strip(),
                amount=match.group(3),
                units=match.group(4),
                nav=match.group(5),
                balance=None
            )

        return None

    def _create_transaction(
        self,
        date_str: str,
        description: str,
        amount: str,
        nav: str,
        units: str,
        balance: Optional[str]
    ) -> Optional[CASTransaction]:
        """Create CASTransaction from parsed values."""
        txn_date = self._parse_date(date_str)
        if not txn_date:
            return None

        units_decimal = self._parse_decimal(units)

        # Determine transaction type
        txn_type = TransactionType.from_description(description, units_decimal)

        return CASTransaction(
            date=txn_date,
            description=description,
            amount=self._parse_decimal(amount),
            units=units_decimal,
            nav=self._parse_decimal(nav),
            balance=self._parse_decimal(balance) if balance else None,
            transaction_type=txn_type
        )

    def _finalize_scheme(self, scheme: CASScheme):
        """Calculate close_calculated from transactions."""
        calculated = scheme.open
        for txn in scheme.transactions:
            if txn.units:
                calculated += txn.units
        scheme.close_calculated = calculated

    def _consolidate_folios(
        self,
        folios: List[CASFolio]
    ) -> Tuple[List[CASFolio], ConsolidationResult]:
        """
        Consolidate folios with the same folio number.

        When the same folio appears multiple times in the CAS (e.g., different
        AMC sections with same folio number), merge all schemes under one folio.

        Args:
            folios: List of parsed folios (may have duplicates)

        Returns:
            Tuple of (consolidated folios, consolidation result)
        """
        original_count = len(folios)
        folio_map: Dict[str, CASFolio] = {}
        consolidation_entries = []

        for folio in folios:
            folio_key = folio.folio

            if folio_key in folio_map:
                # Merge schemes into existing folio
                existing = folio_map[folio_key]

                # Track schemes being merged
                schemes_merged = [s.scheme for s in folio.schemes]

                # Add schemes to existing folio
                existing.schemes.extend(folio.schemes)

                # Track this consolidation
                # Find existing entry or create new
                entry_found = False
                for entry in consolidation_entries:
                    if entry.folio == folio_key:
                        entry.merged_count += 1
                        entry.schemes_merged.extend(schemes_merged)
                        entry_found = True
                        break

                if not entry_found:
                    consolidation_entries.append(FolioConsolidationEntry(
                        folio=folio_key,
                        amc=folio.amc,
                        original_count=2,  # First occurrence + this one
                        merged_count=1,
                        schemes_merged=schemes_merged
                    ))

                logger.debug(f"Merged folio {folio_key}: {len(folio.schemes)} schemes added")
            else:
                # First occurrence of this folio
                folio_map[folio_key] = folio

        consolidated_folios = list(folio_map.values())
        consolidated_count = len(consolidated_folios)

        result = ConsolidationResult(
            original_folio_count=original_count,
            consolidated_folio_count=consolidated_count,
            consolidation_entries=consolidation_entries
        )

        if original_count != consolidated_count:
            logger.info(
                f"Folio consolidation: {original_count} -> {consolidated_count} "
                f"({original_count - consolidated_count} duplicates merged)"
            )

        return consolidated_folios, result

    def _clean_scheme_name(self, name: str) -> str:
        """
        Clean scheme name by removing prefix codes.

        Examples:
            "B92Z-Aditya Birla Sun Life Large Cap Fund" -> "Aditya Birla Sun Life Large Cap Fund"
            "D860-HDFC Nifty 50 Index Fund" -> "HDFC Nifty 50 Index Fund"

        Args:
            name: Original scheme name

        Returns:
            Cleaned scheme name
        """
        # Remove prefix codes like "B92Z-", "D860-", "P8017-"
        cleaned = SCHEME_PREFIX_PATTERN.sub('', name)

        # Remove leading/trailing whitespace
        cleaned = cleaned.strip()

        return cleaned

    def _clean_all_scheme_names(self, folios: List[CASFolio]):
        """
        Clean all scheme names in the folio list.

        Updates schemes in-place and tracks renames in consolidation_result.

        Args:
            folios: List of folios with schemes to clean
        """
        if not self.consolidation_result:
            self.consolidation_result = ConsolidationResult(
                original_folio_count=len(folios),
                consolidated_folio_count=len(folios)
            )

        for folio in folios:
            for scheme in folio.schemes:
                original_name = scheme.scheme
                cleaned_name = self._clean_scheme_name(original_name)

                if cleaned_name != original_name:
                    scheme.scheme = cleaned_name
                    self.consolidation_result.schemes_renamed[original_name] = cleaned_name

    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string to date object."""
        formats = [
            '%d-%b-%Y',   # 31-Jan-2024
            '%d/%b/%Y',   # 31/Jan/2024
            '%d-%m-%Y',   # 31-01-2024
            '%d/%m/%Y',   # 31/01/2024
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        return None

    def _parse_decimal(self, value: str) -> Decimal:
        """Parse string to Decimal, handling commas."""
        if not value:
            return Decimal("0")

        try:
            # Remove commas and parentheses (for negative)
            clean = value.replace(',', '').replace('(', '-').replace(')', '')
            return Decimal(clean)
        except (InvalidOperation, ValueError):
            return Decimal("0")


def parse_cas_pdf(
    pdf_path: Path,
    password: Optional[str] = None,
    consolidate_folios: bool = True,
    clean_scheme_names: bool = True,
    parse_stamp_duty: bool = True,
    parse_valuation: bool = True
) -> Tuple[CASData, Optional[ConsolidationResult]]:
    """
    Convenience function to parse a CAS PDF.

    Args:
        pdf_path: Path to CAS PDF file
        password: PDF password (if encrypted)
        consolidate_folios: Merge schemes under same folio number (default: True)
        clean_scheme_names: Remove prefix codes like "B92Z-" (default: True)
        parse_stamp_duty: Parse stamp duty as separate transactions (default: True)
        parse_valuation: Parse NAV, Cost, Value from valuation lines (default: True)

    Returns:
        Tuple of (CASData with all parsed information, ConsolidationResult if consolidation was performed)
    """
    parser = CASPDFParser(
        consolidate_folios=consolidate_folios,
        clean_scheme_names=clean_scheme_names,
        parse_stamp_duty=parse_stamp_duty,
        parse_valuation=parse_valuation
    )
    cas_data = parser.parse(pdf_path, password)
    return cas_data, parser.consolidation_result


def check_cas_support() -> Dict[str, bool]:
    """Check which PDF libraries are available."""
    return {
        "pymupdf": HAS_PYMUPDF,
        "pdfplumber": HAS_PDFPLUMBER,
        "any": HAS_PYMUPDF or HAS_PDFPLUMBER
    }
