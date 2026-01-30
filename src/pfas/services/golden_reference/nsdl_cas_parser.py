"""
NSDL CAS Parser for Golden Reference.

Parses NSDL Consolidated Account Statements (CAS) from password-protected PDFs.
Extracts holdings for Stocks, Mutual Funds, and NPS.
"""

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

from .models import (
    AssetClass,
    SourceType,
    GoldenReference,
    GoldenHolding,
    GoldenCapitalGains,
)

logger = logging.getLogger(__name__)


class NSDLCASParseError(Exception):
    """Base exception for NSDL CAS parsing errors."""
    pass


class PasswordRequiredError(NSDLCASParseError):
    """Raised when password is required but not provided."""
    pass


class InvalidPasswordError(NSDLCASParseError):
    """Raised when provided password is incorrect."""
    pass


class UnsupportedFormatError(NSDLCASParseError):
    """Raised when CAS format is not recognized."""
    pass


@dataclass
class NSDLInvestorInfo:
    """Investor information from NSDL CAS."""
    name: str = ""
    pan: str = ""
    email: str = ""
    mobile: str = ""
    address: str = ""
    dp_id: str = ""
    client_id: str = ""


@dataclass
class NSDLHolding:
    """Single holding from NSDL CAS."""
    isin: str
    name: str
    asset_type: str  # EQUITY, MF, NPS, BOND, etc.
    quantity: Decimal = Decimal("0")
    face_value: Optional[Decimal] = None
    market_price: Optional[Decimal] = None
    market_value: Decimal = Decimal("0")
    cost_value: Optional[Decimal] = None
    pledge_qty: Optional[Decimal] = None
    dp_id: str = ""
    client_id: str = ""
    folio_number: Optional[str] = None


@dataclass
class NSDLCASData:
    """Parsed NSDL CAS data."""
    statement_date: date = field(default_factory=date.today)
    period_start: Optional[date] = None
    period_end: Optional[date] = None
    investor_info: NSDLInvestorInfo = field(default_factory=NSDLInvestorInfo)
    equity_holdings: List[NSDLHolding] = field(default_factory=list)
    mf_holdings: List[NSDLHolding] = field(default_factory=list)
    nps_holdings: List[NSDLHolding] = field(default_factory=list)
    bond_holdings: List[NSDLHolding] = field(default_factory=list)
    sgb_holdings: List[NSDLHolding] = field(default_factory=list)
    raw_text: str = ""

    @property
    def all_holdings(self) -> List[NSDLHolding]:
        """Get all holdings combined."""
        return (
            self.equity_holdings +
            self.mf_holdings +
            self.nps_holdings +
            self.bond_holdings +
            self.sgb_holdings
        )

    @property
    def total_value(self) -> Decimal:
        """Calculate total portfolio value."""
        return sum(h.market_value for h in self.all_holdings)


class NSDLCASParser:
    """
    Parser for NSDL Consolidated Account Statements.

    Handles password-protected PDFs using PyMuPDF (fitz) or pdfplumber.
    Extracts holdings for:
    - Equity stocks
    - Mutual funds
    - NPS
    - Bonds
    - Sovereign Gold Bonds

    Usage:
        parser = NSDLCASParser()

        # Parse with explicit password
        data = parser.parse("/path/to/nsdl_cas.pdf", password="secret")

        # Parse using PathResolver for password
        from pfas.core.paths import PathResolver
        resolver = PathResolver(root, "Sanjay")
        password = resolver.get_file_password(Path("/path/to/nsdl_cas.pdf"))
        data = parser.parse("/path/to/nsdl_cas.pdf", password=password)
    """

    # Regex patterns for NSDL CAS
    DATE_PATTERNS = [
        r'(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4})',  # 15-Mar-2024
        r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})',       # 15/03/2024
        r'(\d{4}[-/]\d{2}[-/]\d{2})',           # 2024-03-15
    ]

    PERIOD_PATTERN = re.compile(
        r'(?:Statement\s+Period|Period)\s*[:\-]?\s*'
        r'(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4})\s+'
        r'(?:To|to|-)\s+'
        r'(\d{1,2}[-/][A-Za-z]{3}[-/]\d{4})',
        re.IGNORECASE
    )

    PAN_PATTERN = re.compile(r'[A-Z]{5}[0-9]{4}[A-Z]')
    ISIN_PATTERN = re.compile(r'IN[A-Z0-9]{10}')

    # Asset type detection patterns
    EQUITY_SECTION_PATTERN = re.compile(r'(?:EQUITY|Equity\s+Holdings?)', re.IGNORECASE)
    # NSDL CAS uses "MUTUAL FUND FOLIOS" as section header
    MF_SECTION_PATTERN = re.compile(
        r'(?:MUTUAL\s+FUND\s+FOLIOS?|MUTUAL\s+FUND|MF\s+Holdings?)',
        re.IGNORECASE
    )
    # MF holdings table header pattern
    MF_TABLE_HEADER_PATTERN = re.compile(
        r'ISIN\s+ISIN\s+Description\s+Folio\s+No',
        re.IGNORECASE
    )
    NPS_SECTION_PATTERN = re.compile(r'(?:NPS|National\s+Pension)', re.IGNORECASE)
    BOND_SECTION_PATTERN = re.compile(r'(?:BONDS?|Government\s+Securities)', re.IGNORECASE)
    SGB_SECTION_PATTERN = re.compile(r'(?:SGB|Sovereign\s+Gold\s+Bond)', re.IGNORECASE)

    def __init__(self):
        """Initialize NSDL CAS parser."""
        self._fitz_available = self._check_fitz()
        self._pdfplumber_available = self._check_pdfplumber()

        if not self._fitz_available and not self._pdfplumber_available:
            logger.warning("Neither PyMuPDF nor pdfplumber available. PDF parsing limited.")

    def _check_fitz(self) -> bool:
        """Check if PyMuPDF is available."""
        try:
            import fitz
            return True
        except ImportError:
            return False

    def _check_pdfplumber(self) -> bool:
        """Check if pdfplumber is available."""
        try:
            import pdfplumber
            return True
        except ImportError:
            return False

    def parse(
        self,
        file_path: Path | str,
        password: Optional[str] = None
    ) -> NSDLCASData:
        """
        Parse NSDL CAS PDF file.

        Args:
            file_path: Path to PDF file
            password: Password for encrypted PDF

        Returns:
            NSDLCASData with parsed holdings

        Raises:
            PasswordRequiredError: If password is needed
            InvalidPasswordError: If password is wrong
            NSDLCASParseError: For other parsing errors
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise NSDLCASParseError(f"File not found: {file_path}")

        # Extract text from PDF
        text = self._extract_text(file_path, password)

        # Verify this is NSDL CAS
        if not self._is_nsdl_cas(text):
            raise UnsupportedFormatError("File does not appear to be NSDL CAS")

        # Parse the content
        data = NSDLCASData(raw_text=text)

        # Extract statement period
        period = self._extract_period(text)
        if period:
            data.period_start, data.period_end = period
            data.statement_date = data.period_end

        # Extract investor info
        data.investor_info = self._extract_investor_info(text)

        # Extract holdings by section
        data.equity_holdings = self._extract_equity_holdings(text)
        data.mf_holdings = self._extract_mf_holdings(text)
        data.nps_holdings = self._extract_nps_holdings(text)
        data.bond_holdings = self._extract_bond_holdings(text)
        data.sgb_holdings = self._extract_sgb_holdings(text)

        logger.info(
            f"Parsed NSDL CAS: {len(data.equity_holdings)} equity, "
            f"{len(data.mf_holdings)} MF, {len(data.nps_holdings)} NPS, "
            f"Total value: {data.total_value:,.2f}"
        )

        return data

    def _extract_text(self, file_path: Path, password: Optional[str]) -> str:
        """Extract text from PDF using available library."""
        if self._fitz_available:
            return self._extract_with_fitz(file_path, password)
        elif self._pdfplumber_available:
            return self._extract_with_pdfplumber(file_path, password)
        else:
            raise NSDLCASParseError("No PDF library available")

    def _extract_with_fitz(self, file_path: Path, password: Optional[str]) -> str:
        """Extract text using PyMuPDF (fitz)."""
        import fitz

        doc = fitz.open(str(file_path))

        if doc.is_encrypted:
            if not password:
                doc.close()
                raise PasswordRequiredError(str(file_path))

            if not doc.authenticate(password):
                doc.close()
                raise InvalidPasswordError(str(file_path))

        text_parts = []
        for page in doc:
            text_parts.append(page.get_text())

        doc.close()
        return "\n".join(text_parts)

    def _extract_with_pdfplumber(self, file_path: Path, password: Optional[str]) -> str:
        """Extract text using pdfplumber."""
        import pdfplumber

        try:
            pdf = pdfplumber.open(str(file_path), password=password)
        except Exception as e:
            if "password" in str(e).lower():
                if password:
                    raise InvalidPasswordError(str(file_path))
                raise PasswordRequiredError(str(file_path))
            raise NSDLCASParseError(f"Failed to open PDF: {e}")

        text_parts = []
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                text_parts.append(text)

        pdf.close()
        raw_text = "\n".join(text_parts)

        # NSDL CAS PDFs often have duplicate characters (e.g., "NNaattiioonnaall")
        # Detect and fix this issue
        if self._has_duplicate_chars(raw_text):
            raw_text = self._fix_duplicate_chars(raw_text)

        return raw_text

    def _has_duplicate_chars(self, text: str) -> bool:
        """Check if text has duplicate character issue (NNaattiioonnaall -> National)."""
        # Check for patterns like "NNaatt" which would indicate duplication
        sample = text[:500] if len(text) > 500 else text
        duplicate_patterns = ["NNaa", "SSee", "DDee", "CCoo", "AAcc"]
        return any(pattern in sample for pattern in duplicate_patterns)

    def _fix_duplicate_chars(self, text: str) -> str:
        """Fix duplicate characters in text."""
        result = []
        i = 0
        while i < len(text):
            if i + 1 < len(text) and text[i] == text[i + 1] and text[i].isalpha():
                result.append(text[i])
                i += 2
            else:
                result.append(text[i])
                i += 1
        return ''.join(result)

    def _is_nsdl_cas(self, text: str) -> bool:
        """Check if text is from NSDL CAS."""
        text_lower = text.lower()
        indicators = [
            "nsdl",
            "national securities depository",
            "consolidated account statement",
            "depository participant",
        ]
        return any(ind in text_lower for ind in indicators)

    def _extract_period(self, text: str) -> Optional[Tuple[date, date]]:
        """Extract statement period from text."""
        match = self.PERIOD_PATTERN.search(text)
        if match:
            try:
                start = self._parse_date(match.group(1))
                end = self._parse_date(match.group(2))
                return (start, end)
            except ValueError:
                pass
        return None

    def _parse_date(self, date_str: str) -> date:
        """Parse date string to date object."""
        formats = [
            "%d-%b-%Y", "%d/%b/%Y",
            "%d-%m-%Y", "%d/%m/%Y",
            "%Y-%m-%d", "%Y/%m/%d",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        raise ValueError(f"Cannot parse date: {date_str}")

    def _extract_investor_info(self, text: str) -> NSDLInvestorInfo:
        """Extract investor information."""
        info = NSDLInvestorInfo()

        # Extract PAN
        pan_match = self.PAN_PATTERN.search(text)
        if pan_match:
            info.pan = pan_match.group()

        # Extract name (usually near PAN or at top)
        name_patterns = [
            r'Name\s*[:\-]?\s*([A-Z][A-Z\s]+)',
            r'Account\s+Holder\s*[:\-]?\s*([A-Z][A-Z\s]+)',
        ]
        for pattern in name_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                info.name = match.group(1).strip()
                break

        # Extract DP ID and Client ID
        dp_match = re.search(r'DP\s*ID\s*[:\-]?\s*(\w+)', text, re.IGNORECASE)
        if dp_match:
            info.dp_id = dp_match.group(1)

        client_match = re.search(r'Client\s*ID\s*[:\-]?\s*(\d+)', text, re.IGNORECASE)
        if client_match:
            info.client_id = client_match.group(1)

        return info

    def _extract_equity_holdings(self, text: str) -> List[NSDLHolding]:
        """
        Extract equity holdings from text.

        NSDL CAS has multiple equity sections:
        1. NSDL Demat Account - format: ISIN | Company | Face Value | Shares | Market Price | Value
        2. CDSL Demat Account - format: ISIN | Security | Current Bal | Safekeep | Pledged | Price | Value
        """
        holdings = []

        # Find ALL equity sections (there may be multiple demat accounts)
        equity_sections = list(re.finditer(
            r'Equities\s*\(E\)\s*\n'
            r'(?:Equity\s+Shares|ISIN)',
            text,
            re.IGNORECASE
        ))

        if not equity_sections:
            # Try fallback pattern
            equity_match = self.EQUITY_SECTION_PATTERN.search(text)
            if equity_match:
                equity_sections = [equity_match]

        for section_match in equity_sections:
            start_pos = section_match.end()

            # Find end of this equity section
            end_patterns = [
                r'\bSovereign\s+Gold\s+Bonds\b',
                r'\bPreference\s+Shares\b',
                r'\bMutual\s+Fund\b',
                r'\bNPS\b',
                r'\bBonds?\b',
                r'\bETF\b',
                r'\bSummary\s+of\s+value\b',
                r'\bPORTFOLIO\s+COMPOSITION\b',
            ]
            end_pos = len(text)

            for pattern in end_patterns:
                match = re.search(pattern, text[start_pos:], re.IGNORECASE)
                if match and start_pos + match.start() < end_pos:
                    end_pos = start_pos + match.start()

            section_text = text[start_pos:end_pos]

            # Parse equity rows - looking for ISIN (INE...) + details
            for line in section_text.split('\n'):
                # Equity ISINs start with INE
                isin_match = re.match(r'^(INE[A-Z0-9]{9})\s+', line.strip())
                if isin_match:
                    isin = isin_match.group(1)
                    remaining = line[isin_match.end():].strip()
                    holding = self._parse_equity_line(isin, remaining)
                    if holding:
                        holdings.append(holding)

        return holdings

    def _parse_equity_line(self, isin: str, remaining: str) -> Optional[NSDLHolding]:
        """
        Parse equity holding line.

        NSDL format: Company Name | Face Value | Shares | Market Price | Value
        CDSL format: Security Name | Current Bal | Safekeep | Pledged | Price | Value
        """
        try:
            # Extract all numbers from the line
            numbers = re.findall(r'[\d,]+\.?\d*', remaining)

            if len(numbers) < 3:
                return None

            # Extract company name (text before first number)
            name_match = re.match(r'^([A-Za-z\s\-\.&()]+)', remaining)
            name = name_match.group(1).strip() if name_match else "Unknown"

            # Last number is usually market value
            # Second to last is market price
            # Third to last or earlier is quantity
            market_value = self._parse_decimal(numbers[-1])
            market_price = self._parse_decimal(numbers[-2]) if len(numbers) >= 2 else None

            # Find quantity - it's typically a whole number (shares)
            # In NSDL format: Face Value, Shares, Price, Value
            # In CDSL format: Current Bal, Safekeep, Pledged, Price, Value
            quantity = Decimal("0")
            for num_str in numbers[:-2]:  # Exclude price and value
                num = self._parse_decimal(num_str)
                # Shares are typically whole numbers > 0 and < 1 million
                if num > 0 and num < Decimal("1000000") and num == int(num):
                    quantity = num
                    break
                # For fractional shares, check if it's a reasonable quantity
                elif num > 0 and num < Decimal("100000"):
                    quantity = num

            # Skip if market value is 0 or too small
            if market_value < Decimal("1"):
                return None

            return NSDLHolding(
                isin=isin,
                name=name,
                asset_type="EQUITY",
                quantity=quantity,
                market_price=market_price,
                market_value=market_value,
            )

        except (ValueError, InvalidOperation, IndexError) as e:
            logger.debug(f"Failed to parse equity holding: {isin} - {e}")
            return None

    def _extract_mf_holdings(self, text: str) -> List[NSDLHolding]:
        """Extract mutual fund holdings from text."""
        holdings = []

        # Find MF holdings table(s) - there may be multiple tables
        # Look for table headers: "ISIN ISIN Description Folio No. No. of"
        table_headers = list(re.finditer(
            r'ISIN\s+ISIN\s+Description\s+Folio\s+No',
            text,
            re.IGNORECASE
        ))

        if not table_headers:
            # Fallback to section-based parsing
            mf_match = self.MF_SECTION_PATTERN.search(text)
            if not mf_match:
                return holdings
            start_pos = mf_match.end()
            section_text = text[start_pos:start_pos + 5000]
            return self._parse_mf_section_fallback(section_text)

        # Parse each table
        for idx, header_match in enumerate(table_headers):
            # Get text from this header to next header or end
            start_pos = header_match.end()
            if idx + 1 < len(table_headers):
                end_pos = table_headers[idx + 1].start()
            else:
                # End at next major section or limit
                end_pos = start_pos + 10000
                for pattern in [self.NPS_SECTION_PATTERN, self.BOND_SECTION_PATTERN]:
                    match = pattern.search(text, start_pos)
                    if match and match.start() < end_pos:
                        end_pos = match.start()

            table_text = text[start_pos:end_pos]
            holdings.extend(self._parse_mf_table(table_text))

        return holdings

    def _parse_mf_table(self, table_text: str) -> List[NSDLHolding]:
        """
        Parse MF holdings table.

        NSDL CAS MF table format:
        ISIN | Description | Folio No | Units | Avg Cost | Total Cost | NAV | Value | Unrealised | Ann%
        INF740KA1LG1 | DSP Healthcare | 889416 | 2,296.696 | 43.5408 | 1,00,000.00 | 43.5340 | 99,984.36 | -15.64 | -0.01
        """
        holdings = []
        lines = table_text.split('\n')
        current_holding_data = {}

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check for ISIN at start of line (MF ISINs start with INF)
            isin_match = re.match(r'^(INF[A-Z0-9]{9})\s+', line)
            if isin_match:
                isin = isin_match.group(1)
                remaining = line[isin_match.end():].strip()

                # Parse the remaining data
                # Format: Description FolioNo Units AvgCost TotalCost NAV Value Unrealised Ann%
                holding = self._parse_mf_holding_line(isin, remaining)
                if holding:
                    holdings.append(holding)

        return holdings

    def _parse_mf_holding_line(
        self,
        isin: str,
        remaining: str
    ) -> Optional[NSDLHolding]:
        """
        Parse a single MF holding line after ISIN.

        NSDL CAS MF line format (after ISIN):
        Description | Folio | Units | AvgCost | TotalCost | NAV | Value | Unrealised | Ann%
        DSP Healthcare | 889416 | 2,296.696 | 43.5408 | 1,00,000.00 | 43.5340 | 99,984.36 | -15.64 | -0.01
        """
        try:
            # First, identify folio number - it's typically 6-12 digits with no decimals
            # and appears after the scheme name
            folio_match = re.search(r'\b(\d{5,12})\b', remaining)
            folio_number = folio_match.group(1) if folio_match else None

            if not folio_match:
                return None

            # Extract description (text before folio number)
            name = remaining[:folio_match.start()].strip()

            # Clean up name - remove trailing scheme codes like "MFHDFC0004"
            name = re.sub(r'\s*MF[A-Z]+\d+\s*$', '', name)

            # Get text after folio number
            after_folio = remaining[folio_match.end():].strip()

            # Extract numeric values after folio
            # Format: Units | AvgCost | TotalCost | NAV | Value | Unrealised | Ann%
            numbers = re.findall(r'-?[\d,]+\.?\d*', after_folio)

            if len(numbers) < 4:
                return None

            # Parse values by position
            # Index 0: Units (can have decimals like 2,296.696)
            # Index 1: Average cost
            # Index 2: Total cost
            # Index 3: NAV
            # Index 4: Current Value
            # Index 5+: Unrealised, Annualised (optional)
            units = self._parse_decimal(numbers[0])
            avg_cost = self._parse_decimal(numbers[1]) if len(numbers) > 1 else None
            total_cost = self._parse_decimal(numbers[2]) if len(numbers) > 2 else None
            nav = self._parse_decimal(numbers[3]) if len(numbers) > 3 else None
            market_value = self._parse_decimal(numbers[4]) if len(numbers) > 4 else Decimal("0")

            # Sanity check - units should typically be less than 1 million
            # and market_value should be reasonable
            if units > Decimal("10000000") or market_value < Decimal("1"):
                # Likely parsing error - try alternative interpretation
                return None

            return NSDLHolding(
                isin=isin,
                name=name,
                asset_type="MF",
                quantity=units,
                market_price=nav,
                market_value=market_value,
                cost_value=total_cost,
                folio_number=folio_number,
            )

        except (ValueError, InvalidOperation, IndexError) as e:
            logger.debug(f"Failed to parse MF holding: {isin} - {e}")
            return None

    def _parse_mf_section_fallback(self, section_text: str) -> List[NSDLHolding]:
        """Fallback MF parsing when table header not found."""
        holdings = []
        for line in section_text.split('\n'):
            isin_match = self.ISIN_PATTERN.search(line)
            if isin_match and isin_match.group().startswith('INF'):
                holding = self._parse_holding_line(line, isin_match.group(), "MF")
                if holding:
                    folio_match = re.search(r'(?:Folio\s*[:\-]?\s*)?(\d{6,12})', line)
                    if folio_match:
                        holding.folio_number = folio_match.group(1)
                    holdings.append(holding)
        return holdings

    def _extract_nps_holdings(self, text: str) -> List[NSDLHolding]:
        """
        Extract NPS holdings from text.

        NSDL CAS NPS format (Lines 728-744):
        National Pension System (NPS) Holding Details
        Investor Name: SANJAY SHANKAR PRAN:110091211424
        PFM-Scheme Name | No. of units | Current NAV per Unit | Current Value
        TIER I
        ICICI PRUDENTIAL PENSION FUND SCHEME E - TIER I 44,286.3740 77.1077 3,414,820.44
        """
        holdings = []

        # Look for NPS holding details section
        nps_section_match = re.search(
            r'National\s+Pension\s+System\s*\(NPS\)\s*Holding\s+Details',
            text,
            re.IGNORECASE
        )

        if not nps_section_match:
            return holdings

        start_pos = nps_section_match.end()

        # Find end of NPS section (usually at "Transactions" or next major section)
        end_match = re.search(r'\bTransactions\b|\bNSDL Demat\b', text[start_pos:])
        end_pos = start_pos + end_match.start() if end_match else start_pos + 2000

        section_text = text[start_pos:end_pos]

        # Parse NPS pension fund scheme lines
        # Format: SCHEME NAME - TIER X | Units | NAV | Value
        nps_pattern = re.compile(
            r'((?:ICICI|HDFC|SBI|UTI|KOTAK|BIRLA|LIC)\s+[A-Z\s]+SCHEME\s+[ECG]\s*-\s*TIER\s+[I]+)\s+'
            r'([\d,]+\.?\d*)\s+'  # Units
            r'([\d,]+\.?\d*)\s+'  # NAV
            r'([\d,]+\.?\d*)',    # Value
            re.IGNORECASE
        )

        for match in nps_pattern.finditer(section_text):
            scheme_name = match.group(1).strip()
            units = self._parse_decimal(match.group(2))
            nav = self._parse_decimal(match.group(3))
            value = self._parse_decimal(match.group(4))

            # Skip if units is 0
            if units == Decimal("0"):
                continue

            holding = NSDLHolding(
                isin="",  # NPS doesn't have ISIN
                name=scheme_name,
                asset_type="NPS",
                quantity=units,
                market_price=nav,
                market_value=value,
            )
            holdings.append(holding)

        return holdings

    def _extract_bond_holdings(self, text: str) -> List[NSDLHolding]:
        """Extract bond holdings from text."""
        holdings = []

        bond_match = self.BOND_SECTION_PATTERN.search(text)
        if not bond_match:
            return holdings

        start_pos = bond_match.end()
        end_patterns = [self.SGB_SECTION_PATTERN]
        end_pos = len(text)

        for pattern in end_patterns:
            match = pattern.search(text, start_pos)
            if match and match.start() < end_pos:
                end_pos = match.start()

        section_text = text[start_pos:end_pos]

        for line in section_text.split('\n'):
            isin_match = self.ISIN_PATTERN.search(line)
            if isin_match:
                holding = self._parse_holding_line(line, isin_match.group(), "BOND")
                if holding:
                    holdings.append(holding)

        return holdings

    def _extract_sgb_holdings(self, text: str) -> List[NSDLHolding]:
        """
        Extract Sovereign Gold Bond holdings from text.

        NSDL CAS SGB format:
        Sovereign Gold Bonds (SGB)
        ISIN | Issuer Name | Coupon Rate | Maturity Date | No. of Units | Face Value | Market Price | Value
        IN0020200146 | Government of India-SGB 2020-21 SERIES IV | 2.50 | 14-Jul-2028 | 125 | 4,852.00 | 14,349.96 | 17,93,745.00
        """
        holdings = []

        # Find SGB section - look for "Sovereign Gold Bonds (SGB)" header
        # followed by ISIN column header
        sgb_section_match = re.search(
            r'Sovereign\s+Gold\s+Bonds\s*\(SGB\)\s*\n'
            r'ISIN\s+Isue?r',  # Handle both "Issuer" and "Isuer" (typo from dedup)
            text,
            re.IGNORECASE
        )

        if not sgb_section_match:
            return holdings

        start_pos = sgb_section_match.end()

        # Find end of SGB section (at "Sub Total" or "Total" or next section)
        end_match = re.search(r'\bSub\s+Total\b|\bTotal\b', text[start_pos:])
        end_pos = start_pos + end_match.start() + 50 if end_match else start_pos + 2000

        section_text = text[start_pos:end_pos]

        # Parse SGB lines - ISIN starts with IN00
        # Format: IN0020200146 Government of India-SGB... 2.50 14-Jul-2028 125 4,852.00 14,349.96 17,93,745.00
        for line in section_text.split('\n'):
            # SGB ISINs start with IN00 (Government securities)
            isin_match = re.match(r'^(IN00\d{8})\s+', line.strip())
            if isin_match:
                isin = isin_match.group(1)
                remaining = line[isin_match.end():].strip()

                holding = self._parse_sgb_line(isin, remaining)
                if holding:
                    holdings.append(holding)

        return holdings

    def _parse_sgb_line(self, isin: str, remaining: str) -> Optional[NSDLHolding]:
        """
        Parse SGB holding line.

        Format after ISIN:
        Government of India-SGB 2020-21 SERIES IV | 2.50 | 14-Jul-2028 | 125 | 4,852.00 | 14,349.96 | 17,93,745.00
        """
        try:
            # Extract all numbers
            numbers = re.findall(r'[\d,]+\.?\d*', remaining)

            if len(numbers) < 4:
                return None

            # Extract name (text before first date-like pattern or numeric)
            name_match = re.match(r'^([A-Za-z\s\-]+)', remaining)
            name = name_match.group(1).strip() if name_match else "Sovereign Gold Bond"

            # Parse values:
            # Coupon rate (small decimal), Maturity date (skip), Units, Face Value, Market Price, Value
            # Last 4 numbers are: Units, Face Value, Market Price, Value
            units = self._parse_decimal(numbers[-4])
            face_value = self._parse_decimal(numbers[-3])
            market_price = self._parse_decimal(numbers[-2])
            market_value = self._parse_decimal(numbers[-1])

            return NSDLHolding(
                isin=isin,
                name=name,
                asset_type="SGB",
                quantity=units,
                face_value=face_value,
                market_price=market_price,
                market_value=market_value,
            )

        except (ValueError, InvalidOperation, IndexError) as e:
            logger.debug(f"Failed to parse SGB holding: {isin} - {e}")
            return None

    def _parse_holding_line(
        self,
        line: str,
        isin: str,
        asset_type: str
    ) -> Optional[NSDLHolding]:
        """Parse a holding line to extract details."""
        # Remove ISIN to get remaining content
        remaining = line.replace(isin, "").strip()

        # Extract numbers (quantity, values)
        numbers = re.findall(r'[\d,]+\.?\d*', remaining)

        if len(numbers) < 2:
            return None

        try:
            # Extract name (text before first number)
            name_match = re.match(r'^([A-Za-z\s\-\.&]+)', remaining)
            name = name_match.group(1).strip() if name_match else "Unknown"

            # Parse numbers based on position
            # Typical: Qty, Face Value, Market Price, Market Value
            quantity = self._parse_decimal(numbers[0])
            market_value = self._parse_decimal(numbers[-1])  # Usually last number

            market_price = None
            if len(numbers) >= 3:
                market_price = self._parse_decimal(numbers[-2])

            return NSDLHolding(
                isin=isin,
                name=name,
                asset_type=asset_type,
                quantity=quantity,
                market_price=market_price,
                market_value=market_value,
            )

        except (ValueError, InvalidOperation) as e:
            logger.debug(f"Failed to parse holding line: {line} - {e}")
            return None

    def _parse_decimal(self, value: str) -> Decimal:
        """Parse string to Decimal, handling commas."""
        cleaned = value.replace(",", "").strip()
        return Decimal(cleaned) if cleaned else Decimal("0")

    def calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of file."""
        hasher = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)
        return hasher.hexdigest()


class GoldenReferenceIngester:
    """
    Ingests parsed CAS data into golden reference tables.

    Handles:
    - Creating golden_reference record
    - Inserting golden_holdings for each holding
    - Triggering reconciliation

    Usage:
        parser = NSDLCASParser()
        cas_data = parser.parse("/path/to/cas.pdf", password="secret")

        ingester = GoldenReferenceIngester(conn, user_id=1)
        ref_id = ingester.ingest_nsdl_cas(cas_data, file_path="/path/to/cas.pdf")

        # Trigger reconciliation
        from .cross_correlator import CrossCorrelator
        correlator = CrossCorrelator(conn, user_id=1)
        correlator.reconcile_holdings(AssetClass.MUTUAL_FUND, ref_id)
    """

    def __init__(self, db_connection, user_id: int):
        """Initialize ingester."""
        self.conn = db_connection
        self.user_id = user_id

    def ingest_nsdl_cas(
        self,
        cas_data: NSDLCASData,
        file_path: Optional[Path] = None,
        file_hash: Optional[str] = None
    ) -> int:
        """
        Ingest NSDL CAS data into golden reference tables.

        Args:
            cas_data: Parsed NSDL CAS data
            file_path: Original file path
            file_hash: Pre-calculated file hash

        Returns:
            Golden reference ID
        """
        # Check for duplicate
        if file_hash:
            cursor = self.conn.execute(
                "SELECT id FROM golden_reference WHERE file_hash = ? AND user_id = ?",
                (file_hash, self.user_id)
            )
            existing = cursor.fetchone()
            if existing:
                logger.info(f"Golden reference already exists: {existing[0]}")
                return existing[0]

        # Create golden reference record
        cursor = self.conn.execute("""
            INSERT INTO golden_reference (
                user_id, source_type, statement_date, period_start, period_end,
                file_path, file_hash, raw_data, investor_name, investor_pan, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            self.user_id,
            SourceType.NSDL_CAS.value,
            cas_data.statement_date.isoformat(),
            cas_data.period_start.isoformat() if cas_data.period_start else None,
            cas_data.period_end.isoformat() if cas_data.period_end else None,
            str(file_path) if file_path else None,
            file_hash,
            json.dumps({
                "investor": {
                    "name": cas_data.investor_info.name,
                    "pan": cas_data.investor_info.pan,
                },
                "holdings_count": len(cas_data.all_holdings),
                "total_value": str(cas_data.total_value),
            }),
            cas_data.investor_info.name,
            cas_data.investor_info.pan,
            "ACTIVE",
        ))
        golden_ref_id = cursor.lastrowid

        # Insert holdings
        holdings_inserted = 0
        for holding in cas_data.all_holdings:
            asset_class = self._map_asset_type(holding.asset_type)
            fy = self._get_financial_year(cas_data.statement_date)

            self.conn.execute("""
                INSERT INTO golden_holdings (
                    golden_ref_id, user_id, asset_type, isin, symbol, name,
                    folio_number, units, nav, market_value, cost_basis,
                    currency, as_of_date, financial_year
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                golden_ref_id,
                self.user_id,
                asset_class.value,
                holding.isin or None,
                None,  # symbol
                holding.name,
                holding.folio_number,
                str(holding.quantity),
                str(holding.market_price) if holding.market_price else None,
                str(holding.market_value),
                str(holding.cost_value) if holding.cost_value else None,
                "INR",
                cas_data.statement_date.isoformat(),
                fy,
            ))
            holdings_inserted += 1

        self.conn.commit()

        logger.info(
            f"Ingested golden reference {golden_ref_id}: "
            f"{holdings_inserted} holdings from NSDL CAS"
        )

        return golden_ref_id

    def _map_asset_type(self, nsdl_type: str) -> AssetClass:
        """Map NSDL asset type to AssetClass enum."""
        mapping = {
            "EQUITY": AssetClass.STOCKS,
            "MF": AssetClass.MUTUAL_FUND,
            "NPS": AssetClass.NPS,
            "BOND": AssetClass.BONDS,
            "SGB": AssetClass.SGB,
        }
        return mapping.get(nsdl_type, AssetClass.STOCKS)

    def _get_financial_year(self, as_of: date) -> str:
        """Get financial year string for a date."""
        if as_of.month >= 4:
            return f"{as_of.year}-{str(as_of.year + 1)[-2:]}"
        return f"{as_of.year - 1}-{str(as_of.year)[-2:]}"

    def mark_superseded(self, golden_ref_id: int) -> None:
        """Mark a golden reference as superseded by newer data."""
        self.conn.execute(
            "UPDATE golden_reference SET status = 'SUPERSEDED' WHERE id = ? AND user_id = ?",
            (golden_ref_id, self.user_id)
        )
        self.conn.commit()
