"""
ICICI Direct PDF Transaction Statement Parser

Extracts detailed transaction data from ICICI Direct equity transaction PDFs.
Supports both buy and sell transactions with full charge breakdown.

Features:
- Extracts user info (PAN, Client Code, Name)
- Parses individual transactions with ISIN, quantity, price, charges
- Extracts summary with STT, stamp duty, settlement amounts
- Handles multi-page PDFs
- Calculates actual cost basis including all charges

Usage:
    from pfas.parsers.stock.icici_pdf_parser import ICICIDirectPDFParser

    parser = ICICIDirectPDFParser()
    result = parser.parse(Path("TRX-Equity_06-01-2026_326158.PDF"))

    print(f"User: {result.user_info['name']}")
    print(f"Transactions: {len(result.transactions)}")
    for txn in result.transactions:
        print(f"  {txn.trade_date} {txn.buy_sell} {txn.quantity} {txn.security_name} @ {txn.price}")
"""

import re
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class ICICITransaction:
    """Single ICICI Direct transaction record."""
    # Identifiers
    contract_number: str
    settlement_no: str
    trade_no: str
    order_no: str

    # Dates & Times
    trade_date: date
    trade_time: Optional[str] = None
    order_time: Optional[str] = None
    settlement_date: Optional[date] = None

    # Security Info
    exchange: str = "NSE"  # NSE or BSE
    isin: str = ""
    security_name: str = ""
    symbol: Optional[str] = None  # Derived from security name

    # Transaction Details
    buy_sell: str = "B"  # B=Buy, S=Sell
    quantity: int = 0
    gross_rate: Decimal = Decimal("0")  # Price per unit
    total_value: Decimal = Decimal("0")  # Gross value

    # Charges
    brokerage: Decimal = Decimal("0")
    gst: Decimal = Decimal("0")
    net_amount: Decimal = Decimal("0")

    # For summary matching
    stt: Decimal = Decimal("0")
    transaction_charges: Decimal = Decimal("0")
    stamp_duty: Decimal = Decimal("0")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return {
            "contract_number": self.contract_number,
            "settlement_no": self.settlement_no,
            "trade_no": self.trade_no,
            "order_no": self.order_no,
            "trade_date": self.trade_date.isoformat() if self.trade_date else None,
            "trade_time": self.trade_time,
            "settlement_date": self.settlement_date.isoformat() if self.settlement_date else None,
            "exchange": self.exchange,
            "isin": self.isin,
            "security_name": self.security_name,
            "symbol": self.symbol,
            "buy_sell": self.buy_sell,
            "quantity": self.quantity,
            "gross_rate": float(self.gross_rate),
            "total_value": float(self.total_value),
            "brokerage": float(self.brokerage),
            "gst": float(self.gst),
            "net_amount": float(self.net_amount),
            "stt": float(self.stt),
            "transaction_charges": float(self.transaction_charges),
            "stamp_duty": float(self.stamp_duty),
        }

    @property
    def is_buy(self) -> bool:
        return self.buy_sell == "B"

    @property
    def is_sell(self) -> bool:
        return self.buy_sell == "S"

    @property
    def total_cost(self) -> Decimal:
        """Total cost including all charges (for buys)."""
        if self.is_buy:
            return self.net_amount
        return Decimal("0")

    @property
    def net_proceeds(self) -> Decimal:
        """Net proceeds after charges (for sells)."""
        if self.is_sell:
            return self.net_amount
        return Decimal("0")


@dataclass
class SettlementSummary:
    """Summary for a settlement period."""
    contract_date: date
    contract_number: str
    settlement_no: str
    settlement_date: date
    stt: Decimal = Decimal("0")
    transaction_charges: Decimal = Decimal("0")
    stamp_duty: Decimal = Decimal("0")
    net_receivable_payable: Decimal = Decimal("0")
    is_payable: bool = True  # True if client pays, False if client receives


@dataclass
class ICICIPDFParseResult:
    """Result of parsing ICICI Direct PDF."""
    success: bool = True
    source_file: str = ""

    # User Information
    user_info: Dict[str, str] = field(default_factory=dict)

    # Statement Period
    period_start: Optional[date] = None
    period_end: Optional[date] = None

    # Transactions
    transactions: List[ICICITransaction] = field(default_factory=list)
    settlements: List[SettlementSummary] = field(default_factory=list)

    # Aggregates
    total_buys: int = 0
    total_sells: int = 0
    total_buy_value: Decimal = Decimal("0")
    total_sell_value: Decimal = Decimal("0")
    total_brokerage: Decimal = Decimal("0")
    total_stt: Decimal = Decimal("0")
    total_gst: Decimal = Decimal("0")

    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class ICICIDirectPDFParser:
    """
    Parser for ICICI Direct Equity Transaction Statement PDFs.

    Extracts:
    - User details (PAN, Client Code, Name)
    - Individual transactions with all charges
    - Settlement summaries with STT, stamp duty
    """

    # Patterns for header extraction
    PAN_PATTERN = re.compile(r'PAN\s*:\s*([A-Z]{5}[0-9]{4}[A-Z])')
    CLIENT_CODE_PATTERN = re.compile(r'UNIQUE CLIENT CODE\s*:\s*(\d+)')
    TRADING_CODE_PATTERN = re.compile(r'TRADING CODE NO\s*:\s*(\d+)')
    NAME_PATTERN = re.compile(r'To,\s*\n\s*([A-Z\s]+)\n')
    PERIOD_PATTERN = re.compile(
        r'Equity Transaction Statement from\s+(\d{2}-\w{3}-\d{4})\s+to\s+(\d{2}-\w{3}-\d{4})'
    )

    # Date formats
    DATE_FORMATS = [
        "%d-%m-%Y",
        "%d-%b-%Y",
        "%d-%b-%y",
        "%d/%m/%Y",
    ]

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self._security_symbol_map = self._load_symbol_map()

    def _load_symbol_map(self) -> Dict[str, str]:
        """Load ISIN to symbol mapping."""
        # Common mappings - can be extended from config or DB
        return {
            "INF204KB17I5": "GOLDBEES",
            "INF732E01037": "LIQUIDBEES",
            "INE844O01030": "GUJGAS",
            "INE002A01018": "RELIANCE",
            "INE009A01021": "INFY",
            "INE467B01029": "TCS",
            "INE020B01018": "RECLTD",
            "INE139A01034": "NATIONALUM",
            "INE736A01011": "CDSL",
            "INE280A01028": "TITAN",
            "INE752E01010": "POWERGRID",
            "INE531E01026": "HINDCOPPER",
            "INE041025011": "EMBASSY",
            "INE0CCU25019": "MINDSPACE",
            "INE596I01012": "CAMS",
            "INE118H01025": "BSE",
            "INE292B01021": "HBLENGINE",
            "INE379A01028": "ITCHOTELS",
            "INE0FS801015": "MOTHERSON",
            "INE022Q01020": "IEX",
            "INE200A01026": "GEVERNOVA",
        }

    def parse(self, file_path: Path, password: Optional[str] = None) -> ICICIPDFParseResult:
        """
        Parse ICICI Direct PDF transaction statement.

        Args:
            file_path: Path to PDF file
            password: Optional PDF password

        Returns:
            ICICIPDFParseResult with transactions and summaries
        """
        result = ICICIPDFParseResult(source_file=str(file_path))

        if not file_path.exists():
            result.success = False
            result.errors.append(f"File not found: {file_path}")
            return result

        try:
            # Try pdfplumber first (better for tables)
            text, tables = self._extract_with_pdfplumber(file_path, password)

            if not text:
                result.success = False
                result.errors.append("Could not extract text from PDF")
                return result

            # Extract user info
            result.user_info = self._extract_user_info(text)

            # Extract period
            result.period_start, result.period_end = self._extract_period(text)

            # Parse transactions from tables
            if tables:
                result.transactions = self._parse_transactions_from_tables(tables)
            else:
                # Fallback to text parsing
                result.transactions = self._parse_transactions_from_text(text)

            # Parse settlement summaries
            result.settlements = self._parse_settlements(text, tables)

            # Distribute STT/charges to transactions
            self._distribute_charges(result)

            # Calculate aggregates
            self._calculate_aggregates(result)

            # Derive symbols from ISIN
            for txn in result.transactions:
                if txn.isin and not txn.symbol:
                    txn.symbol = self._security_symbol_map.get(txn.isin, self._derive_symbol(txn.security_name))

            logger.info(f"Parsed {len(result.transactions)} transactions from {file_path.name}")

        except Exception as e:
            result.success = False
            result.errors.append(f"Parse error: {e}")
            logger.exception(f"Failed to parse {file_path}")

        return result

    def _extract_with_pdfplumber(
        self, file_path: Path, password: Optional[str]
    ) -> Tuple[str, List[List[List[str]]]]:
        """Extract text and tables using pdfplumber."""
        try:
            import pdfplumber

            all_text = []
            all_tables = []

            with pdfplumber.open(file_path, password=password) as pdf:
                for page in pdf.pages:
                    # Extract text
                    text = page.extract_text()
                    if text:
                        all_text.append(text)

                    # Extract tables
                    tables = page.extract_tables()
                    if tables:
                        all_tables.extend(tables)

            return "\n".join(all_text), all_tables

        except ImportError:
            logger.warning("pdfplumber not available, trying PyPDF2")
            return self._extract_with_pypdf2(file_path, password)
        except Exception as e:
            logger.warning(f"pdfplumber failed: {e}, trying PyPDF2")
            return self._extract_with_pypdf2(file_path, password)

    def _extract_with_pypdf2(
        self, file_path: Path, password: Optional[str]
    ) -> Tuple[str, List]:
        """Fallback extraction using PyPDF2."""
        try:
            from PyPDF2 import PdfReader

            reader = PdfReader(str(file_path))
            if password and reader.is_encrypted:
                reader.decrypt(password)

            all_text = []
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    all_text.append(text)

            return "\n".join(all_text), []

        except Exception as e:
            logger.error(f"PyPDF2 extraction failed: {e}")
            return "", []

    def _extract_user_info(self, text: str) -> Dict[str, str]:
        """Extract user information from PDF text."""
        info = {}

        # PAN
        pan_match = self.PAN_PATTERN.search(text)
        if pan_match:
            info["pan"] = pan_match.group(1)

        # Client Code
        code_match = self.CLIENT_CODE_PATTERN.search(text)
        if code_match:
            info["client_code"] = code_match.group(1)

        # Trading Code
        trading_match = self.TRADING_CODE_PATTERN.search(text)
        if trading_match:
            info["trading_code"] = trading_match.group(1)

        # Name - look for pattern after "To,"
        name_match = re.search(r'To,\s*\n?\s*([A-Z][A-Z\s]+[A-Z])\s*\n', text)
        if name_match:
            info["name"] = name_match.group(1).strip()

        return info

    def _extract_period(self, text: str) -> Tuple[Optional[date], Optional[date]]:
        """Extract statement period from text."""
        match = self.PERIOD_PATTERN.search(text)
        if match:
            start_str, end_str = match.groups()
            start = self._parse_date(start_str)
            end = self._parse_date(end_str)
            return start, end
        return None, None

    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string in various formats."""
        if not date_str:
            return None

        date_str = date_str.strip()

        for fmt in self.DATE_FORMATS:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        # Try extracting date from datetime string like "01-10-202511:49"
        match = re.match(r'(\d{2}-\d{2}-\d{4})', date_str)
        if match:
            try:
                return datetime.strptime(match.group(1), "%d-%m-%Y").date()
            except ValueError:
                pass

        return None

    def _parse_decimal(self, value: Any) -> Decimal:
        """Parse decimal value, handling various formats."""
        if value is None:
            return Decimal("0")

        if isinstance(value, (int, float)):
            return Decimal(str(value))

        value_str = str(value).strip()

        # Remove currency symbols and commas
        for char in ["₹", "Rs.", "Rs", ",", " "]:
            value_str = value_str.replace(char, "")

        if not value_str or value_str == "-":
            return Decimal("0")

        try:
            return Decimal(value_str)
        except InvalidOperation:
            return Decimal("0")

    def _parse_transactions_from_tables(
        self, tables: List[List[List[str]]]
    ) -> List[ICICITransaction]:
        """Parse transactions from extracted tables."""
        transactions = []

        for table in tables:
            if not table or len(table) < 2:
                continue

            # Find header row
            header_idx = self._find_header_row(table)
            if header_idx is None:
                continue

            headers = [str(h).strip().lower() if h else "" for h in table[header_idx]]

            # Map column indices
            col_map = self._map_columns(headers)
            if not col_map:
                continue

            # Parse data rows
            for row_idx in range(header_idx + 1, len(table)):
                row = table[row_idx]
                if not row or len(row) < 5:
                    continue

                txn = self._parse_transaction_row(row, col_map)
                if txn and txn.isin:
                    transactions.append(txn)

        return transactions

    def _find_header_row(self, table: List[List[str]]) -> Optional[int]:
        """Find the header row in a table."""
        for idx, row in enumerate(table):
            row_str = " ".join([str(c).lower() if c else "" for c in row])
            if "isin" in row_str and ("quantity" in row_str or "buy" in row_str):
                return idx
        return None

    def _map_columns(self, headers: List[str]) -> Dict[str, int]:
        """Map column names to indices."""
        col_map = {}

        mappings = {
            "contract": ["contract number", "contract no"],
            "settlement_no": ["settlement no", "settlement"],
            "exchange": ["exchange code", "exchange"],
            "order_no": ["order no", "order no."],
            "order_time": ["order time"],
            "trade_no": ["trade no", "trade no."],
            "trade_date": ["trade date", "trade date &"],
            "settlement_date": ["settlement date"],
            "isin": ["isin"],
            "security": ["security", "security name"],
            "buy_sell": ["buy", "buy/sell", "b/s"],
            "quantity": ["quantity", "qty"],
            "rate": ["gross rate", "rate per security", "price"],
            "total": ["total", "total ("],
            "brokerage": ["brokerage"],
            "gst": ["gst", "goods &", "service tax"],
            "net_amount": ["net amount", "net amt"],
        }

        for key, patterns in mappings.items():
            for idx, header in enumerate(headers):
                for pattern in patterns:
                    if pattern in header:
                        col_map[key] = idx
                        break
                if key in col_map:
                    break

        return col_map

    def _parse_transaction_row(
        self, row: List[str], col_map: Dict[str, int]
    ) -> Optional[ICICITransaction]:
        """Parse a single transaction row."""
        try:
            def get_val(key: str, default: str = "") -> str:
                idx = col_map.get(key)
                if idx is not None and idx < len(row):
                    val = row[idx]
                    return str(val).strip() if val else default
                return default

            isin = get_val("isin")
            if not isin or len(isin) < 10:
                return None

            # Parse trade date
            trade_date_str = get_val("trade_date")
            trade_date = self._parse_date(trade_date_str)
            if not trade_date:
                return None

            # Extract time from trade_date if present
            trade_time = None
            if trade_date_str and len(trade_date_str) > 10:
                time_match = re.search(r'(\d{2}:\d{2})', trade_date_str)
                if time_match:
                    trade_time = time_match.group(1)

            # Parse settlement date
            settlement_date = self._parse_date(get_val("settlement_date"))

            # Buy/Sell
            buy_sell = get_val("buy_sell", "B").upper()
            if buy_sell not in ["B", "S"]:
                buy_sell = "B"

            # Quantity
            qty_str = get_val("quantity", "0")
            quantity = int(self._parse_decimal(qty_str))

            if quantity <= 0:
                return None

            return ICICITransaction(
                contract_number=get_val("contract"),
                settlement_no=get_val("settlement_no"),
                trade_no=get_val("trade_no"),
                order_no=get_val("order_no"),
                trade_date=trade_date,
                trade_time=trade_time,
                order_time=get_val("order_time"),
                settlement_date=settlement_date,
                exchange=get_val("exchange", "NSE"),
                isin=isin,
                security_name=get_val("security"),
                buy_sell=buy_sell,
                quantity=quantity,
                gross_rate=self._parse_decimal(get_val("rate")),
                total_value=self._parse_decimal(get_val("total")),
                brokerage=self._parse_decimal(get_val("brokerage")),
                gst=self._parse_decimal(get_val("gst")),
                net_amount=self._parse_decimal(get_val("net_amount")),
            )

        except Exception as e:
            logger.debug(f"Failed to parse row: {e}")
            return None

    def _parse_transactions_from_text(self, text: str) -> List[ICICITransaction]:
        """Fallback: Parse transactions from raw text."""
        transactions = []

        # Pattern to match transaction lines
        # ISEC/2025188/01801 2721 2025188 NSE ... INF204KB17I5 ... B 1000 96.65 ...
        txn_pattern = re.compile(
            r'ISEC/(\d+/\d+)\s+'  # Contract number
            r'(\d+)\s+'           # Settlement no
            r'(\d+)\s+'           # Some ID
            r'(NSE|BSE)\s+'       # Exchange
            r'(\d+)\s+'           # Order/Trade no
            r'(\d{2}-\d{2}-\d{4})'  # Trade date
        )

        for match in txn_pattern.finditer(text):
            # This is a simplified fallback - full parsing from text is complex
            logger.debug(f"Found transaction pattern: {match.group()}")

        return transactions

    def _parse_settlements(
        self, text: str, tables: List[List[List[str]]]
    ) -> List[SettlementSummary]:
        """Parse settlement summaries from text or tables."""
        settlements = []

        # Look for Summary section in tables
        for table in tables:
            if not table:
                continue

            # Check if this is a summary table
            header_row = None
            for idx, row in enumerate(table):
                row_str = " ".join([str(c).lower() if c else "" for c in row])
                if "securities transaction tax" in row_str or "stamp duty" in row_str:
                    header_row = idx
                    break

            if header_row is None:
                continue

            # Parse summary rows
            for row_idx in range(header_row + 1, len(table)):
                row = table[row_idx]
                if not row or len(row) < 5:
                    continue

                try:
                    # Contract date is first column
                    contract_date = self._parse_date(str(row[0]) if row[0] else "")
                    if not contract_date:
                        continue

                    summary = SettlementSummary(
                        contract_date=contract_date,
                        contract_number=str(row[1]) if len(row) > 1 and row[1] else "",
                        settlement_no=str(row[2]) if len(row) > 2 and row[2] else "",
                        settlement_date=self._parse_date(str(row[3]) if len(row) > 3 and row[3] else ""),
                        stt=self._parse_decimal(row[4] if len(row) > 4 else 0),
                        transaction_charges=self._parse_decimal(row[5] if len(row) > 5 else 0),
                        stamp_duty=self._parse_decimal(row[6] if len(row) > 6 else 0),
                    )

                    # Parse net amount and direction from last column
                    if len(row) > 7 and row[7]:
                        last_col = str(row[7]).lower()
                        summary.is_payable = "payable by client" in last_col
                        amt_match = re.search(r'rs\.?\s*([\d,.]+)', last_col)
                        if amt_match:
                            summary.net_receivable_payable = self._parse_decimal(amt_match.group(1))

                    settlements.append(summary)

                except Exception as e:
                    logger.debug(f"Failed to parse settlement row: {e}")

        return settlements

    def _distribute_charges(self, result: ICICIPDFParseResult):
        """Distribute STT and other charges from summaries to transactions."""
        # Group transactions by settlement
        by_settlement = {}
        for txn in result.transactions:
            key = txn.settlement_no
            if key not in by_settlement:
                by_settlement[key] = []
            by_settlement[key].append(txn)

        # Match summaries to transactions
        for summary in result.settlements:
            txns = by_settlement.get(summary.settlement_no, [])
            if not txns:
                continue

            # Distribute STT proportionally by value
            total_value = sum(t.total_value for t in txns)
            if total_value > 0:
                for txn in txns:
                    ratio = txn.total_value / total_value
                    txn.stt = summary.stt * ratio
                    txn.transaction_charges = summary.transaction_charges * ratio
                    txn.stamp_duty = summary.stamp_duty * ratio

    def _calculate_aggregates(self, result: ICICIPDFParseResult):
        """Calculate aggregate statistics."""
        for txn in result.transactions:
            if txn.is_buy:
                result.total_buys += 1
                result.total_buy_value += txn.total_value
            else:
                result.total_sells += 1
                result.total_sell_value += txn.total_value

            result.total_brokerage += txn.brokerage
            result.total_gst += txn.gst
            result.total_stt += txn.stt

    def _derive_symbol(self, security_name: str) -> str:
        """Derive trading symbol from security name."""
        if not security_name:
            return ""

        # Remove common suffixes
        name = security_name.upper()
        for suffix in [" LIMITED", " LTD", " LTD.", " CORPORATION", " CORP", " INDIA"]:
            name = name.replace(suffix, "")

        # Take first word or acronym
        words = name.split()
        if len(words) > 1:
            # Check if it's an ETF
            if "ETF" in name:
                return "".join([w[0] for w in words if w not in ["ETF", "NIP", "IND"]])
            return words[0][:10]

        return name[:10]


def parse_icici_pdf(
    file_path: Path,
    password: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None
) -> ICICIPDFParseResult:
    """
    Convenience function to parse ICICI Direct PDF.

    Args:
        file_path: Path to PDF file
        password: Optional PDF password
        config: Optional parser configuration

    Returns:
        ICICIPDFParseResult
    """
    parser = ICICIDirectPDFParser(config)
    return parser.parse(file_path, password)


# Export classes
__all__ = [
    "ICICIDirectPDFParser",
    "ICICITransaction",
    "SettlementSummary",
    "ICICIPDFParseResult",
    "parse_icici_pdf",
]
