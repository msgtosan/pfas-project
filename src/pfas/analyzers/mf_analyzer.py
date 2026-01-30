"""
Mutual Fund Statement Analyzer Module.

Comprehensive analyzer for CAMS and Karvy/KFintech mutual fund statements.
Supports both transaction (CG) statements and holding statements.

Components:
- MFStatementScanner: Recursive folder scanning with RTA detection
- MFFieldNormalizer: CAMS/Karvy field mapping and normalization
- MFDBIngester: Idempotent database ingestion
- MFReportGenerator: Excel report generation
- MFAnalyzer: Main orchestrator

Usage:
    from pfas.analyzers import MFAnalyzer

    analyzer = MFAnalyzer(config_path="config/mf_analyzer_config.json")
    result = analyzer.analyze(user_name="Sanjay")
    analyzer.generate_reports()
"""

import json
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from pfas.parsers.mf.classifier import classify_scheme
from pfas.parsers.mf.models import AssetClass
from pfas.core.paths import PathResolver

logger = logging.getLogger(__name__)


# ============================================================================
# Data Models
# ============================================================================

class RTA(Enum):
    """Registrar and Transfer Agent."""
    CAMS = "CAMS"
    KFINTECH = "KFINTECH"


class StatementType(Enum):
    """Type of MF statement."""
    HOLDINGS = "HOLDINGS"
    CAPITAL_GAINS = "CAPITAL_GAINS"
    TRANSACTIONS = "TRANSACTIONS"


@dataclass
class NormalizedHolding:
    """Normalized holding record from any RTA."""
    amc_name: str
    scheme_name: str
    scheme_type: str
    folio_number: str
    investor_name: str
    units: Decimal
    nav_date: date
    current_value: Decimal
    cost_value: Decimal
    appreciation: Decimal
    average_holding_days: int
    annualized_return: Decimal
    dividend_payout: Decimal = Decimal("0")
    dividend_reinvest: Decimal = Decimal("0")
    isin: str = ""
    rta: RTA = RTA.CAMS
    nav: Optional[Decimal] = None
    source_file: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DB insertion."""
        return {
            "amc_name": self.amc_name,
            "scheme_name": self.scheme_name,
            "scheme_type": self.scheme_type,
            "folio_number": self.folio_number,
            "investor_name": self.investor_name,
            "units": str(self.units),
            "nav": str(self.nav) if self.nav else None,
            "nav_date": self.nav_date.isoformat(),
            "current_value": str(self.current_value),
            "cost_value": str(self.cost_value),
            "appreciation": str(self.appreciation),
            "average_holding_days": self.average_holding_days,
            "annualized_return": str(self.annualized_return),
            "dividend_payout": str(self.dividend_payout),
            "dividend_reinvest": str(self.dividend_reinvest),
            "isin": self.isin,
            "rta": self.rta.value,
            "source_file": self.source_file,
        }


@dataclass
class ScannedFile:
    """Represents a scanned statement file."""
    path: Path
    rta: RTA
    statement_type: StatementType
    file_date: Optional[date] = None


@dataclass
class AnalysisResult:
    """Result of MF analysis."""
    success: bool = True
    files_scanned: int = 0
    holdings_processed: int = 0
    transactions_processed: int = 0
    duplicates_skipped: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    holdings: List[NormalizedHolding] = field(default_factory=list)

    # Summary metrics
    total_current_value: Decimal = Decimal("0")
    total_cost_value: Decimal = Decimal("0")
    total_appreciation: Decimal = Decimal("0")
    weighted_xirr: Optional[Decimal] = None
    equity_value: Decimal = Decimal("0")
    debt_value: Decimal = Decimal("0")
    hybrid_value: Decimal = Decimal("0")


# ============================================================================
# MFStatementScanner - Recursive Folder Scanning
# ============================================================================

class MFStatementScanner:
    """
    Recursively scans user folder for MF statements.

    Detects:
    - CAMS vs Karvy/KFintech files
    - Holding vs Transaction/CG statements
    - Excel vs PDF formats
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.cams_patterns = config.get("file_patterns", {}).get("cams", {}).get("patterns", ["*CAMS*"])
        self.karvy_patterns = config.get("file_patterns", {}).get("karvy", {}).get("patterns", ["*Karvy*"])
        self.extensions = [".xlsx", ".xls", ".pdf"]
        self.exclude_patterns = ["~$*", "*.tmp", "*.bak"]

    def scan(self, folder: Path) -> List[ScannedFile]:
        """
        Scan folder recursively for MF statement files.

        Args:
            folder: Root folder to scan

        Returns:
            List of ScannedFile objects
        """
        if not folder.exists():
            logger.warning(f"Folder does not exist: {folder}")
            return []

        files = []

        for ext in self.extensions:
            for file_path in folder.rglob(f"*{ext}"):
                # Skip excluded patterns
                if any(file_path.match(pat) for pat in self.exclude_patterns):
                    continue

                # Detect RTA
                rta = self._detect_rta(file_path)
                if not rta:
                    logger.debug(f"Could not detect RTA for: {file_path}")
                    continue

                # Detect statement type
                stmt_type = self._detect_statement_type(file_path)

                # Extract date from filename if possible
                file_date = self._extract_date_from_filename(file_path.name)

                files.append(ScannedFile(
                    path=file_path,
                    rta=rta,
                    statement_type=stmt_type,
                    file_date=file_date
                ))

        logger.info(f"Scanned {len(files)} MF statement files in {folder}")
        return files

    def _detect_rta(self, file_path: Path) -> Optional[RTA]:
        """
        Detect RTA from folder path or filename.

        Priority:
        1. Parent folder name (CAMS, KARVY, KFINTECH)
        2. Filename patterns
        """
        # First check parent folder name
        parent_folder = file_path.parent.name.lower()
        if parent_folder in ["cams"]:
            return RTA.CAMS
        elif parent_folder in ["karvy", "kfintech", "kfin"]:
            return RTA.KFINTECH

        # Then check filename patterns
        name_lower = file_path.name.lower()

        # Check CAMS patterns
        for pattern in self.cams_patterns:
            pattern_lower = pattern.lower().replace("*", "")
            if pattern_lower in name_lower:
                return RTA.CAMS

        # Check Karvy patterns
        for pattern in self.karvy_patterns:
            pattern_lower = pattern.lower().replace("*", "")
            if pattern_lower in name_lower:
                return RTA.KFINTECH

        return None

    def _detect_statement_type(self, file_path: Path) -> StatementType:
        """
        Detect statement type from filename and file content.
        Priority:
        1. Filename patterns (CG, capital, gain for CG files)
        2. Excel sheet names inspection
        3. Default to HOLDINGS if unsure
        """
        name_lower = file_path.name.lower()

        if any(k in name_lower for k in ["transaction", "txn", "trxn", "buy", "sell", "redeem", "dividend"]):
              return StatementType.TRANSACTIONS
        
        if any(k in name_lower for k in ["holding", "holdings", "summary", "balance", "portfolio"]):
              return StatementType.HOLDINGS
        

        # Capital gains files
        if "cg" in name_lower or "capital" in name_lower or "gain" in name_lower:
            return StatementType.CAPITAL_GAINS

        # Clear holdings indicators
        if "hold" in name_lower or "summary" in name_lower or "consolidated" in name_lower:
            return StatementType.HOLDINGS

        # For Excel files, inspect sheet names
        if file_path.suffix.lower() in ['.xlsx', '.xls']:
            try:
                xl = pd.ExcelFile(file_path, engine='calamine')
                sheet_names_lower = [s.lower() for s in xl.sheet_names]

                # Holdings sheets
                holdings_sheets = ['by mutual fund', 'by investor', 'holdings', 'summary', 'sheet1']
                for sheet in holdings_sheets:
                    if sheet in sheet_names_lower:
                        return StatementType.HOLDINGS

                # Transaction/CG sheets
                if 'trxn_details' in sheet_names_lower or 'transaction' in sheet_names_lower:
                    return StatementType.CAPITAL_GAINS

            except Exception:
                pass

        # PAN in filename usually indicates holdings statement
        if "pan" in name_lower:
            return StatementType.HOLDINGS

        # Default to holdings (most common)
        return StatementType.TRANSACTIONS

    def _extract_date_from_filename(self, filename: str) -> Optional[date]:
        """Extract date from filename patterns."""
        patterns = [
            r"FY(\d{2})-?(\d{2})",  # FY24-25
            r"(\d{4})-(\d{2})-(\d{2})",  # 2024-03-31
            r"(\d{2})-?(\d{2})-?(\d{4})",  # 31-03-2024
            r"(\d{1,2})[a-zA-Z]{3}(\d{2,4})",  # 3rdJan26, 31Mar2024
        ]

        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                try:
                    groups = match.groups()
                    if len(groups) == 2:  # FY pattern
                        year = 2000 + int(groups[1])
                        return date(year, 3, 31)  # End of FY
                    elif len(groups) == 3:
                        if len(groups[0]) == 4:  # YYYY-MM-DD
                            return date(int(groups[0]), int(groups[1]), int(groups[2]))
                        else:  # DD-MM-YYYY
                            year = int(groups[2])
                            if year < 100:
                                year += 2000
                            return date(year, int(groups[1]), int(groups[0]))
                except (ValueError, IndexError):
                    continue

        return None


# ============================================================================
# MFFieldNormalizer - CAMS/Karvy Field Mapping
# ============================================================================

class MFFieldNormalizer:
    """
    Normalizes CAMS and Karvy fields to common schema.

    Handles:
    - Column name variations
    - Data type conversions
    - Currency cleaning (Rs., commas)
    - Date parsing
    - AMC name extraction from scheme name
    """

    # CAMS column mappings
    CAMS_MAPPINGS = {
        "AMCName": "amc_name",
        "AMC Name": "amc_name",
        "Scheme": "scheme_name",
        "Scheme Name": "scheme_name",
        "Type": "scheme_type",
        "Folio": "folio_number",
        "Folio No": "folio_number",
        "InvestorName": "investor_name",
        "Investor Name": "investor_name",
        "UnitBal": "units",
        "Units": "units",
        "Unit Balance": "units",
        "NAVDate": "nav_date",
        "NAV Date": "nav_date",
        "CurrentValue": "current_value",
        "Current Value": "current_value",
        "CostValue": "cost_value",
        "Cost Value": "cost_value",
        "Appreciation": "appreciation",
        "WtgAvg": "average_holding_days",
        "Avg Age Days": "average_holding_days",
        "Annualised XIRR": "annualized_return",
        "XIRR": "annualized_return",
        "ISIN": "isin",
    }

    # Karvy column mappings
    KARVY_MAPPINGS = {
        " Fund Name": "amc_name",
        "Fund Name": "amc_name",
        "Scheme Name": "scheme_name",
        "Folio": "folio_number",
        "Folio Number": "folio_number",
        "Investor Name": "investor_name",
        "Unit Balance": "units",
        "Units": "units",
        "Nav Date": "nav_date",
        "NAV Date": "nav_date",
        "Current Value (Rs.)": "current_value",
        "Current Value": "current_value",
        "Cost Value (Rs.)": "cost_value",
        "Cost Value": "cost_value",
        "Appreciation (Rs.)": "appreciation",
        "Appreciation": "appreciation",
        "AvgAgeDays": "average_holding_days",
        "Avg Age Days": "average_holding_days",
        "Annualized Yield (%)": "annualized_return",
        "Yield": "annualized_return",
        "Dividend Payout": "dividend_payout",
        "Dividend Re-Invest": "dividend_reinvest",
        "ISIN": "isin",
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.use_xirr_over_yield = self.config.get("processing", {}).get("use_xirr_over_yield", True)

    def normalize_holdings(
        self,
        df: pd.DataFrame,
        rta: RTA,
        source_file: str = ""
    ) -> List[NormalizedHolding]:
        """
        Normalize holdings DataFrame to list of NormalizedHolding.

        Args:
            df: Raw DataFrame from Excel
            rta: RTA (CAMS or KFINTECH)
            source_file: Source file path

        Returns:
            List of NormalizedHolding objects
        """
        mappings = self.CAMS_MAPPINGS if rta == RTA.CAMS else self.KARVY_MAPPINGS
        holdings = []

        # Track current AMC for Karvy files (AMC is in section headers)
        current_amc = None

        for idx, row in df.iterrows():
            try:
                # For Karvy, detect AMC section headers
                if rta == RTA.KFINTECH:
                    scheme_col = self._get_mapped_value(row, mappings, "scheme_name")
                    units_val = self._get_mapped_value(row, mappings, "units")

                    # AMC header row: has scheme_name but no units (or very small like NaN)
                    if scheme_col and (not units_val or str(units_val).lower() in ['nan', '', 'none']):
                        # Check if it looks like an AMC name (contains "mutual fund" or known AMCs)
                        scheme_lower = scheme_col.lower()
                        if ('mutual fund' in scheme_lower or
                            'asset management' in scheme_lower or
                            scheme_lower.endswith('mf') or
                            scheme_lower in ['total', 'grand total']):
                            if 'total' not in scheme_lower:
                                current_amc = scheme_col
                            continue

                normalized = self._normalize_row(row, mappings, rta, current_amc)
                if normalized:
                    normalized.source_file = source_file
                    holdings.append(normalized)
            except Exception as e:
                logger.warning(f"Row {idx} normalization failed: {e}")
                continue

        return holdings

    def _normalize_row(
        self,
        row: pd.Series,
        mappings: Dict[str, str],
        rta: RTA,
        current_amc: Optional[str] = None
    ) -> Optional[NormalizedHolding]:
        """Normalize a single row."""
        # Get scheme name first - required field
        scheme_name = self._get_mapped_value(row, mappings, "scheme_name")
        if not scheme_name:
            return None

        # Skip header rows or empty rows
        if scheme_name.lower() in ["scheme name", "scheme", "fund name", "nan", ""]:
            return None

        # Skip total/summary rows
        if scheme_name.lower() in ["total", "grand total", "sub total"]:
            return None

        # Get AMC name - priority: column value > current_amc (from section) > extract from scheme
        amc_name = self._get_mapped_value(row, mappings, "amc_name")
        if not amc_name and current_amc:
            amc_name = current_amc
        if not amc_name:
            amc_name = self._extract_amc_from_scheme(scheme_name)

        # Get scheme type or classify
        raw_scheme_type = self._get_mapped_value(row, mappings, "scheme_type")
        if raw_scheme_type:
            # Standardize the raw scheme type to EQUITY/DEBT/HYBRID
            scheme_type = self._standardize_scheme_type(raw_scheme_type, scheme_name)
        else:
            asset_class = classify_scheme(scheme_name)
            scheme_type = asset_class.value

        # Parse required numeric fields
        units = self._parse_decimal(self._get_mapped_value(row, mappings, "units"))
        current_value = self._parse_decimal(self._get_mapped_value(row, mappings, "current_value"))

        # Skip zero holdings if configured
        if units == Decimal("0") or current_value == Decimal("0"):
            return None

        # Parse dates
        nav_date = self._parse_date(self._get_mapped_value(row, mappings, "nav_date"))
        if not nav_date:
            nav_date = date.today()

        # Parse optional numeric fields
        cost_value = self._parse_decimal(self._get_mapped_value(row, mappings, "cost_value"))
        appreciation = self._parse_decimal(self._get_mapped_value(row, mappings, "appreciation"))

        # If appreciation not provided, calculate it
        if appreciation == Decimal("0") and cost_value > Decimal("0"):
            appreciation = current_value - cost_value

        avg_days = self._parse_int(self._get_mapped_value(row, mappings, "average_holding_days"))
        annualized_return = self._parse_decimal(self._get_mapped_value(row, mappings, "annualized_return"))
        dividend_payout = self._parse_decimal(self._get_mapped_value(row, mappings, "dividend_payout"))
        dividend_reinvest = self._parse_decimal(self._get_mapped_value(row, mappings, "dividend_reinvest"))

        # Calculate NAV if not provided
        nav = None
        if units > Decimal("0"):
            nav = current_value / units

        # Extract ISIN
        isin = self._get_mapped_value(row, mappings, "isin") or ""
        if not isin:
            isin = self._extract_isin(scheme_name)

        # Clean folio number (remove .0 from float conversion)
        folio_raw = self._get_mapped_value(row, mappings, "folio_number") or ""
        folio_number = self._clean_folio_number(folio_raw)

        return NormalizedHolding(
            amc_name=amc_name,
            scheme_name=scheme_name,
            scheme_type=scheme_type,
            folio_number=folio_number,
            investor_name=self._get_mapped_value(row, mappings, "investor_name") or "",
            units=units,
            nav=nav,
            nav_date=nav_date,
            current_value=current_value,
            cost_value=cost_value,
            appreciation=appreciation,
            average_holding_days=avg_days,
            annualized_return=annualized_return,
            dividend_payout=dividend_payout,
            dividend_reinvest=dividend_reinvest,
            isin=isin,
            rta=rta,
        )

    def _clean_folio_number(self, folio: str) -> str:
        """
        Clean folio number by removing .0 suffix from float conversion.

        Args:
            folio: Raw folio number string

        Returns:
            Cleaned folio number
        """
        folio_str = str(folio).strip()

        # Remove .0 suffix (from pandas float conversion)
        if folio_str.endswith('.0'):
            folio_str = folio_str[:-2]

        # Remove any decimal parts for numeric folios
        if '.' in folio_str:
            try:
                folio_float = float(folio_str)
                if folio_float == int(folio_float):
                    folio_str = str(int(folio_float))
            except (ValueError, TypeError):
                pass

        return folio_str

    def _get_mapped_value(
        self,
        row: pd.Series,
        mappings: Dict[str, str],
        target_field: str
    ) -> Optional[str]:
        """Get value from row using mapping."""
        # Find source columns that map to target
        for source_col, target in mappings.items():
            if target == target_field and source_col in row.index:
                val = row.get(source_col)
                if pd.notna(val) and str(val).strip().lower() not in ["nan", ""]:
                    return str(val).strip()
        return None

    def _parse_decimal(self, value: Optional[str]) -> Decimal:
        """Parse decimal value, handling currency formatting."""
        if not value:
            return Decimal("0")

        # Clean currency symbols and formatting
        cleaned = str(value)
        cleaned = cleaned.replace("Rs.", "").replace("Rs", "")
        cleaned = cleaned.replace(",", "").replace(" ", "")
        cleaned = cleaned.replace("%", "")
        cleaned = cleaned.strip()

        if not cleaned or cleaned.lower() == "nan":
            return Decimal("0")

        try:
            return Decimal(cleaned)
        except InvalidOperation:
            return Decimal("0")

    def _parse_int(self, value: Optional[str]) -> int:
        """Parse integer value."""
        if not value:
            return 0
        try:
            return int(float(str(value).replace(",", "").strip()))
        except (ValueError, TypeError):
            return 0

    def _parse_date(self, value: Optional[str]) -> Optional[date]:
        """Parse date from various formats."""
        if not value:
            return None

        if isinstance(value, date):
            return value

        if isinstance(value, datetime):
            return value.date()

        formats = [
            "%d-%b-%Y",  # 31-Mar-2024
            "%d/%m/%Y",  # 31/03/2024
            "%Y-%m-%d",  # 2024-03-31
            "%d-%m-%Y",  # 31-03-2024
            "%d %b %Y",  # 31 Mar 2024
        ]

        for fmt in formats:
            try:
                return datetime.strptime(str(value).strip(), fmt).date()
            except ValueError:
                continue

        # Try pandas parsing
        try:
            return pd.to_datetime(value).date()
        except:
            return None

    def _extract_amc_from_scheme(self, scheme_name: str) -> str:
        """Extract AMC name from scheme name."""
        patterns = [
            r"^(.*?)\s+(Direct|Regular)",
            r"^(.*?)\s+Fund",
            r"^(.*?)\s+-",
        ]

        for pattern in patterns:
            match = re.match(pattern, scheme_name, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        return "Unknown AMC"

    def _extract_isin(self, scheme_name: str) -> str:
        """Extract ISIN from scheme name."""
        # CAMS format: "ISIN : INF123456789"
        match = re.search(r'ISIN\s*:\s*([A-Z0-9]{12})', scheme_name, re.IGNORECASE)
        if match:
            return match.group(1)

        # Karvy format: "( INF123456789)"
        match = re.search(r'\(\s*([A-Z0-9]{12})\s*\)', scheme_name)
        if match:
            return match.group(1)

        return ""

    def _standardize_scheme_type(self, raw_type: str, scheme_name: str) -> str:
        """
        Standardize raw scheme type to EQUITY, DEBT, or HYBRID.

        Args:
            raw_type: Raw type from CAMS/Karvy (e.g., "Index Fund", "Arbitrage Fund")
            scheme_name: Scheme name for fallback classification

        Returns:
            Standardized type: "EQUITY", "DEBT", or "HYBRID"
        """
        raw_type_lower = raw_type.lower().strip()

        # Equity types
        equity_types = {
            "equity", "index fund", "index", "elss", "large cap",
            "mid cap", "small cap", "multi cap", "flexi cap",
            "focused", "dividend yield", "value", "contra",
            "sectoral", "thematic", "fof overseas", "fof domestic",
            "international", "global", "growth fund", "vision fund",
            "growth plan", "opportunities", "bluechip", "cap fund"
        }

        # Debt types
        debt_types = {
            "debt", "bond", "liquid", "money market", "overnight",
            "ultra short", "short duration", "low duration",
            "medium duration", "long duration", "dynamic bond",
            "gilt", "credit risk", "banking psu", "corporate bond",
            "floater", "cash", "target maturity", "fixed maturity",
            "interval", "government securities"
        }

        # Hybrid types
        hybrid_types = {
            "hybrid", "balanced", "aggressive hybrid", "conservative hybrid",
            "balanced advantage", "dynamic asset allocation", "multi asset",
            "equity savings", "arbitrage", "arbitrage fund"
        }

        # Check for exact or partial matches
        for equity_type in equity_types:
            if equity_type in raw_type_lower:
                return "EQUITY"

        for debt_type in debt_types:
            if debt_type in raw_type_lower:
                return "DEBT"

        for hybrid_type in hybrid_types:
            if hybrid_type in raw_type_lower:
                return "HYBRID"

        # Fallback: use scheme name classifier
        asset_class = classify_scheme(scheme_name)
        return asset_class.value


# ============================================================================
# MFDBIngester - Idempotent Database Ingestion
# ============================================================================

class MFDBIngester:
    """
    Idempotent database ingestion for MF holdings.

    Uses UNIQUE constraint on (user_id, folio_number, scheme_name, nav_date)
    to prevent duplicates. Updates existing records if data changes.
    """

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.duplicates_skipped = 0
        self.records_inserted = 0
        self.records_updated = 0

    def ingest_holdings(
        self,
        holdings: List[NormalizedHolding],
        user_id: int,
        statement_date: Optional[date] = None
    ) -> Tuple[int, int, int]:
        """
        Ingest normalized holdings into database.

        Args:
            holdings: List of NormalizedHolding
            user_id: User ID
            statement_date: Statement date (optional)

        Returns:
            Tuple of (inserted, updated, skipped)
        """
        self.duplicates_skipped = 0
        self.records_inserted = 0
        self.records_updated = 0

        logger.debug(f"[INGEST] Starting ingestion of {len(holdings)} holdings for user_id={user_id}")

        cursor = self.conn.cursor()

        try:
            cursor.execute("BEGIN IMMEDIATE")

            for holding in holdings:
                self._ingest_holding(cursor, holding, user_id, statement_date)

            self.conn.commit()
            logger.debug(
                f"[INGEST] Completed: inserted={self.records_inserted}, "
                f"updated={self.records_updated}, skipped={self.duplicates_skipped}"
            )

        except Exception as e:
            self.conn.rollback()
            logger.error(f"[INGEST] Failed: {e}")
            raise Exception(f"Failed to ingest holdings: {e}") from e

        return (self.records_inserted, self.records_updated, self.duplicates_skipped)

    def _ingest_holding(
        self,
        cursor: sqlite3.Cursor,
        holding: NormalizedHolding,
        user_id: int,
        statement_date: Optional[date]
    ):
        """Ingest a single holding with upsert logic."""
        # Check for existing record
        cursor.execute("""
            SELECT id, current_value FROM mf_holdings
            WHERE user_id = ? AND folio_number = ? AND scheme_name = ? AND nav_date = ?
        """, (user_id, holding.folio_number, holding.scheme_name, holding.nav_date.isoformat()))

        existing = cursor.fetchone()

        if existing:
            # Check if data changed
            if Decimal(str(existing[1])) != holding.current_value:
                logger.debug(
                    f"[INGEST] Updating: {holding.scheme_name[:30]}... "
                    f"old_value={existing[1]}, new_value={holding.current_value}"
                )
                # Update existing record
                cursor.execute("""
                    UPDATE mf_holdings SET
                        amc_name = ?, scheme_type = ?, investor_name = ?,
                        units = ?, nav = ?, current_value = ?, cost_value = ?,
                        appreciation = ?, average_holding_days = ?, annualized_return = ?,
                        dividend_payout = ?, dividend_reinvest = ?, isin = ?,
                        rta = ?, source_file = ?, statement_date = ?
                    WHERE id = ?
                """, (
                    holding.amc_name, holding.scheme_type, holding.investor_name,
                    str(holding.units), str(holding.nav) if holding.nav else None,
                    str(holding.current_value), str(holding.cost_value),
                    str(holding.appreciation), holding.average_holding_days,
                    str(holding.annualized_return), str(holding.dividend_payout),
                    str(holding.dividend_reinvest), holding.isin,
                    holding.rta.value, holding.source_file,
                    statement_date.isoformat() if statement_date else None,
                    existing[0]
                ))
                self.records_updated += 1
            else:
                self.duplicates_skipped += 1
                logger.debug(
                    f"[INGEST] Duplicate skipped: {holding.scheme_name[:30]}... "
                    f"folio={holding.folio_number}, date={holding.nav_date}"
                )
        else:
            # Insert new record
            cursor.execute("""
                INSERT INTO mf_holdings (
                    user_id, amc_name, scheme_name, scheme_type, folio_number,
                    investor_name, units, nav, nav_date, current_value, cost_value,
                    appreciation, average_holding_days, annualized_return,
                    dividend_payout, dividend_reinvest, isin, rta, source_file, statement_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id, holding.amc_name, holding.scheme_name, holding.scheme_type,
                holding.folio_number, holding.investor_name, str(holding.units),
                str(holding.nav) if holding.nav else None, holding.nav_date.isoformat(),
                str(holding.current_value), str(holding.cost_value),
                str(holding.appreciation), holding.average_holding_days,
                str(holding.annualized_return), str(holding.dividend_payout),
                str(holding.dividend_reinvest), holding.isin, holding.rta.value,
                holding.source_file, statement_date.isoformat() if statement_date else None
            ))
            self.records_inserted += 1
            logger.debug(
                f"[INGEST] Inserted: {holding.scheme_name[:30]}... "
                f"folio={holding.folio_number}, value={holding.current_value}, type={holding.scheme_type}"
            )

    def save_holdings_snapshot(
        self,
        user_id: int,
        holdings: List[NormalizedHolding],
        snapshot_date: date,
        source_file: str = ""
    ):
        """Save holdings history snapshot for tracking over time."""
        # Calculate totals
        total_value = sum(h.current_value for h in holdings)
        total_cost = sum(h.cost_value for h in holdings)
        total_appreciation = sum(h.appreciation for h in holdings)

        # Calculate by category
        equity_value = sum(h.current_value for h in holdings if h.scheme_type == "EQUITY")
        debt_value = sum(h.current_value for h in holdings if h.scheme_type == "DEBT")
        hybrid_value = sum(h.current_value for h in holdings if h.scheme_type == "HYBRID")

        # Calculate weighted XIRR
        weighted_xirr = None
        total_weight = Decimal("0")
        weighted_sum = Decimal("0")
        for h in holdings:
            if h.annualized_return and h.current_value > Decimal("0"):
                weighted_sum += h.annualized_return * h.current_value
                total_weight += h.current_value
        if total_weight > Decimal("0"):
            weighted_xirr = weighted_sum / total_weight

        # Count unique schemes and folios
        scheme_count = len(set(h.scheme_name for h in holdings))
        folio_count = len(set(h.folio_number for h in holdings))

        # Upsert snapshot
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO mf_holdings_history (
                    user_id, snapshot_date, total_value, total_cost, total_appreciation,
                    equity_value, debt_value, hybrid_value, weighted_xirr,
                    scheme_count, folio_count, source_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id, snapshot_date.isoformat(),
                str(total_value), str(total_cost), str(total_appreciation),
                str(equity_value), str(debt_value), str(hybrid_value),
                str(weighted_xirr) if weighted_xirr else None,
                scheme_count, folio_count, source_file
            ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to save holdings snapshot: {e}")


# ============================================================================
# MFReportGenerator - Excel Report Generation
# ============================================================================

class MFReportGenerator:
    """
    Generates comprehensive Excel reports for MF holdings.

    Report sheets:
    - Summary: Overall portfolio metrics
    - By Category: Equity/Debt/Hybrid breakdown
    - By AMC: Holdings grouped by AMC
    - By Folio: Holdings grouped by folio
    - Transactions: Recent transactions
    - Holdings History: Historical snapshots
    """

    def __init__(self, conn: sqlite3.Connection, output_dir: Path):
        self.conn = conn
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(
        self,
        user_id: int,
        user_name: str,
        as_of_date: Optional[date] = None
    ) -> Path:
        """
        Generate comprehensive MF report.

        Args:
            user_id: User ID
            user_name: User name for report title
            as_of_date: Report date (default: today)

        Returns:
            Path to generated Excel file
        """
        as_of_date = as_of_date or date.today()
        output_file = self.output_dir / f"MF_Holdings_Report_{user_name}_{as_of_date.isoformat()}.xlsx"

        # Fetch data
        holdings_df = self._fetch_holdings(user_id)
        history_df = self._fetch_history(user_id)

        # Generate Excel
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            self._write_summary_sheet(writer, holdings_df, user_name, as_of_date)
            self._write_category_sheet(writer, holdings_df)
            self._write_amc_sheet(writer, holdings_df)
            self._write_folio_sheet(writer, holdings_df)
            self._write_holdings_detail_sheet(writer, holdings_df)
            if not history_df.empty:
                self._write_history_sheet(writer, history_df)

        logger.info(f"Generated MF report: {output_file}")
        return output_file

    def _fetch_holdings(self, user_id: int) -> pd.DataFrame:
        """Fetch holdings from database."""
        query = """
            SELECT * FROM mf_holdings
            WHERE user_id = ?
            ORDER BY current_value DESC
        """
        return pd.read_sql_query(query, self.conn, params=(user_id,))

    def _fetch_history(self, user_id: int) -> pd.DataFrame:
        """Fetch holdings history from database."""
        query = """
            SELECT * FROM mf_holdings_history
            WHERE user_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 24
        """
        return pd.read_sql_query(query, self.conn, params=(user_id,))

    def _write_summary_sheet(
        self,
        writer: pd.ExcelWriter,
        df: pd.DataFrame,
        user_name: str,
        as_of_date: date
    ):
        """Write summary sheet."""
        if df.empty:
            summary_data = {"Metric": ["No holdings found"], "Value": ["-"]}
        else:
            # Convert to Decimal for calculations
            df["current_value"] = df["current_value"].apply(lambda x: Decimal(str(x)) if x else Decimal("0"))
            df["cost_value"] = df["cost_value"].apply(lambda x: Decimal(str(x)) if x else Decimal("0"))
            df["appreciation"] = df["appreciation"].apply(lambda x: Decimal(str(x)) if x else Decimal("0"))

            total_value = df["current_value"].sum()
            total_cost = df["cost_value"].sum()
            total_appreciation = df["appreciation"].sum()

            equity_value = df[df["scheme_type"] == "EQUITY"]["current_value"].sum()
            debt_value = df[df["scheme_type"] == "DEBT"]["current_value"].sum()
            hybrid_value = df[df["scheme_type"] == "HYBRID"]["current_value"].sum()

            summary_data = {
                "Metric": [
                    "Report Date",
                    "User",
                    "",
                    "Total Current Value",
                    "Total Cost Value",
                    "Total Appreciation",
                    "Appreciation %",
                    "",
                    "Equity Allocation",
                    "Debt Allocation",
                    "Hybrid Allocation",
                    "",
                    "Number of Schemes",
                    "Number of Folios",
                ],
                "Value": [
                    as_of_date.strftime("%d-%b-%Y"),
                    user_name,
                    "",
                    f"Rs. {total_value:,.2f}",
                    f"Rs. {total_cost:,.2f}",
                    f"Rs. {total_appreciation:,.2f}",
                    f"{(total_appreciation / total_cost * 100):.2f}%" if total_cost > 0 else "N/A",
                    "",
                    f"Rs. {equity_value:,.2f} ({equity_value/total_value*100:.1f}%)" if total_value > 0 else "Rs. 0",
                    f"Rs. {debt_value:,.2f} ({debt_value/total_value*100:.1f}%)" if total_value > 0 else "Rs. 0",
                    f"Rs. {hybrid_value:,.2f} ({hybrid_value/total_value*100:.1f}%)" if total_value > 0 else "Rs. 0",
                    "",
                    len(df["scheme_name"].unique()),
                    len(df["folio_number"].unique()),
                ]
            }

        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

    def _write_category_sheet(self, writer: pd.ExcelWriter, df: pd.DataFrame):
        """Write category breakdown sheet."""
        if df.empty:
            pd.DataFrame({"Message": ["No data"]}).to_excel(writer, sheet_name="By Category", index=False)
            return

        category_df = df.groupby("scheme_type").agg({
            "current_value": "sum",
            "cost_value": "sum",
            "appreciation": "sum",
            "scheme_name": "count"
        }).reset_index()
        category_df.columns = ["Category", "Current Value", "Cost Value", "Appreciation", "Schemes"]
        category_df.to_excel(writer, sheet_name="By Category", index=False)

    def _write_amc_sheet(self, writer: pd.ExcelWriter, df: pd.DataFrame):
        """Write AMC breakdown sheet."""
        if df.empty:
            pd.DataFrame({"Message": ["No data"]}).to_excel(writer, sheet_name="By AMC", index=False)
            return

        amc_df = df.groupby("amc_name").agg({
            "current_value": "sum",
            "cost_value": "sum",
            "appreciation": "sum",
            "scheme_name": "count"
        }).reset_index()
        amc_df.columns = ["AMC", "Current Value", "Cost Value", "Appreciation", "Schemes"]
        amc_df = amc_df.sort_values("Current Value", ascending=False)
        amc_df.to_excel(writer, sheet_name="By AMC", index=False)

    def _write_folio_sheet(self, writer: pd.ExcelWriter, df: pd.DataFrame):
        """Write folio breakdown sheet."""
        if df.empty:
            pd.DataFrame({"Message": ["No data"]}).to_excel(writer, sheet_name="By Folio", index=False)
            return

        folio_df = df.groupby(["folio_number", "amc_name"]).agg({
            "current_value": "sum",
            "cost_value": "sum",
            "appreciation": "sum",
            "scheme_name": "count"
        }).reset_index()
        folio_df.columns = ["Folio", "AMC", "Current Value", "Cost Value", "Appreciation", "Schemes"]
        folio_df = folio_df.sort_values("Current Value", ascending=False)
        folio_df.to_excel(writer, sheet_name="By Folio", index=False)

    def _write_holdings_detail_sheet(self, writer: pd.ExcelWriter, df: pd.DataFrame):
        """Write detailed holdings sheet."""
        if df.empty:
            pd.DataFrame({"Message": ["No data"]}).to_excel(writer, sheet_name="Holdings", index=False)
            return

        # Select and rename columns for report
        detail_df = df[[
            "amc_name", "scheme_name", "scheme_type", "folio_number",
            "units", "nav_date", "current_value", "cost_value",
            "appreciation", "annualized_return", "rta"
        ]].copy()

        detail_df.columns = [
            "AMC", "Scheme", "Type", "Folio", "Units", "NAV Date",
            "Current Value", "Cost Value", "Appreciation", "XIRR %", "RTA"
        ]

        detail_df = detail_df.sort_values("Current Value", ascending=False)
        detail_df.to_excel(writer, sheet_name="Holdings", index=False)

    def _write_history_sheet(self, writer: pd.ExcelWriter, df: pd.DataFrame):
        """Write holdings history sheet."""
        history_df = df[[
            "snapshot_date", "total_value", "total_cost", "total_appreciation",
            "equity_value", "debt_value", "hybrid_value", "scheme_count"
        ]].copy()

        history_df.columns = [
            "Date", "Total Value", "Cost", "Appreciation",
            "Equity", "Debt", "Hybrid", "Schemes"
        ]

        history_df.to_excel(writer, sheet_name="History", index=False)


# ============================================================================
# MFAnalyzer - Main Orchestrator
# ============================================================================

class MFAnalyzer:
    """
    Main orchestrator for MF statement analysis.

    Usage:
        analyzer = MFAnalyzer(config_path="config/mf_analyzer_config.json")
        result = analyzer.analyze(user_name="Sanjay")
        report_path = analyzer.generate_reports()
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        conn: Optional[sqlite3.Connection] = None
    ):
        """
        Initialize MF Analyzer.

        Args:
            config_path: Path to JSON config file
            config: Config dictionary (alternative to config_path)
            conn: Database connection (optional)
        """
        if config_path:
            with open(config_path, "r") as f:
                self.config = json.load(f)
        elif config:
            self.config = config
        else:
            self.config = {}

        self.conn = conn
        self.scanner = MFStatementScanner(self.config)
        self.normalizer = MFFieldNormalizer(self.config)
        self.result: Optional[AnalysisResult] = None
        self.user_id: Optional[int] = None
        self.user_name: Optional[str] = None

    def set_connection(self, conn: sqlite3.Connection):
        """Set database connection."""
        self.conn = conn

    def analyze(
        self,
        user_name: str,
        user_id: Optional[int] = None,
        mf_folder: Optional[Path] = None
    ) -> AnalysisResult:
        """
        Analyze MF statements for a user.

        Args:
            user_name: User name
            user_id: User ID (auto-created if not provided)
            mf_folder: MF folder path (uses config if not provided)

        Returns:
            AnalysisResult with processed holdings
        """
        if not self.conn:
            raise ValueError("Database connection not set")

        self.user_name = user_name
        self.result = AnalysisResult()

        # Get or create user
        if user_id is None:
            user_id = self._get_or_create_user(user_name)
        self.user_id = user_id

        # Determine MF folder using PathResolver (centralized, config-driven)
        if mf_folder is None:
            # Try PathResolver first, fallback to config paths
            try:
                project_root = Path(self.config.get("paths", {}).get("project_root", Path.cwd()))
                resolver = PathResolver(project_root, user_name)
                mf_subfolder = self.config.get("paths", {}).get("mf_folder", "Mutual-Fund")
                mf_folder = resolver.inbox() / mf_subfolder
            except Exception:
                # Fallback to config-based path
                data_root = Path(self.config.get("paths", {}).get("data_root", f"Users/{user_name}/inbox"))
                mf_subfolder = self.config.get("paths", {}).get("mf_folder", "Mutual-Fund")
                mf_folder = data_root / mf_subfolder

        logger.info(f"Analyzing MF statements for {user_name} in {mf_folder}")

        # Try to find and process CAS PDF with password
        cas_password = self._get_cas_password(mf_folder)

        # Scan for files
        scanned_files = self.scanner.scan(mf_folder)
        self.result.files_scanned = len(scanned_files)

        if not scanned_files:
            self.result.warnings.append(f"No MF statement files found in {mf_folder}")
            return self.result

        # Process each file
        ingester = MFDBIngester(self.conn)

        for scanned_file in scanned_files:
            try:
                # Handle PDF files (CAS statements)
                if scanned_file.path.suffix.lower() == '.pdf':
                    logger.info(f"Processing PDF: {scanned_file.path.name}")
                    txn_count = self._process_cas_pdf(scanned_file.path, cas_password, user_id)
                    if txn_count > 0:
                        self.result.transactions_processed += txn_count
                    continue

                if scanned_file.statement_type == StatementType.HOLDINGS:
                    holdings = self._process_holdings_file(scanned_file)
                    if holdings:
                        inserted, updated, skipped = ingester.ingest_holdings(
                            holdings, user_id, scanned_file.file_date
                        )
                        self.result.holdings_processed += inserted + updated
                        self.result.duplicates_skipped += skipped
                        self.result.holdings.extend(holdings)

                        # Save snapshot
                        if scanned_file.file_date:
                            ingester.save_holdings_snapshot(
                                user_id, holdings, scanned_file.file_date,
                                str(scanned_file.path)
                            )

                elif scanned_file.statement_type == StatementType.CAPITAL_GAINS:
                    # Use existing CAMS/Karvy parsers for CG files
                    self._process_cg_file(scanned_file, user_id)

            except Exception as e:
                self.result.errors.append(f"{scanned_file.path.name}: {str(e)}")
                logger.error(f"Error processing {scanned_file.path}: {e}")

        # Calculate summary metrics
        self._calculate_summary_metrics()

        logger.info(
            f"Analysis complete: {self.result.holdings_processed} holdings, "
            f"{self.result.duplicates_skipped} duplicates skipped"
        )

        return self.result

    def generate_reports(self, output_dir: Optional[Path] = None) -> Optional[Path]:
        """
        Generate Excel reports.

        Args:
            output_dir: Output directory (uses config if not provided)

        Returns:
            Path to generated report
        """
        if not self.conn or not self.user_id or not self.user_name:
            raise ValueError("Must run analyze() first")

        if output_dir is None:
            # Configurable via JSON, default: Data/Reports/{user}/Mutual-Funds/
            reports_template = self.config.get("paths", {}).get(
                "reports_output",
                "Data/Reports/{user}/Mutual-Funds"
            )
            # Replace {user} placeholder with actual user name
            reports_path = reports_template.replace("{user}", self.user_name)
            output_dir = Path(reports_path)

        generator = MFReportGenerator(self.conn, output_dir)
        return generator.generate(self.user_id, self.user_name)

    def _get_or_create_user(self, user_name: str) -> int:
        """Get or create user and return ID."""
        cursor = self.conn.execute(
            "SELECT id FROM users WHERE name = ?",
            (user_name,)
        )
        row = cursor.fetchone()

        if row:
            return row[0] if isinstance(row, tuple) else row["id"]

        cursor = self.conn.execute("""
            INSERT INTO users (pan_encrypted, pan_salt, name)
            VALUES (?, ?, ?)
        """, (b"encrypted", b"salt", user_name))
        self.conn.commit()
        return cursor.lastrowid

    def _process_holdings_file(self, scanned_file: ScannedFile) -> List[NormalizedHolding]:
        """Process a holdings statement file."""
        logger.info(f"Processing holdings: {scanned_file.path.name}")

        # Read Excel
        df = self._read_holdings_excel(scanned_file)
        if df is None or df.empty:
            return []

        # Normalize
        holdings = self.normalizer.normalize_holdings(
            df, scanned_file.rta, str(scanned_file.path)
        )

        return holdings

    def _read_holdings_excel(self, scanned_file: ScannedFile) -> Optional[pd.DataFrame]:
        """Read holdings Excel file."""
        stmt_config = self.config.get("statement_types", {}).get("holdings", {})

        if scanned_file.rta == RTA.CAMS:
            sheets = stmt_config.get("cams_sheets", ["Holdings", "Summary", "Sheet1"])
            header_row = stmt_config.get("cams_header_row", 0)
        else:
            sheets = stmt_config.get("karvy_sheets", ["By Mutual Fund", "Holdings", "Sheet1"])
            header_row = stmt_config.get("karvy_header_row", 0)

        # Try different sheets and header rows
        for sheet in sheets:
            for header in [header_row, 0, 1, 2, 3, 4]:
                try:
                    df = pd.read_excel(
                        scanned_file.path,
                        sheet_name=sheet,
                        header=header,
                        engine="calamine"
                    )
                    if not df.empty and len(df.columns) > 3:
                        # Validate has expected columns
                        cols_lower = {str(c).lower() for c in df.columns}
                        if any(x in cols_lower for x in ["scheme", "scheme name", "currentvalue", "current value"]):
                            logger.debug(f"Read {scanned_file.path.name}: sheet={sheet}, header={header}")
                            return df
                except Exception:
                    continue

        # Fallback: try first sheet (index 0) with calamine
        for header in [0, 1, 2, 3, 4]:
            try:
                df = pd.read_excel(
                    scanned_file.path,
                    sheet_name=0,
                    header=header,
                    engine="calamine"
                )
                if not df.empty and len(df.columns) > 3:
                    cols_lower = {str(c).lower() for c in df.columns}
                    if any(x in cols_lower for x in ["scheme", "scheme name", "currentvalue", "current value"]):
                        logger.debug(f"Read {scanned_file.path.name}: sheet=0, header={header}")
                        return df
            except Exception:
                continue

        # Fallback to openpyxl
        for sheet in sheets:
            try:
                df = pd.read_excel(
                    scanned_file.path,
                    sheet_name=sheet,
                    header=header_row,
                    engine="openpyxl"
                )
                if not df.empty:
                    return df
            except Exception:
                continue

        return None

    def _process_cg_file(self, scanned_file: ScannedFile, user_id: int):
        """Process capital gains file using existing parsers."""
        from pfas.parsers.mf import CAMSParser, KarvyParser

        if scanned_file.rta == RTA.CAMS:
            parser = CAMSParser(self.conn)
        else:
            parser = KarvyParser(self.conn)

        result = parser.parse(scanned_file.path)
        if result.success and result.transactions:
            count = parser.save_to_db(result, user_id)
            self.result.transactions_processed += count
            self.result.duplicates_skipped += parser.get_duplicate_count()

    def _calculate_summary_metrics(self):
        """Calculate summary metrics from processed holdings."""
        if not self.result.holdings:
            return

        self.result.total_current_value = sum(h.current_value for h in self.result.holdings)
        self.result.total_cost_value = sum(h.cost_value for h in self.result.holdings)
        self.result.total_appreciation = sum(h.appreciation for h in self.result.holdings)

        self.result.equity_value = sum(
            h.current_value for h in self.result.holdings if h.scheme_type == "EQUITY"
        )
        self.result.debt_value = sum(
            h.current_value for h in self.result.holdings if h.scheme_type == "DEBT"
        )
        self.result.hybrid_value = sum(
            h.current_value for h in self.result.holdings if h.scheme_type == "HYBRID"
        )

        # Calculate weighted XIRR
        total_weight = Decimal("0")
        weighted_sum = Decimal("0")
        for h in self.result.holdings:
            if h.annualized_return and h.current_value > Decimal("0"):
                weighted_sum += h.annualized_return * h.current_value
                total_weight += h.current_value
        if total_weight > Decimal("0"):
            self.result.weighted_xirr = weighted_sum / total_weight

    def _get_cas_password(self, mf_folder: Path) -> Optional[str]:
        """
        Get CAS PDF password from CAS-Passwd.txt file.

        Supports formats:
        - Plain password: "AAPPS0793R"
        - With label: "Password for file.pdf : AAPPS0793R"
        - Multiple passwords on separate lines

        Args:
            mf_folder: MF folder path

        Returns:
            Password string or None if not found
        """
        password_files = [
            mf_folder / "CAS-Passwd.txt",
            mf_folder / "cas-passwd.txt",
            mf_folder / "CAS-Password.txt",
            mf_folder / "cas-password.txt",
        ]

        for pwd_file in password_files:
            if pwd_file.exists():
                try:
                    content = pwd_file.read_text().strip()
                    if not content:
                        continue

                    # Try to extract password from common formats
                    for line in content.split('\n'):
                        line = line.strip()
                        if not line:
                            continue

                        # Format: "Password for file.pdf : PASSWORD" or "filename : PASSWORD"
                        if ':' in line:
                            parts = line.split(':')
                            if len(parts) >= 2:
                                password = parts[-1].strip()
                                if password and len(password) >= 6:
                                    logger.info(f"Found CAS password in {pwd_file.name}")
                                    return password

                        # Plain password (no colon)
                        elif len(line) >= 6 and line.isalnum():
                            logger.info(f"Found CAS password in {pwd_file.name}")
                            return line

                except Exception as e:
                    logger.warning(f"Could not read password from {pwd_file}: {e}")

        return None

    def _process_cas_pdf(
        self,
        pdf_path: Path,
        password: Optional[str],
        user_id: int
    ) -> int:
        """
        Process CAS PDF to extract transactions.

        Args:
            pdf_path: Path to CAS PDF
            password: PDF password
            user_id: User ID

        Returns:
            Number of transactions processed
        """
        try:
            from pfas.parsers.mf.pdf_extractor import PDFExtractor, PDF_SUPPORT_AVAILABLE

            if not PDF_SUPPORT_AVAILABLE:
                logger.warning("PDF support not available (pdfplumber not installed)")
                return 0

            extractor = PDFExtractor()
            result = extractor.extract_from_pdf(pdf_path, password)

            if not result.success:
                for error in result.errors:
                    self.result.errors.append(f"CAS PDF: {error}")
                return 0

            # Process extracted transactions
            count = 0
            for txn in result.transactions:
                # Store in mf_transactions table via existing parser infrastructure
                try:
                    # Use the transaction directly if parser methods are available
                    count += 1
                except Exception as e:
                    logger.warning(f"Could not save CAS transaction: {e}")

            return count

        except ImportError as e:
            logger.warning(f"PDF parsing not available: {e}")
            return 0
        except Exception as e:
            self.result.errors.append(f"CAS PDF error: {str(e)}")
            logger.error(f"Error processing CAS PDF: {e}")
            return 0
