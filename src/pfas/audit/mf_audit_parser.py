"""
MF Audit Parser - Parse holdings/gains Excel files for reconciliation.

Extracts holding snapshots and capital gains data from secondary statements
for comparison against database records.
"""

import logging
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class HoldingRecord:
    """
    Represents a holding record from statement.

    Used for reconciliation with database mf_holdings table.
    """
    scheme_name: str
    folio_number: str
    units: Decimal
    nav: Decimal
    nav_date: date
    current_value: Decimal
    cost_value: Optional[Decimal] = None
    unrealized_gain: Optional[Decimal] = None
    stcg: Decimal = Decimal("0")
    ltcg: Decimal = Decimal("0")
    amc_name: Optional[str] = None
    isin: Optional[str] = None
    rta: str = "UNKNOWN"


@dataclass
class GainsRecord:
    """
    Represents a capital gains record from statement.

    Used for reconciliation with mf_transactions capital gains.
    """
    scheme_name: str
    folio_number: str
    redemption_date: date
    units_redeemed: Decimal
    redemption_amount: Decimal
    purchase_date: Optional[date] = None
    purchase_amount: Optional[Decimal] = None
    stcg: Decimal = Decimal("0")
    ltcg: Decimal = Decimal("0")
    holding_period_days: Optional[int] = None


@dataclass
class AuditData:
    """Result of parsing audit/holdings file."""
    holdings: List[HoldingRecord] = field(default_factory=list)
    gains: List[GainsRecord] = field(default_factory=list)
    source_file: str = ""
    statement_date: Optional[date] = None
    rta: str = "UNKNOWN"
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Return True if parsing had no errors."""
        return len(self.errors) == 0

    @property
    def total_value(self) -> Decimal:
        """Calculate total holdings value."""
        return sum(h.current_value for h in self.holdings)

    @property
    def total_cost(self) -> Decimal:
        """Calculate total cost basis."""
        return sum(h.cost_value or Decimal("0") for h in self.holdings)


class MFAuditParser:
    """
    Parser for MF holdings and capital gains audit files.

    Supports:
    - CAMS Holdings Statement Excel
    - CAMS Capital Gains Excel
    - Karvy Holdings Statement Excel
    - Generic holdings Excel with standard columns

    Usage:
        parser = MFAuditParser()
        data = parser.parse(Path("holdings.xlsx"))

        for holding in data.holdings:
            print(f"{holding.scheme_name}: {holding.units} units")
    """

    # Column mappings for different formats
    HOLDINGS_COLUMNS = {
        'scheme_name': ['Scheme Name', 'scheme_name', 'SCHEME NAME', 'Fund Name'],
        'folio_number': ['Folio No', 'Folio Number', 'folio_number', 'FOLIO NO'],
        'units': ['Units', 'units', 'Current Units', 'Closing Units', 'Balance Units'],
        'nav': ['NAV', 'nav', 'Current NAV', 'Price'],
        'nav_date': ['NAV Date', 'nav_date', 'As on Date', 'Date'],
        'current_value': ['Current Value', 'Market Value', 'current_value', 'Amount'],
        'cost_value': ['Cost Value', 'Purchase Value', 'cost_value', 'Invested Amount'],
        'amc_name': ['AMC Name', 'amc_name', 'Fund House', 'AMC'],
        'isin': ['ISIN', 'isin'],
    }

    GAINS_COLUMNS = {
        'scheme_name': ['Scheme Name', 'scheme_name'],
        'folio_number': ['Folio No', 'Folio Number', 'folio_number'],
        'redemption_date': ['Date', 'Redemption Date', 'Sale Date'],
        'units_redeemed': ['Units', 'Redeemed Units', 'Sale Units'],
        'redemption_amount': ['Amount', 'Sale Amount', 'Redemption Amount'],
        'purchase_date': ['Purchase Date', 'Buy Date', 'Date_1'],
        'purchase_amount': ['Purchase Amount', 'Cost', 'Buy Amount'],
        'stcg': ['Short Term', 'STCG', 'Short Term Gain'],
        'ltcg': ['Long Term', 'LTCG', 'Long Term Gain', 'Long Term Without Index'],
    }

    def __init__(self):
        """Initialize parser."""
        pass

    def parse(self, file_path: Path) -> AuditData:
        """
        Parse holdings/gains Excel file.

        Args:
            file_path: Path to Excel file

        Returns:
            AuditData with holdings and/or gains
        """
        file_path = Path(file_path)
        result = AuditData(source_file=str(file_path))

        if not file_path.exists():
            result.errors.append(f"File not found: {file_path}")
            return result

        try:
            # Detect RTA from path
            result.rta = self._detect_rta(file_path)

            # Read Excel and detect sheet types
            xl = pd.ExcelFile(file_path)

            for sheet_name in xl.sheet_names:
                try:
                    df = pd.read_excel(xl, sheet_name=sheet_name, header=None)

                    # Find header row
                    header_row = self._find_header_row(df)
                    if header_row is not None:
                        df = pd.read_excel(xl, sheet_name=sheet_name, header=header_row)

                        # Determine if holdings or gains sheet
                        if self._is_holdings_sheet(df):
                            holdings = self._parse_holdings_sheet(df, result.rta)
                            result.holdings.extend(holdings)
                        elif self._is_gains_sheet(df):
                            gains = self._parse_gains_sheet(df)
                            result.gains.extend(gains)

                except Exception as e:
                    result.warnings.append(f"Error parsing sheet '{sheet_name}': {str(e)}")

            # Extract statement date if available
            if result.holdings:
                dates = [h.nav_date for h in result.holdings if h.nav_date]
                if dates:
                    result.statement_date = max(dates)

        except Exception as e:
            result.errors.append(f"Error parsing file: {str(e)}")
            logger.exception(f"Error parsing {file_path}")

        return result

    def _detect_rta(self, file_path: Path) -> str:
        """Detect RTA from file path."""
        path_parts = [p.upper() for p in file_path.parts]

        if 'CAMS' in path_parts:
            return 'CAMS'
        elif 'KARVY' in path_parts or 'KFINTECH' in path_parts:
            return 'KFINTECH'

        return 'UNKNOWN'

    def _find_header_row(self, df: pd.DataFrame) -> Optional[int]:
        """
        Find the row containing column headers.

        Args:
            df: DataFrame read without headers

        Returns:
            Header row index or None
        """
        # Look for rows containing expected column names
        expected_cols = ['scheme', 'folio', 'units', 'nav', 'amount', 'value']

        for idx, row in df.iterrows():
            if idx > 10:  # Don't search too far
                break

            row_text = ' '.join(str(v).lower() for v in row.values if pd.notna(v))

            matches = sum(1 for col in expected_cols if col in row_text)
            if matches >= 2:  # At least 2 expected columns found
                return idx

        return None

    def _is_holdings_sheet(self, df: pd.DataFrame) -> bool:
        """Check if sheet contains holdings data."""
        cols_lower = [str(c).lower() for c in df.columns]
        holdings_indicators = ['units', 'nav', 'current value', 'market value']

        return any(ind in ' '.join(cols_lower) for ind in holdings_indicators)

    def _is_gains_sheet(self, df: pd.DataFrame) -> bool:
        """Check if sheet contains capital gains data."""
        cols_lower = [str(c).lower() for c in df.columns]
        gains_indicators = ['short term', 'long term', 'stcg', 'ltcg', 'capital gain']

        return any(ind in ' '.join(cols_lower) for ind in gains_indicators)

    def _parse_holdings_sheet(self, df: pd.DataFrame, rta: str) -> List[HoldingRecord]:
        """
        Parse holdings from DataFrame.

        Args:
            df: DataFrame with holdings data
            rta: RTA identifier

        Returns:
            List of HoldingRecord
        """
        holdings = []

        for idx, row in df.iterrows():
            try:
                scheme_name = self._get_value(row, self.HOLDINGS_COLUMNS['scheme_name'])
                if not scheme_name:
                    continue

                folio = self._get_value(row, self.HOLDINGS_COLUMNS['folio_number'])
                if not folio:
                    continue

                holding = HoldingRecord(
                    scheme_name=str(scheme_name).strip(),
                    folio_number=str(folio).strip(),
                    units=self._to_decimal(self._get_value(row, self.HOLDINGS_COLUMNS['units'])),
                    nav=self._to_decimal(self._get_value(row, self.HOLDINGS_COLUMNS['nav'])),
                    nav_date=self._to_date(self._get_value(row, self.HOLDINGS_COLUMNS['nav_date'])),
                    current_value=self._to_decimal(self._get_value(row, self.HOLDINGS_COLUMNS['current_value'])),
                    cost_value=self._to_decimal(self._get_value(row, self.HOLDINGS_COLUMNS['cost_value'])),
                    amc_name=self._get_value(row, self.HOLDINGS_COLUMNS['amc_name']),
                    isin=self._get_value(row, self.HOLDINGS_COLUMNS['isin']),
                    rta=rta
                )

                # Calculate unrealized gain if cost available
                if holding.cost_value:
                    holding.unrealized_gain = holding.current_value - holding.cost_value

                holdings.append(holding)

            except Exception as e:
                logger.debug(f"Error parsing holding row {idx}: {e}")

        return holdings

    def _parse_gains_sheet(self, df: pd.DataFrame) -> List[GainsRecord]:
        """
        Parse capital gains from DataFrame.

        Args:
            df: DataFrame with gains data

        Returns:
            List of GainsRecord
        """
        gains = []

        for idx, row in df.iterrows():
            try:
                scheme_name = self._get_value(row, self.GAINS_COLUMNS['scheme_name'])
                if not scheme_name:
                    continue

                folio = self._get_value(row, self.GAINS_COLUMNS['folio_number'])
                if not folio:
                    continue

                record = GainsRecord(
                    scheme_name=str(scheme_name).strip(),
                    folio_number=str(folio).strip(),
                    redemption_date=self._to_date(self._get_value(row, self.GAINS_COLUMNS['redemption_date'])),
                    units_redeemed=self._to_decimal(self._get_value(row, self.GAINS_COLUMNS['units_redeemed'])),
                    redemption_amount=self._to_decimal(self._get_value(row, self.GAINS_COLUMNS['redemption_amount'])),
                    purchase_date=self._to_date(self._get_value(row, self.GAINS_COLUMNS['purchase_date'])),
                    purchase_amount=self._to_decimal(self._get_value(row, self.GAINS_COLUMNS['purchase_amount'])),
                    stcg=self._to_decimal(self._get_value(row, self.GAINS_COLUMNS['stcg'])),
                    ltcg=self._to_decimal(self._get_value(row, self.GAINS_COLUMNS['ltcg'])),
                )

                gains.append(record)

            except Exception as e:
                logger.debug(f"Error parsing gains row {idx}: {e}")

        return gains

    def _get_value(self, row: pd.Series, column_names: List[str]) -> Optional[str]:
        """Get value from row trying multiple column names."""
        for col in column_names:
            if col in row.index:
                val = row.get(col)
                if pd.notna(val) and str(val).strip():
                    return str(val).strip()
        return None

    def _to_decimal(self, value) -> Decimal:
        """Convert value to Decimal."""
        if pd.isna(value) or value is None or value == '':
            return Decimal("0")
        try:
            # Remove currency symbols and commas
            cleaned = str(value).replace(',', '').replace('₹', '').replace('Rs.', '').strip()
            return Decimal(cleaned)
        except:
            return Decimal("0")

    def _to_date(self, value) -> Optional[date]:
        """Convert value to date."""
        if pd.isna(value) or value is None:
            return None
        try:
            if isinstance(value, date):
                return value
            return pd.to_datetime(value).date()
        except:
            return None


def parse_mf_holdings_excel(file_path: Path) -> AuditData:
    """
    Convenience function to parse MF holdings Excel.

    Args:
        file_path: Path to Excel file

    Returns:
        AuditData

    Example:
        data = parse_mf_holdings_excel(Path("holdings.xlsx"))
        print(f"Total holdings value: {data.total_value}")
    """
    parser = MFAuditParser()
    return parser.parse(file_path)
