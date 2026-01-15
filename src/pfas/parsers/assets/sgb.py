"""
Sovereign Gold Bonds (SGB) Module for PFAS.

Handles:
- Parse SGB holdings from Excel file
- Track semi-annual interest (2.5% p.a.)
- Capital gains on maturity (8 years) - EXEMPT
- Interest income from bank statements

New Tax Regime: SGB interest taxable at slab rate.
"""

import re
import sqlite3
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List
from pathlib import Path

import pandas as pd

from .models import SGBHolding, SGBInterest, SGBSummary


# Database schema for SGB tables
SGB_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sgb_holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    series TEXT NOT NULL,
    isin TEXT,
    issue_date DATE,
    maturity_date DATE,
    quantity INTEGER NOT NULL DEFAULT 0,
    issue_price DECIMAL(15,2) NOT NULL,
    current_price DECIMAL(15,2),
    interest_rate DECIMAL(5,2) DEFAULT 2.5,
    interest_earned DECIMAL(15,2) DEFAULT 0,
    accrued_interest DECIMAL(15,2) DEFAULT 0,
    unrealized_gain DECIMAL(15,2) DEFAULT 0,
    source_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, series)
);

CREATE TABLE IF NOT EXISTS sgb_interest (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sgb_holding_id INTEGER,
    series TEXT NOT NULL,
    payment_date DATE NOT NULL,
    financial_year TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    rate DECIMAL(5,2) DEFAULT 2.5,
    amount DECIMAL(15,2) NOT NULL,
    tds_deducted DECIMAL(15,2) DEFAULT 0,
    source TEXT DEFAULT 'BANK_STATEMENT',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(series, payment_date),
    FOREIGN KEY (sgb_holding_id) REFERENCES sgb_holdings(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_sgb_holdings_user ON sgb_holdings(user_id);
CREATE INDEX IF NOT EXISTS idx_sgb_holdings_series ON sgb_holdings(series);
CREATE INDEX IF NOT EXISTS idx_sgb_interest_fy ON sgb_interest(financial_year);
CREATE INDEX IF NOT EXISTS idx_sgb_interest_holding ON sgb_interest(sgb_holding_id);
"""


class SGBParser:
    """Parse SGB holdings from Excel file."""

    def __init__(self):
        """Initialize parser."""
        self.holdings: List[SGBHolding] = []

    def parse_file(self, file_path: str, user_id: Optional[int] = None) -> List[SGBHolding]:
        """
        Parse SGB holdings from Excel file.

        Expected format (from ICICI Direct or similar):
        - Security Name column with SGB series
        - Qty column with quantity in grams
        - Average Cost Price column
        - Interest Earned column
        - Accrued Interest column
        - Unrealized Profit/Loss column
        """
        df = pd.read_excel(file_path, header=None)

        holdings = []

        # Scan all rows for SGB series patterns
        for idx, row in df.iterrows():
            row_values = row.tolist()
            row_str = " ".join(str(v) for v in row_values)

            # Look for SGB series pattern in row
            if "Sov. Gold Bond" in row_str or "SGB" in row_str.upper():
                holding = self._parse_sgb_row(row_values, user_id, file_path)
                if holding:
                    holdings.append(holding)

        self.holdings = holdings
        return holdings

    def _parse_sgb_row(
        self,
        row_values: list,
        user_id: Optional[int],
        source_file: str
    ) -> Optional[SGBHolding]:
        """Parse a row with SGB data.

        Row format from ICICI Direct:
        [nan, +, series_name, qty, avg_cost, market_rate, nan, cost_value,
         market_value, realized_pl, interest_earned, unrealized_pl, accrued_int, action, ...]
        """
        # Find series name (contains "Sov. Gold Bond")
        series = None
        series_idx = -1
        for i, val in enumerate(row_values):
            val_str = str(val)
            if "Sov. Gold Bond" in val_str:
                series = val_str.strip()
                series_idx = i
                break

        if not series:
            return None

        # Skip TOTAL row
        if "TOTAL" in str(row_values).upper():
            return None

        try:
            # Based on actual file format:
            # Index 2: series name
            # Index 3: quantity (small number like 125, 600)
            # Index 4: issue price (around 4000-6000)
            # Index 5: current market rate (around 15000)
            # Index 7: value at cost (large number)
            # Index 10: interest earned (5-6 digit)
            # Index 11: unrealized gain (large 7 digit)
            # Index 12: accrued interest (4 digit)

            # Quantity - first number after series that's small (< 1000)
            quantity = 0
            for i in range(series_idx + 1, min(series_idx + 3, len(row_values))):
                val_f = self._safe_float(row_values[i])
                if 1 < val_f < 1000:  # Quantity is typically < 1000 grams
                    quantity = val_f
                    break

            if quantity == 0:
                return None

            # Issue price - typically 4000-6000 range (gold price per gram at issue)
            issue_price = 0.0
            for i in range(series_idx + 1, min(series_idx + 5, len(row_values))):
                val_f = self._safe_float(row_values[i])
                if 3000 < val_f < 8000:  # Issue price range
                    issue_price = val_f
                    break

            # Current market rate - around 15000 (current gold price)
            current_price = 0.0
            for i in range(series_idx + 1, min(series_idx + 6, len(row_values))):
                val_f = self._safe_float(row_values[i])
                if 10000 < val_f < 20000:  # Current gold price range
                    current_price = val_f
                    break

            # Interest Earned - look for value in 5-6 digit range after cost values
            interest_earned = Decimal("0")
            for i in range(8, min(12, len(row_values))):
                val_f = self._safe_float(row_values[i])
                if 10000 < val_f < 500000:
                    interest_earned = Decimal(str(val_f))
                    break

            # Unrealized gain - large 7 digit number
            unrealized_gain = Decimal("0")
            for i in range(10, min(14, len(row_values))):
                val_f = self._safe_float(row_values[i])
                if 500000 < val_f < 10000000:
                    unrealized_gain = Decimal(str(val_f))
                    break

            # Accrued Interest - smaller 4 digit number near end
            accrued_interest = Decimal("0")
            for i in range(11, min(14, len(row_values))):
                val_f = self._safe_float(row_values[i])
                if 1000 < val_f < 20000:
                    accrued_interest = Decimal(str(val_f))
                    break

            maturity_date = self._extract_maturity_date(series)

            return SGBHolding(
                user_id=user_id,
                series=series,
                maturity_date=maturity_date,
                quantity=int(quantity),
                issue_price=Decimal(str(issue_price)) if issue_price else Decimal("0"),
                current_price=Decimal(str(current_price)) if current_price else None,
                interest_rate=Decimal("2.5"),
                interest_earned=interest_earned,
                accrued_interest=accrued_interest,
                unrealized_gain=unrealized_gain,
            )

        except Exception as e:
            return None

    def _safe_float(self, val) -> float:
        """Safely convert value to float."""
        if pd.isna(val):
            return 0.0
        try:
            return float(str(val).replace(",", ""))
        except (ValueError, TypeError):
            return 0.0

    def _find_header_row(self, df: pd.DataFrame) -> int:
        """Find header row containing 'Security Name' or similar."""
        for idx in range(min(10, len(df))):
            row_text = " ".join(df.iloc[idx].astype(str)).upper()
            if "SECURITY" in row_text or "GOLD BOND" in row_text:
                return idx
        return -1

    def _parse_row(
        self,
        row: pd.Series,
        user_id: Optional[int],
        source_file: str
    ) -> Optional[SGBHolding]:
        """Parse a single row into SGBHolding."""
        # Find series name
        series = None
        for col in row.index:
            val = str(row[col])
            if "Sov. Gold Bond" in val or "SGB" in val.upper():
                series = val.strip()
                break

        if not series or series.lower() in ['nan', 'total', '']:
            return None

        # Extract maturity date from series name
        maturity_date = self._extract_maturity_date(series)

        # Parse quantity
        quantity = self._parse_numeric_column(row, ["Qty", "Quantity", "Units"])
        if quantity == 0:
            return None

        # Parse prices and amounts
        issue_price = self._parse_numeric_column(row, ["Average Cost", "Cost Price", "Avg Cost"])
        interest_earned = self._parse_numeric_column(row, ["Interest Earned", "Interest"])
        accrued_interest = self._parse_numeric_column(row, ["Accrued Interest", "Accrued"])
        unrealized_gain = self._parse_numeric_column(row, ["Unrealized", "Profit/Loss", "Unrealizedb"])

        return SGBHolding(
            user_id=user_id,
            series=series,
            maturity_date=maturity_date,
            quantity=int(quantity),
            issue_price=Decimal(str(issue_price)) if issue_price else Decimal("0"),
            interest_rate=Decimal("2.5"),
            interest_earned=Decimal(str(interest_earned)) if interest_earned else Decimal("0"),
            accrued_interest=Decimal(str(accrued_interest)) if accrued_interest else Decimal("0"),
            unrealized_gain=Decimal(str(unrealized_gain)) if unrealized_gain else Decimal("0"),
        )

    def _extract_maturity_date(self, series: str) -> Optional[date]:
        """Extract maturity date from series name like '2.50% Sov. Gold Bond 8 Sep 28'."""
        # Pattern: day month year (2-digit)
        pattern = r'(\d{1,2})\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s*(\d{2})'
        match = re.search(pattern, series, re.IGNORECASE)

        if match:
            day = int(match.group(1))
            month_str = match.group(2)
            year_2digit = int(match.group(3))

            # Convert month
            months = {
                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
            }
            month = months.get(month_str.lower(), 1)

            # Convert to 4-digit year (assume 20xx for SGB)
            year = 2000 + year_2digit

            try:
                return date(year, month, day)
            except ValueError:
                pass

        return None

    def _parse_numeric_column(self, row: pd.Series, possible_names: List[str]) -> float:
        """Parse numeric value from column with possible names."""
        for name in possible_names:
            for col in row.index:
                if name.upper() in str(col).upper():
                    val = row[col]
                    if pd.notna(val):
                        try:
                            return float(str(val).replace(",", ""))
                        except ValueError:
                            pass
        return 0.0


class SGBTracker:
    """Track SGB holdings, interest, and calculate returns."""

    INTEREST_RATE = Decimal("2.5")  # 2.5% per annum
    MATURITY_YEARS = 8

    def __init__(self, db_connection: sqlite3.Connection):
        """Initialize with database connection."""
        self.conn = db_connection
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize database schema."""
        self.conn.executescript(SGB_SCHEMA_SQL)
        self.conn.commit()

    def import_holdings(
        self,
        file_path: str,
        user_id: Optional[int] = None
    ) -> List[SGBHolding]:
        """Import SGB holdings from Excel file."""
        parser = SGBParser()
        holdings = parser.parse_file(file_path, user_id)

        for holding in holdings:
            self.save_holding(holding)

        return holdings

    def save_holding(self, holding: SGBHolding) -> int:
        """Save or update SGB holding."""
        cursor = self.conn.execute(
            """
            INSERT OR REPLACE INTO sgb_holdings
            (user_id, series, isin, issue_date, maturity_date, quantity,
             issue_price, current_price, interest_rate, interest_earned,
             accrued_interest, unrealized_gain, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                holding.user_id,
                holding.series,
                holding.isin,
                holding.issue_date.isoformat() if holding.issue_date else None,
                holding.maturity_date.isoformat() if holding.maturity_date else None,
                holding.quantity,
                str(holding.issue_price),
                str(holding.current_price) if holding.current_price else None,
                str(holding.interest_rate),
                str(holding.interest_earned),
                str(holding.accrued_interest),
                str(holding.unrealized_gain),
            )
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_holdings(self, user_id: Optional[int] = None) -> List[SGBHolding]:
        """Get all SGB holdings for a user."""
        if user_id:
            cursor = self.conn.execute(
                "SELECT * FROM sgb_holdings WHERE user_id = ?", (user_id,)
            )
        else:
            cursor = self.conn.execute("SELECT * FROM sgb_holdings")

        return [self._row_to_holding(row) for row in cursor.fetchall()]

    def add_interest_from_bank(
        self,
        amount: Decimal,
        payment_date: date,
        financial_year: str,
        series: Optional[str] = None,
        tds: Decimal = Decimal("0")
    ) -> int:
        """
        Add SGB interest from bank statement.

        If series is not specified, tries to match based on amount.
        """
        # Try to find matching holding
        sgb_holding_id = None
        if series:
            cursor = self.conn.execute(
                "SELECT id FROM sgb_holdings WHERE series LIKE ?",
                (f"%{series}%",)
            )
            row = cursor.fetchone()
            if row:
                sgb_holding_id = row["id"]
                series = self._get_series_name(sgb_holding_id)

        if not series:
            series = "UNKNOWN_SGB"

        cursor = self.conn.execute(
            """
            INSERT OR REPLACE INTO sgb_interest
            (sgb_holding_id, series, payment_date, financial_year,
             quantity, rate, amount, tds_deducted, source)
            VALUES (?, ?, ?, ?, 0, ?, ?, ?, 'BANK_STATEMENT')
            """,
            (
                sgb_holding_id,
                series,
                payment_date.isoformat(),
                financial_year,
                str(self.INTEREST_RATE),
                str(amount),
                str(tds),
            )
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_interest_for_fy(self, financial_year: str, user_id: Optional[int] = None) -> List[SGBInterest]:
        """Get all SGB interest payments for a financial year."""
        query = """
            SELECT si.*, sh.user_id
            FROM sgb_interest si
            LEFT JOIN sgb_holdings sh ON si.sgb_holding_id = sh.id
            WHERE si.financial_year = ?
        """
        params = [financial_year]

        if user_id:
            query += " AND (sh.user_id = ? OR sh.user_id IS NULL)"
            params.append(user_id)

        cursor = self.conn.execute(query, params)
        return [self._row_to_interest(row) for row in cursor.fetchall()]

    def get_total_interest_for_fy(self, financial_year: str, user_id: Optional[int] = None) -> Decimal:
        """Get total SGB interest for a financial year."""
        query = """
            SELECT COALESCE(SUM(CAST(si.amount AS REAL)), 0) as total
            FROM sgb_interest si
            LEFT JOIN sgb_holdings sh ON si.sgb_holding_id = sh.id
            WHERE si.financial_year = ?
        """
        params = [financial_year]

        if user_id:
            query += " AND (sh.user_id = ? OR sh.user_id IS NULL)"
            params.append(user_id)

        cursor = self.conn.execute(query, params)
        row = cursor.fetchone()
        return Decimal(str(row[0])) if row else Decimal("0")

    def get_summary(self, user_id: Optional[int] = None, financial_year: Optional[str] = None) -> SGBSummary:
        """Get SGB summary for a user."""
        holdings = self.get_holdings(user_id)

        total_quantity = sum(h.quantity for h in holdings)
        total_cost = sum(h.cost_value for h in holdings)
        total_market = sum(h.market_value or Decimal("0") for h in holdings)
        total_interest = sum(h.interest_earned for h in holdings)
        total_unrealized = sum(h.unrealized_gain for h in holdings)

        return SGBSummary(
            financial_year=financial_year or "",
            total_holdings=len(holdings),
            total_quantity=total_quantity,
            total_cost=total_cost,
            total_market_value=total_market,
            total_interest_earned=total_interest,
            total_unrealized_gain=total_unrealized,
            holdings=holdings,
        )

    def calculate_semi_annual_interest(self, holding: SGBHolding) -> Decimal:
        """
        Calculate semi-annual interest for an SGB holding.

        Interest = Issue Price x Quantity x 2.5% / 2
        """
        annual_interest = holding.issue_price * holding.quantity * (self.INTEREST_RATE / 100)
        return annual_interest / 2

    def calculate_maturity_cg(
        self,
        holding: SGBHolding,
        redemption_price: Decimal
    ) -> tuple:
        """
        Calculate capital gains on redemption.

        If held till maturity (8 years): CG is EXEMPT
        If sold before: LTCG at 12.5% (if >12 months)

        Returns: (capital_gain, is_exempt)
        """
        cost = holding.issue_price * holding.quantity
        sale_value = redemption_price * holding.quantity
        gain = sale_value - cost

        # Check if maturity
        if holding.issue_date:
            holding_days = (date.today() - holding.issue_date).days
            is_maturity = holding_days >= (self.MATURITY_YEARS * 365)
        else:
            is_maturity = False

        return gain, is_maturity

    def _get_series_name(self, holding_id: int) -> str:
        """Get series name for a holding ID."""
        cursor = self.conn.execute(
            "SELECT series FROM sgb_holdings WHERE id = ?", (holding_id,)
        )
        row = cursor.fetchone()
        return row["series"] if row else "UNKNOWN"

    def _row_to_holding(self, row: sqlite3.Row) -> SGBHolding:
        """Convert database row to SGBHolding."""
        return SGBHolding(
            id=row["id"],
            user_id=row["user_id"],
            series=row["series"],
            isin=row["isin"],
            issue_date=date.fromisoformat(row["issue_date"]) if row["issue_date"] else None,
            maturity_date=date.fromisoformat(row["maturity_date"]) if row["maturity_date"] else None,
            quantity=row["quantity"],
            issue_price=Decimal(str(row["issue_price"])),
            current_price=Decimal(str(row["current_price"])) if row["current_price"] else None,
            interest_rate=Decimal(str(row["interest_rate"])),
            interest_earned=Decimal(str(row["interest_earned"])) if row["interest_earned"] else Decimal("0"),
            accrued_interest=Decimal(str(row["accrued_interest"])) if row["accrued_interest"] else Decimal("0"),
            unrealized_gain=Decimal(str(row["unrealized_gain"])) if row["unrealized_gain"] else Decimal("0"),
        )

    def _row_to_interest(self, row: sqlite3.Row) -> SGBInterest:
        """Convert database row to SGBInterest."""
        return SGBInterest(
            id=row["id"],
            sgb_holding_id=row["sgb_holding_id"],
            series=row["series"],
            payment_date=date.fromisoformat(row["payment_date"]),
            financial_year=row["financial_year"],
            quantity=row["quantity"],
            rate=Decimal(str(row["rate"])),
            amount=Decimal(str(row["amount"])),
            tds_deducted=Decimal(str(row["tds_deducted"])) if row["tds_deducted"] else Decimal("0"),
            source=row["source"],
        )
