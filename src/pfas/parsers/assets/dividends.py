"""
Dividends Module for PFAS.

Handles:
- Track dividend income from stocks and mutual funds
- TDS tracking (Section 194)
- Annual dividend summary for tax computation

New Tax Regime: Dividends taxable at slab rate (from AY 2021-22).
"""

import sqlite3
from datetime import date
from decimal import Decimal
from typing import Optional, List, Dict

from .models import DividendRecord, DividendSummary


# Database schema for dividend tables
DIVIDEND_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dividend_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    symbol TEXT NOT NULL,
    company_name TEXT,
    isin TEXT,
    record_date DATE,
    payment_date DATE NOT NULL,
    financial_year TEXT NOT NULL,
    dividend_type TEXT DEFAULT 'INTERIM',
    gross_amount DECIMAL(15,2) NOT NULL,
    tds_deducted DECIMAL(15,2) DEFAULT 0,
    net_amount DECIMAL(15,2) NOT NULL,
    source TEXT DEFAULT 'BANK_STATEMENT',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, symbol, payment_date, gross_amount)
);

CREATE INDEX IF NOT EXISTS idx_dividend_user ON dividend_records(user_id);
CREATE INDEX IF NOT EXISTS idx_dividend_symbol ON dividend_records(symbol);
CREATE INDEX IF NOT EXISTS idx_dividend_fy ON dividend_records(financial_year);
CREATE INDEX IF NOT EXISTS idx_dividend_date ON dividend_records(payment_date);
"""


class DividendTracker:
    """Track dividend income from stocks and mutual funds."""

    def __init__(self, db_connection: sqlite3.Connection):
        """Initialize with database connection."""
        self.conn = db_connection
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize database schema."""
        self.conn.executescript(DIVIDEND_SCHEMA_SQL)
        self.conn.commit()

    def add_dividend(self, dividend: DividendRecord) -> int:
        """Add dividend record."""
        cursor = self.conn.execute(
            """
            INSERT OR REPLACE INTO dividend_records
            (user_id, symbol, company_name, isin, record_date, payment_date,
             financial_year, dividend_type, gross_amount, tds_deducted,
             net_amount, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dividend.user_id,
                dividend.symbol,
                dividend.company_name,
                dividend.isin,
                dividend.record_date.isoformat() if dividend.record_date else None,
                dividend.payment_date.isoformat(),
                dividend.financial_year,
                dividend.dividend_type,
                str(dividend.gross_amount),
                str(dividend.tds_deducted),
                str(dividend.net_amount),
                dividend.source,
            )
        )
        self.conn.commit()
        return cursor.lastrowid

    def add_dividend_from_bank(
        self,
        amount: Decimal,
        payment_date: date,
        financial_year: str,
        symbol: str = "UNKNOWN",
        company_name: Optional[str] = None,
        tds: Decimal = Decimal("0"),
        user_id: Optional[int] = None,
        description: str = ""
    ) -> int:
        """
        Add dividend from bank statement.

        Tries to extract symbol/company from description if possible.
        """
        # Try to extract symbol from description
        if symbol == "UNKNOWN" and description:
            symbol = self._extract_symbol(description)

        dividend = DividendRecord(
            user_id=user_id,
            symbol=symbol,
            company_name=company_name,
            payment_date=payment_date,
            financial_year=financial_year,
            dividend_type="INTERIM",
            gross_amount=amount,
            tds_deducted=tds,
            net_amount=amount - tds,
            source="BANK_STATEMENT",
        )

        return self.add_dividend(dividend)

    def get_dividends_for_fy(
        self,
        financial_year: str,
        user_id: Optional[int] = None
    ) -> List[DividendRecord]:
        """Get all dividend records for a financial year."""
        if user_id:
            cursor = self.conn.execute(
                """
                SELECT * FROM dividend_records
                WHERE financial_year = ? AND user_id = ?
                ORDER BY payment_date
                """,
                (financial_year, user_id)
            )
        else:
            cursor = self.conn.execute(
                """
                SELECT * FROM dividend_records
                WHERE financial_year = ?
                ORDER BY payment_date
                """,
                (financial_year,)
            )

        return [self._row_to_dividend(row) for row in cursor.fetchall()]

    def get_summary_for_fy(
        self,
        financial_year: str,
        user_id: Optional[int] = None
    ) -> DividendSummary:
        """Get dividend summary for a financial year."""
        dividends = self.get_dividends_for_fy(financial_year, user_id)

        total_dividend = sum(d.gross_amount for d in dividends)
        total_tds = sum(d.tds_deducted for d in dividends)

        return DividendSummary(
            financial_year=financial_year,
            total_dividend_income=total_dividend,
            total_tds_deducted=total_tds,
            dividend_count=len(dividends),
            dividends=dividends,
        )

    def get_total_dividend_for_fy(
        self,
        financial_year: str,
        user_id: Optional[int] = None
    ) -> Decimal:
        """Get total dividend income for a financial year."""
        query = """
            SELECT COALESCE(SUM(CAST(gross_amount AS REAL)), 0) as total
            FROM dividend_records
            WHERE financial_year = ?
        """
        params = [financial_year]

        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)

        cursor = self.conn.execute(query, params)
        row = cursor.fetchone()
        return Decimal(str(row[0])) if row else Decimal("0")

    def get_total_tds_for_fy(
        self,
        financial_year: str,
        user_id: Optional[int] = None
    ) -> Decimal:
        """Get total TDS on dividends for a financial year."""
        query = """
            SELECT COALESCE(SUM(CAST(tds_deducted AS REAL)), 0) as total
            FROM dividend_records
            WHERE financial_year = ?
        """
        params = [financial_year]

        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)

        cursor = self.conn.execute(query, params)
        row = cursor.fetchone()
        return Decimal(str(row[0])) if row else Decimal("0")

    def get_dividends_by_symbol(
        self,
        symbol: str,
        financial_year: Optional[str] = None
    ) -> List[DividendRecord]:
        """Get all dividends for a symbol."""
        if financial_year:
            cursor = self.conn.execute(
                """
                SELECT * FROM dividend_records
                WHERE symbol = ? AND financial_year = ?
                ORDER BY payment_date
                """,
                (symbol, financial_year)
            )
        else:
            cursor = self.conn.execute(
                """
                SELECT * FROM dividend_records
                WHERE symbol = ?
                ORDER BY payment_date
                """,
                (symbol,)
            )

        return [self._row_to_dividend(row) for row in cursor.fetchall()]

    def get_symbol_summary(
        self,
        financial_year: str,
        user_id: Optional[int] = None
    ) -> Dict[str, Dict]:
        """Get dividend summary grouped by symbol."""
        query = """
            SELECT symbol,
                   COALESCE(SUM(CAST(gross_amount AS REAL)), 0) as total_amount,
                   COALESCE(SUM(CAST(tds_deducted AS REAL)), 0) as total_tds,
                   COUNT(*) as count
            FROM dividend_records
            WHERE financial_year = ?
        """
        params = [financial_year]

        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)

        query += " GROUP BY symbol ORDER BY total_amount DESC"

        cursor = self.conn.execute(query, params)

        return {
            row["symbol"]: {
                "total_amount": Decimal(str(row["total_amount"])),
                "total_tds": Decimal(str(row["total_tds"])),
                "count": row["count"],
            }
            for row in cursor.fetchall()
        }

    def _extract_symbol(self, description: str) -> str:
        """Try to extract stock symbol from bank description."""
        # Common patterns: "DIV-RELIANCE", "DIVIDEND HDFC", "ACH-TCS"
        description_upper = description.upper()

        # List of known stock symbols (can be extended)
        known_symbols = [
            "RELIANCE", "TCS", "HDFC", "INFY", "ICICIBANK", "SBIN", "BHARTIARTL",
            "ITC", "KOTAKBANK", "LT", "HCLTECH", "AXISBANK", "MARUTI", "SUNPHARMA",
            "TITAN", "ONGC", "NTPC", "BAJFINANCE", "ASIANPAINT", "TATAMOTORS",
            "WIPRO", "NESTLEIND", "ULTRACEMCO", "POWERGRID", "COALINDIA",
            "JSWSTEEL", "TATASTEEL", "INDUSINDBK", "TECHM", "HINDALCO",
        ]

        for symbol in known_symbols:
            if symbol in description_upper:
                return symbol

        # Try to extract from common patterns
        import re

        # Pattern: DIV-SYMBOL or DIVIDEND SYMBOL
        patterns = [
            r'DIV[/-]([A-Z]+)',
            r'DIVIDEND\s+([A-Z]+)',
            r'ACH[/-]([A-Z]+)',
            r'NSDL[/-]([A-Z]+)',
        ]

        for pattern in patterns:
            match = re.search(pattern, description_upper)
            if match:
                return match.group(1)

        return "UNKNOWN"

    def _row_to_dividend(self, row: sqlite3.Row) -> DividendRecord:
        """Convert database row to DividendRecord."""
        return DividendRecord(
            id=row["id"],
            user_id=row["user_id"],
            symbol=row["symbol"],
            company_name=row["company_name"],
            isin=row["isin"],
            record_date=date.fromisoformat(row["record_date"]) if row["record_date"] else None,
            payment_date=date.fromisoformat(row["payment_date"]),
            financial_year=row["financial_year"],
            dividend_type=row["dividend_type"],
            gross_amount=Decimal(str(row["gross_amount"])),
            tds_deducted=Decimal(str(row["tds_deducted"])) if row["tds_deducted"] else Decimal("0"),
            net_amount=Decimal(str(row["net_amount"])),
            source=row["source"],
        )
