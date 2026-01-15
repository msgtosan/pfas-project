"""
REIT/InvIT Module for PFAS.

Handles:
- Track REIT/InvIT holdings
- Distribution breakdowns (dividend/interest/other)
- Dividend portion: Exempt (from SPV)
- Interest portion: Taxable at slab rate
- Capital reduction: Reduces cost basis

New Tax Regime: Interest taxable, dividend exempt.
"""

import sqlite3
from datetime import date
from decimal import Decimal
from typing import Optional, List, Dict, Any

from .models import REITHolding, REITDistribution, DistributionType


# Database schema for REIT tables
REIT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS reit_holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    symbol TEXT NOT NULL,
    name TEXT,
    isin TEXT,
    units DECIMAL(15,4) NOT NULL DEFAULT 0,
    purchase_date DATE,
    purchase_price DECIMAL(15,2) DEFAULT 0,
    current_price DECIMAL(15,2),
    cost_basis DECIMAL(15,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, symbol)
);

CREATE TABLE IF NOT EXISTS reit_distributions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reit_holding_id INTEGER,
    symbol TEXT NOT NULL,
    record_date DATE NOT NULL,
    payment_date DATE,
    financial_year TEXT NOT NULL,
    distribution_type TEXT NOT NULL DEFAULT 'DIVIDEND',
    gross_amount DECIMAL(15,2) NOT NULL,
    tds_deducted DECIMAL(15,2) DEFAULT 0,
    net_amount DECIMAL(15,2) NOT NULL,
    source TEXT DEFAULT 'BANK_STATEMENT',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, record_date, distribution_type),
    FOREIGN KEY (reit_holding_id) REFERENCES reit_holdings(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_reit_holdings_user ON reit_holdings(user_id);
CREATE INDEX IF NOT EXISTS idx_reit_holdings_symbol ON reit_holdings(symbol);
CREATE INDEX IF NOT EXISTS idx_reit_dist_fy ON reit_distributions(financial_year);
CREATE INDEX IF NOT EXISTS idx_reit_dist_type ON reit_distributions(distribution_type);
"""


class REITTracker:
    """Track REIT/InvIT holdings and distributions."""

    def __init__(self, db_connection: sqlite3.Connection):
        """Initialize with database connection."""
        self.conn = db_connection
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize database schema."""
        self.conn.executescript(REIT_SCHEMA_SQL)
        self.conn.commit()

    def add_holding(self, holding: REITHolding) -> int:
        """Add or update REIT holding."""
        cursor = self.conn.execute(
            """
            INSERT OR REPLACE INTO reit_holdings
            (user_id, symbol, name, isin, units, purchase_date,
             purchase_price, current_price, cost_basis, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                holding.user_id,
                holding.symbol,
                holding.name,
                holding.isin,
                str(holding.units),
                holding.purchase_date.isoformat() if holding.purchase_date else None,
                str(holding.purchase_price),
                str(holding.current_price) if holding.current_price else None,
                str(holding.cost_basis),
            )
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_holdings(self, user_id: Optional[int] = None) -> List[REITHolding]:
        """Get all REIT holdings for a user."""
        if user_id:
            cursor = self.conn.execute(
                "SELECT * FROM reit_holdings WHERE user_id = ?", (user_id,)
            )
        else:
            cursor = self.conn.execute("SELECT * FROM reit_holdings")

        return [self._row_to_holding(row) for row in cursor.fetchall()]

    def add_distribution(self, distribution: REITDistribution) -> int:
        """Add REIT distribution."""
        cursor = self.conn.execute(
            """
            INSERT OR REPLACE INTO reit_distributions
            (reit_holding_id, symbol, record_date, payment_date, financial_year,
             distribution_type, gross_amount, tds_deducted, net_amount, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                distribution.reit_holding_id,
                distribution.symbol,
                distribution.record_date.isoformat(),
                distribution.payment_date.isoformat() if distribution.payment_date else None,
                distribution.financial_year,
                distribution.distribution_type.value,
                str(distribution.gross_amount),
                str(distribution.tds_deducted),
                str(distribution.net_amount),
                distribution.source,
            )
        )
        self.conn.commit()

        # If OTHER (capital reduction), update cost basis
        if distribution.distribution_type == DistributionType.OTHER:
            self._apply_cost_reduction(distribution.symbol, distribution.gross_amount)

        return cursor.lastrowid

    def add_distribution_from_bank(
        self,
        symbol: str,
        amount: Decimal,
        payment_date: date,
        financial_year: str,
        distribution_type: DistributionType = DistributionType.DIVIDEND,
        tds: Decimal = Decimal("0")
    ) -> int:
        """Add REIT distribution from bank statement."""
        # Find holding
        holding_id = None
        cursor = self.conn.execute(
            "SELECT id FROM reit_holdings WHERE symbol = ?", (symbol,)
        )
        row = cursor.fetchone()
        if row:
            holding_id = row["id"]

        distribution = REITDistribution(
            reit_holding_id=holding_id,
            symbol=symbol,
            record_date=payment_date,
            payment_date=payment_date,
            financial_year=financial_year,
            distribution_type=distribution_type,
            gross_amount=amount,
            tds_deducted=tds,
            net_amount=amount - tds,
            source="BANK_STATEMENT",
        )

        return self.add_distribution(distribution)

    def get_distributions_for_fy(
        self,
        financial_year: str,
        user_id: Optional[int] = None
    ) -> List[REITDistribution]:
        """Get all REIT distributions for a financial year."""
        query = """
            SELECT rd.*, rh.user_id
            FROM reit_distributions rd
            LEFT JOIN reit_holdings rh ON rd.reit_holding_id = rh.id
            WHERE rd.financial_year = ?
        """
        params = [financial_year]

        if user_id:
            query += " AND (rh.user_id = ? OR rh.user_id IS NULL)"
            params.append(user_id)

        cursor = self.conn.execute(query, params)
        return [self._row_to_distribution(row) for row in cursor.fetchall()]

    def get_distribution_summary(
        self,
        financial_year: str,
        user_id: Optional[int] = None
    ) -> Dict[str, Decimal]:
        """
        Get REIT distribution summary by type for a financial year.

        Returns dict with:
        - dividend: Exempt amount
        - interest: Taxable amount
        - other: Cost reduction amount
        - tds: Total TDS deducted
        """
        distributions = self.get_distributions_for_fy(financial_year, user_id)

        summary = {
            "dividend": Decimal("0"),
            "interest": Decimal("0"),
            "other": Decimal("0"),
            "tds": Decimal("0"),
        }

        for dist in distributions:
            if dist.distribution_type == DistributionType.DIVIDEND:
                summary["dividend"] += dist.gross_amount
            elif dist.distribution_type == DistributionType.INTEREST:
                summary["interest"] += dist.gross_amount
            elif dist.distribution_type == DistributionType.OTHER:
                summary["other"] += dist.gross_amount

            summary["tds"] += dist.tds_deducted

        return summary

    def process_distribution(
        self,
        distribution: REITDistribution,
        holding_cost: Decimal
    ) -> Dict[str, Any]:
        """
        Process REIT distribution and return tax treatment.

        Returns dict with:
        - taxable_amount: Amount taxable at slab rate
        - exempt_amount: Tax-free dividend portion
        - cost_reduction: Amount to reduce cost basis
        - tds_credit: TDS available for credit
        """
        result = {
            "gross_amount": distribution.gross_amount,
            "taxable_amount": Decimal("0"),
            "exempt_amount": Decimal("0"),
            "cost_reduction": Decimal("0"),
            "tds_credit": distribution.tds_deducted,
        }

        if distribution.distribution_type == DistributionType.DIVIDEND:
            result["exempt_amount"] = distribution.gross_amount

        elif distribution.distribution_type == DistributionType.INTEREST:
            result["taxable_amount"] = distribution.gross_amount

        elif distribution.distribution_type == DistributionType.OTHER:
            result["cost_reduction"] = distribution.gross_amount
            new_cost = max(Decimal("0"), holding_cost - distribution.gross_amount)
            result["new_cost_basis"] = new_cost

        return result

    def _apply_cost_reduction(self, symbol: str, amount: Decimal) -> None:
        """Apply cost reduction to holding."""
        self.conn.execute(
            """
            UPDATE reit_holdings
            SET cost_basis = MAX(0, CAST(cost_basis AS REAL) - ?),
                updated_at = CURRENT_TIMESTAMP
            WHERE symbol = ?
            """,
            (float(amount), symbol)
        )
        self.conn.commit()

    def _row_to_holding(self, row: sqlite3.Row) -> REITHolding:
        """Convert database row to REITHolding."""
        return REITHolding(
            id=row["id"],
            user_id=row["user_id"],
            symbol=row["symbol"],
            name=row["name"],
            isin=row["isin"],
            units=Decimal(str(row["units"])),
            purchase_date=date.fromisoformat(row["purchase_date"]) if row["purchase_date"] else None,
            purchase_price=Decimal(str(row["purchase_price"])),
            current_price=Decimal(str(row["current_price"])) if row["current_price"] else None,
            cost_basis=Decimal(str(row["cost_basis"])),
        )

    def _row_to_distribution(self, row: sqlite3.Row) -> REITDistribution:
        """Convert database row to REITDistribution."""
        return REITDistribution(
            id=row["id"],
            reit_holding_id=row["reit_holding_id"],
            symbol=row["symbol"],
            record_date=date.fromisoformat(row["record_date"]),
            payment_date=date.fromisoformat(row["payment_date"]) if row["payment_date"] else None,
            financial_year=row["financial_year"],
            distribution_type=DistributionType(row["distribution_type"]),
            gross_amount=Decimal(str(row["gross_amount"])),
            tds_deducted=Decimal(str(row["tds_deducted"])) if row["tds_deducted"] else Decimal("0"),
            net_amount=Decimal(str(row["net_amount"])),
            source=row["source"],
        )
