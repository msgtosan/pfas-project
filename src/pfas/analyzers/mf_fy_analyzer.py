"""
MF Financial Year Analyzer - FY-specific summaries and YoY growth tracking.

Generates:
- Financial year transaction summaries by scheme type, AMC, and RTA
- Holdings snapshots at FY start/end
- Year-over-year growth comparisons
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

logger = logging.getLogger(__name__)


@dataclass
class FYSummary:
    """Financial year transaction summary."""
    user_id: int
    financial_year: str
    scheme_type: str  # EQUITY, DEBT, HYBRID, OTHER, ALL
    amc_name: Optional[str] = None
    rta: Optional[str] = None

    # Opening balance
    opening_units: Decimal = Decimal("0")
    opening_value: Decimal = Decimal("0")
    opening_cost: Decimal = Decimal("0")

    # Transactions during FY
    purchase_units: Decimal = Decimal("0")
    purchase_amount: Decimal = Decimal("0")
    purchase_count: int = 0

    redemption_units: Decimal = Decimal("0")
    redemption_amount: Decimal = Decimal("0")
    redemption_count: int = 0

    switch_in_units: Decimal = Decimal("0")
    switch_in_amount: Decimal = Decimal("0")
    switch_out_units: Decimal = Decimal("0")
    switch_out_amount: Decimal = Decimal("0")

    dividend_payout: Decimal = Decimal("0")
    dividend_reinvest: Decimal = Decimal("0")

    # Capital gains
    stcg_realized: Decimal = Decimal("0")
    ltcg_realized: Decimal = Decimal("0")

    # Closing balance
    closing_units: Decimal = Decimal("0")
    closing_value: Decimal = Decimal("0")
    closing_cost: Decimal = Decimal("0")

    # Performance
    absolute_return: Decimal = Decimal("0")
    xirr: Optional[Decimal] = None


@dataclass
class HoldingsSnapshot:
    """Point-in-time holdings snapshot."""
    user_id: int
    snapshot_date: date
    snapshot_type: str  # FY_START, FY_END, QUARTERLY, MONTHLY, ADHOC
    financial_year: Optional[str] = None

    # Holdings details (JSON serializable)
    holdings: List[Dict[str, Any]] = field(default_factory=list)

    # Aggregated totals
    total_schemes: int = 0
    total_folios: int = 0
    total_value: Decimal = Decimal("0")
    total_cost: Decimal = Decimal("0")
    total_appreciation: Decimal = Decimal("0")

    # Category breakdown
    equity_value: Decimal = Decimal("0")
    equity_schemes: int = 0
    debt_value: Decimal = Decimal("0")
    debt_schemes: int = 0
    hybrid_value: Decimal = Decimal("0")
    hybrid_schemes: int = 0

    weighted_xirr: Optional[Decimal] = None


@dataclass
class YoYGrowth:
    """Year-over-year growth comparison."""
    user_id: int
    base_year: str
    compare_year: str

    base_value: Decimal = Decimal("0")
    compare_value: Decimal = Decimal("0")
    value_change: Decimal = Decimal("0")
    value_change_pct: Decimal = Decimal("0")

    base_cost: Decimal = Decimal("0")
    compare_cost: Decimal = Decimal("0")
    net_investment: Decimal = Decimal("0")

    base_appreciation: Decimal = Decimal("0")
    compare_appreciation: Decimal = Decimal("0")
    appreciation_change: Decimal = Decimal("0")

    equity_growth_pct: Decimal = Decimal("0")
    debt_growth_pct: Decimal = Decimal("0")
    hybrid_growth_pct: Decimal = Decimal("0")

    schemes_added: int = 0
    schemes_removed: int = 0
    schemes_unchanged: int = 0


class MFFYAnalyzer:
    """
    Generates FY summaries and YoY growth analysis.

    Usage:
        analyzer = MFFYAnalyzer(conn, config)

        # Generate FY summary
        summary = analyzer.generate_fy_summary(user_id=1, financial_year="2024-25")

        # Take holdings snapshot
        snapshot = analyzer.take_holdings_snapshot(
            user_id=1,
            snapshot_date=date(2025, 3, 31),
            snapshot_type="FY_END"
        )

        # Generate YoY growth report
        growth = analyzer.calculate_yoy_growth(
            user_id=1,
            base_year="2023-24",
            compare_year="2024-25"
        )
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        config: Optional[Dict[str, Any]] = None
    ):
        self.conn = conn
        self.config = config or {}

    def generate_fy_summary(
        self,
        user_id: int,
        financial_year: str,
        group_by: Optional[List[str]] = None
    ) -> List[FYSummary]:
        """
        Generate FY transaction summary.

        Args:
            user_id: User ID
            financial_year: e.g., "2024-25"
            group_by: List of grouping dimensions ["scheme_type", "amc_name", "rta"]

        Returns:
            List of FYSummary objects
        """
        group_by = group_by or ["scheme_type"]
        fy_start, fy_end = self._get_fy_dates(financial_year)
        summaries = []

        # Build grouping clause
        group_columns = []
        if "scheme_type" in group_by:
            group_columns.append("s.asset_class")
        if "amc_name" in group_by:
            group_columns.append("a.name")
        if "rta" in group_by:
            group_columns.append("COALESCE(h.rta, 'UNKNOWN')")

        group_clause = ", ".join(group_columns) if group_columns else "'ALL'"

        # Get transaction summary
        query = f"""
            WITH scheme_info AS (
                SELECT
                    f.id as folio_id,
                    s.asset_class,
                    COALESCE(a.name, 'Unknown AMC') as amc_name
                FROM mf_folios f
                JOIN mf_schemes s ON f.scheme_id = s.id
                LEFT JOIN mf_amcs a ON s.amc_id = a.id
                WHERE f.user_id = ?
            )
            SELECT
                si.asset_class as scheme_type,
                si.amc_name,
                SUM(CASE WHEN t.transaction_type = 'PURCHASE' THEN t.units ELSE 0 END) as purchase_units,
                SUM(CASE WHEN t.transaction_type = 'PURCHASE' THEN t.amount ELSE 0 END) as purchase_amount,
                COUNT(CASE WHEN t.transaction_type = 'PURCHASE' THEN 1 END) as purchase_count,
                SUM(CASE WHEN t.transaction_type = 'REDEMPTION' THEN t.units ELSE 0 END) as redemption_units,
                SUM(CASE WHEN t.transaction_type = 'REDEMPTION' THEN t.amount ELSE 0 END) as redemption_amount,
                COUNT(CASE WHEN t.transaction_type = 'REDEMPTION' THEN 1 END) as redemption_count,
                SUM(CASE WHEN t.transaction_type = 'SWITCH_IN' THEN t.units ELSE 0 END) as switch_in_units,
                SUM(CASE WHEN t.transaction_type = 'SWITCH_IN' THEN t.amount ELSE 0 END) as switch_in_amount,
                SUM(CASE WHEN t.transaction_type = 'SWITCH_OUT' THEN t.units ELSE 0 END) as switch_out_units,
                SUM(CASE WHEN t.transaction_type = 'SWITCH_OUT' THEN t.amount ELSE 0 END) as switch_out_amount,
                SUM(CASE WHEN t.transaction_type = 'DIVIDEND' THEN t.amount ELSE 0 END) as dividend_payout,
                SUM(CASE WHEN t.transaction_type = 'DIVIDEND_REINVEST' THEN t.amount ELSE 0 END) as dividend_reinvest,
                SUM(COALESCE(t.short_term_gain, 0)) as stcg_realized,
                SUM(COALESCE(t.long_term_gain, 0)) as ltcg_realized
            FROM mf_transactions t
            JOIN scheme_info si ON t.folio_id = si.folio_id
            WHERE t.user_id = ? AND t.date >= ? AND t.date <= ?
            GROUP BY si.asset_class, si.amc_name
        """

        cursor = self.conn.execute(query, (user_id, user_id, fy_start, fy_end))

        for row in cursor.fetchall():
            summary = FYSummary(
                user_id=user_id,
                financial_year=financial_year,
                scheme_type=row[0] or "OTHER",
                amc_name=row[1],
                purchase_units=Decimal(str(row[2] or 0)),
                purchase_amount=Decimal(str(row[3] or 0)),
                purchase_count=row[4] or 0,
                redemption_units=Decimal(str(row[5] or 0)),
                redemption_amount=Decimal(str(row[6] or 0)),
                redemption_count=row[7] or 0,
                switch_in_units=Decimal(str(row[8] or 0)),
                switch_in_amount=Decimal(str(row[9] or 0)),
                switch_out_units=Decimal(str(row[10] or 0)),
                switch_out_amount=Decimal(str(row[11] or 0)),
                dividend_payout=Decimal(str(row[12] or 0)),
                dividend_reinvest=Decimal(str(row[13] or 0)),
                stcg_realized=Decimal(str(row[14] or 0)),
                ltcg_realized=Decimal(str(row[15] or 0))
            )

            # Get opening/closing balances from holdings snapshots
            self._add_opening_closing(summary, fy_start, fy_end)
            summaries.append(summary)

        # Generate aggregate summary (ALL)
        if summaries:
            all_summary = self._aggregate_summaries(summaries, user_id, financial_year)
            summaries.insert(0, all_summary)

        return summaries

    def _add_opening_closing(self, summary: FYSummary, fy_start: str, fy_end: str):
        """Add opening and closing balances from holdings."""
        # Get opening balance (from FY start or closest prior snapshot)
        opening_query = """
            SELECT
                SUM(CAST(current_value AS DECIMAL)) as value,
                SUM(CAST(cost_value AS DECIMAL)) as cost,
                SUM(CAST(units AS DECIMAL)) as units
            FROM mf_holdings
            WHERE user_id = ? AND nav_date <= ?
              AND scheme_type = ?
            GROUP BY user_id
        """

        # Get closing balance
        closing_query = """
            SELECT
                SUM(CAST(current_value AS DECIMAL)) as value,
                SUM(CAST(cost_value AS DECIMAL)) as cost,
                SUM(CAST(units AS DECIMAL)) as units
            FROM mf_holdings
            WHERE user_id = ? AND nav_date <= ?
              AND scheme_type = ?
            GROUP BY user_id
        """

        # Opening
        cursor = self.conn.execute(
            opening_query.replace("scheme_type = ?", "scheme_type = ?" if summary.scheme_type != "ALL" else "1=1"),
            (summary.user_id, fy_start, summary.scheme_type) if summary.scheme_type != "ALL"
            else (summary.user_id, fy_start)
        )
        row = cursor.fetchone()
        if row:
            summary.opening_value = Decimal(str(row[0] or 0))
            summary.opening_cost = Decimal(str(row[1] or 0))
            summary.opening_units = Decimal(str(row[2] or 0))

        # Closing
        cursor = self.conn.execute(
            closing_query.replace("scheme_type = ?", "scheme_type = ?" if summary.scheme_type != "ALL" else "1=1"),
            (summary.user_id, fy_end, summary.scheme_type) if summary.scheme_type != "ALL"
            else (summary.user_id, fy_end)
        )
        row = cursor.fetchone()
        if row:
            summary.closing_value = Decimal(str(row[0] or 0))
            summary.closing_cost = Decimal(str(row[1] or 0))
            summary.closing_units = Decimal(str(row[2] or 0))

        # Calculate absolute return
        if summary.closing_cost > 0:
            summary.absolute_return = summary.closing_value - summary.closing_cost

    def _aggregate_summaries(
        self,
        summaries: List[FYSummary],
        user_id: int,
        financial_year: str
    ) -> FYSummary:
        """Aggregate individual summaries into ALL summary."""
        all_summary = FYSummary(
            user_id=user_id,
            financial_year=financial_year,
            scheme_type="ALL"
        )

        for s in summaries:
            all_summary.opening_units += s.opening_units
            all_summary.opening_value += s.opening_value
            all_summary.opening_cost += s.opening_cost
            all_summary.purchase_units += s.purchase_units
            all_summary.purchase_amount += s.purchase_amount
            all_summary.purchase_count += s.purchase_count
            all_summary.redemption_units += s.redemption_units
            all_summary.redemption_amount += s.redemption_amount
            all_summary.redemption_count += s.redemption_count
            all_summary.switch_in_units += s.switch_in_units
            all_summary.switch_in_amount += s.switch_in_amount
            all_summary.switch_out_units += s.switch_out_units
            all_summary.switch_out_amount += s.switch_out_amount
            all_summary.dividend_payout += s.dividend_payout
            all_summary.dividend_reinvest += s.dividend_reinvest
            all_summary.stcg_realized += s.stcg_realized
            all_summary.ltcg_realized += s.ltcg_realized
            all_summary.closing_units += s.closing_units
            all_summary.closing_value += s.closing_value
            all_summary.closing_cost += s.closing_cost

        all_summary.absolute_return = all_summary.closing_value - all_summary.closing_cost

        return all_summary

    def take_holdings_snapshot(
        self,
        user_id: int,
        snapshot_date: date,
        snapshot_type: str = "ADHOC",
        financial_year: Optional[str] = None
    ) -> HoldingsSnapshot:
        """
        Take a point-in-time holdings snapshot.

        Args:
            user_id: User ID
            snapshot_date: Date for snapshot
            snapshot_type: FY_START, FY_END, QUARTERLY, MONTHLY, ADHOC
            financial_year: Optional FY reference

        Returns:
            HoldingsSnapshot object
        """
        snapshot = HoldingsSnapshot(
            user_id=user_id,
            snapshot_date=snapshot_date,
            snapshot_type=snapshot_type,
            financial_year=financial_year
        )

        # Get holdings as of snapshot date
        query = """
            SELECT
                scheme_name, amc_name, folio_number, scheme_type,
                units, nav, current_value, cost_value, appreciation,
                annualized_return, isin, rta
            FROM mf_holdings
            WHERE user_id = ? AND nav_date <= ?
            ORDER BY current_value DESC
        """

        cursor = self.conn.execute(query, (user_id, snapshot_date.isoformat()))
        unique_schemes = set()
        unique_folios = set()

        for row in cursor.fetchall():
            holding = {
                "scheme_name": row[0],
                "amc_name": row[1],
                "folio_number": row[2],
                "scheme_type": row[3],
                "units": str(row[4] or 0),
                "nav": str(row[5] or 0),
                "current_value": str(row[6] or 0),
                "cost_value": str(row[7] or 0),
                "appreciation": str(row[8] or 0),
                "annualized_return": str(row[9] or 0),
                "isin": row[10],
                "rta": row[11]
            }
            snapshot.holdings.append(holding)

            unique_schemes.add(row[0])
            unique_folios.add(row[2])

            value = Decimal(str(row[6] or 0))
            cost = Decimal(str(row[7] or 0))
            scheme_type = row[3]

            snapshot.total_value += value
            snapshot.total_cost += cost
            snapshot.total_appreciation += Decimal(str(row[8] or 0))

            if scheme_type == "EQUITY":
                snapshot.equity_value += value
                snapshot.equity_schemes += 1
            elif scheme_type == "DEBT":
                snapshot.debt_value += value
                snapshot.debt_schemes += 1
            elif scheme_type == "HYBRID":
                snapshot.hybrid_value += value
                snapshot.hybrid_schemes += 1

        snapshot.total_schemes = len(unique_schemes)
        snapshot.total_folios = len(unique_folios)

        # Calculate weighted XIRR
        total_weight = Decimal("0")
        weighted_sum = Decimal("0")
        for h in snapshot.holdings:
            xirr = Decimal(h["annualized_return"]) if h["annualized_return"] else Decimal("0")
            value = Decimal(h["current_value"])
            if xirr and value > 0:
                weighted_sum += xirr * value
                total_weight += value

        if total_weight > 0:
            snapshot.weighted_xirr = weighted_sum / total_weight

        return snapshot

    def save_holdings_snapshot(self, snapshot: HoldingsSnapshot) -> int:
        """Save holdings snapshot to database."""
        holdings_json = json.dumps(snapshot.holdings)

        cursor = self.conn.execute("""
            INSERT INTO mf_holdings_snapshot (
                user_id, snapshot_date, snapshot_type, financial_year,
                holdings_json, total_schemes, total_folios, total_units,
                total_value, total_cost, total_appreciation,
                equity_value, equity_schemes, debt_value, debt_schemes,
                hybrid_value, hybrid_schemes, weighted_xirr
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, snapshot_date, snapshot_type) DO UPDATE SET
                holdings_json = excluded.holdings_json,
                total_schemes = excluded.total_schemes,
                total_folios = excluded.total_folios,
                total_value = excluded.total_value,
                total_cost = excluded.total_cost,
                total_appreciation = excluded.total_appreciation,
                equity_value = excluded.equity_value,
                debt_value = excluded.debt_value,
                hybrid_value = excluded.hybrid_value,
                weighted_xirr = excluded.weighted_xirr
            RETURNING id
        """, (
            snapshot.user_id, snapshot.snapshot_date.isoformat(),
            snapshot.snapshot_type, snapshot.financial_year,
            holdings_json, snapshot.total_schemes, snapshot.total_folios,
            "0",  # total_units placeholder
            str(snapshot.total_value), str(snapshot.total_cost),
            str(snapshot.total_appreciation), str(snapshot.equity_value),
            snapshot.equity_schemes, str(snapshot.debt_value), snapshot.debt_schemes,
            str(snapshot.hybrid_value), snapshot.hybrid_schemes,
            str(snapshot.weighted_xirr) if snapshot.weighted_xirr else None
        ))

        row = cursor.fetchone()
        self.conn.commit()
        return row[0] if row else cursor.lastrowid

    def calculate_yoy_growth(
        self,
        user_id: int,
        base_year: str,
        compare_year: str
    ) -> YoYGrowth:
        """
        Calculate year-over-year growth between two financial years.

        Args:
            user_id: User ID
            base_year: Base FY (e.g., "2023-24")
            compare_year: Comparison FY (e.g., "2024-25")

        Returns:
            YoYGrowth object with growth metrics
        """
        growth = YoYGrowth(
            user_id=user_id,
            base_year=base_year,
            compare_year=compare_year
        )

        # Get FY end dates
        base_end = self._get_fy_dates(base_year)[1]
        compare_end = self._get_fy_dates(compare_year)[1]

        # Get base year snapshot
        base_snapshot = self._get_snapshot_for_date(user_id, base_end)
        compare_snapshot = self._get_snapshot_for_date(user_id, compare_end)

        if base_snapshot:
            growth.base_value = base_snapshot["total_value"]
            growth.base_cost = base_snapshot["total_cost"]
            growth.base_appreciation = base_snapshot["total_appreciation"]

        if compare_snapshot:
            growth.compare_value = compare_snapshot["total_value"]
            growth.compare_cost = compare_snapshot["total_cost"]
            growth.compare_appreciation = compare_snapshot["total_appreciation"]

        # Calculate changes
        growth.value_change = growth.compare_value - growth.base_value
        if growth.base_value > 0:
            growth.value_change_pct = (growth.value_change / growth.base_value) * 100

        growth.net_investment = growth.compare_cost - growth.base_cost
        growth.appreciation_change = growth.compare_appreciation - growth.base_appreciation

        # Category-wise growth
        if base_snapshot and compare_snapshot:
            base_equity = base_snapshot.get("equity_value", Decimal("0"))
            compare_equity = compare_snapshot.get("equity_value", Decimal("0"))
            if base_equity > 0:
                growth.equity_growth_pct = ((compare_equity - base_equity) / base_equity) * 100

            base_debt = base_snapshot.get("debt_value", Decimal("0"))
            compare_debt = compare_snapshot.get("debt_value", Decimal("0"))
            if base_debt > 0:
                growth.debt_growth_pct = ((compare_debt - base_debt) / base_debt) * 100

            base_hybrid = base_snapshot.get("hybrid_value", Decimal("0"))
            compare_hybrid = compare_snapshot.get("hybrid_value", Decimal("0"))
            if base_hybrid > 0:
                growth.hybrid_growth_pct = ((compare_hybrid - base_hybrid) / base_hybrid) * 100

            # Scheme changes
            base_schemes = set(base_snapshot.get("schemes", []))
            compare_schemes = set(compare_snapshot.get("schemes", []))
            growth.schemes_added = len(compare_schemes - base_schemes)
            growth.schemes_removed = len(base_schemes - compare_schemes)
            growth.schemes_unchanged = len(base_schemes & compare_schemes)

        return growth

    def _get_snapshot_for_date(
        self,
        user_id: int,
        date_str: str
    ) -> Optional[Dict[str, Any]]:
        """Get holdings snapshot closest to a date."""
        cursor = self.conn.execute("""
            SELECT
                total_value, total_cost, total_appreciation,
                equity_value, debt_value, hybrid_value,
                holdings_json
            FROM mf_holdings_snapshot
            WHERE user_id = ? AND snapshot_date <= ?
            ORDER BY snapshot_date DESC
            LIMIT 1
        """, (user_id, date_str))

        row = cursor.fetchone()
        if row:
            holdings = json.loads(row[6]) if row[6] else []
            return {
                "total_value": Decimal(str(row[0] or 0)),
                "total_cost": Decimal(str(row[1] or 0)),
                "total_appreciation": Decimal(str(row[2] or 0)),
                "equity_value": Decimal(str(row[3] or 0)),
                "debt_value": Decimal(str(row[4] or 0)),
                "hybrid_value": Decimal(str(row[5] or 0)),
                "schemes": [h["scheme_name"] for h in holdings]
            }
        return None

    def save_yoy_growth(self, growth: YoYGrowth) -> int:
        """Save YoY growth to database."""
        cursor = self.conn.execute("""
            INSERT INTO mf_yoy_growth (
                user_id, base_year, compare_year,
                base_value, compare_value, value_change, value_change_pct,
                base_cost, compare_cost, net_investment,
                base_appreciation, compare_appreciation, appreciation_change,
                equity_growth_pct, debt_growth_pct, hybrid_growth_pct,
                schemes_added, schemes_removed, schemes_unchanged
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, base_year, compare_year) DO UPDATE SET
                base_value = excluded.base_value,
                compare_value = excluded.compare_value,
                value_change = excluded.value_change,
                value_change_pct = excluded.value_change_pct,
                net_investment = excluded.net_investment,
                appreciation_change = excluded.appreciation_change,
                equity_growth_pct = excluded.equity_growth_pct,
                debt_growth_pct = excluded.debt_growth_pct,
                hybrid_growth_pct = excluded.hybrid_growth_pct
            RETURNING id
        """, (
            growth.user_id, growth.base_year, growth.compare_year,
            str(growth.base_value), str(growth.compare_value),
            str(growth.value_change), str(growth.value_change_pct),
            str(growth.base_cost), str(growth.compare_cost), str(growth.net_investment),
            str(growth.base_appreciation), str(growth.compare_appreciation),
            str(growth.appreciation_change), str(growth.equity_growth_pct),
            str(growth.debt_growth_pct), str(growth.hybrid_growth_pct),
            growth.schemes_added, growth.schemes_removed, growth.schemes_unchanged
        ))

        row = cursor.fetchone()
        self.conn.commit()
        return row[0] if row else cursor.lastrowid

    def save_fy_summary(self, summary: FYSummary) -> int:
        """Save FY summary to database."""
        cursor = self.conn.execute("""
            INSERT INTO mf_fy_summary (
                user_id, financial_year, scheme_type, amc_name, rta,
                opening_units, opening_value, opening_cost,
                purchase_units, purchase_amount, purchase_count,
                redemption_units, redemption_amount, redemption_count,
                switch_in_units, switch_in_amount, switch_out_units, switch_out_amount,
                dividend_payout, dividend_reinvest,
                stcg_realized, ltcg_realized,
                closing_units, closing_value, closing_cost,
                absolute_return, xirr
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, financial_year, scheme_type, amc_name, rta) DO UPDATE SET
                opening_units = excluded.opening_units,
                opening_value = excluded.opening_value,
                purchase_units = excluded.purchase_units,
                purchase_amount = excluded.purchase_amount,
                redemption_units = excluded.redemption_units,
                redemption_amount = excluded.redemption_amount,
                closing_value = excluded.closing_value,
                absolute_return = excluded.absolute_return,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
        """, (
            summary.user_id, summary.financial_year, summary.scheme_type,
            summary.amc_name, summary.rta,
            str(summary.opening_units), str(summary.opening_value), str(summary.opening_cost),
            str(summary.purchase_units), str(summary.purchase_amount), summary.purchase_count,
            str(summary.redemption_units), str(summary.redemption_amount), summary.redemption_count,
            str(summary.switch_in_units), str(summary.switch_in_amount),
            str(summary.switch_out_units), str(summary.switch_out_amount),
            str(summary.dividend_payout), str(summary.dividend_reinvest),
            str(summary.stcg_realized), str(summary.ltcg_realized),
            str(summary.closing_units), str(summary.closing_value), str(summary.closing_cost),
            str(summary.absolute_return), str(summary.xirr) if summary.xirr else None
        ))

        row = cursor.fetchone()
        self.conn.commit()
        return row[0] if row else cursor.lastrowid

    def _get_fy_dates(self, financial_year: str) -> Tuple[str, str]:
        """Get start and end dates for a financial year."""
        parts = financial_year.split("-")
        start_year = int(parts[0])
        end_year = start_year + 1
        return (f"{start_year}-04-01", f"{end_year}-03-31")

    def get_multi_year_growth_report(
        self,
        user_id: int,
        years: int = 5
    ) -> pd.DataFrame:
        """Get multi-year growth trend report."""
        query = """
            SELECT
                snapshot_date, financial_year,
                total_value, total_cost, total_appreciation,
                equity_value, debt_value, hybrid_value,
                total_schemes, weighted_xirr
            FROM mf_holdings_snapshot
            WHERE user_id = ? AND snapshot_type = 'FY_END'
            ORDER BY snapshot_date DESC
            LIMIT ?
        """
        return pd.read_sql_query(query, self.conn, params=(user_id, years))
