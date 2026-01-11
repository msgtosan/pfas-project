"""RSU processing module.

Handles RSU vest and sale tax calculations:
- Perquisite calculation at vest (taxed as salary)
- Capital gains calculation at sale
- Correlation with payslip RSU tax credits
- FIFO lot matching for sales
"""

import sqlite3
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional, List, Dict

from pfas.parsers.foreign.models import RSUVest, RSUSale
from pfas.services.currency import SBITTRateProvider


@dataclass
class RSULot:
    """
    RSU lot for FIFO tracking.

    A lot represents shares from a single vest event.
    """
    vest_id: int
    vest_date: date
    shares_available: Decimal
    cost_basis_usd: Decimal
    cost_basis_inr: Decimal
    tt_rate: Decimal
    grant_number: str = ""


@dataclass
class RSUSaleResult:
    """Result of processing an RSU sale."""

    sale_date: date
    shares_sold: Decimal
    sell_price_usd: Decimal
    sell_value_usd: Decimal
    sell_value_inr: Decimal

    # Cost basis (from matched vests)
    cost_basis_usd: Decimal
    cost_basis_inr: Decimal

    # Gain/loss
    gain_usd: Decimal
    gain_inr: Decimal
    is_ltcg: bool
    holding_period_days: int

    # Matched lots
    matched_lots: List[Dict] = field(default_factory=list)

    # Fees
    fees_usd: Decimal = Decimal("0")
    fees_inr: Decimal = Decimal("0")

    @property
    def net_gain_usd(self) -> Decimal:
        """Net gain after fees."""
        return self.gain_usd - self.fees_usd

    @property
    def net_gain_inr(self) -> Decimal:
        """Net gain in INR after fees."""
        return self.gain_inr - self.fees_inr


@dataclass
class RSUAnnualSummary:
    """Annual RSU summary for tax reporting."""

    financial_year: str

    # Perquisites (vest events)
    total_perquisite_usd: Decimal = Decimal("0")
    total_perquisite_inr: Decimal = Decimal("0")
    vest_count: int = 0
    total_shares_vested: Decimal = Decimal("0")

    # Capital gains (sales)
    ltcg_usd: Decimal = Decimal("0")
    ltcg_inr: Decimal = Decimal("0")
    stcg_usd: Decimal = Decimal("0")
    stcg_inr: Decimal = Decimal("0")
    sale_count: int = 0
    total_shares_sold: Decimal = Decimal("0")

    # Tax credits/withholding
    us_tax_withheld_usd: Decimal = Decimal("0")
    us_tax_withheld_inr: Decimal = Decimal("0")


class RSUProcessor:
    """
    Processes RSU vest and sale transactions.

    Tax Treatment:
    - Vest: Perquisite = FMV × Shares × TT Rate (taxed as salary)
    - Sale: Capital Gain = Sale Price - Cost Basis (FMV at vest)
    - LTCG: Holding period > 24 months (foreign stocks)
    """

    LTCG_THRESHOLD_DAYS = 730  # 24 months for foreign stocks

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize RSU processor.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection
        self.rate_provider = SBITTRateProvider(db_connection)
        self._lots: List[RSULot] = []

    def process_vest(self, vest: RSUVest, user_id: int) -> RSUVest:
        """
        Process an RSU vest event.

        Calculates perquisite in INR and saves to database.

        Args:
            vest: RSU vest record
            user_id: User ID

        Returns:
            RSUVest with INR calculations
        """
        # Get TT rate for vest date
        tt_rate = self.rate_provider.get_rate(vest.vest_date)
        vest.calculate_perquisite(tt_rate)

        # Save to database
        vest_id = self._save_vest(vest, user_id)

        # Add to lot tracking
        self._lots.append(RSULot(
            vest_id=vest_id,
            vest_date=vest.vest_date,
            shares_available=vest.net_shares,
            cost_basis_usd=vest.fmv_usd,
            cost_basis_inr=vest.cost_basis_per_share_inr or (vest.fmv_usd * tt_rate),
            tt_rate=tt_rate,
            grant_number=vest.grant_number,
        ))

        return vest

    def process_sale(self, sale: RSUSale, user_id: int) -> RSUSaleResult:
        """
        Process an RSU sale with FIFO lot matching.

        Args:
            sale: RSU sale record
            user_id: User ID

        Returns:
            RSUSaleResult with matched lots and gain calculation
        """
        # Get TT rate for sale date
        tt_rate = self.rate_provider.get_rate(sale.sell_date)

        # Load lots if not loaded
        if not self._lots:
            self._load_lots(user_id)

        # FIFO match
        matched_lots, total_cost_usd, total_cost_inr = self._fifo_match(
            sale.shares_sold, sale.sell_date
        )

        # Calculate sale value
        sell_value_usd = sale.shares_sold * sale.sell_price_usd
        sell_value_inr = sell_value_usd * tt_rate

        # Calculate gain
        gain_usd = sell_value_usd - total_cost_usd - sale.fees_usd
        gain_inr = sell_value_inr - total_cost_inr - (sale.fees_usd * tt_rate)

        # Determine if LTCG (use earliest matched lot date)
        earliest_vest = min(lot['vest_date'] for lot in matched_lots) if matched_lots else sale.sell_date
        holding_days = (sale.sell_date - earliest_vest).days
        is_ltcg = holding_days > self.LTCG_THRESHOLD_DAYS

        result = RSUSaleResult(
            sale_date=sale.sell_date,
            shares_sold=sale.shares_sold,
            sell_price_usd=sale.sell_price_usd,
            sell_value_usd=sell_value_usd,
            sell_value_inr=sell_value_inr,
            cost_basis_usd=total_cost_usd,
            cost_basis_inr=total_cost_inr,
            gain_usd=gain_usd,
            gain_inr=gain_inr,
            is_ltcg=is_ltcg,
            holding_period_days=holding_days,
            matched_lots=matched_lots,
            fees_usd=sale.fees_usd,
            fees_inr=sale.fees_usd * tt_rate,
        )

        # Save to database
        self._save_sale(result, user_id)

        return result

    def _fifo_match(
        self,
        shares_to_sell: Decimal,
        sell_date: date
    ) -> tuple[List[Dict], Decimal, Decimal]:
        """
        Match shares using FIFO method.

        Args:
            shares_to_sell: Number of shares to sell
            sell_date: Sale date

        Returns:
            Tuple of (matched lots, total cost USD, total cost INR)
        """
        matched = []
        remaining = shares_to_sell
        total_cost_usd = Decimal("0")
        total_cost_inr = Decimal("0")

        # Sort lots by vest date (FIFO)
        sorted_lots = sorted(self._lots, key=lambda x: x.vest_date)

        for lot in sorted_lots:
            if remaining <= Decimal("0"):
                break

            if lot.shares_available <= Decimal("0"):
                continue

            # Use shares from this lot
            shares_from_lot = min(remaining, lot.shares_available)
            cost_usd = shares_from_lot * lot.cost_basis_usd
            cost_inr = shares_from_lot * lot.cost_basis_inr

            matched.append({
                'vest_id': lot.vest_id,
                'vest_date': lot.vest_date,
                'shares': shares_from_lot,
                'cost_basis_usd': lot.cost_basis_usd,
                'cost_basis_inr': lot.cost_basis_inr,
                'holding_days': (sell_date - lot.vest_date).days,
                'grant_number': lot.grant_number,
            })

            total_cost_usd += cost_usd
            total_cost_inr += cost_inr
            lot.shares_available -= shares_from_lot
            remaining -= shares_from_lot

        if remaining > Decimal("0"):
            # Warning: not enough lots to cover sale
            pass

        return matched, total_cost_usd, total_cost_inr

    def get_annual_summary(self, user_id: int, financial_year: str) -> RSUAnnualSummary:
        """
        Get annual RSU summary for tax reporting.

        Args:
            user_id: User ID
            financial_year: FY in format '2024-25'

        Returns:
            RSUAnnualSummary
        """
        # Parse FY dates
        start_year = int(financial_year.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)

        summary = RSUAnnualSummary(financial_year=financial_year)

        # Get vests in FY
        cursor = self.conn.execute(
            """SELECT vest_date, shares_vested, fmv_usd, perquisite_inr, net_shares
            FROM rsu_vests
            WHERE user_id = ?
                AND vest_date >= ?
                AND vest_date <= ?""",
            (user_id, fy_start.isoformat(), fy_end.isoformat())
        )

        for row in cursor.fetchall():
            summary.vest_count += 1
            summary.total_shares_vested += Decimal(str(row['shares_vested']))
            summary.total_perquisite_usd += (
                Decimal(str(row['shares_vested'])) * Decimal(str(row['fmv_usd']))
            )
            if row['perquisite_inr']:
                summary.total_perquisite_inr += Decimal(str(row['perquisite_inr']))

        # Get sales in FY
        cursor = self.conn.execute(
            """SELECT sale_date, shares_sold, gain_inr, gain_usd, is_ltcg
            FROM rsu_sales
            WHERE user_id = ?
                AND sale_date >= ?
                AND sale_date <= ?""",
            (user_id, fy_start.isoformat(), fy_end.isoformat())
        )

        for row in cursor.fetchall():
            summary.sale_count += 1
            summary.total_shares_sold += Decimal(str(row['shares_sold']))
            gain_inr = Decimal(str(row['gain_inr']))
            gain_usd = Decimal(str(row['gain_usd']))

            if row['is_ltcg']:
                summary.ltcg_inr += gain_inr
                summary.ltcg_usd += gain_usd
            else:
                summary.stcg_inr += gain_inr
                summary.stcg_usd += gain_usd

        return summary

    def correlate_with_salary(self, user_id: int, vest_id: int, salary_record_id: int) -> bool:
        """
        Correlate RSU vest with salary record.

        Links the vest perquisite with the corresponding payslip entry.

        Args:
            user_id: User ID
            vest_id: RSU vest ID
            salary_record_id: Salary record ID

        Returns:
            True if successful
        """
        self.conn.execute(
            """UPDATE rsu_vests
            SET salary_record_id = ?, correlation_status = 'MATCHED'
            WHERE id = ? AND user_id = ?""",
            (salary_record_id, vest_id, user_id)
        )
        self.conn.commit()
        return True

    def get_uncorrelated_vests(self, user_id: int) -> List[Dict]:
        """
        Get vests not yet correlated with salary records.

        Args:
            user_id: User ID

        Returns:
            List of uncorrelated vest records
        """
        cursor = self.conn.execute(
            """SELECT id, vest_date, shares_vested, fmv_usd, perquisite_inr, grant_number
            FROM rsu_vests
            WHERE user_id = ?
                AND correlation_status = 'PENDING'
            ORDER BY vest_date""",
            (user_id,)
        )

        vests = []
        for row in cursor.fetchall():
            vests.append({
                'vest_id': row['id'],
                'vest_date': date.fromisoformat(row['vest_date'])
                if isinstance(row['vest_date'], str) else row['vest_date'],
                'shares_vested': Decimal(str(row['shares_vested'])),
                'fmv_usd': Decimal(str(row['fmv_usd'])),
                'perquisite_inr': Decimal(str(row['perquisite_inr']))
                if row['perquisite_inr'] else None,
                'grant_number': row['grant_number'],
            })

        return vests

    def _save_vest(self, vest: RSUVest, user_id: int) -> int:
        """Save RSU vest to database."""
        cursor = self.conn.execute(
            """INSERT INTO rsu_vests
            (user_id, grant_number, vest_date, shares_vested, fmv_usd,
             shares_withheld_for_tax, net_shares, tt_rate, perquisite_inr,
             correlation_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                user_id,
                vest.grant_number,
                vest.vest_date.isoformat(),
                str(vest.shares_vested),
                str(vest.fmv_usd),
                str(vest.shares_withheld_for_tax),
                str(vest.net_shares),
                str(vest.tt_rate) if vest.tt_rate else None,
                str(vest.perquisite_inr) if vest.perquisite_inr else None,
                vest.correlation_status,
            )
        )
        self.conn.commit()
        return cursor.lastrowid

    def _save_sale(self, result: RSUSaleResult, user_id: int) -> int:
        """Save RSU sale to database."""
        cursor = self.conn.execute(
            """INSERT INTO rsu_sales
            (user_id, sale_date, shares_sold, sell_price_usd, sell_value_usd,
             sell_value_inr, cost_basis_usd, cost_basis_inr, gain_usd, gain_inr,
             is_ltcg, holding_period_days, fees_usd, fees_inr, matched_lots)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                user_id,
                result.sale_date.isoformat(),
                str(result.shares_sold),
                str(result.sell_price_usd),
                str(result.sell_value_usd),
                str(result.sell_value_inr),
                str(result.cost_basis_usd),
                str(result.cost_basis_inr),
                str(result.gain_usd),
                str(result.gain_inr),
                result.is_ltcg,
                result.holding_period_days,
                str(result.fees_usd),
                str(result.fees_inr),
                str(result.matched_lots),  # JSON serialization
            )
        )
        self.conn.commit()
        return cursor.lastrowid

    def _load_lots(self, user_id: int) -> None:
        """Load available lots from database."""
        cursor = self.conn.execute(
            """SELECT id, vest_date, net_shares, fmv_usd, tt_rate, grant_number
            FROM rsu_vests
            WHERE user_id = ?
            ORDER BY vest_date""",
            (user_id,)
        )

        self._lots = []
        for row in cursor.fetchall():
            tt_rate = Decimal(str(row['tt_rate'])) if row['tt_rate'] else Decimal("83")
            fmv_usd = Decimal(str(row['fmv_usd']))

            self._lots.append(RSULot(
                vest_id=row['id'],
                vest_date=date.fromisoformat(row['vest_date'])
                if isinstance(row['vest_date'], str) else row['vest_date'],
                shares_available=Decimal(str(row['net_shares'])),
                cost_basis_usd=fmv_usd,
                cost_basis_inr=fmv_usd * tt_rate,
                tt_rate=tt_rate,
                grant_number=row['grant_number'] or "",
            ))
