"""ESPP processing module.

Handles ESPP purchase and sale tax calculations:
- Perquisite calculation at purchase (discount taxable)
- TCS tracking on LRS remittance
- Capital gains calculation at sale
- FIFO lot matching for sales
"""

import sqlite3
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional, List, Dict

from pfas.parsers.foreign.models import ESPPPurchase, ESPPSale
from pfas.services.currency import SBITTRateProvider


@dataclass
class ESPPLot:
    """
    ESPP lot for FIFO tracking.

    A lot represents shares from a single purchase event.
    """
    purchase_id: int
    purchase_date: date
    shares_available: Decimal
    cost_basis_usd: Decimal  # Purchase price (discounted)
    cost_basis_inr: Decimal
    market_price_usd: Decimal  # FMV at purchase
    tt_rate: Decimal


@dataclass
class ESPPSaleResult:
    """Result of processing an ESPP sale."""

    sale_date: date
    shares_sold: Decimal
    sell_price_usd: Decimal
    sell_value_usd: Decimal
    sell_value_inr: Decimal

    # Cost basis (purchase price, not market price)
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
class ESPPAnnualSummary:
    """Annual ESPP summary for tax reporting."""

    financial_year: str

    # Perquisites (purchase discount)
    total_perquisite_usd: Decimal = Decimal("0")
    total_perquisite_inr: Decimal = Decimal("0")
    purchase_count: int = 0
    total_shares_purchased: Decimal = Decimal("0")

    # LRS and TCS
    total_lrs_inr: Decimal = Decimal("0")
    total_tcs_collected: Decimal = Decimal("0")

    # Capital gains (sales)
    ltcg_usd: Decimal = Decimal("0")
    ltcg_inr: Decimal = Decimal("0")
    stcg_usd: Decimal = Decimal("0")
    stcg_inr: Decimal = Decimal("0")
    sale_count: int = 0
    total_shares_sold: Decimal = Decimal("0")


@dataclass
class TCSTracking:
    """TCS tracking for LRS remittances."""

    financial_year: str
    cumulative_lrs: Decimal = Decimal("0")
    tcs_collected: Decimal = Decimal("0")
    remaining_exemption: Decimal = Decimal("700000")  # ₹7L threshold

    TCS_THRESHOLD = Decimal("700000")
    TCS_RATE = Decimal("0.20")

    def add_remittance(self, amount_inr: Decimal) -> Decimal:
        """
        Add LRS remittance and calculate TCS.

        Args:
            amount_inr: Remittance amount in INR

        Returns:
            TCS amount on this remittance
        """
        self.cumulative_lrs += amount_inr

        # Calculate TCS on amount exceeding threshold
        if self.cumulative_lrs > self.TCS_THRESHOLD:
            taxable_amount = min(amount_inr, self.cumulative_lrs - self.TCS_THRESHOLD)
            tcs = taxable_amount * self.TCS_RATE
            self.tcs_collected += tcs
            self.remaining_exemption = Decimal("0")
            return tcs

        self.remaining_exemption = self.TCS_THRESHOLD - self.cumulative_lrs
        return Decimal("0")


class ESPPProcessor:
    """
    Processes ESPP purchase and sale transactions.

    Tax Treatment:
    - Purchase: Perquisite = (Market Price - Purchase Price) × Shares
                (taxed as salary in the year of allotment)
    - TCS: 20% on LRS remittance exceeding ₹7L
    - Sale: Capital Gain = Sale Price - Purchase Price (not market price)
    - LTCG: Holding period > 24 months (foreign stocks)
    """

    LTCG_THRESHOLD_DAYS = 730  # 24 months for foreign stocks
    TCS_THRESHOLD = Decimal("700000")  # ₹7L
    TCS_RATE = Decimal("0.20")  # 20%

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize ESPP processor.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection
        self.rate_provider = SBITTRateProvider(db_connection)
        self._lots: List[ESPPLot] = []
        self._tcs_tracking: Dict[str, TCSTracking] = {}

    def process_purchase(self, purchase: ESPPPurchase, user_id: int) -> ESPPPurchase:
        """
        Process an ESPP purchase event.

        Calculates perquisite and TCS.

        Args:
            purchase: ESPP purchase record
            user_id: User ID

        Returns:
            ESPPPurchase with INR calculations
        """
        # Get TT rate for purchase date
        tt_rate = self.rate_provider.get_rate(purchase.purchase_date)
        purchase.calculate_perquisite(tt_rate)

        # Track TCS for the financial year
        fy = self._get_financial_year(purchase.purchase_date)
        tcs = self._calculate_tcs(fy, purchase.lrs_amount_inr)
        purchase.tcs_collected = tcs

        # Save to database
        purchase_id = self._save_purchase(purchase, user_id)

        # Add to lot tracking
        self._lots.append(ESPPLot(
            purchase_id=purchase_id,
            purchase_date=purchase.purchase_date,
            shares_available=purchase.shares_purchased,
            cost_basis_usd=purchase.purchase_price_usd,
            cost_basis_inr=purchase.purchase_price_usd * tt_rate,
            market_price_usd=purchase.market_price_usd,
            tt_rate=tt_rate,
        ))

        return purchase

    def process_sale(self, sale: ESPPSale, user_id: int) -> ESPPSaleResult:
        """
        Process an ESPP sale with FIFO lot matching.

        Args:
            sale: ESPP sale record
            user_id: User ID

        Returns:
            ESPPSaleResult with matched lots and gain calculation
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

        # Calculate gain (use purchase price as cost basis, not market price)
        gain_usd = sell_value_usd - total_cost_usd
        gain_inr = sell_value_inr - total_cost_inr

        # Determine if LTCG (use earliest matched lot date)
        earliest_purchase = min(lot['purchase_date'] for lot in matched_lots) if matched_lots else sale.sell_date
        holding_days = (sale.sell_date - earliest_purchase).days
        is_ltcg = holding_days > self.LTCG_THRESHOLD_DAYS

        result = ESPPSaleResult(
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

        # Sort lots by purchase date (FIFO)
        sorted_lots = sorted(self._lots, key=lambda x: x.purchase_date)

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
                'purchase_id': lot.purchase_id,
                'purchase_date': lot.purchase_date,
                'shares': shares_from_lot,
                'cost_basis_usd': lot.cost_basis_usd,
                'cost_basis_inr': lot.cost_basis_inr,
                'market_price_usd': lot.market_price_usd,
                'holding_days': (sell_date - lot.purchase_date).days,
            })

            total_cost_usd += cost_usd
            total_cost_inr += cost_inr
            lot.shares_available -= shares_from_lot
            remaining -= shares_from_lot

        return matched, total_cost_usd, total_cost_inr

    def _calculate_tcs(self, financial_year: str, lrs_amount: Decimal) -> Decimal:
        """Calculate TCS on LRS remittance."""
        if financial_year not in self._tcs_tracking:
            self._tcs_tracking[financial_year] = TCSTracking(financial_year=financial_year)

        return self._tcs_tracking[financial_year].add_remittance(lrs_amount)

    def get_tcs_summary(self, user_id: int, financial_year: str) -> Dict:
        """
        Get TCS summary for a financial year.

        Args:
            user_id: User ID
            financial_year: FY in format '2024-25'

        Returns:
            TCS summary dict
        """
        cursor = self.conn.execute(
            """SELECT SUM(lrs_amount_inr) as total_lrs,
                      SUM(tcs_collected) as total_tcs
            FROM espp_purchases
            WHERE user_id = ?
                AND financial_year = ?""",
            (user_id, financial_year)
        )

        row = cursor.fetchone()
        return {
            'financial_year': financial_year,
            'total_lrs_inr': Decimal(str(row['total_lrs'])) if row['total_lrs'] else Decimal("0"),
            'total_tcs_collected': Decimal(str(row['total_tcs'])) if row['total_tcs'] else Decimal("0"),
            'tcs_threshold': self.TCS_THRESHOLD,
            'tcs_rate': self.TCS_RATE,
        }

    def get_annual_summary(self, user_id: int, financial_year: str) -> ESPPAnnualSummary:
        """
        Get annual ESPP summary for tax reporting.

        Args:
            user_id: User ID
            financial_year: FY in format '2024-25'

        Returns:
            ESPPAnnualSummary
        """
        # Parse FY dates
        start_year = int(financial_year.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)

        summary = ESPPAnnualSummary(financial_year=financial_year)

        # Get purchases in FY
        cursor = self.conn.execute(
            """SELECT purchase_date, shares_purchased, perquisite_inr,
                      total_perquisite_usd, lrs_amount_inr, tcs_collected
            FROM espp_purchases
            WHERE user_id = ?
                AND purchase_date >= ?
                AND purchase_date <= ?""",
            (user_id, fy_start.isoformat(), fy_end.isoformat())
        )

        for row in cursor.fetchall():
            summary.purchase_count += 1
            summary.total_shares_purchased += Decimal(str(row['shares_purchased']))
            if row['perquisite_inr']:
                summary.total_perquisite_inr += Decimal(str(row['perquisite_inr']))
            if row['total_perquisite_usd']:
                summary.total_perquisite_usd += Decimal(str(row['total_perquisite_usd']))
            if row['lrs_amount_inr']:
                summary.total_lrs_inr += Decimal(str(row['lrs_amount_inr']))
            if row['tcs_collected']:
                summary.total_tcs_collected += Decimal(str(row['tcs_collected']))

        # Get sales in FY
        cursor = self.conn.execute(
            """SELECT sale_date, shares_sold, gain_inr, gain_usd, is_ltcg
            FROM espp_sales
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

    def _get_financial_year(self, dt: date) -> str:
        """Get financial year for a date."""
        if dt.month >= 4:
            return f"{dt.year}-{str(dt.year + 1)[2:]}"
        else:
            return f"{dt.year - 1}-{str(dt.year)[2:]}"

    def _save_purchase(self, purchase: ESPPPurchase, user_id: int) -> int:
        """Save ESPP purchase to database."""
        fy = self._get_financial_year(purchase.purchase_date)

        cursor = self.conn.execute(
            """INSERT INTO espp_purchases
            (user_id, purchase_date, shares_purchased, purchase_price_usd,
             market_price_usd, discount_percentage, perquisite_per_share_usd,
             total_perquisite_usd, tt_rate, perquisite_inr, purchase_value_inr,
             lrs_amount_inr, tcs_collected, financial_year)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                user_id,
                purchase.purchase_date.isoformat(),
                str(purchase.shares_purchased),
                str(purchase.purchase_price_usd),
                str(purchase.market_price_usd),
                str(purchase.discount_percentage),
                str(purchase.perquisite_per_share_usd),
                str(purchase.total_perquisite_usd),
                str(purchase.tt_rate) if purchase.tt_rate else None,
                str(purchase.perquisite_inr),
                str(purchase.purchase_value_inr),
                str(purchase.lrs_amount_inr),
                str(purchase.tcs_collected),
                fy,
            )
        )
        self.conn.commit()
        return cursor.lastrowid

    def _save_sale(self, result: ESPPSaleResult, user_id: int) -> int:
        """Save ESPP sale to database."""
        cursor = self.conn.execute(
            """INSERT INTO espp_sales
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
                str(result.matched_lots),
            )
        )
        self.conn.commit()
        return cursor.lastrowid

    def _load_lots(self, user_id: int) -> None:
        """Load available lots from database."""
        cursor = self.conn.execute(
            """SELECT id, purchase_date, shares_purchased, purchase_price_usd,
                      market_price_usd, tt_rate
            FROM espp_purchases
            WHERE user_id = ?
            ORDER BY purchase_date""",
            (user_id,)
        )

        self._lots = []
        for row in cursor.fetchall():
            tt_rate = Decimal(str(row['tt_rate'])) if row['tt_rate'] else Decimal("83")
            purchase_price = Decimal(str(row['purchase_price_usd']))

            self._lots.append(ESPPLot(
                purchase_id=row['id'],
                purchase_date=date.fromisoformat(row['purchase_date'])
                if isinstance(row['purchase_date'], str) else row['purchase_date'],
                shares_available=Decimal(str(row['shares_purchased'])),
                cost_basis_usd=purchase_price,
                cost_basis_inr=purchase_price * tt_rate,
                market_price_usd=Decimal(str(row['market_price_usd'])),
                tt_rate=tt_rate,
            ))
