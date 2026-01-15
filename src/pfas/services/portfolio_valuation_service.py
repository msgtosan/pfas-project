"""
Portfolio Valuation Service.

Provides:
1. Current portfolio valuation with unrealized gains
2. Portfolio summary by asset class
3. XIRR calculation for investment performance
4. Holdings breakdown with cost basis

Note: Current prices/NAVs would require external API integration.
This implementation uses last known prices from database.
"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional, Dict, Any
import sqlite3
import math

from pfas.core.models import AssetHolding, AssetCategory


@dataclass
class PortfolioSummary:
    """Summary of portfolio by asset class."""
    as_of_date: date

    # Totals
    total_invested: Decimal = Decimal("0")
    total_current_value: Decimal = Decimal("0")
    total_unrealized_gain: Decimal = Decimal("0")

    # By asset class
    mutual_funds_invested: Decimal = Decimal("0")
    mutual_funds_current: Decimal = Decimal("0")
    stocks_invested: Decimal = Decimal("0")
    stocks_current: Decimal = Decimal("0")
    foreign_invested: Decimal = Decimal("0")
    foreign_current: Decimal = Decimal("0")

    # Holdings details
    holdings: List[AssetHolding] = field(default_factory=list)

    @property
    def overall_return_percent(self) -> Optional[Decimal]:
        """Overall return percentage."""
        if self.total_invested and self.total_invested > 0:
            return ((self.total_current_value - self.total_invested) / self.total_invested * 100
                    ).quantize(Decimal("0.01"))
        return None


@dataclass
class XIRRResult:
    """Result of XIRR calculation."""
    asset_type: str
    xirr_percent: Optional[Decimal]
    total_invested: Decimal
    total_current_value: Decimal
    investment_period_days: int
    error: Optional[str] = None


class PortfolioValuationService:
    """
    Service for portfolio valuation and performance calculation.

    Example:
        service = PortfolioValuationService(conn)
        summary = service.get_portfolio_summary(user_id=1)
        print(f"Total Value: {summary.total_current_value}")
        print(f"Return: {summary.overall_return_percent}%")
    """

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize with database connection.

        Args:
            db_connection: SQLite connection object
        """
        self.conn = db_connection

    def get_portfolio_summary(
        self,
        user_id: int,
        as_of: Optional[date] = None
    ) -> PortfolioSummary:
        """
        Get complete portfolio summary.

        Args:
            user_id: User ID
            as_of: Valuation date (default: today)

        Returns:
            PortfolioSummary with all holdings and totals
        """
        if as_of is None:
            as_of = date.today()

        summary = PortfolioSummary(as_of_date=as_of)

        # Get mutual fund holdings
        mf_holdings = self.value_mf_holdings(user_id, as_of)
        for h in mf_holdings:
            summary.holdings.append(h)
            summary.mutual_funds_invested += h.cost_basis
            summary.mutual_funds_current += h.total_value

        # Get stock holdings
        stock_holdings = self.value_stock_holdings(user_id, as_of)
        for h in stock_holdings:
            summary.holdings.append(h)
            if h.asset_type == AssetCategory.STOCK_INDIAN:
                summary.stocks_invested += h.cost_basis
                summary.stocks_current += h.total_value
            else:
                summary.foreign_invested += h.cost_basis
                summary.foreign_current += h.total_value

        # Calculate totals
        summary.total_invested = (
            summary.mutual_funds_invested + summary.stocks_invested + summary.foreign_invested
        )
        summary.total_current_value = (
            summary.mutual_funds_current + summary.stocks_current + summary.foreign_current
        )
        summary.total_unrealized_gain = summary.total_current_value - summary.total_invested

        return summary

    def value_mf_holdings(
        self,
        user_id: int,
        as_of: Optional[date] = None
    ) -> List[AssetHolding]:
        """
        Value mutual fund holdings.

        Args:
            user_id: User ID
            as_of: Valuation date

        Returns:
            List of AssetHolding with current values
        """
        if as_of is None:
            as_of = date.today()

        holdings = []

        cursor = self.conn.execute("""
            SELECT ms.id, ms.name, ms.asset_class, ms.isin,
                   SUM(CASE WHEN mt.transaction_type IN ('PURCHASE', 'SWITCH_IN', 'DIVIDEND_REINVEST')
                       THEN mt.units ELSE -mt.units END) as net_units,
                   SUM(CASE WHEN mt.transaction_type IN ('PURCHASE', 'SWITCH_IN', 'DIVIDEND_REINVEST')
                       THEN mt.amount ELSE 0 END) as total_invested,
                   SUM(CASE WHEN mt.transaction_type IN ('PURCHASE', 'SWITCH_IN', 'DIVIDEND_REINVEST')
                       THEN mt.units ELSE 0 END) as total_units_bought
            FROM mf_transactions mt
            JOIN mf_folios mf ON mt.folio_id = mf.id
            JOIN mf_schemes ms ON mf.scheme_id = ms.id
            WHERE mf.user_id = ? AND mt.date <= ?
            GROUP BY ms.id
            HAVING net_units > 0.001
        """, (user_id, as_of.isoformat()))

        for row in cursor.fetchall():
            scheme_id = row[0]
            scheme_name = row[1]
            asset_class = row[2]
            isin = row[3]
            units = Decimal(str(row[4] or 0))
            total_invested = Decimal(str(row[5] or 0))
            total_units_bought = Decimal(str(row[6] or 0))

            # Calculate average cost per unit
            avg_cost_per_unit = total_invested / total_units_bought if total_units_bought > 0 else Decimal("0")
            cost_basis = units * avg_cost_per_unit

            # Get current NAV
            current_nav = self._get_current_nav(scheme_id, as_of)
            current_value = units * current_nav

            # Determine asset category
            if asset_class == 'EQUITY':
                asset_cat = AssetCategory.MUTUAL_FUND_EQUITY
            elif asset_class == 'DEBT':
                asset_cat = AssetCategory.MUTUAL_FUND_DEBT
            elif asset_class == 'HYBRID':
                asset_cat = AssetCategory.MUTUAL_FUND_HYBRID
            else:
                asset_cat = AssetCategory.MUTUAL_FUND_LIQUID

            holdings.append(AssetHolding(
                asset_type=asset_cat,
                asset_identifier=isin or str(scheme_id),
                asset_name=scheme_name,
                quantity=units,
                unit_price=current_nav,
                total_value=current_value,
                cost_basis=cost_basis,
                unrealized_gain=current_value - cost_basis,
                source_table="mf_schemes",
                source_id=scheme_id,
                as_of_date=as_of,
            ))

        return holdings

    def value_stock_holdings(
        self,
        user_id: int,
        as_of: Optional[date] = None
    ) -> List[AssetHolding]:
        """
        Value stock holdings (Indian and foreign).

        Args:
            user_id: User ID
            as_of: Valuation date

        Returns:
            List of AssetHolding with current values
        """
        if as_of is None:
            as_of = date.today()

        holdings = []

        # Indian stocks
        cursor = self.conn.execute("""
            SELECT symbol, isin,
                   SUM(CASE WHEN trade_type = 'BUY' THEN quantity ELSE -quantity END) as net_qty,
                   SUM(CASE WHEN trade_type = 'BUY' THEN net_amount ELSE 0 END) as total_buy,
                   SUM(CASE WHEN trade_type = 'BUY' THEN quantity ELSE 0 END) as total_buy_qty
            FROM stock_trades
            WHERE user_id = ? AND trade_date <= ?
              AND trade_category = 'DELIVERY'
            GROUP BY symbol
            HAVING net_qty > 0
        """, (user_id, as_of.isoformat()))

        for row in cursor.fetchall():
            symbol = row[0]
            isin = row[1]
            net_qty = int(row[2] or 0)
            total_buy_cost = Decimal(str(row[3] or 0))
            total_buy_qty = int(row[4] or 0)

            avg_cost = total_buy_cost / total_buy_qty if total_buy_qty > 0 else Decimal("0")
            cost_basis = net_qty * avg_cost

            # Get current price (placeholder)
            current_price = self._get_current_stock_price(symbol, as_of)
            current_value = net_qty * current_price

            holdings.append(AssetHolding(
                asset_type=AssetCategory.STOCK_INDIAN,
                asset_identifier=isin or symbol,
                asset_name=symbol,
                quantity=Decimal(str(net_qty)),
                unit_price=current_price,
                total_value=current_value,
                cost_basis=cost_basis,
                unrealized_gain=current_value - cost_basis,
                source_table="stock_trades",
                as_of_date=as_of,
            ))

        # Foreign stocks
        cursor = self.conn.execute("""
            SELECT symbol, SUM(shares_held) as shares, SUM(total_value_usd) as value_usd
            FROM foreign_holdings
            WHERE user_id = ?
            GROUP BY symbol
            HAVING shares > 0
        """, (user_id,))

        exchange_rate = self._get_exchange_rate(as_of, "USD")

        for row in cursor.fetchall():
            symbol = row[0]
            shares = Decimal(str(row[1] or 0))
            value_usd = Decimal(str(row[2] or 0))

            current_value = value_usd * exchange_rate
            unit_price = current_value / shares if shares > 0 else Decimal("0")

            holdings.append(AssetHolding(
                asset_type=AssetCategory.STOCK_FOREIGN,
                asset_identifier=symbol,
                asset_name=f"{symbol} (US)",
                quantity=shares,
                unit_price=unit_price,
                total_value=current_value,
                cost_basis=current_value,  # Would need proper cost tracking
                unrealized_gain=Decimal("0"),
                currency="USD",
                source_table="foreign_holdings",
                as_of_date=as_of,
            ))

        return holdings

    def calculate_xirr(
        self,
        user_id: int,
        asset_type: Optional[str] = None
    ) -> XIRRResult:
        """
        Calculate XIRR for investments.

        Args:
            user_id: User ID
            asset_type: Optional filter ('MF', 'STOCK', 'ALL')

        Returns:
            XIRRResult with XIRR percentage
        """
        # Collect all cash flows (investments as negative, current value as positive)
        cash_flows = []
        dates = []

        today = date.today()

        # Get MF transactions
        if asset_type in (None, 'ALL', 'MF'):
            cursor = self.conn.execute("""
                SELECT mt.date, mt.transaction_type, mt.amount
                FROM mf_transactions mt
                JOIN mf_folios mf ON mt.folio_id = mf.id
                WHERE mf.user_id = ?
                ORDER BY mt.date
            """, (user_id,))

            for row in cursor.fetchall():
                txn_date = date.fromisoformat(row[0]) if isinstance(row[0], str) else row[0]
                txn_type = row[1]
                amount = Decimal(str(row[2] or 0))

                if txn_type in ('PURCHASE', 'SWITCH_IN'):
                    cash_flows.append(float(-amount))  # Outflow
                elif txn_type in ('REDEMPTION', 'SWITCH_OUT'):
                    cash_flows.append(float(amount))  # Inflow
                dates.append(txn_date)

        # Get stock transactions
        if asset_type in (None, 'ALL', 'STOCK'):
            cursor = self.conn.execute("""
                SELECT trade_date, trade_type, net_amount
                FROM stock_trades
                WHERE user_id = ? AND trade_category = 'DELIVERY'
                ORDER BY trade_date
            """, (user_id,))

            for row in cursor.fetchall():
                trade_date = date.fromisoformat(row[0]) if isinstance(row[0], str) else row[0]
                trade_type = row[1]
                amount = Decimal(str(row[2] or 0))

                if trade_type == 'BUY':
                    cash_flows.append(float(-amount))
                elif trade_type == 'SELL':
                    cash_flows.append(float(amount))
                dates.append(trade_date)

        if not cash_flows:
            return XIRRResult(
                asset_type=asset_type or 'ALL',
                xirr_percent=None,
                total_invested=Decimal("0"),
                total_current_value=Decimal("0"),
                investment_period_days=0,
                error="No transactions found"
            )

        # Add current portfolio value as final positive cash flow
        summary = self.get_portfolio_summary(user_id, today)
        if asset_type == 'MF':
            current_value = summary.mutual_funds_current
        elif asset_type == 'STOCK':
            current_value = summary.stocks_current + summary.foreign_current
        else:
            current_value = summary.total_current_value

        cash_flows.append(float(current_value))
        dates.append(today)

        # Calculate XIRR
        try:
            xirr = self._calculate_xirr(cash_flows, dates)
            xirr_percent = Decimal(str(xirr * 100)).quantize(Decimal("0.01")) if xirr else None
        except Exception as e:
            return XIRRResult(
                asset_type=asset_type or 'ALL',
                xirr_percent=None,
                total_invested=summary.total_invested,
                total_current_value=summary.total_current_value,
                investment_period_days=(today - min(dates)).days if dates else 0,
                error=str(e)
            )

        return XIRRResult(
            asset_type=asset_type or 'ALL',
            xirr_percent=xirr_percent,
            total_invested=summary.total_invested,
            total_current_value=summary.total_current_value,
            investment_period_days=(today - min(dates)).days if dates else 0,
        )

    def _calculate_xirr(
        self,
        cash_flows: List[float],
        dates: List[date]
    ) -> Optional[float]:
        """
        Calculate XIRR using Newton-Raphson method.

        Args:
            cash_flows: List of cash flows (negative for outflows)
            dates: List of corresponding dates

        Returns:
            XIRR as decimal (0.10 = 10%)
        """
        if len(cash_flows) < 2:
            return None

        # Convert dates to year fractions from first date
        first_date = min(dates)
        year_fracs = [(d - first_date).days / 365.0 for d in dates]

        def npv(rate):
            return sum(cf / (1 + rate) ** yf for cf, yf in zip(cash_flows, year_fracs))

        def npv_derivative(rate):
            return sum(-yf * cf / (1 + rate) ** (yf + 1) for cf, yf in zip(cash_flows, year_fracs))

        # Newton-Raphson
        rate = 0.1  # Initial guess
        for _ in range(100):
            npv_val = npv(rate)
            if abs(npv_val) < 0.001:
                return rate

            derivative = npv_derivative(rate)
            if abs(derivative) < 1e-10:
                break

            rate = rate - npv_val / derivative

            if rate < -1:
                rate = -0.99

        return rate if abs(npv(rate)) < 1 else None

    def _get_current_nav(self, scheme_id: int, as_of: date) -> Decimal:
        """Get current NAV for a scheme."""
        cursor = self.conn.execute("""
            SELECT nav FROM mf_transactions
            WHERE folio_id IN (SELECT id FROM mf_folios WHERE scheme_id = ?)
              AND date <= ?
            ORDER BY date DESC
            LIMIT 1
        """, (scheme_id, as_of.isoformat()))

        row = cursor.fetchone()
        return Decimal(str(row[0])) if row and row[0] else Decimal("0")

    def _get_current_stock_price(self, symbol: str, as_of: date) -> Decimal:
        """Get current stock price (uses last trade price as placeholder)."""
        cursor = self.conn.execute("""
            SELECT price FROM stock_trades
            WHERE symbol = ? AND trade_date <= ?
            ORDER BY trade_date DESC
            LIMIT 1
        """, (symbol, as_of.isoformat()))

        row = cursor.fetchone()
        return Decimal(str(row[0])) if row and row[0] else Decimal("0")

    def _get_exchange_rate(self, as_of: date, currency: str) -> Decimal:
        """Get exchange rate."""
        cursor = self.conn.execute("""
            SELECT rate FROM exchange_rates
            WHERE from_currency = ? AND to_currency = 'INR'
              AND date <= ?
            ORDER BY date DESC
            LIMIT 1
        """, (currency, as_of.isoformat()))

        row = cursor.fetchone()
        return Decimal(str(row[0])) if row and row[0] else Decimal("83.5")
