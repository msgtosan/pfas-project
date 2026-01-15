"""
Balance Sheet Service.

Generates balance sheet snapshots by:
1. Aggregating assets from MF, Stock, Bank, EPF, PPF, NPS, SGB tables
2. Aggregating liabilities from liabilities table
3. Computing net worth

All data is fetched from database - no file parsing.
"""

from datetime import date
from decimal import Decimal
from typing import List, Optional, Dict, Any
import json
import sqlite3

from pfas.core.models import (
    AssetHolding,
    AssetCategory,
    Liability,
    LiabilityType,
    BalanceSheetSnapshot,
)


class BalanceSheetService:
    """
    Service for generating balance sheet snapshots.

    Aggregates assets from:
    - Bank accounts (balances)
    - Mutual funds (current holdings)
    - Stocks (current holdings)
    - EPF (accumulated balance)
    - PPF (accumulated balance)
    - NPS (accumulated balance)
    - SGB (holdings)

    Aggregates liabilities from:
    - Liabilities table (loans, credit cards)

    Example:
        service = BalanceSheetService(conn)
        snapshot = service.get_balance_sheet(user_id=1, as_of=date.today())
        print(f"Net Worth: {snapshot.net_worth}")
    """

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize with database connection.

        Args:
            db_connection: SQLite connection object
        """
        self.conn = db_connection

    def get_balance_sheet(
        self,
        user_id: int,
        as_of: date
    ) -> BalanceSheetSnapshot:
        """
        Generate complete balance sheet as of a specific date.

        Args:
            user_id: User ID
            as_of: Snapshot date

        Returns:
            BalanceSheetSnapshot with all assets and liabilities
        """
        snapshot = BalanceSheetSnapshot(snapshot_date=as_of)

        # Aggregate assets
        self._populate_bank_balances(snapshot, user_id, as_of)
        self._populate_mutual_funds(snapshot, user_id, as_of)
        self._populate_stocks(snapshot, user_id, as_of)
        self._populate_retirement_funds(snapshot, user_id, as_of)
        self._populate_other_investments(snapshot, user_id, as_of)

        # Aggregate liabilities
        self._populate_liabilities(snapshot, user_id, as_of)

        return snapshot

    def _populate_bank_balances(
        self,
        snapshot: BalanceSheetSnapshot,
        user_id: int,
        as_of: date
    ) -> None:
        """Populate bank balances from bank_transactions."""
        # Get latest balance for each account
        cursor = self.conn.execute("""
            SELECT ba.account_type, bt.balance
            FROM bank_accounts ba
            JOIN bank_transactions bt ON ba.id = bt.bank_account_id
            WHERE ba.user_id = ?
              AND bt.date <= ?
              AND bt.id = (
                  SELECT MAX(bt2.id)
                  FROM bank_transactions bt2
                  WHERE bt2.bank_account_id = ba.id
                    AND bt2.date <= ?
              )
        """, (user_id, as_of.isoformat(), as_of.isoformat()))

        for row in cursor.fetchall():
            account_type = row[0]
            balance = Decimal(str(row[1] or 0))

            if account_type == 'SAVINGS':
                snapshot.bank_savings += balance
            elif account_type == 'CURRENT':
                snapshot.bank_current += balance
            elif account_type == 'FD':
                snapshot.bank_fd += balance

    def _populate_mutual_funds(
        self,
        snapshot: BalanceSheetSnapshot,
        user_id: int,
        as_of: date
    ) -> None:
        """Populate mutual fund holdings from mf_transactions."""
        # Get current holdings per scheme
        cursor = self.conn.execute("""
            SELECT ms.id, ms.name, ms.asset_class, ms.isin,
                   SUM(CASE WHEN mt.transaction_type IN ('PURCHASE', 'SWITCH_IN', 'DIVIDEND_REINVEST')
                       THEN mt.units ELSE -mt.units END) as net_units,
                   AVG(mt.nav) as avg_nav
            FROM mf_transactions mt
            JOIN mf_folios mf ON mt.folio_id = mf.id
            JOIN mf_schemes ms ON mf.scheme_id = ms.id
            WHERE mf.user_id = ? AND mt.date <= ?
            GROUP BY ms.id
            HAVING net_units > 0
        """, (user_id, as_of.isoformat()))

        for row in cursor.fetchall():
            scheme_id = row[0]
            scheme_name = row[1]
            asset_class = row[2]
            isin = row[3]
            units = Decimal(str(row[4] or 0))
            avg_nav = Decimal(str(row[5] or 0))

            # Get latest NAV (or use average if not available)
            current_nav = self._get_latest_nav(scheme_id) or avg_nav
            total_value = units * current_nav

            # Categorize by asset class
            if asset_class == 'EQUITY':
                snapshot.mutual_funds_equity += total_value
                asset_cat = AssetCategory.MUTUAL_FUND_EQUITY
            elif asset_class == 'DEBT':
                snapshot.mutual_funds_debt += total_value
                asset_cat = AssetCategory.MUTUAL_FUND_DEBT
            elif asset_class == 'HYBRID':
                snapshot.mutual_funds_hybrid += total_value
                asset_cat = AssetCategory.MUTUAL_FUND_HYBRID
            else:
                snapshot.mutual_funds_liquid += total_value
                asset_cat = AssetCategory.MUTUAL_FUND_LIQUID

            # Add to holdings list for drill-down
            snapshot.asset_holdings.append(AssetHolding(
                asset_type=asset_cat,
                asset_identifier=isin or str(scheme_id),
                asset_name=scheme_name,
                quantity=units,
                unit_price=current_nav,
                total_value=total_value,
                cost_basis=units * avg_nav,  # Approximate cost
                unrealized_gain=total_value - (units * avg_nav),
                source_table="mf_schemes",
                source_id=scheme_id,
                as_of_date=as_of,
            ))

    def _populate_stocks(
        self,
        snapshot: BalanceSheetSnapshot,
        user_id: int,
        as_of: date
    ) -> None:
        """Populate stock holdings from stock_trades."""
        # Calculate net holdings per symbol
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

            # Calculate average cost
            avg_cost = total_buy_cost / total_buy_qty if total_buy_qty > 0 else Decimal("0")

            # Get current price (placeholder - would need external API)
            current_price = avg_cost  # Use avg cost as placeholder
            total_value = net_qty * current_price
            cost_basis = net_qty * avg_cost

            snapshot.stocks_indian += total_value

            snapshot.asset_holdings.append(AssetHolding(
                asset_type=AssetCategory.STOCK_INDIAN,
                asset_identifier=isin or symbol,
                asset_name=symbol,
                quantity=Decimal(str(net_qty)),
                unit_price=current_price,
                total_value=total_value,
                cost_basis=cost_basis,
                unrealized_gain=total_value - cost_basis,
                source_table="stock_trades",
                as_of_date=as_of,
            ))

        # Foreign stocks (from foreign holdings)
        cursor = self.conn.execute("""
            SELECT symbol, SUM(shares_held), SUM(total_value_usd)
            FROM foreign_holdings
            WHERE user_id = ? AND valuation_date <= ?
            GROUP BY symbol
            HAVING SUM(shares_held) > 0
        """, (user_id, as_of.isoformat()))

        for row in cursor.fetchall():
            symbol = row[0]
            shares = Decimal(str(row[1] or 0))
            value_usd = Decimal(str(row[2] or 0))

            # Convert to INR (approximate)
            exchange_rate = self._get_exchange_rate(as_of, "USD")
            total_value = value_usd * exchange_rate

            snapshot.stocks_foreign += total_value

            snapshot.asset_holdings.append(AssetHolding(
                asset_type=AssetCategory.STOCK_FOREIGN,
                asset_identifier=symbol,
                asset_name=f"{symbol} (US)",
                quantity=shares,
                unit_price=total_value / shares if shares > 0 else Decimal("0"),
                total_value=total_value,
                cost_basis=total_value,  # Would need proper cost tracking
                unrealized_gain=Decimal("0"),
                currency="USD",
                source_table="foreign_holdings",
                as_of_date=as_of,
            ))

    def _populate_retirement_funds(
        self,
        snapshot: BalanceSheetSnapshot,
        user_id: int,
        as_of: date
    ) -> None:
        """Populate EPF, PPF, NPS balances."""
        # EPF Balance
        cursor = self.conn.execute("""
            SELECT et.employee_balance + et.employer_balance
            FROM epf_transactions et
            JOIN epf_accounts ea ON et.epf_account_id = ea.id
            WHERE ea.user_id = ?
              AND et.transaction_date <= ?
            ORDER BY et.transaction_date DESC, et.id DESC
            LIMIT 1
        """, (user_id, as_of.isoformat()))

        row = cursor.fetchone()
        if row and row[0]:
            snapshot.epf_balance = Decimal(str(row[0]))

        # PPF Balance
        cursor = self.conn.execute("""
            SELECT pt.balance
            FROM ppf_transactions pt
            JOIN ppf_accounts pa ON pt.ppf_account_id = pa.id
            WHERE pa.user_id = ?
              AND pt.transaction_date <= ?
            ORDER BY pt.transaction_date DESC, pt.id DESC
            LIMIT 1
        """, (user_id, as_of.isoformat()))

        row = cursor.fetchone()
        if row and row[0]:
            snapshot.ppf_balance = Decimal(str(row[0]))

        # NPS Balance (sum all contributions - no NAV tracking yet)
        cursor = self.conn.execute("""
            SELECT tier, SUM(amount)
            FROM nps_transactions nt
            JOIN nps_accounts na ON nt.nps_account_id = na.id
            WHERE na.user_id = ?
              AND nt.transaction_date <= ?
              AND nt.transaction_type = 'CONTRIBUTION'
            GROUP BY tier
        """, (user_id, as_of.isoformat()))

        for row in cursor.fetchall():
            tier = row[0]
            amount = Decimal(str(row[1] or 0))
            if tier == 'I':
                snapshot.nps_tier1 = amount
            elif tier == 'II':
                snapshot.nps_tier2 = amount

    def _populate_other_investments(
        self,
        snapshot: BalanceSheetSnapshot,
        user_id: int,
        as_of: date
    ) -> None:
        """Populate SGB, REIT, and other investment holdings."""
        # SGB Holdings (from assets module if available)
        # This is a placeholder - would integrate with SGBTracker
        pass

    def _populate_liabilities(
        self,
        snapshot: BalanceSheetSnapshot,
        user_id: int,
        as_of: date
    ) -> None:
        """Populate liabilities from liabilities table."""
        cursor = self.conn.execute("""
            SELECT id, liability_type, lender_name, principal_amount,
                   outstanding_amount, interest_rate, emi_amount,
                   start_date, end_date, is_active
            FROM liabilities
            WHERE user_id = ? AND is_active = TRUE
              AND start_date <= ?
        """, (user_id, as_of.isoformat()))

        for row in cursor.fetchall():
            liability_id = row[0]
            liability_type = row[1]
            lender = row[2]
            principal = Decimal(str(row[3] or 0))
            outstanding = Decimal(str(row[4] or principal))
            interest_rate = Decimal(str(row[5] or 0))
            emi = Decimal(str(row[6] or 0)) if row[6] else None
            start_date = date.fromisoformat(row[7]) if isinstance(row[7], str) else row[7]
            end_date = date.fromisoformat(row[8]) if row[8] and isinstance(row[8], str) else None

            # Get latest outstanding if liability_transactions exist
            latest_outstanding = self._get_latest_outstanding(liability_id, as_of)
            if latest_outstanding is not None:
                outstanding = latest_outstanding

            # Categorize
            if liability_type == 'HOME_LOAN':
                snapshot.home_loans += outstanding
            elif liability_type == 'CAR_LOAN':
                snapshot.car_loans += outstanding
            elif liability_type == 'PERSONAL_LOAN':
                snapshot.personal_loans += outstanding
            elif liability_type == 'EDUCATION_LOAN':
                snapshot.education_loans += outstanding
            elif liability_type == 'CREDIT_CARD':
                snapshot.credit_cards += outstanding
            else:
                snapshot.other_liabilities += outstanding

            # Add to liability details
            snapshot.liability_details.append(Liability(
                id=liability_id,
                liability_type=LiabilityType(liability_type),
                lender_name=lender,
                principal_amount=principal,
                outstanding_amount=outstanding,
                interest_rate=interest_rate,
                emi_amount=emi,
                start_date=start_date,
                end_date=end_date,
                is_active=True,
            ))

    def _get_latest_nav(self, scheme_id: int) -> Optional[Decimal]:
        """Get latest NAV for a scheme (placeholder)."""
        # Would integrate with NAV API or stored NAV history
        cursor = self.conn.execute("""
            SELECT nav FROM mf_transactions
            WHERE folio_id IN (SELECT id FROM mf_folios WHERE scheme_id = ?)
            ORDER BY date DESC
            LIMIT 1
        """, (scheme_id,))

        row = cursor.fetchone()
        return Decimal(str(row[0])) if row and row[0] else None

    def _get_exchange_rate(self, as_of: date, currency: str) -> Decimal:
        """Get exchange rate for a currency."""
        cursor = self.conn.execute("""
            SELECT rate FROM exchange_rates
            WHERE from_currency = ? AND to_currency = 'INR'
              AND date <= ?
            ORDER BY date DESC
            LIMIT 1
        """, (currency, as_of.isoformat()))

        row = cursor.fetchone()
        return Decimal(str(row[0])) if row and row[0] else Decimal("83.5")  # Default

    def _get_latest_outstanding(self, liability_id: int, as_of: date) -> Optional[Decimal]:
        """Get latest outstanding amount from transactions."""
        cursor = self.conn.execute("""
            SELECT outstanding_after FROM liability_transactions
            WHERE liability_id = ? AND transaction_date <= ?
            ORDER BY transaction_date DESC, id DESC
            LIMIT 1
        """, (liability_id, as_of.isoformat()))

        row = cursor.fetchone()
        return Decimal(str(row[0])) if row and row[0] else None

    def save_balance_sheet(
        self,
        user_id: int,
        snapshot: BalanceSheetSnapshot
    ) -> int:
        """
        Save balance sheet snapshot to database.

        Args:
            user_id: User ID
            snapshot: BalanceSheetSnapshot to save

        Returns:
            ID of saved record
        """
        breakdown = snapshot.to_breakdown_dict()

        # Check if exists and update, or insert new
        cursor = self.conn.execute("""
            SELECT id FROM balance_sheet_snapshots
            WHERE user_id = ? AND snapshot_date = ?
        """, (user_id, snapshot.snapshot_date.isoformat()))

        row = cursor.fetchone()

        if row:
            # Update existing
            self.conn.execute("""
                UPDATE balance_sheet_snapshots
                SET total_assets = ?, total_liabilities = ?, net_worth = ?,
                    assets_breakdown = ?, liabilities_breakdown = ?
                WHERE id = ?
            """, (
                float(snapshot.total_assets),
                float(snapshot.total_liabilities),
                float(snapshot.net_worth),
                json.dumps(breakdown["assets"]),
                json.dumps(breakdown["liabilities"]),
                row[0],
            ))
            self.conn.commit()
            return row[0]
        else:
            # Insert new
            cursor = self.conn.execute("""
                INSERT INTO balance_sheet_snapshots (
                    user_id, snapshot_date, total_assets, total_liabilities, net_worth,
                    assets_breakdown, liabilities_breakdown
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id,
                snapshot.snapshot_date.isoformat(),
                float(snapshot.total_assets),
                float(snapshot.total_liabilities),
                float(snapshot.net_worth),
                json.dumps(breakdown["assets"]),
                json.dumps(breakdown["liabilities"]),
            ))
            self.conn.commit()
            return cursor.lastrowid

    def save_asset_holdings(
        self,
        user_id: int,
        holdings: List[AssetHolding],
        snapshot_date: date
    ) -> int:
        """
        Save individual asset holdings snapshot.

        Args:
            user_id: User ID
            holdings: List of AssetHolding objects
            snapshot_date: Date of snapshot

        Returns:
            Number of holdings saved
        """
        count = 0
        for holding in holdings:
            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO asset_holdings_snapshot (
                        user_id, snapshot_date, asset_type, asset_identifier,
                        asset_name, quantity, unit_price, total_value,
                        cost_basis, unrealized_gain, currency, source_table, source_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    user_id,
                    snapshot_date.isoformat(),
                    holding.asset_type.value,
                    holding.asset_identifier,
                    holding.asset_name,
                    float(holding.quantity),
                    float(holding.unit_price),
                    float(holding.total_value),
                    float(holding.cost_basis),
                    float(holding.unrealized_gain),
                    holding.currency,
                    holding.source_table,
                    holding.source_id,
                ))
                count += 1
            except sqlite3.IntegrityError:
                continue

        self.conn.commit()
        return count

    def get_net_worth_history(
        self,
        user_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """
        Get net worth history from saved snapshots.

        Args:
            user_id: User ID
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            List of {date, net_worth, total_assets, total_liabilities}
        """
        query = """
            SELECT snapshot_date, total_assets, total_liabilities, net_worth
            FROM balance_sheet_snapshots
            WHERE user_id = ?
        """
        params = [user_id]

        if start_date:
            query += " AND snapshot_date >= ?"
            params.append(start_date.isoformat())
        if end_date:
            query += " AND snapshot_date <= ?"
            params.append(end_date.isoformat())

        query += " ORDER BY snapshot_date"

        cursor = self.conn.execute(query, params)

        return [
            {
                "date": row[0],
                "total_assets": Decimal(str(row[1])),
                "total_liabilities": Decimal(str(row[2])),
                "net_worth": Decimal(str(row[3])),
            }
            for row in cursor.fetchall()
        ]
