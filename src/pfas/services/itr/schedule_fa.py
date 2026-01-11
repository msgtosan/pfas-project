"""Schedule FA (Foreign Assets) generator.

Generates Schedule FA data for ITR-2/ITR-3 filing.

Schedule FA requires disclosure of:
- A1: Foreign depository accounts
- A2: Foreign custodial accounts
- A3: Foreign equity/debt interest
- A4: Foreign cash value insurance
- B: Details of financial interest in any entity
- C: Details of immovable property
- D: Other capital assets
- E: Accounts with signing authority
- F: Trusts/beneficial ownership
"""

import sqlite3
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional, List, Dict

from pfas.services.currency import SBITTRateProvider


@dataclass
class ForeignAccount:
    """Foreign bank/depository account (Schedule FA - A1)."""

    country_code: str
    country_name: str
    account_name: str  # Name of institution
    account_address: str
    zip_code: str
    account_number: str
    account_status: str  # 'O' for Owner, 'B' for Beneficial owner
    account_opening_date: Optional[date] = None

    # Peak balance during year
    peak_balance_foreign: Decimal = Decimal("0")
    peak_balance_inr: Decimal = Decimal("0")

    # Closing balance (March 31)
    closing_balance_foreign: Decimal = Decimal("0")
    closing_balance_inr: Decimal = Decimal("0")

    # Income from account
    gross_interest_credited: Decimal = Decimal("0")
    gross_interest_inr: Decimal = Decimal("0")

    # Nature: 'Savings', 'Current', 'Fixed Deposit'
    nature_of_account: str = "Savings"


@dataclass
class ForeignCustodialAccount:
    """Foreign custodial account (Schedule FA - A2)."""

    country_code: str
    country_name: str
    institution_name: str
    institution_address: str
    zip_code: str
    account_number: str
    account_status: str  # 'O' or 'B'
    account_opening_date: Optional[date] = None

    # Peak value during year
    peak_value_foreign: Decimal = Decimal("0")
    peak_value_inr: Decimal = Decimal("0")

    # Closing value (March 31)
    closing_value_foreign: Decimal = Decimal("0")
    closing_value_inr: Decimal = Decimal("0")

    # Income from account
    gross_income_foreign: Decimal = Decimal("0")
    gross_income_inr: Decimal = Decimal("0")


@dataclass
class ForeignEquityHolding:
    """Foreign equity/debt interest (Schedule FA - A3)."""

    country_code: str
    country_name: str
    entity_name: str  # Company name
    entity_address: str
    zip_code: str
    nature_of_entity: str  # 'Company', 'LLP', 'Trust'
    date_of_acquisition: Optional[date] = None

    # Initial investment
    initial_investment_foreign: Decimal = Decimal("0")
    initial_investment_inr: Decimal = Decimal("0")

    # Peak value during year
    peak_value_foreign: Decimal = Decimal("0")
    peak_value_inr: Decimal = Decimal("0")

    # Closing value (March 31)
    closing_value_foreign: Decimal = Decimal("0")
    closing_value_inr: Decimal = Decimal("0")

    # Total investment at cost
    total_investment_foreign: Decimal = Decimal("0")
    total_investment_inr: Decimal = Decimal("0")

    # Income earned
    income_accrued_foreign: Decimal = Decimal("0")
    income_accrued_inr: Decimal = Decimal("0")

    # Nature of income: 'Dividend', 'Capital Gain'
    nature_of_income: str = "Dividend"

    # Stock details
    symbol: str = ""
    shares_held: Decimal = Decimal("0")
    share_price_closing: Decimal = Decimal("0")


@dataclass
class ScheduleFAData:
    """Complete Schedule FA data for ITR filing."""

    financial_year: str
    assessment_year: str

    # A1: Foreign Depository Accounts
    depository_accounts: List[ForeignAccount] = field(default_factory=list)

    # A2: Foreign Custodial Accounts
    custodial_accounts: List[ForeignCustodialAccount] = field(default_factory=list)

    # A3: Foreign Equity/Debt Interest
    equity_holdings: List[ForeignEquityHolding] = field(default_factory=list)

    # Totals
    total_peak_value_inr: Decimal = Decimal("0")
    total_closing_value_inr: Decimal = Decimal("0")
    total_income_inr: Decimal = Decimal("0")

    def calculate_totals(self) -> None:
        """Calculate totals from all sections."""
        self.total_peak_value_inr = Decimal("0")
        self.total_closing_value_inr = Decimal("0")
        self.total_income_inr = Decimal("0")

        # A1 totals
        for account in self.depository_accounts:
            self.total_peak_value_inr += account.peak_balance_inr
            self.total_closing_value_inr += account.closing_balance_inr
            self.total_income_inr += account.gross_interest_inr

        # A2 totals
        for account in self.custodial_accounts:
            self.total_peak_value_inr += account.peak_value_inr
            self.total_closing_value_inr += account.closing_value_inr
            self.total_income_inr += account.gross_income_inr

        # A3 totals
        for holding in self.equity_holdings:
            self.total_peak_value_inr += holding.peak_value_inr
            self.total_closing_value_inr += holding.closing_value_inr
            self.total_income_inr += holding.income_accrued_inr


class ScheduleFAGenerator:
    """
    Generates Schedule FA data for ITR filing.

    Schedule FA (Foreign Assets) is mandatory for:
    - Resident individuals with foreign assets
    - Resident individuals who are beneficial owners of foreign assets
    - Signing authority on foreign accounts

    Valuation Rules:
    - Peak value: Highest value during the financial year
    - Closing value: Value as on March 31 (FY end)
    - Exchange rate: SBI TT Buying Rate
    """

    # Country codes for common jurisdictions
    COUNTRY_CODES = {
        'United States': '1',
        'US': '1',
        'USA': '1',
        'United Kingdom': '2',
        'UK': '2',
        'Singapore': '3',
        'UAE': '4',
        'Canada': '5',
        'Australia': '6',
    }

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize Schedule FA generator.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection
        self.rate_provider = SBITTRateProvider(db_connection)

    def generate(self, user_id: int, financial_year: str) -> ScheduleFAData:
        """
        Generate Schedule FA data for a financial year.

        Args:
            user_id: User ID
            financial_year: FY in format '2024-25'

        Returns:
            ScheduleFAData for ITR filing
        """
        start_year = int(financial_year.split('-')[0])
        assessment_year = f"{start_year + 1}-{str(start_year + 2)[2:]}"
        fy_end = date(start_year + 1, 3, 31)

        # Get FY end rate for closing valuations
        fy_end_rate = self.rate_provider.get_rate(fy_end)

        schedule_fa = ScheduleFAData(
            financial_year=financial_year,
            assessment_year=assessment_year,
        )

        # Generate A2: Custodial accounts (from broker data)
        schedule_fa.custodial_accounts = self._generate_custodial_accounts(
            user_id, financial_year, fy_end_rate
        )

        # Generate A3: Equity holdings (from RSU/ESPP data)
        schedule_fa.equity_holdings = self._generate_equity_holdings(
            user_id, financial_year, fy_end_rate
        )

        # Calculate totals
        schedule_fa.calculate_totals()

        return schedule_fa

    def _generate_custodial_accounts(
        self,
        user_id: int,
        financial_year: str,
        fy_end_rate: Decimal
    ) -> List[ForeignCustodialAccount]:
        """Generate A2 custodial account entries."""
        start_year = int(financial_year.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)

        accounts = []

        # Get broker accounts from foreign assets
        cursor = self.conn.execute(
            """SELECT DISTINCT account_number, broker_name
            FROM foreign_accounts
            WHERE user_id = ?""",
            (user_id,)
        )

        for row in cursor.fetchall():
            account_number = row['account_number']
            broker_name = row['broker_name']

            # Get peak value during year
            peak_cursor = self.conn.execute(
                """SELECT MAX(total_value_usd) as peak_value
                FROM foreign_holdings
                WHERE user_id = ?
                    AND account_number = ?
                    AND valuation_date >= ?
                    AND valuation_date <= ?""",
                (user_id, account_number, fy_start.isoformat(), fy_end.isoformat())
            )
            peak_row = peak_cursor.fetchone()
            peak_value_usd = Decimal(str(peak_row['peak_value'])) if peak_row and peak_row['peak_value'] else Decimal("0")

            # Get closing value
            closing_cursor = self.conn.execute(
                """SELECT total_value_usd
                FROM foreign_holdings
                WHERE user_id = ?
                    AND account_number = ?
                    AND valuation_date <= ?
                ORDER BY valuation_date DESC
                LIMIT 1""",
                (user_id, account_number, fy_end.isoformat())
            )
            closing_row = closing_cursor.fetchone()
            closing_value_usd = Decimal(str(closing_row['total_value_usd'])) if closing_row else Decimal("0")

            # Get total income (dividends)
            income_cursor = self.conn.execute(
                """SELECT SUM(gross_dividend_usd) as total_income
                FROM foreign_dividends
                WHERE user_id = ?
                    AND dividend_date >= ?
                    AND dividend_date <= ?""",
                (user_id, fy_start.isoformat(), fy_end.isoformat())
            )
            income_row = income_cursor.fetchone()
            total_income_usd = Decimal(str(income_row['total_income'])) if income_row and income_row['total_income'] else Decimal("0")

            account = ForeignCustodialAccount(
                country_code='1',  # USA
                country_name='United States',
                institution_name=broker_name or 'Morgan Stanley',
                institution_address='1585 Broadway, New York, NY',
                zip_code='10036',
                account_number=account_number or '',
                account_status='O',  # Owner
                peak_value_foreign=peak_value_usd,
                peak_value_inr=peak_value_usd * fy_end_rate,
                closing_value_foreign=closing_value_usd,
                closing_value_inr=closing_value_usd * fy_end_rate,
                gross_income_foreign=total_income_usd,
                gross_income_inr=total_income_usd * fy_end_rate,
            )
            accounts.append(account)

        return accounts

    def _generate_equity_holdings(
        self,
        user_id: int,
        financial_year: str,
        fy_end_rate: Decimal
    ) -> List[ForeignEquityHolding]:
        """Generate A3 equity holding entries."""
        start_year = int(financial_year.split('-')[0])
        fy_start = date(start_year, 4, 1)
        fy_end = date(start_year + 1, 3, 31)

        holdings = []

        # Get distinct symbols from RSU vests
        cursor = self.conn.execute(
            """SELECT DISTINCT symbol, company_name
            FROM rsu_vests rv
            LEFT JOIN stock_plans sp ON rv.grant_number = sp.grant_number
            WHERE rv.user_id = ?""",
            (user_id,)
        )

        for row in cursor.fetchall():
            symbol = row['symbol'] or 'UNKNOWN'
            company_name = row['company_name'] or symbol

            # Calculate total shares held (vests - sales)
            vest_cursor = self.conn.execute(
                """SELECT SUM(net_shares) as total_vested
                FROM rsu_vests
                WHERE user_id = ?
                    AND vest_date <= ?""",
                (user_id, fy_end.isoformat())
            )
            vest_row = vest_cursor.fetchone()
            total_vested = Decimal(str(vest_row['total_vested'])) if vest_row and vest_row['total_vested'] else Decimal("0")

            sale_cursor = self.conn.execute(
                """SELECT SUM(shares_sold) as total_sold
                FROM rsu_sales
                WHERE user_id = ?
                    AND sale_date <= ?""",
                (user_id, fy_end.isoformat())
            )
            sale_row = sale_cursor.fetchone()
            total_sold = Decimal(str(sale_row['total_sold'])) if sale_row and sale_row['total_sold'] else Decimal("0")

            shares_held = total_vested - total_sold

            if shares_held <= 0:
                continue

            # Get closing price (use last vest FMV as approximation)
            price_cursor = self.conn.execute(
                """SELECT fmv_usd
                FROM rsu_vests
                WHERE user_id = ?
                ORDER BY vest_date DESC
                LIMIT 1""",
                (user_id,)
            )
            price_row = price_cursor.fetchone()
            share_price = Decimal(str(price_row['fmv_usd'])) if price_row else Decimal("0")

            closing_value_usd = shares_held * share_price

            # Get total cost basis
            cost_cursor = self.conn.execute(
                """SELECT SUM(net_shares * fmv_usd) as total_cost
                FROM rsu_vests
                WHERE user_id = ?
                    AND vest_date <= ?""",
                (user_id, fy_end.isoformat())
            )
            cost_row = cost_cursor.fetchone()
            total_cost_usd = Decimal(str(cost_row['total_cost'])) if cost_row and cost_row['total_cost'] else Decimal("0")

            # Get dividend income for this symbol
            div_cursor = self.conn.execute(
                """SELECT SUM(gross_dividend_usd) as total_div
                FROM foreign_dividends
                WHERE user_id = ?
                    AND symbol = ?
                    AND dividend_date >= ?
                    AND dividend_date <= ?""",
                (user_id, symbol, fy_start.isoformat(), fy_end.isoformat())
            )
            div_row = div_cursor.fetchone()
            dividend_income_usd = Decimal(str(div_row['total_div'])) if div_row and div_row['total_div'] else Decimal("0")

            # Get first acquisition date
            first_vest_cursor = self.conn.execute(
                """SELECT MIN(vest_date) as first_vest
                FROM rsu_vests
                WHERE user_id = ?""",
                (user_id,)
            )
            first_vest_row = first_vest_cursor.fetchone()
            first_vest_date = None
            if first_vest_row and first_vest_row['first_vest']:
                first_vest_date = date.fromisoformat(first_vest_row['first_vest']) if isinstance(first_vest_row['first_vest'], str) else first_vest_row['first_vest']

            holding = ForeignEquityHolding(
                country_code='1',  # USA
                country_name='United States',
                entity_name=company_name,
                entity_address='USA',
                zip_code='',
                nature_of_entity='Company',
                date_of_acquisition=first_vest_date,
                initial_investment_foreign=total_cost_usd,
                initial_investment_inr=total_cost_usd * fy_end_rate,
                peak_value_foreign=closing_value_usd,  # Simplified
                peak_value_inr=closing_value_usd * fy_end_rate,
                closing_value_foreign=closing_value_usd,
                closing_value_inr=closing_value_usd * fy_end_rate,
                total_investment_foreign=total_cost_usd,
                total_investment_inr=total_cost_usd * fy_end_rate,
                income_accrued_foreign=dividend_income_usd,
                income_accrued_inr=dividend_income_usd * fy_end_rate,
                nature_of_income='Dividend' if dividend_income_usd > 0 else '',
                symbol=symbol,
                shares_held=shares_held,
                share_price_closing=share_price,
            )
            holdings.append(holding)

        # Also add ESPP holdings
        espp_holdings = self._get_espp_holdings(user_id, financial_year, fy_end_rate)
        holdings.extend(espp_holdings)

        return holdings

    def _get_espp_holdings(
        self,
        user_id: int,
        financial_year: str,
        fy_end_rate: Decimal
    ) -> List[ForeignEquityHolding]:
        """Get ESPP holdings for Schedule FA."""
        start_year = int(financial_year.split('-')[0])
        fy_end = date(start_year + 1, 3, 31)

        holdings = []

        # Get ESPP purchases not yet sold
        cursor = self.conn.execute(
            """SELECT SUM(shares_purchased) as total_purchased,
                      SUM(purchase_price_usd * shares_purchased) as total_cost,
                      MIN(purchase_date) as first_purchase
            FROM espp_purchases
            WHERE user_id = ?
                AND purchase_date <= ?""",
            (user_id, fy_end.isoformat())
        )

        row = cursor.fetchone()
        if not row or not row['total_purchased']:
            return holdings

        total_purchased = Decimal(str(row['total_purchased']))

        # Get total sold
        sale_cursor = self.conn.execute(
            """SELECT SUM(shares_sold) as total_sold
            FROM espp_sales
            WHERE user_id = ?
                AND sale_date <= ?""",
            (user_id, fy_end.isoformat())
        )
        sale_row = sale_cursor.fetchone()
        total_sold = Decimal(str(sale_row['total_sold'])) if sale_row and sale_row['total_sold'] else Decimal("0")

        shares_held = total_purchased - total_sold

        if shares_held <= 0:
            return holdings

        total_cost_usd = Decimal(str(row['total_cost']))
        first_purchase = None
        if row['first_purchase']:
            first_purchase = date.fromisoformat(row['first_purchase']) if isinstance(row['first_purchase'], str) else row['first_purchase']

        # Get average cost per share
        avg_cost = total_cost_usd / total_purchased if total_purchased > 0 else Decimal("0")
        closing_value_usd = shares_held * avg_cost

        holding = ForeignEquityHolding(
            country_code='1',
            country_name='United States',
            entity_name='ESPP Holdings',
            entity_address='USA',
            zip_code='',
            nature_of_entity='Company',
            date_of_acquisition=first_purchase,
            initial_investment_foreign=total_cost_usd,
            initial_investment_inr=total_cost_usd * fy_end_rate,
            peak_value_foreign=closing_value_usd,
            peak_value_inr=closing_value_usd * fy_end_rate,
            closing_value_foreign=closing_value_usd,
            closing_value_inr=closing_value_usd * fy_end_rate,
            total_investment_foreign=total_cost_usd,
            total_investment_inr=total_cost_usd * fy_end_rate,
            nature_of_income='',
            symbol='ESPP',
            shares_held=shares_held,
            share_price_closing=avg_cost,
        )
        holdings.append(holding)

        return holdings

    def save_schedule_fa(self, schedule_fa: ScheduleFAData, user_id: int) -> int:
        """Save Schedule FA data to database."""
        cursor = self.conn.execute(
            """INSERT INTO schedule_fa
            (user_id, financial_year, assessment_year, total_peak_value_inr,
             total_closing_value_inr, total_income_inr, data_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                user_id,
                schedule_fa.financial_year,
                schedule_fa.assessment_year,
                str(schedule_fa.total_peak_value_inr),
                str(schedule_fa.total_closing_value_inr),
                str(schedule_fa.total_income_inr),
                self._to_json(schedule_fa),
            )
        )
        self.conn.commit()
        return cursor.lastrowid

    def _to_json(self, schedule_fa: ScheduleFAData) -> str:
        """Convert Schedule FA data to JSON string."""
        import json
        from dataclasses import asdict

        def decimal_default(obj):
            if isinstance(obj, Decimal):
                return str(obj)
            if isinstance(obj, date):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        return json.dumps(asdict(schedule_fa), default=decimal_default)
