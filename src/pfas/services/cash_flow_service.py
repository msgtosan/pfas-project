"""
Cash Flow Statement Service.

Generates cash flow statements by:
1. Extracting transactions from various database tables
2. Classifying them into Operating/Investing/Financing activities
3. Aggregating into a complete Cash Flow Statement

All data is fetched from database - no file parsing.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import List, Optional, Dict, Any
import json
import sqlite3

from pfas.core.models import (
    CashFlow,
    CashFlowStatement,
    ActivityType,
    FlowDirection,
    CashFlowCategory,
    get_financial_year,
    get_fy_dates,
)


@dataclass
class CashFlowClassificationRule:
    """Rule for classifying bank transactions into cash flow categories."""
    keywords: List[str]
    category: CashFlowCategory
    activity_type: ActivityType
    flow_direction: FlowDirection


class CashFlowStatementService:
    """
    Service for generating cash flow statements.

    Extracts cash flow data from:
    - Bank transactions (primary source)
    - Salary records
    - MF transactions
    - Stock trades
    - Loan transactions

    Example:
        service = CashFlowStatementService(conn)
        statement = service.get_cash_flow_statement(user_id=1, financial_year="2024-25")
        print(f"Net Operating: {statement.net_operating}")
    """

    # Classification rules for bank transaction descriptions
    CLASSIFICATION_RULES = [
        # Operating - Inflows
        CashFlowClassificationRule(
            keywords=["SALARY", "SAL CR", "QUALCOMM", "EMPLOYER"],
            category=CashFlowCategory.SALARY,
            activity_type=ActivityType.OPERATING,
            flow_direction=FlowDirection.INFLOW,
        ),
        CashFlowClassificationRule(
            keywords=["DIVIDEND", "DIV CR"],
            category=CashFlowCategory.DIVIDEND_INDIAN,
            activity_type=ActivityType.OPERATING,
            flow_direction=FlowDirection.INFLOW,
        ),
        CashFlowClassificationRule(
            keywords=["INT PD", "INTEREST", "INT.CREDIT", "INT CR"],
            category=CashFlowCategory.INTEREST_BANK,
            activity_type=ActivityType.OPERATING,
            flow_direction=FlowDirection.INFLOW,
        ),
        CashFlowClassificationRule(
            keywords=["RENT", "RENTAL"],
            category=CashFlowCategory.RENT_RECEIVED,
            activity_type=ActivityType.OPERATING,
            flow_direction=FlowDirection.INFLOW,
        ),

        # Operating - Outflows
        CashFlowClassificationRule(
            keywords=["INCOME TAX", "ADVANCE TAX", "TDS", "GST"],
            category=CashFlowCategory.TAX_PAID,
            activity_type=ActivityType.OPERATING,
            flow_direction=FlowDirection.OUTFLOW,
        ),
        CashFlowClassificationRule(
            keywords=["LIC", "INSURANCE", "PREMIUM"],
            category=CashFlowCategory.INSURANCE_PREMIUM,
            activity_type=ActivityType.OPERATING,
            flow_direction=FlowDirection.OUTFLOW,
        ),

        # Investing - Inflows
        CashFlowClassificationRule(
            keywords=["MF REDEMPTION", "FUND REDEMP"],
            category=CashFlowCategory.MF_REDEMPTION,
            activity_type=ActivityType.INVESTING,
            flow_direction=FlowDirection.INFLOW,
        ),
        CashFlowClassificationRule(
            keywords=["STOCK SALE", "ZERODHA CR", "GROWW CR"],
            category=CashFlowCategory.STOCK_SALE,
            activity_type=ActivityType.INVESTING,
            flow_direction=FlowDirection.INFLOW,
        ),
        CashFlowClassificationRule(
            keywords=["FD MATURITY", "TDR MATURITY"],
            category=CashFlowCategory.FD_MATURITY,
            activity_type=ActivityType.INVESTING,
            flow_direction=FlowDirection.INFLOW,
        ),

        # Investing - Outflows
        CashFlowClassificationRule(
            keywords=["SIP", "MF PURCHASE", "BSE", "MUTUAL FUND"],
            category=CashFlowCategory.MF_PURCHASE,
            activity_type=ActivityType.INVESTING,
            flow_direction=FlowDirection.OUTFLOW,
        ),
        CashFlowClassificationRule(
            keywords=["ZERODHA", "GROWW", "STOCK"],
            category=CashFlowCategory.STOCK_PURCHASE,
            activity_type=ActivityType.INVESTING,
            flow_direction=FlowDirection.OUTFLOW,
        ),
        CashFlowClassificationRule(
            keywords=["PPF", "PUBLIC PROVIDENT"],
            category=CashFlowCategory.PPF_DEPOSIT,
            activity_type=ActivityType.INVESTING,
            flow_direction=FlowDirection.OUTFLOW,
        ),
        CashFlowClassificationRule(
            keywords=["NPS", "NATIONAL PENSION"],
            category=CashFlowCategory.NPS_CONTRIBUTION,
            activity_type=ActivityType.INVESTING,
            flow_direction=FlowDirection.OUTFLOW,
        ),
        CashFlowClassificationRule(
            keywords=["SGB", "SOVEREIGN GOLD"],
            category=CashFlowCategory.SGB_PURCHASE,
            activity_type=ActivityType.INVESTING,
            flow_direction=FlowDirection.OUTFLOW,
        ),

        # Financing - Inflows
        CashFlowClassificationRule(
            keywords=["LOAN DISB", "LOAN CR"],
            category=CashFlowCategory.LOAN_DISBURSEMENT,
            activity_type=ActivityType.FINANCING,
            flow_direction=FlowDirection.INFLOW,
        ),

        # Financing - Outflows
        CashFlowClassificationRule(
            keywords=["EMI", "LOAN EMI", "HOME LOAN", "CAR LOAN"],
            category=CashFlowCategory.LOAN_EMI,
            activity_type=ActivityType.FINANCING,
            flow_direction=FlowDirection.OUTFLOW,
        ),
        CashFlowClassificationRule(
            keywords=["CREDIT CARD", "CC PAYMENT", "AMEX", "HDFC CC"],
            category=CashFlowCategory.CREDIT_CARD_PAYMENT,
            activity_type=ActivityType.FINANCING,
            flow_direction=FlowDirection.OUTFLOW,
        ),
    ]

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize with database connection.

        Args:
            db_connection: SQLite connection object
        """
        self.conn = db_connection

    def get_cash_flow_statement(
        self,
        user_id: int,
        financial_year: str
    ) -> CashFlowStatement:
        """
        Generate complete cash flow statement for a financial year.

        Args:
            user_id: User ID
            financial_year: FY string (e.g., "2024-25")

        Returns:
            CashFlowStatement with all activities populated
        """
        fy_start, fy_end = get_fy_dates(financial_year)

        statement = CashFlowStatement(
            period_start=fy_start,
            period_end=fy_end,
            financial_year=financial_year,
        )

        # Extract and aggregate cash flows
        all_flows = self.extract_cash_flows_from_db(user_id, financial_year)

        # Aggregate into statement
        self._aggregate_flows(statement, all_flows)

        # Get opening and closing cash balances
        statement.opening_cash = self._get_bank_balance_as_of(user_id, fy_start)
        statement.closing_cash = self._get_bank_balance_as_of(user_id, fy_end)

        return statement

    def extract_cash_flows_from_db(
        self,
        user_id: int,
        financial_year: str
    ) -> List[CashFlow]:
        """
        Extract cash flows from all relevant database tables.

        Args:
            user_id: User ID
            financial_year: FY string

        Returns:
            List of CashFlow objects
        """
        flows = []
        fy_start, fy_end = get_fy_dates(financial_year)

        # 1. Bank transactions (primary source)
        flows.extend(self._extract_from_bank_transactions(user_id, fy_start, fy_end))

        # 2. Salary records
        flows.extend(self._extract_from_salary_records(user_id, fy_start, fy_end))

        # 3. MF transactions
        flows.extend(self._extract_from_mf_transactions(user_id, fy_start, fy_end))

        # 4. Stock trades
        flows.extend(self._extract_from_stock_trades(user_id, fy_start, fy_end))

        # 5. Liability transactions
        flows.extend(self._extract_from_liability_transactions(user_id, fy_start, fy_end))

        return flows

    def _extract_from_bank_transactions(
        self,
        user_id: int,
        start_date: date,
        end_date: date
    ) -> List[CashFlow]:
        """Extract and classify bank transactions."""
        flows = []

        cursor = self.conn.execute("""
            SELECT bt.id, bt.date, bt.description, bt.debit, bt.credit, bt.category
            FROM bank_transactions bt
            JOIN bank_accounts ba ON bt.bank_account_id = ba.id
            WHERE ba.user_id = ? AND bt.date BETWEEN ? AND ?
            ORDER BY bt.date
        """, (user_id, start_date.isoformat(), end_date.isoformat()))

        for row in cursor.fetchall():
            txn_id = row[0]
            txn_date = date.fromisoformat(row[1]) if isinstance(row[1], str) else row[1]
            description = row[2] or ""
            debit = Decimal(str(row[3] or 0))
            credit = Decimal(str(row[4] or 0))
            category = row[5]

            # Determine flow direction and amount
            if credit > 0:
                amount = credit
                is_credit = True
            else:
                amount = debit
                is_credit = False

            # Classify the transaction
            classification = self._classify_transaction(description, is_credit, category)

            if classification:
                flows.append(CashFlow(
                    flow_date=txn_date,
                    activity_type=classification["activity_type"],
                    flow_direction=classification["flow_direction"],
                    amount=amount,
                    category=classification["category"],
                    description=description[:100],
                    source_table="bank_transactions",
                    source_id=txn_id,
                    financial_year=get_financial_year(txn_date),
                ))

        return flows

    def _extract_from_salary_records(
        self,
        user_id: int,
        start_date: date,
        end_date: date
    ) -> List[CashFlow]:
        """Extract salary inflows from salary_records."""
        flows = []

        cursor = self.conn.execute("""
            SELECT id, pay_date, net_pay, income_tax_deducted
            FROM salary_records
            WHERE user_id = ? AND pay_date BETWEEN ? AND ?
            ORDER BY pay_date
        """, (user_id, start_date.isoformat(), end_date.isoformat()))

        for row in cursor.fetchall():
            pay_date = date.fromisoformat(row[1]) if isinstance(row[1], str) else row[1]
            net_pay = Decimal(str(row[2] or 0))
            tax_deducted = Decimal(str(row[3] or 0))

            if net_pay > 0:
                flows.append(CashFlow(
                    flow_date=pay_date,
                    activity_type=ActivityType.OPERATING,
                    flow_direction=FlowDirection.INFLOW,
                    amount=net_pay,
                    category=CashFlowCategory.SALARY.value,
                    description="Monthly Salary",
                    source_table="salary_records",
                    source_id=row[0],
                    financial_year=get_financial_year(pay_date),
                ))

        return flows

    def _extract_from_mf_transactions(
        self,
        user_id: int,
        start_date: date,
        end_date: date
    ) -> List[CashFlow]:
        """Extract MF purchases and redemptions."""
        flows = []

        cursor = self.conn.execute("""
            SELECT mt.id, mt.date, mt.transaction_type, mt.amount, ms.name
            FROM mf_transactions mt
            JOIN mf_folios mf ON mt.folio_id = mf.id
            JOIN mf_schemes ms ON mf.scheme_id = ms.id
            WHERE mf.user_id = ? AND mt.date BETWEEN ? AND ?
            ORDER BY mt.date
        """, (user_id, start_date.isoformat(), end_date.isoformat()))

        for row in cursor.fetchall():
            txn_date = date.fromisoformat(row[1]) if isinstance(row[1], str) else row[1]
            txn_type = row[2]
            amount = abs(Decimal(str(row[3] or 0)))
            scheme_name = row[4] or ""

            if txn_type in ('PURCHASE', 'SWITCH_IN'):
                flows.append(CashFlow(
                    flow_date=txn_date,
                    activity_type=ActivityType.INVESTING,
                    flow_direction=FlowDirection.OUTFLOW,
                    amount=amount,
                    category=CashFlowCategory.MF_PURCHASE.value,
                    description=f"MF: {scheme_name[:50]}",
                    source_table="mf_transactions",
                    source_id=row[0],
                    financial_year=get_financial_year(txn_date),
                ))
            elif txn_type in ('REDEMPTION', 'SWITCH_OUT'):
                flows.append(CashFlow(
                    flow_date=txn_date,
                    activity_type=ActivityType.INVESTING,
                    flow_direction=FlowDirection.INFLOW,
                    amount=amount,
                    category=CashFlowCategory.MF_REDEMPTION.value,
                    description=f"MF: {scheme_name[:50]}",
                    source_table="mf_transactions",
                    source_id=row[0],
                    financial_year=get_financial_year(txn_date),
                ))

        return flows

    def _extract_from_stock_trades(
        self,
        user_id: int,
        start_date: date,
        end_date: date
    ) -> List[CashFlow]:
        """Extract stock buys and sells."""
        flows = []

        cursor = self.conn.execute("""
            SELECT id, trade_date, trade_type, net_amount, symbol
            FROM stock_trades
            WHERE user_id = ? AND trade_date BETWEEN ? AND ?
              AND trade_category = 'DELIVERY'
            ORDER BY trade_date
        """, (user_id, start_date.isoformat(), end_date.isoformat()))

        for row in cursor.fetchall():
            trade_date = date.fromisoformat(row[1]) if isinstance(row[1], str) else row[1]
            trade_type = row[2]
            amount = abs(Decimal(str(row[3] or 0)))
            symbol = row[4] or ""

            if trade_type == 'BUY':
                flows.append(CashFlow(
                    flow_date=trade_date,
                    activity_type=ActivityType.INVESTING,
                    flow_direction=FlowDirection.OUTFLOW,
                    amount=amount,
                    category=CashFlowCategory.STOCK_PURCHASE.value,
                    description=f"Buy: {symbol}",
                    source_table="stock_trades",
                    source_id=row[0],
                    financial_year=get_financial_year(trade_date),
                ))
            elif trade_type == 'SELL':
                flows.append(CashFlow(
                    flow_date=trade_date,
                    activity_type=ActivityType.INVESTING,
                    flow_direction=FlowDirection.INFLOW,
                    amount=amount,
                    category=CashFlowCategory.STOCK_SALE.value,
                    description=f"Sell: {symbol}",
                    source_table="stock_trades",
                    source_id=row[0],
                    financial_year=get_financial_year(trade_date),
                ))

        return flows

    def _extract_from_liability_transactions(
        self,
        user_id: int,
        start_date: date,
        end_date: date
    ) -> List[CashFlow]:
        """Extract loan EMIs and disbursements."""
        flows = []

        cursor = self.conn.execute("""
            SELECT lt.id, lt.transaction_date, lt.transaction_type, lt.amount, l.lender_name
            FROM liability_transactions lt
            JOIN liabilities l ON lt.liability_id = l.id
            WHERE lt.user_id = ? AND lt.transaction_date BETWEEN ? AND ?
            ORDER BY lt.transaction_date
        """, (user_id, start_date.isoformat(), end_date.isoformat()))

        for row in cursor.fetchall():
            txn_date = date.fromisoformat(row[1]) if isinstance(row[1], str) else row[1]
            txn_type = row[2]
            amount = abs(Decimal(str(row[3] or 0)))
            lender = row[4] or ""

            if txn_type == 'DISBURSEMENT':
                flows.append(CashFlow(
                    flow_date=txn_date,
                    activity_type=ActivityType.FINANCING,
                    flow_direction=FlowDirection.INFLOW,
                    amount=amount,
                    category=CashFlowCategory.LOAN_DISBURSEMENT.value,
                    description=f"Loan from {lender}",
                    source_table="liability_transactions",
                    source_id=row[0],
                    financial_year=get_financial_year(txn_date),
                ))
            elif txn_type in ('EMI', 'PREPAYMENT'):
                category = CashFlowCategory.LOAN_EMI if txn_type == 'EMI' else CashFlowCategory.LOAN_PREPAYMENT
                flows.append(CashFlow(
                    flow_date=txn_date,
                    activity_type=ActivityType.FINANCING,
                    flow_direction=FlowDirection.OUTFLOW,
                    amount=amount,
                    category=category.value,
                    description=f"Payment to {lender}",
                    source_table="liability_transactions",
                    source_id=row[0],
                    financial_year=get_financial_year(txn_date),
                ))

        return flows

    def _classify_transaction(
        self,
        description: str,
        is_credit: bool,
        category: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Classify a bank transaction based on description.

        Args:
            description: Transaction description
            is_credit: True if credit, False if debit
            category: Pre-assigned category from bank intelligence

        Returns:
            Dict with activity_type, flow_direction, category or None
        """
        desc_upper = description.upper()

        # Try rule-based classification
        for rule in self.CLASSIFICATION_RULES:
            if any(kw in desc_upper for kw in rule.keywords):
                # Check if direction matches
                expected_credit = rule.flow_direction == FlowDirection.INFLOW
                if is_credit == expected_credit:
                    return {
                        "activity_type": rule.activity_type,
                        "flow_direction": rule.flow_direction,
                        "category": rule.category.value,
                    }

        # Default classification based on debit/credit
        if is_credit:
            return {
                "activity_type": ActivityType.OPERATING,
                "flow_direction": FlowDirection.INFLOW,
                "category": CashFlowCategory.OTHER_OPERATING_INFLOW.value,
            }
        else:
            return {
                "activity_type": ActivityType.OPERATING,
                "flow_direction": FlowDirection.OUTFLOW,
                "category": CashFlowCategory.OTHER_OPERATING_OUTFLOW.value,
            }

    def _aggregate_flows(
        self,
        statement: CashFlowStatement,
        flows: List[CashFlow]
    ) -> None:
        """Aggregate flows into statement categories."""
        for flow in flows:
            # Store in details lists
            if flow.activity_type == ActivityType.OPERATING:
                statement.operating_details.append(flow)
            elif flow.activity_type == ActivityType.INVESTING:
                statement.investing_details.append(flow)
            elif flow.activity_type == ActivityType.FINANCING:
                statement.financing_details.append(flow)

            # Aggregate amounts
            category = flow.category

            # Operating - Inflows
            if category == CashFlowCategory.SALARY.value:
                statement.salary_received += flow.amount
            elif category in (CashFlowCategory.DIVIDEND_INDIAN.value, CashFlowCategory.DIVIDEND_FOREIGN.value):
                statement.dividends_received += flow.amount
            elif category in (CashFlowCategory.INTEREST_BANK.value, CashFlowCategory.INTEREST_FD.value,
                              CashFlowCategory.INTEREST_SGB.value, CashFlowCategory.INTEREST_OTHER.value):
                statement.interest_received += flow.amount
            elif category == CashFlowCategory.RENT_RECEIVED.value:
                statement.rent_received += flow.amount
            elif category == CashFlowCategory.BUSINESS_INCOME.value:
                statement.business_income += flow.amount
            elif category == CashFlowCategory.OTHER_OPERATING_INFLOW.value:
                if flow.flow_direction == FlowDirection.INFLOW:
                    statement.other_operating_inflow += flow.amount

            # Operating - Outflows
            elif category == CashFlowCategory.TAX_PAID.value:
                statement.taxes_paid += flow.amount
            elif category == CashFlowCategory.INSURANCE_PREMIUM.value:
                statement.insurance_paid += flow.amount
            elif category == CashFlowCategory.RENT_PAID.value:
                statement.rent_paid += flow.amount
            elif category == CashFlowCategory.HOUSEHOLD_EXPENSE.value:
                statement.household_expenses += flow.amount
            elif category == CashFlowCategory.OTHER_OPERATING_OUTFLOW.value:
                if flow.flow_direction == FlowDirection.OUTFLOW:
                    statement.other_operating_outflow += flow.amount

            # Investing - Inflows
            elif category == CashFlowCategory.MF_REDEMPTION.value:
                statement.mf_redemptions += flow.amount
            elif category == CashFlowCategory.STOCK_SALE.value:
                statement.stock_sells += flow.amount
            elif category == CashFlowCategory.FD_MATURITY.value:
                statement.fd_maturities += flow.amount
            elif category == CashFlowCategory.PROPERTY_SALE.value:
                statement.property_sales += flow.amount
            elif category == CashFlowCategory.OTHER_INVESTMENT_INFLOW.value:
                statement.other_investing_inflow += flow.amount

            # Investing - Outflows
            elif category == CashFlowCategory.MF_PURCHASE.value:
                statement.mf_purchases += flow.amount
            elif category == CashFlowCategory.STOCK_PURCHASE.value:
                statement.stock_buys += flow.amount
            elif category == CashFlowCategory.FD_INVESTMENT.value:
                statement.fd_investments += flow.amount
            elif category == CashFlowCategory.PPF_DEPOSIT.value:
                statement.ppf_deposits += flow.amount
            elif category == CashFlowCategory.NPS_CONTRIBUTION.value:
                statement.nps_contributions += flow.amount
            elif category == CashFlowCategory.EPF_CONTRIBUTION.value:
                statement.epf_contributions += flow.amount
            elif category == CashFlowCategory.SGB_PURCHASE.value:
                statement.sgb_purchases += flow.amount
            elif category == CashFlowCategory.PROPERTY_PURCHASE.value:
                statement.property_purchases += flow.amount
            elif category == CashFlowCategory.OTHER_INVESTMENT_OUTFLOW.value:
                statement.other_investing_outflow += flow.amount

            # Financing - Inflows
            elif category == CashFlowCategory.LOAN_DISBURSEMENT.value:
                statement.loan_proceeds += flow.amount
            elif category == CashFlowCategory.OTHER_FINANCING_INFLOW.value:
                statement.other_financing_inflow += flow.amount

            # Financing - Outflows
            elif category == CashFlowCategory.LOAN_EMI.value:
                statement.loan_repayments += flow.amount
            elif category == CashFlowCategory.LOAN_PREPAYMENT.value:
                statement.loan_prepayments += flow.amount
            elif category == CashFlowCategory.CREDIT_CARD_PAYMENT.value:
                statement.credit_card_payments += flow.amount
            elif category == CashFlowCategory.OTHER_FINANCING_OUTFLOW.value:
                statement.other_financing_outflow += flow.amount

    def _get_bank_balance_as_of(self, user_id: int, as_of: date) -> Decimal:
        """Get total bank balance as of a date."""
        cursor = self.conn.execute("""
            SELECT SUM(bt.balance)
            FROM bank_transactions bt
            JOIN bank_accounts ba ON bt.bank_account_id = ba.id
            WHERE ba.user_id = ?
              AND bt.date <= ?
              AND bt.id IN (
                  SELECT MAX(bt2.id)
                  FROM bank_transactions bt2
                  WHERE bt2.bank_account_id = bt.bank_account_id
                    AND bt2.date <= ?
              )
        """, (user_id, as_of.isoformat(), as_of.isoformat()))

        row = cursor.fetchone()
        return Decimal(str(row[0] or 0)) if row and row[0] else Decimal("0")

    def save_cash_flow_statement(
        self,
        user_id: int,
        statement: CashFlowStatement
    ) -> int:
        """
        Save cash flow statement to database.

        Args:
            user_id: User ID
            statement: CashFlowStatement to save

        Returns:
            ID of saved record
        """
        breakdown = statement.to_breakdown_dict()

        # Check if exists and update, or insert new
        cursor = self.conn.execute("""
            SELECT id FROM cash_flow_statements
            WHERE user_id = ? AND financial_year = ?
        """, (user_id, statement.financial_year))

        row = cursor.fetchone()

        if row:
            # Update existing
            self.conn.execute("""
                UPDATE cash_flow_statements
                SET period_start = ?, period_end = ?,
                    net_operating = ?, net_investing = ?, net_financing = ?,
                    net_change_in_cash = ?,
                    operating_breakdown = ?, investing_breakdown = ?, financing_breakdown = ?
                WHERE id = ?
            """, (
                statement.period_start.isoformat(),
                statement.period_end.isoformat(),
                float(statement.net_operating),
                float(statement.net_investing),
                float(statement.net_financing),
                float(statement.net_change_in_cash),
                json.dumps(breakdown["operating"]),
                json.dumps(breakdown["investing"]),
                json.dumps(breakdown["financing"]),
                row[0],
            ))
            self.conn.commit()
            return row[0]
        else:
            # Insert new
            cursor = self.conn.execute("""
                INSERT INTO cash_flow_statements (
                    user_id, financial_year, period_start, period_end,
                    net_operating, net_investing, net_financing, net_change_in_cash,
                    operating_breakdown, investing_breakdown, financing_breakdown
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id,
                statement.financial_year,
                statement.period_start.isoformat(),
                statement.period_end.isoformat(),
                float(statement.net_operating),
                float(statement.net_investing),
                float(statement.net_financing),
                float(statement.net_change_in_cash),
                json.dumps(breakdown["operating"]),
                json.dumps(breakdown["investing"]),
                json.dumps(breakdown["financing"]),
            ))
            self.conn.commit()
            return cursor.lastrowid

    def save_cash_flows(
        self,
        user_id: int,
        flows: List[CashFlow]
    ) -> int:
        """
        Save individual cash flows to database.

        Args:
            user_id: User ID
            flows: List of CashFlow objects

        Returns:
            Number of flows saved
        """
        count = 0
        for flow in flows:
            try:
                self.conn.execute("""
                    INSERT INTO cash_flows (
                        user_id, flow_date, activity_type, flow_direction,
                        amount, category, sub_category, description,
                        source_table, source_id, financial_year
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    user_id,
                    flow.flow_date.isoformat(),
                    flow.activity_type.value,
                    flow.flow_direction.value,
                    float(flow.amount),
                    flow.category,
                    flow.sub_category,
                    flow.description,
                    flow.source_table,
                    flow.source_id,
                    flow.financial_year,
                ))
                count += 1
            except sqlite3.IntegrityError:
                continue  # Skip duplicates

        self.conn.commit()
        return count
