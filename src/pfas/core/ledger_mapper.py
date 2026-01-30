"""
Centralized Ledger Mapper for PFAS.

This module provides a unified mapping function that converts normalized
transaction records into double-entry journal entries for all 18 asset classes.

Asset Classes Supported:
1. Bank (Savings, Current, FD)
2. Mutual Funds (Equity, Debt)
3. Indian Stocks
4. SGB (Sovereign Gold Bonds)
5. RBI Bonds
6. REIT/InvIT
7. EPF (Employee, Employer)
8. PPF
9. NPS (Tier I, Tier II)
10. Foreign Assets (US Stocks - RSU, ESPP, DRIP)
11. Unlisted Shares
12. Real Estate
13. Tax Assets (TDS, TCS, Advance Tax, Foreign Tax Credit)
14. Salary Income
15. Investment Income (Interest, Dividends)
16. Capital Gains (STCG, LTCG)
17. Rental Income
18. Liabilities

Usage:
    from pfas.core.ledger_mapper import map_to_journal

    entries = map_to_journal(normalized_record, db_connection)
    if entries:
        result = transaction_service.record(entries=entries, ...)
"""

import logging
from decimal import Decimal
from typing import Dict, Any, List, Optional, Callable
from enum import Enum
import sqlite3

from pfas.core.journal import JournalEntry
from pfas.core.accounts import get_account_by_code

logger = logging.getLogger(__name__)


# =============================================================================
# Account Code Constants
# =============================================================================

class AccountCode(str, Enum):
    """Account codes from the Chart of Accounts."""

    # Assets - Bank Accounts (1100s)
    BANK_SAVINGS = "1101"
    BANK_CURRENT = "1102"
    BANK_FD = "1103"
    CASH_IN_HAND = "1104"

    # Assets - Investments (1200s)
    MF_EQUITY = "1201"
    MF_DEBT = "1202"
    INDIAN_STOCKS = "1203"
    SGB = "1204"
    RBI_BONDS = "1205"
    REIT_INVIT = "1206"

    # Assets - Retirement (1300s)
    EPF_EMPLOYEE = "1301"
    EPF_EMPLOYER = "1302"
    PPF = "1303"
    NPS_TIER1 = "1304"
    NPS_TIER2 = "1305"

    # Assets - Foreign (1400s)
    US_STOCKS_RSU = "1401"
    US_STOCKS_ESPP = "1402"
    US_STOCKS_DRIP = "1403"
    US_BROKERAGE_CASH = "1404"

    # Assets - Other (1500s)
    UNLISTED_SHARES = "1501"
    REAL_ESTATE = "1502"

    # Assets - Tax (1600s)
    TDS_RECEIVABLE = "1601"
    TCS_RECEIVABLE = "1602"
    ADVANCE_TAX_PAID = "1603"
    FOREIGN_TAX_CREDIT = "1604"

    # Liabilities (2000s)
    INCOME_TAX_PAYABLE = "2101"
    PROFESSIONAL_TAX_PAYABLE = "2102"

    # Equity (3000s)
    OPENING_BALANCE = "3100"
    RETAINED_EARNINGS = "3200"

    # Income - Salary (4100s)
    SALARY_INCOME = "4100"
    BASIC_SALARY = "4101"
    HRA_INCOME = "4102"
    SPECIAL_ALLOWANCE = "4103"
    RSU_PERQUISITE = "4104"
    ESPP_PERQUISITE = "4105"
    OTHER_PERQUISITES = "4106"
    EMPLOYER_PF_CONTRIBUTION = "4107"
    FOREIGN_SALARY_INCOME = "4108"
    GROSS_SALARY_COMPOSITE = "4109"

    # Income - Investment (4200s)
    BANK_INTEREST = "4201"
    FD_INTEREST = "4202"
    DIVIDEND_INDIAN = "4203"
    DIVIDEND_FOREIGN = "4204"
    EPF_INTEREST = "4205"
    PPF_INTEREST = "4206"
    SGB_INTEREST = "4207"

    # Income - Capital Gains (4300s)
    STCG_EQUITY = "4301"
    LTCG_EQUITY = "4302"
    CG_DEBT = "4303"
    STCG_FOREIGN = "4304"
    LTCG_FOREIGN = "4305"
    FOREX_GAIN_LOSS = "4306"
    REALIZED_CAPITAL_GAIN = "4307"
    REALIZED_CAPITAL_LOSS = "4308"

    # Income - Rental (4400s)
    RENTAL_INCOME_GROSS = "4401"

    # Expenses - Investment (5200s)
    STT_PAID = "5201"
    BROKERAGE = "5202"
    EXCHANGE_CHARGES = "5203"
    SEBI_CHARGES = "5204"
    FOREX_LOSS = "5205"

    # Expenses - Salary Deductions (5300s)
    PROFESSIONAL_TAX_PAID = "5301"
    INCOME_TAX_EXPENSE = "5302"
    OTHER_SALARY_DEDUCTIONS = "5303"


# =============================================================================
# Transaction Type Constants
# =============================================================================

class TransactionType(str, Enum):
    """Transaction types for normalized records."""

    # Mutual Fund
    MF_PURCHASE = "MF_PURCHASE"
    MF_REDEMPTION = "MF_REDEMPTION"
    MF_SWITCH_IN = "MF_SWITCH_IN"
    MF_SWITCH_OUT = "MF_SWITCH_OUT"
    MF_DIVIDEND = "MF_DIVIDEND"
    MF_DIVIDEND_REINVEST = "MF_DIVIDEND_REINVEST"

    # Stock
    STOCK_BUY = "STOCK_BUY"
    STOCK_SELL = "STOCK_SELL"
    STOCK_DIVIDEND = "STOCK_DIVIDEND"
    STOCK_BONUS = "STOCK_BONUS"
    STOCK_SPLIT = "STOCK_SPLIT"

    # Bank
    BANK_CREDIT = "BANK_CREDIT"
    BANK_DEBIT = "BANK_DEBIT"
    BANK_INTEREST = "BANK_INTEREST"
    FD_DEPOSIT = "FD_DEPOSIT"
    FD_MATURITY = "FD_MATURITY"
    FD_INTEREST = "FD_INTEREST"

    # Salary
    SALARY = "SALARY"
    SALARY_CREDIT = "SALARY_CREDIT"
    BONUS = "BONUS"

    # Retirement
    EPF_CONTRIBUTION = "EPF_CONTRIBUTION"
    EPF_INTEREST = "EPF_INTEREST"
    EPF_WITHDRAWAL = "EPF_WITHDRAWAL"
    PPF_DEPOSIT = "PPF_DEPOSIT"
    PPF_INTEREST = "PPF_INTEREST"
    PPF_WITHDRAWAL = "PPF_WITHDRAWAL"
    NPS_CONTRIBUTION = "NPS_CONTRIBUTION"
    NPS_WITHDRAWAL = "NPS_WITHDRAWAL"

    # Foreign Assets
    RSU_VEST = "RSU_VEST"
    RSU_SALE = "RSU_SALE"
    ESPP_PURCHASE = "ESPP_PURCHASE"
    ESPP_SALE = "ESPP_SALE"
    FOREIGN_DIVIDEND = "FOREIGN_DIVIDEND"

    # Other Assets
    SGB_PURCHASE = "SGB_PURCHASE"
    SGB_INTEREST = "SGB_INTEREST"
    SGB_MATURITY = "SGB_MATURITY"
    REIT_PURCHASE = "REIT_PURCHASE"
    REIT_SALE = "REIT_SALE"
    REIT_DISTRIBUTION = "REIT_DISTRIBUTION"

    # Rental
    RENTAL_INCOME = "RENTAL_INCOME"

    # Tax
    TDS_CREDIT = "TDS_CREDIT"
    ADVANCE_TAX = "ADVANCE_TAX"

    # Generic
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    TRANSFER = "TRANSFER"


# =============================================================================
# Asset Category Constants
# =============================================================================

class AssetCategory(str, Enum):
    """Asset categories for classification."""

    BANK = "BANK"
    FD = "FD"
    MF_EQUITY = "MF_EQUITY"
    MF_DEBT = "MF_DEBT"
    INDIAN_STOCKS = "INDIAN_STOCKS"
    SGB = "SGB"
    RBI_BONDS = "RBI_BONDS"
    REIT = "REIT"
    EPF = "EPF"
    PPF = "PPF"
    NPS = "NPS"
    US_STOCKS = "US_STOCKS"
    UNLISTED = "UNLISTED"
    REAL_ESTATE = "REAL_ESTATE"
    SALARY = "SALARY"
    RENTAL = "RENTAL"
    TAX = "TAX"


# =============================================================================
# Helper Functions
# =============================================================================

def _get_account_id(conn: sqlite3.Connection, code: str) -> Optional[int]:
    """Get account ID from account code, returning None if not found."""
    try:
        account = get_account_by_code(conn, code)
        return account.id if account else None
    except Exception:
        return None


def _to_decimal(value: Any) -> Decimal:
    """Convert value to Decimal safely."""
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except:
        return Decimal("0")


# =============================================================================
# Mapping Functions for Each Asset Class
# =============================================================================

def _map_mf_purchase(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map MF purchase to journal entries.

    Dr MF Asset (1201/1202)  | Amount
    Cr Bank Account (1101)   | Amount
    """
    entries = []
    amount = _to_decimal(record.get('amount', 0))
    if amount <= 0:
        return entries

    # Determine equity vs debt
    is_equity = record.get('is_equity', True)
    asset_code = AccountCode.MF_EQUITY if is_equity else AccountCode.MF_DEBT

    mf_account_id = _get_account_id(conn, asset_code.value)
    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)

    if mf_account_id and bank_account_id:
        scheme_name = record.get('asset_name', 'MF')[:50]
        entries.append(JournalEntry(
            account_id=mf_account_id,
            debit=amount,
            narration=f"MF Purchase: {scheme_name}"
        ))
        entries.append(JournalEntry(
            account_id=bank_account_id,
            credit=amount,
            narration=f"Payment for MF: {scheme_name}"
        ))

    return entries


def _map_mf_redemption(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map MF redemption to journal entries.

    Dr Bank Account (1101)       | Proceeds
    Cr MF Asset (1201/1202)      | Cost Basis
    Cr/Dr Capital Gains (4301/4302) | Gain/Loss
    """
    entries = []
    proceeds = _to_decimal(record.get('amount', 0))
    cost_basis = _to_decimal(record.get('cost_basis', proceeds))
    is_equity = record.get('is_equity', True)
    is_long_term = record.get('is_long_term', False)

    if proceeds <= 0:
        return entries

    asset_code = AccountCode.MF_EQUITY if is_equity else AccountCode.MF_DEBT
    cg_code = AccountCode.LTCG_EQUITY if is_long_term else AccountCode.STCG_EQUITY

    mf_account_id = _get_account_id(conn, asset_code.value)
    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)
    cg_account_id = _get_account_id(conn, cg_code.value)

    if mf_account_id and bank_account_id:
        scheme_name = record.get('asset_name', 'MF')[:50]
        gain_loss = proceeds - cost_basis

        # Dr Bank
        entries.append(JournalEntry(
            account_id=bank_account_id,
            debit=proceeds,
            narration=f"MF Redemption: {scheme_name}"
        ))

        # Cr MF Asset
        if cost_basis > 0:
            entries.append(JournalEntry(
                account_id=mf_account_id,
                credit=cost_basis,
                narration=f"Cost basis: {scheme_name}"
            ))

        # Capital Gain/Loss
        if gain_loss != Decimal("0") and cg_account_id:
            if gain_loss > 0:
                entries.append(JournalEntry(
                    account_id=cg_account_id,
                    credit=gain_loss,
                    narration=f"{'LTCG' if is_long_term else 'STCG'}: {scheme_name}"
                ))
            else:
                entries.append(JournalEntry(
                    account_id=cg_account_id,
                    debit=abs(gain_loss),
                    narration=f"{'LTCL' if is_long_term else 'STCL'}: {scheme_name}"
                ))

    return entries


def _map_stock_buy(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map stock purchase to journal entries.

    Dr Indian Stocks (1203)  | Amount
    Cr Bank Account (1101)   | Amount
    """
    entries = []
    amount = _to_decimal(record.get('amount', 0))
    if amount <= 0:
        return entries

    stock_account_id = _get_account_id(conn, AccountCode.INDIAN_STOCKS.value)
    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)

    if stock_account_id and bank_account_id:
        symbol = record.get('asset_identifier', record.get('asset_name', 'Stock'))[:20]
        quantity = record.get('quantity', 0)

        entries.append(JournalEntry(
            account_id=stock_account_id,
            debit=amount,
            narration=f"Buy: {symbol} x {quantity}"
        ))
        entries.append(JournalEntry(
            account_id=bank_account_id,
            credit=amount,
            narration=f"Payment for stock: {symbol}"
        ))

    return entries


def _map_stock_sell(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map stock sale to journal entries.

    Dr Bank Account (1101)       | Proceeds
    Cr Indian Stocks (1203)      | Cost Basis
    Cr/Dr Capital Gains (4301/4302) | Gain/Loss
    """
    entries = []
    proceeds = _to_decimal(record.get('amount', 0))
    cost_basis = _to_decimal(record.get('cost_basis', proceeds))
    is_long_term = record.get('is_long_term', False)

    if proceeds <= 0:
        return entries

    cg_code = AccountCode.LTCG_EQUITY if is_long_term else AccountCode.STCG_EQUITY

    stock_account_id = _get_account_id(conn, AccountCode.INDIAN_STOCKS.value)
    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)
    cg_account_id = _get_account_id(conn, cg_code.value)

    if stock_account_id and bank_account_id:
        symbol = record.get('asset_identifier', record.get('asset_name', 'Stock'))[:20]
        gain_loss = proceeds - cost_basis

        # Dr Bank
        entries.append(JournalEntry(
            account_id=bank_account_id,
            debit=proceeds,
            narration=f"Proceeds: {symbol}"
        ))

        # Cr Stock Asset
        if cost_basis > 0:
            entries.append(JournalEntry(
                account_id=stock_account_id,
                credit=cost_basis,
                narration=f"Cost basis: {symbol}"
            ))

        # Capital Gain/Loss
        if gain_loss != Decimal("0") and cg_account_id:
            if gain_loss > 0:
                entries.append(JournalEntry(
                    account_id=cg_account_id,
                    credit=gain_loss,
                    narration=f"{'LTCG' if is_long_term else 'STCG'}: {symbol}"
                ))
            else:
                entries.append(JournalEntry(
                    account_id=cg_account_id,
                    debit=abs(gain_loss),
                    narration=f"{'LTCL' if is_long_term else 'STCL'}: {symbol}"
                ))

    return entries


def _map_stock_dividend(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map stock dividend to journal entries.

    Dr Bank Account (1101)       | Net Amount
    Dr TDS Receivable (1601)     | TDS Amount
    Cr Dividend Income (4203)    | Gross Amount
    """
    entries = []
    net_amount = _to_decimal(record.get('amount', 0))
    tds_amount = _to_decimal(record.get('tds_amount', 0))
    gross_amount = net_amount + tds_amount

    if gross_amount <= 0:
        return entries

    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)
    tds_account_id = _get_account_id(conn, AccountCode.TDS_RECEIVABLE.value)
    dividend_account_id = _get_account_id(conn, AccountCode.DIVIDEND_INDIAN.value)

    if bank_account_id and dividend_account_id:
        symbol = record.get('asset_identifier', record.get('asset_name', 'Stock'))[:20]

        # Dr Bank for net amount
        if net_amount > 0:
            entries.append(JournalEntry(
                account_id=bank_account_id,
                debit=net_amount,
                narration=f"Dividend: {symbol}"
            ))

        # Dr TDS Receivable
        if tds_amount > 0 and tds_account_id:
            entries.append(JournalEntry(
                account_id=tds_account_id,
                debit=tds_amount,
                narration=f"TDS on dividend: {symbol}"
            ))

        # Cr Dividend Income
        entries.append(JournalEntry(
            account_id=dividend_account_id,
            credit=gross_amount,
            narration=f"Dividend from {symbol}"
        ))

    return entries


def _map_bank_credit(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map bank credit (deposit) to journal entries.

    Dr Bank Account (1101)     | Amount
    Cr Appropriate Income      | Amount (based on category)
    """
    entries = []
    amount = _to_decimal(record.get('amount', 0))
    if amount <= 0:
        return entries

    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)

    # Determine credit account based on category
    category = record.get('category', '').upper()
    category_to_account = {
        'SALARY': AccountCode.GROSS_SALARY_COMPOSITE,
        'INTEREST': AccountCode.BANK_INTEREST,
        'DIVIDEND': AccountCode.DIVIDEND_INDIAN,
        'REFUND': AccountCode.TDS_RECEIVABLE,
        'RENT': AccountCode.RENTAL_INCOME_GROSS,
    }
    credit_code = category_to_account.get(category, AccountCode.RETAINED_EARNINGS)
    credit_account_id = _get_account_id(conn, credit_code.value)

    if bank_account_id and credit_account_id:
        description = record.get('description', 'Credit')[:50]

        entries.append(JournalEntry(
            account_id=bank_account_id,
            debit=amount,
            narration=f"Credit: {description}"
        ))
        entries.append(JournalEntry(
            account_id=credit_account_id,
            credit=amount,
            narration=f"{category or 'Income'}: {description}"
        ))

    return entries


def _map_bank_debit(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map bank debit (withdrawal/expense) to journal entries.

    Dr Appropriate Expense     | Amount
    Cr Bank Account (1101)     | Amount
    """
    entries = []
    amount = _to_decimal(record.get('amount', 0))
    if amount <= 0:
        return entries

    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)

    # Determine debit account based on category
    category = record.get('category', '').upper()
    category_to_account = {
        'MF_INVESTMENT': AccountCode.MF_EQUITY,
        'STOCK_INVESTMENT': AccountCode.INDIAN_STOCKS,
        'FD': AccountCode.BANK_FD,
        'PPF': AccountCode.PPF,
        'TAX': AccountCode.ADVANCE_TAX_PAID,
    }
    debit_code = category_to_account.get(category, AccountCode.OTHER_SALARY_DEDUCTIONS)
    debit_account_id = _get_account_id(conn, debit_code.value)

    if bank_account_id and debit_account_id:
        description = record.get('description', 'Debit')[:50]

        entries.append(JournalEntry(
            account_id=debit_account_id,
            debit=amount,
            narration=f"Expense: {description}"
        ))
        entries.append(JournalEntry(
            account_id=bank_account_id,
            credit=amount,
            narration=f"Payment: {description}"
        ))

    return entries


def _map_salary(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map salary credit to journal entries.

    Dr Bank Account (1101)       | Net Salary
    Dr TDS Receivable (1601)     | TDS Deducted
    Dr EPF Asset (1301)          | PF Employee
    Cr Gross Salary (4109)       | Gross Salary
    """
    entries = []
    net_salary = _to_decimal(record.get('amount', 0))
    tds_amount = _to_decimal(record.get('tds_amount', 0))
    epf_employee = _to_decimal(record.get('epf_employee', 0))

    # Calculate gross (if not provided, sum up components)
    gross_salary = _to_decimal(record.get('gross_salary', 0))
    if gross_salary == 0:
        gross_salary = net_salary + tds_amount + epf_employee

    if gross_salary <= 0:
        return entries

    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)
    tds_account_id = _get_account_id(conn, AccountCode.TDS_RECEIVABLE.value)
    epf_account_id = _get_account_id(conn, AccountCode.EPF_EMPLOYEE.value)
    salary_account_id = _get_account_id(conn, AccountCode.GROSS_SALARY_COMPOSITE.value)

    if bank_account_id and salary_account_id:
        pay_period = record.get('pay_period', 'Salary')

        # Dr Bank for net
        if net_salary > 0:
            entries.append(JournalEntry(
                account_id=bank_account_id,
                debit=net_salary,
                narration=f"Salary: {pay_period}"
            ))

        # Dr TDS Receivable
        if tds_amount > 0 and tds_account_id:
            entries.append(JournalEntry(
                account_id=tds_account_id,
                debit=tds_amount,
                narration=f"TDS on salary: {pay_period}"
            ))

        # Dr EPF Employee
        if epf_employee > 0 and epf_account_id:
            entries.append(JournalEntry(
                account_id=epf_account_id,
                debit=epf_employee,
                narration=f"EPF contribution: {pay_period}"
            ))

        # Cr Gross Salary
        total_debits = net_salary + tds_amount + epf_employee
        entries.append(JournalEntry(
            account_id=salary_account_id,
            credit=total_debits,
            narration=f"Gross salary: {pay_period}"
        ))

    return entries


def _map_epf_contribution(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map EPF contribution to journal entries.

    Dr EPF Employee (1301)    | Employee Amount
    Dr EPF Employer (1302)    | Employer Amount
    Cr Bank/Salary (1101/4109)| Total
    """
    entries = []
    employee_amount = _to_decimal(record.get('employee_amount', 0))
    employer_amount = _to_decimal(record.get('employer_amount', 0))
    total = employee_amount + employer_amount

    if total <= 0:
        return entries

    epf_ee_account_id = _get_account_id(conn, AccountCode.EPF_EMPLOYEE.value)
    epf_er_account_id = _get_account_id(conn, AccountCode.EPF_EMPLOYER.value)
    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)

    month = record.get('month', 'EPF')

    if epf_ee_account_id and employee_amount > 0:
        entries.append(JournalEntry(
            account_id=epf_ee_account_id,
            debit=employee_amount,
            narration=f"EPF Employee: {month}"
        ))

    if epf_er_account_id and employer_amount > 0:
        entries.append(JournalEntry(
            account_id=epf_er_account_id,
            debit=employer_amount,
            narration=f"EPF Employer: {month}"
        ))

    if bank_account_id and total > 0:
        entries.append(JournalEntry(
            account_id=bank_account_id,
            credit=total,
            narration=f"EPF deduction: {month}"
        ))

    return entries


def _map_epf_interest(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map EPF interest to journal entries.

    Dr EPF Employee (1301)    | Interest Amount
    Cr EPF Interest (4205)    | Interest Amount
    """
    entries = []
    amount = _to_decimal(record.get('amount', 0))
    if amount <= 0:
        return entries

    epf_account_id = _get_account_id(conn, AccountCode.EPF_EMPLOYEE.value)
    interest_account_id = _get_account_id(conn, AccountCode.EPF_INTEREST.value)

    if epf_account_id and interest_account_id:
        fy = record.get('financial_year', 'EPF Interest')

        entries.append(JournalEntry(
            account_id=epf_account_id,
            debit=amount,
            narration=f"EPF Interest: {fy}"
        ))
        entries.append(JournalEntry(
            account_id=interest_account_id,
            credit=amount,
            narration=f"EPF Interest Income: {fy}"
        ))

    return entries


def _map_ppf_deposit(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map PPF deposit to journal entries.

    Dr PPF Asset (1303)       | Amount
    Cr Bank Account (1101)    | Amount
    """
    entries = []
    amount = _to_decimal(record.get('amount', 0))
    if amount <= 0:
        return entries

    ppf_account_id = _get_account_id(conn, AccountCode.PPF.value)
    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)

    if ppf_account_id and bank_account_id:
        entries.append(JournalEntry(
            account_id=ppf_account_id,
            debit=amount,
            narration="PPF Deposit"
        ))
        entries.append(JournalEntry(
            account_id=bank_account_id,
            credit=amount,
            narration="Payment to PPF"
        ))

    return entries


def _map_ppf_interest(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map PPF interest to journal entries.

    Dr PPF Asset (1303)       | Interest
    Cr PPF Interest (4206)    | Interest
    """
    entries = []
    amount = _to_decimal(record.get('amount', 0))
    if amount <= 0:
        return entries

    ppf_account_id = _get_account_id(conn, AccountCode.PPF.value)
    interest_account_id = _get_account_id(conn, AccountCode.PPF_INTEREST.value)

    if ppf_account_id and interest_account_id:
        fy = record.get('financial_year', 'PPF Interest')

        entries.append(JournalEntry(
            account_id=ppf_account_id,
            debit=amount,
            narration=f"PPF Interest: {fy}"
        ))
        entries.append(JournalEntry(
            account_id=interest_account_id,
            credit=amount,
            narration=f"PPF Interest Income: {fy}"
        ))

    return entries


def _map_nps_contribution(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map NPS contribution to journal entries.

    Dr NPS Asset (1304/1305)  | Amount
    Cr Bank Account (1101)    | Amount
    """
    entries = []
    amount = _to_decimal(record.get('amount', 0))
    if amount <= 0:
        return entries

    tier = record.get('tier', 'I')
    nps_code = AccountCode.NPS_TIER1 if tier == 'I' else AccountCode.NPS_TIER2

    nps_account_id = _get_account_id(conn, nps_code.value)
    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)

    if nps_account_id and bank_account_id:
        contribution_type = record.get('contribution_type', 'Contribution')

        entries.append(JournalEntry(
            account_id=nps_account_id,
            debit=amount,
            narration=f"NPS Tier {tier}: {contribution_type}"
        ))
        entries.append(JournalEntry(
            account_id=bank_account_id,
            credit=amount,
            narration=f"Payment to NPS"
        ))

    return entries


def _map_sgb_purchase(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map SGB purchase to journal entries.

    Dr SGB Asset (1204)       | Amount
    Cr Bank Account (1101)    | Amount
    """
    entries = []
    amount = _to_decimal(record.get('amount', 0))
    if amount <= 0:
        return entries

    sgb_account_id = _get_account_id(conn, AccountCode.SGB.value)
    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)

    if sgb_account_id and bank_account_id:
        series = record.get('asset_name', 'SGB')[:30]

        entries.append(JournalEntry(
            account_id=sgb_account_id,
            debit=amount,
            narration=f"SGB Purchase: {series}"
        ))
        entries.append(JournalEntry(
            account_id=bank_account_id,
            credit=amount,
            narration=f"Payment for SGB: {series}"
        ))

    return entries


def _map_sgb_interest(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map SGB interest to journal entries.

    Dr Bank Account (1101)    | Interest
    Cr SGB Interest (4207)    | Interest
    """
    entries = []
    amount = _to_decimal(record.get('amount', 0))
    if amount <= 0:
        return entries

    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)
    interest_account_id = _get_account_id(conn, AccountCode.SGB_INTEREST.value)

    if bank_account_id and interest_account_id:
        series = record.get('asset_name', 'SGB')[:30]

        entries.append(JournalEntry(
            account_id=bank_account_id,
            debit=amount,
            narration=f"SGB Interest: {series}"
        ))
        entries.append(JournalEntry(
            account_id=interest_account_id,
            credit=amount,
            narration=f"SGB Interest Income: {series}"
        ))

    return entries


def _map_rsu_vest(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map RSU vest to journal entries.

    Dr US Stocks RSU (1401)   | FMV in INR
    Cr RSU Perquisite (4104)  | FMV in INR (taxable)
    """
    entries = []
    amount_inr = _to_decimal(record.get('amount', 0))
    if amount_inr <= 0:
        return entries

    rsu_account_id = _get_account_id(conn, AccountCode.US_STOCKS_RSU.value)
    perquisite_account_id = _get_account_id(conn, AccountCode.RSU_PERQUISITE.value)

    if rsu_account_id and perquisite_account_id:
        symbol = record.get('asset_identifier', 'RSU')[:20]
        quantity = record.get('quantity', 0)

        entries.append(JournalEntry(
            account_id=rsu_account_id,
            debit=amount_inr,
            narration=f"RSU Vest: {symbol} x {quantity}"
        ))
        entries.append(JournalEntry(
            account_id=perquisite_account_id,
            credit=amount_inr,
            narration=f"RSU Perquisite: {symbol}"
        ))

    return entries


def _map_rsu_sale(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map RSU sale to journal entries.

    Dr US Brokerage Cash (1404)  | Proceeds USD
    Cr US Stocks RSU (1401)      | Cost Basis USD
    Cr/Dr Capital Gains (4304/4305) | Gain/Loss
    """
    entries = []
    proceeds = _to_decimal(record.get('amount', 0))
    cost_basis = _to_decimal(record.get('cost_basis', proceeds))
    is_long_term = record.get('is_long_term', False)

    if proceeds <= 0:
        return entries

    cg_code = AccountCode.LTCG_FOREIGN if is_long_term else AccountCode.STCG_FOREIGN

    rsu_account_id = _get_account_id(conn, AccountCode.US_STOCKS_RSU.value)
    cash_account_id = _get_account_id(conn, AccountCode.US_BROKERAGE_CASH.value)
    cg_account_id = _get_account_id(conn, cg_code.value)

    if rsu_account_id and cash_account_id:
        symbol = record.get('asset_identifier', 'RSU')[:20]
        gain_loss = proceeds - cost_basis

        entries.append(JournalEntry(
            account_id=cash_account_id,
            debit=proceeds,
            narration=f"RSU Sale proceeds: {symbol}"
        ))

        if cost_basis > 0:
            entries.append(JournalEntry(
                account_id=rsu_account_id,
                credit=cost_basis,
                narration=f"RSU Cost basis: {symbol}"
            ))

        if gain_loss != Decimal("0") and cg_account_id:
            if gain_loss > 0:
                entries.append(JournalEntry(
                    account_id=cg_account_id,
                    credit=gain_loss,
                    narration=f"Foreign {'LTCG' if is_long_term else 'STCG'}: {symbol}"
                ))
            else:
                entries.append(JournalEntry(
                    account_id=cg_account_id,
                    debit=abs(gain_loss),
                    narration=f"Foreign {'LTCL' if is_long_term else 'STCL'}: {symbol}"
                ))

    return entries


def _map_foreign_dividend(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map foreign dividend to journal entries.

    Dr US Brokerage Cash (1404)      | Net Amount
    Dr Foreign Tax Credit (1604)     | Withholding Tax
    Cr Foreign Dividend Income (4204)| Gross Amount
    """
    entries = []
    net_amount = _to_decimal(record.get('amount', 0))
    withholding_tax = _to_decimal(record.get('withholding_tax', 0))
    gross_amount = net_amount + withholding_tax

    if gross_amount <= 0:
        return entries

    cash_account_id = _get_account_id(conn, AccountCode.US_BROKERAGE_CASH.value)
    ftc_account_id = _get_account_id(conn, AccountCode.FOREIGN_TAX_CREDIT.value)
    dividend_account_id = _get_account_id(conn, AccountCode.DIVIDEND_FOREIGN.value)

    if cash_account_id and dividend_account_id:
        symbol = record.get('asset_identifier', 'Foreign')[:20]

        if net_amount > 0:
            entries.append(JournalEntry(
                account_id=cash_account_id,
                debit=net_amount,
                narration=f"Foreign Dividend: {symbol}"
            ))

        if withholding_tax > 0 and ftc_account_id:
            entries.append(JournalEntry(
                account_id=ftc_account_id,
                debit=withholding_tax,
                narration=f"Foreign Tax Credit: {symbol}"
            ))

        entries.append(JournalEntry(
            account_id=dividend_account_id,
            credit=gross_amount,
            narration=f"Foreign Dividend Income: {symbol}"
        ))

    return entries


def _map_rental_income(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map rental income to journal entries.

    Dr Bank Account (1101)        | Gross Rent
    Cr Rental Income (4401)       | Gross Rent
    """
    entries = []
    amount = _to_decimal(record.get('amount', 0))
    if amount <= 0:
        return entries

    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)
    rental_account_id = _get_account_id(conn, AccountCode.RENTAL_INCOME_GROSS.value)

    if bank_account_id and rental_account_id:
        property_name = record.get('asset_name', 'Property')[:30]
        month = record.get('month', '')

        entries.append(JournalEntry(
            account_id=bank_account_id,
            debit=amount,
            narration=f"Rental income: {property_name} {month}"
        ))
        entries.append(JournalEntry(
            account_id=rental_account_id,
            credit=amount,
            narration=f"Rental income: {property_name} {month}"
        ))

    return entries


def _map_fd_deposit(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map FD deposit to journal entries.

    Dr FD Asset (1103)        | Amount
    Cr Bank Account (1101)    | Amount
    """
    entries = []
    amount = _to_decimal(record.get('amount', 0))
    if amount <= 0:
        return entries

    fd_account_id = _get_account_id(conn, AccountCode.BANK_FD.value)
    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)

    if fd_account_id and bank_account_id:
        entries.append(JournalEntry(
            account_id=fd_account_id,
            debit=amount,
            narration="FD Deposit"
        ))
        entries.append(JournalEntry(
            account_id=bank_account_id,
            credit=amount,
            narration="Payment for FD"
        ))

    return entries


def _map_fd_interest(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map FD interest to journal entries.

    Dr Bank Account (1101)    | Net Interest
    Dr TDS Receivable (1601)  | TDS Amount
    Cr FD Interest (4202)     | Gross Interest
    """
    entries = []
    net_amount = _to_decimal(record.get('amount', 0))
    tds_amount = _to_decimal(record.get('tds_amount', 0))
    gross_amount = net_amount + tds_amount

    if gross_amount <= 0:
        return entries

    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)
    tds_account_id = _get_account_id(conn, AccountCode.TDS_RECEIVABLE.value)
    interest_account_id = _get_account_id(conn, AccountCode.FD_INTEREST.value)

    if bank_account_id and interest_account_id:
        if net_amount > 0:
            entries.append(JournalEntry(
                account_id=bank_account_id,
                debit=net_amount,
                narration="FD Interest received"
            ))

        if tds_amount > 0 and tds_account_id:
            entries.append(JournalEntry(
                account_id=tds_account_id,
                debit=tds_amount,
                narration="TDS on FD Interest"
            ))

        entries.append(JournalEntry(
            account_id=interest_account_id,
            credit=gross_amount,
            narration="FD Interest Income"
        ))

    return entries


def _map_bank_interest(record: Dict, conn: sqlite3.Connection) -> List[JournalEntry]:
    """
    Map bank interest to journal entries.

    Dr Bank Account (1101)    | Interest
    Cr Bank Interest (4201)   | Interest
    """
    entries = []
    amount = _to_decimal(record.get('amount', 0))
    if amount <= 0:
        return entries

    bank_account_id = _get_account_id(conn, AccountCode.BANK_SAVINGS.value)
    interest_account_id = _get_account_id(conn, AccountCode.BANK_INTEREST.value)

    if bank_account_id and interest_account_id:
        entries.append(JournalEntry(
            account_id=bank_account_id,
            debit=amount,
            narration="Bank Interest"
        ))
        entries.append(JournalEntry(
            account_id=interest_account_id,
            credit=amount,
            narration="Bank Interest Income"
        ))

    return entries


# =============================================================================
# Transaction Type to Mapper Function Registry
# =============================================================================

_TRANSACTION_MAPPERS: Dict[str, Callable[[Dict, sqlite3.Connection], List[JournalEntry]]] = {
    # Mutual Funds
    TransactionType.MF_PURCHASE.value: _map_mf_purchase,
    TransactionType.MF_REDEMPTION.value: _map_mf_redemption,
    TransactionType.MF_SWITCH_IN.value: _map_mf_purchase,  # Same as purchase
    TransactionType.MF_SWITCH_OUT.value: _map_mf_redemption,  # Same as redemption
    TransactionType.MF_DIVIDEND.value: _map_stock_dividend,  # Similar to stock dividend

    # Stocks
    TransactionType.STOCK_BUY.value: _map_stock_buy,
    TransactionType.STOCK_SELL.value: _map_stock_sell,
    TransactionType.STOCK_DIVIDEND.value: _map_stock_dividend,

    # Bank
    TransactionType.BANK_CREDIT.value: _map_bank_credit,
    TransactionType.BANK_DEBIT.value: _map_bank_debit,
    TransactionType.BANK_INTEREST.value: _map_bank_interest,
    TransactionType.FD_DEPOSIT.value: _map_fd_deposit,
    TransactionType.FD_INTEREST.value: _map_fd_interest,

    # Salary
    TransactionType.SALARY.value: _map_salary,
    TransactionType.SALARY_CREDIT.value: _map_salary,
    TransactionType.BONUS.value: _map_salary,

    # Retirement
    TransactionType.EPF_CONTRIBUTION.value: _map_epf_contribution,
    TransactionType.EPF_INTEREST.value: _map_epf_interest,
    TransactionType.PPF_DEPOSIT.value: _map_ppf_deposit,
    TransactionType.PPF_INTEREST.value: _map_ppf_interest,
    TransactionType.NPS_CONTRIBUTION.value: _map_nps_contribution,

    # Foreign Assets
    TransactionType.RSU_VEST.value: _map_rsu_vest,
    TransactionType.RSU_SALE.value: _map_rsu_sale,
    TransactionType.ESPP_PURCHASE.value: _map_rsu_vest,  # Similar structure
    TransactionType.ESPP_SALE.value: _map_rsu_sale,  # Similar structure
    TransactionType.FOREIGN_DIVIDEND.value: _map_foreign_dividend,

    # Other Assets
    TransactionType.SGB_PURCHASE.value: _map_sgb_purchase,
    TransactionType.SGB_INTEREST.value: _map_sgb_interest,

    # Rental
    TransactionType.RENTAL_INCOME.value: _map_rental_income,
}

# Asset category to default transaction type mapping
_ASSET_CATEGORY_DEFAULTS: Dict[str, str] = {
    AssetCategory.MF_EQUITY.value: TransactionType.MF_PURCHASE.value,
    AssetCategory.MF_DEBT.value: TransactionType.MF_PURCHASE.value,
    AssetCategory.INDIAN_STOCKS.value: TransactionType.STOCK_BUY.value,
    AssetCategory.BANK.value: TransactionType.BANK_CREDIT.value,
    AssetCategory.FD.value: TransactionType.FD_DEPOSIT.value,
    AssetCategory.SALARY.value: TransactionType.SALARY.value,
    AssetCategory.EPF.value: TransactionType.EPF_CONTRIBUTION.value,
    AssetCategory.PPF.value: TransactionType.PPF_DEPOSIT.value,
    AssetCategory.NPS.value: TransactionType.NPS_CONTRIBUTION.value,
    AssetCategory.SGB.value: TransactionType.SGB_PURCHASE.value,
    AssetCategory.US_STOCKS.value: TransactionType.RSU_VEST.value,
    AssetCategory.RENTAL.value: TransactionType.RENTAL_INCOME.value,
}


# =============================================================================
# Main Mapping Function
# =============================================================================

def map_to_journal(
    normalized_record: Dict[str, Any],
    conn: sqlite3.Connection = None
) -> List[JournalEntry]:
    """
    Map a normalized transaction record to journal entries.

    This is the main entry point for the ledger mapper. It determines the
    appropriate mapping function based on transaction_type or asset_category
    and returns the corresponding journal entries.

    Args:
        normalized_record: Dictionary containing normalized transaction data.
            Expected keys:
            - transaction_type: Type of transaction (e.g., 'MF_PURCHASE')
            - asset_category: Asset category (e.g., 'MF_EQUITY')
            - amount: Transaction amount
            - date: Transaction date
            - Other fields depending on transaction type

        conn: Database connection for account lookups.
              If None, returns empty list.

    Returns:
        List of JournalEntry objects for double-entry accounting.
        Returns empty list if:
        - conn is None
        - transaction_type not recognized
        - amount is zero or negative
        - required accounts not found

    Example:
        >>> record = {
        ...     'transaction_type': 'MF_PURCHASE',
        ...     'amount': Decimal('10000'),
        ...     'asset_name': 'HDFC Equity Fund',
        ...     'is_equity': True
        ... }
        >>> entries = map_to_journal(record, conn)
        >>> # Returns: [Dr MF_EQUITY 10000, Cr BANK_SAVINGS 10000]
    """
    if conn is None:
        logger.warning("No database connection provided to map_to_journal")
        return []

    # Get transaction type
    transaction_type = normalized_record.get('transaction_type', '')

    # If no transaction type, try to infer from asset_category
    if not transaction_type:
        asset_category = normalized_record.get('asset_category', '')
        transaction_type = _ASSET_CATEGORY_DEFAULTS.get(asset_category, '')

    # If still no transaction type, try flow_direction + asset_category
    if not transaction_type:
        flow = normalized_record.get('flow_direction', '')
        category = normalized_record.get('asset_category', '')

        if flow == 'IN':
            # Money coming in (credit/deposit/sale)
            if category in ('MF_EQUITY', 'MF_DEBT'):
                transaction_type = TransactionType.MF_REDEMPTION.value
            elif category == 'INDIAN_STOCKS':
                transaction_type = TransactionType.STOCK_SELL.value
            else:
                transaction_type = TransactionType.BANK_CREDIT.value
        elif flow == 'OUT':
            # Money going out (debit/investment/purchase)
            if category in ('MF_EQUITY', 'MF_DEBT'):
                transaction_type = TransactionType.MF_PURCHASE.value
            elif category == 'INDIAN_STOCKS':
                transaction_type = TransactionType.STOCK_BUY.value
            else:
                transaction_type = TransactionType.BANK_DEBIT.value

    # Look up mapper function
    mapper_func = _TRANSACTION_MAPPERS.get(transaction_type)

    if mapper_func is None:
        logger.debug(f"No mapper found for transaction_type: {transaction_type}")
        return []

    try:
        entries = mapper_func(normalized_record, conn)
        return entries
    except Exception as e:
        logger.error(f"Error mapping transaction to journal: {e}")
        return []


def get_supported_transaction_types() -> List[str]:
    """Return list of supported transaction types."""
    return list(_TRANSACTION_MAPPERS.keys())


def register_mapper(
    transaction_type: str,
    mapper_func: Callable[[Dict, sqlite3.Connection], List[JournalEntry]]
):
    """
    Register a custom mapper function for a transaction type.

    Args:
        transaction_type: Transaction type identifier
        mapper_func: Function that takes (record, conn) and returns List[JournalEntry]
    """
    _TRANSACTION_MAPPERS[transaction_type] = mapper_func
    logger.info(f"Registered custom mapper for: {transaction_type}")
