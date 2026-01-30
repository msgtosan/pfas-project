"""
Ledger Integration Layer for PFAS Parsers.

This module provides helper functions to record transactions using the
unified double-entry ledger system via TransactionService.record().

All financial transactions flow through this layer to ensure:
- Double-entry accounting (debits == credits)
- Idempotency (duplicate transactions detected and rejected)
- Proper audit trail
- Multi-user isolation
- Cost basis tracking for inventory accounting
- Salary component validation
- Forex gain/loss calculation for foreign assets
"""

import hashlib
import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Optional, List, Tuple, Any
import sqlite3

from pfas.core.journal import JournalEntry
from pfas.core.transaction_service import (
    TransactionService,
    TransactionSource,
    TransactionResult,
    TransactionRecord,
)
from pfas.core.accounts import get_account_by_code
from pfas.core.exceptions import AccountingBalanceError, ForexRateNotFoundError

logger = logging.getLogger(__name__)


# Account codes from Chart of Accounts (accounts.py)
class AccountCode(str, Enum):
    """Account codes from the Chart of Accounts."""
    # Bank Accounts
    BANK_SAVINGS = "1101"
    BANK_CURRENT = "1102"
    BANK_FD = "1103"

    # Investment Accounts
    MF_EQUITY = "1201"
    MF_DEBT = "1202"
    INDIAN_STOCKS = "1203"
    SGB = "1204"
    RBI_BONDS = "1205"

    # Retirement Accounts
    EPF_EMPLOYEE = "1301"
    EPF_EMPLOYER = "1302"
    PPF = "1303"
    NPS_TIER1 = "1304"
    NPS_TIER2 = "1305"

    # Foreign Assets
    US_STOCKS_RSU = "1401"
    US_STOCKS_ESPP = "1402"
    US_STOCKS_DRIP = "1403"
    US_BROKERAGE_CASH = "1404"

    # Tax Assets
    TDS_RECEIVABLE = "1601"
    TCS_RECEIVABLE = "1602"
    ADVANCE_TAX = "1603"
    FOREIGN_TAX_CREDIT = "1604"

    # Income Accounts - Salary
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

    # Income Accounts - Investment
    BANK_INTEREST = "4201"
    FD_INTEREST = "4202"
    DIVIDEND_INDIAN = "4203"
    DIVIDEND_FOREIGN = "4204"
    EPF_INTEREST = "4205"
    PPF_INTEREST = "4206"
    SGB_INTEREST = "4207"

    # Capital Gains
    STCG_EQUITY = "4301"
    LTCG_EQUITY = "4302"
    CG_DEBT = "4303"
    STCG_FOREIGN = "4304"
    LTCG_FOREIGN = "4305"
    FOREX_GAIN_LOSS = "4306"
    REALIZED_CAPITAL_GAIN = "4307"
    REALIZED_CAPITAL_LOSS = "4308"

    # Investment Expenses
    STT_PAID = "5201"
    BROKERAGE = "5202"
    FOREX_LOSS = "5205"

    # Salary Deduction Expenses
    PROFESSIONAL_TAX_PAID = "5301"
    INCOME_TAX_EXPENSE = "5302"
    OTHER_SALARY_DEDUCTIONS = "5303"


@dataclass
class LedgerRecordResult:
    """Result of recording a transaction to the ledger."""
    success: bool
    is_duplicate: bool = False
    journal_id: Optional[int] = None
    idempotency_key: Optional[str] = None
    error_message: Optional[str] = None


def get_account_id_by_code(conn: sqlite3.Connection, code: str) -> Optional[int]:
    """
    Get account ID from account code.

    Args:
        conn: Database connection
        code: Account code (e.g., "1101")

    Returns:
        Account ID or None if not found
    """
    account = get_account_by_code(conn, code)
    return account.id if account else None


def _get_or_create_account_id(conn: sqlite3.Connection, code: str) -> int:
    """
    Get account ID, raising error if not found.

    Args:
        conn: Database connection
        code: Account code

    Returns:
        Account ID

    Raises:
        ValueError: If account not found
    """
    account_id = get_account_id_by_code(conn, code)
    if account_id is None:
        raise ValueError(f"Account not found for code: {code}")
    return account_id


def generate_file_hash(file_path: str) -> str:
    """
    Generate a short hash from file path for idempotency keys.

    Args:
        file_path: Path to source file

    Returns:
        8-character hash
    """
    return hashlib.md5(file_path.encode()).hexdigest()[:8]


# =============================================================================
# Mutual Fund Transaction Recording
# =============================================================================

def record_mf_purchase(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    folio_number: str,
    scheme_name: str,
    txn_date: date,
    amount: Decimal,
    units: Decimal,
    is_equity: bool,
    source_file: str,
    row_idx: int,
    source: TransactionSource = TransactionSource.PARSER_CAMS,
) -> LedgerRecordResult:
    """
    Record MF purchase transaction with double-entry.

    Dr MF Asset (1201/1202)  | Amount
    Cr Bank Account (1101)   | Amount

    Args:
        txn_service: TransactionService instance
        conn: Database connection
        user_id: User ID
        folio_number: Folio number
        scheme_name: Scheme name
        txn_date: Transaction date
        amount: Purchase amount
        units: Units purchased
        is_equity: True for equity MF, False for debt
        source_file: Source file path
        row_idx: Row index in file
        source: Transaction source

    Returns:
        LedgerRecordResult
    """
    # Determine account codes
    mf_account_code = AccountCode.MF_EQUITY if is_equity else AccountCode.MF_DEBT
    bank_account_code = AccountCode.BANK_SAVINGS

    # Get account IDs
    mf_account_id = _get_or_create_account_id(conn, mf_account_code.value)
    bank_account_id = _get_or_create_account_id(conn, bank_account_code.value)

    # Create journal entries
    entries = [
        JournalEntry(
            account_id=mf_account_id,
            debit=amount,
            narration=f"Purchase: {scheme_name} - {units} units"
        ),
        JournalEntry(
            account_id=bank_account_id,
            credit=amount,
            narration=f"Payment for MF purchase"
        ),
    ]

    # Generate idempotency key
    file_hash = generate_file_hash(source_file)
    idempotency_key = f"mf:{file_hash}:{row_idx}:{folio_number}:{txn_date.isoformat()}:{amount}:{units}"

    # Record transaction
    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"MF Purchase: {scheme_name[:50]}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="MF_PURCHASE",
    )

    return _convert_result(result)


def record_mf_redemption(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    folio_number: str,
    scheme_name: str,
    txn_date: date,
    proceeds: Decimal,
    cost_basis: Decimal,
    units: Decimal,
    is_equity: bool,
    is_long_term: bool,
    source_file: str,
    row_idx: int,
    stt: Decimal = Decimal("0"),
    source: TransactionSource = TransactionSource.PARSER_CAMS,
) -> LedgerRecordResult:
    """
    Record MF redemption transaction with capital gains.

    Dr Bank Account          | Proceeds
    Dr STT Paid (if any)     | STT
    Cr MF Asset (Cost Basis) | Cost
    Cr Capital Gains         | Gain (if positive)
    -- or --
    Dr Capital Loss          | Loss (if negative)

    Args:
        txn_service: TransactionService instance
        conn: Database connection
        user_id: User ID
        folio_number: Folio number
        scheme_name: Scheme name
        txn_date: Transaction date
        proceeds: Sale proceeds
        cost_basis: Cost of units sold
        units: Units sold
        is_equity: True for equity MF, False for debt
        is_long_term: True for LTCG, False for STCG
        source_file: Source file path
        row_idx: Row index in file
        stt: STT paid
        source: Transaction source

    Returns:
        LedgerRecordResult
    """
    # Determine account codes
    mf_account_code = AccountCode.MF_EQUITY if is_equity else AccountCode.MF_DEBT
    bank_account_code = AccountCode.BANK_SAVINGS

    if is_equity:
        cg_account_code = AccountCode.LTCG_EQUITY if is_long_term else AccountCode.STCG_EQUITY
    else:
        cg_account_code = AccountCode.CG_DEBT

    # Get account IDs
    mf_account_id = _get_or_create_account_id(conn, mf_account_code.value)
    bank_account_id = _get_or_create_account_id(conn, bank_account_code.value)
    cg_account_id = _get_or_create_account_id(conn, cg_account_code.value)
    stt_account_id = _get_or_create_account_id(conn, AccountCode.STT_PAID.value)

    # Calculate gain/loss (adjusted for STT)
    net_proceeds = proceeds - stt
    gain_loss = net_proceeds - cost_basis

    entries = []

    # Debit: Bank receives proceeds
    entries.append(JournalEntry(
        account_id=bank_account_id,
        debit=proceeds,
        narration=f"Proceeds from MF redemption"
    ))

    # Debit: STT (if any)
    if stt > 0:
        entries.append(JournalEntry(
            account_id=stt_account_id,
            debit=stt,
            narration=f"STT on MF redemption"
        ))

    # Credit: MF Asset (cost basis)
    entries.append(JournalEntry(
        account_id=mf_account_id,
        credit=cost_basis,
        narration=f"Redemption: {scheme_name} - {units} units @ cost"
    ))

    # Handle capital gain/loss
    if gain_loss > 0:
        # Credit: Capital Gain
        entries.append(JournalEntry(
            account_id=cg_account_id,
            credit=gain_loss,
            narration=f"{'LTCG' if is_long_term else 'STCG'} on MF redemption"
        ))
    elif gain_loss < 0:
        # Debit: Capital Loss (negative gain is positive debit)
        entries.append(JournalEntry(
            account_id=cg_account_id,
            debit=abs(gain_loss),
            narration=f"Capital loss on MF redemption"
        ))

    # Generate idempotency key
    file_hash = generate_file_hash(source_file)
    idempotency_key = f"mf:{file_hash}:{row_idx}:{folio_number}:{txn_date.isoformat()}:{proceeds}:{units}"

    # Record transaction
    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"MF Redemption: {scheme_name[:50]}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="MF_REDEMPTION",
    )

    return _convert_result(result)


def record_mf_switch(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    folio_number: str,
    from_scheme: str,
    to_scheme: str,
    txn_date: date,
    amount: Decimal,
    units_out: Decimal,
    units_in: Decimal,
    is_equity_from: bool,
    is_equity_to: bool,
    source_file: str,
    row_idx: int,
    source: TransactionSource = TransactionSource.PARSER_CAMS,
) -> LedgerRecordResult:
    """
    Record MF switch transaction (switch out + switch in as single entry).

    Dr MF Asset (to scheme)   | Amount
    Cr MF Asset (from scheme) | Amount

    Note: Capital gains should be recorded separately for taxable switches.
    """
    # Determine account codes
    from_account_code = AccountCode.MF_EQUITY if is_equity_from else AccountCode.MF_DEBT
    to_account_code = AccountCode.MF_EQUITY if is_equity_to else AccountCode.MF_DEBT

    # Get account IDs
    from_account_id = _get_or_create_account_id(conn, from_account_code.value)
    to_account_id = _get_or_create_account_id(conn, to_account_code.value)

    entries = [
        JournalEntry(
            account_id=to_account_id,
            debit=amount,
            narration=f"Switch in: {to_scheme[:30]} - {units_in} units"
        ),
        JournalEntry(
            account_id=from_account_id,
            credit=amount,
            narration=f"Switch out: {from_scheme[:30]} - {units_out} units"
        ),
    ]

    # Generate idempotency key
    file_hash = generate_file_hash(source_file)
    idempotency_key = f"mf:switch:{file_hash}:{row_idx}:{folio_number}:{txn_date.isoformat()}:{amount}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"MF Switch: {from_scheme[:25]} -> {to_scheme[:25]}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="MF_SWITCH",
    )

    return _convert_result(result)


def record_mf_dividend(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    folio_number: str,
    scheme_name: str,
    txn_date: date,
    amount: Decimal,
    is_reinvested: bool,
    is_equity: bool,
    source_file: str,
    row_idx: int,
    source: TransactionSource = TransactionSource.PARSER_CAMS,
) -> LedgerRecordResult:
    """
    Record MF dividend transaction.

    Payout:
        Dr Bank Account      | Amount
        Cr Dividend Income   | Amount

    Reinvestment:
        Dr MF Asset          | Amount
        Cr Dividend Income   | Amount
    """
    # Determine account codes
    if is_reinvested:
        target_account_code = AccountCode.MF_EQUITY if is_equity else AccountCode.MF_DEBT
    else:
        target_account_code = AccountCode.BANK_SAVINGS

    dividend_income_code = AccountCode.DIVIDEND_INDIAN

    # Get account IDs
    target_account_id = _get_or_create_account_id(conn, target_account_code.value)
    dividend_account_id = _get_or_create_account_id(conn, dividend_income_code.value)

    entries = [
        JournalEntry(
            account_id=target_account_id,
            debit=amount,
            narration=f"{'Reinvestment' if is_reinvested else 'Payout'}: {scheme_name[:30]}"
        ),
        JournalEntry(
            account_id=dividend_account_id,
            credit=amount,
            narration=f"Dividend from {scheme_name[:30]}"
        ),
    ]

    # Generate idempotency key
    file_hash = generate_file_hash(source_file)
    idempotency_key = f"mf:div:{file_hash}:{row_idx}:{folio_number}:{txn_date.isoformat()}:{amount}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"MF Dividend {'Reinvest' if is_reinvested else 'Payout'}: {scheme_name[:40]}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="MF_DIVIDEND",
    )

    return _convert_result(result)


# =============================================================================
# Bank Transaction Recording
# =============================================================================

def record_bank_credit(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    account_number: str,
    txn_date: date,
    amount: Decimal,
    description: str,
    category: str,
    ref_no: str,
    source_file: str,
    row_idx: int,
    source: TransactionSource = TransactionSource.PARSER_ICICI,
) -> LedgerRecordResult:
    """
    Record bank credit (deposit) transaction.

    Dr Bank Account (1101)     | Amount
    Cr Income/Category Account | Amount
    """
    bank_account_id = _get_or_create_account_id(conn, AccountCode.BANK_SAVINGS.value)

    # Map category to income account
    income_account_code = _map_bank_category_to_account(category, is_credit=True)
    income_account_id = _get_or_create_account_id(conn, income_account_code)

    entries = [
        JournalEntry(
            account_id=bank_account_id,
            debit=amount,
            narration=description[:100]
        ),
        JournalEntry(
            account_id=income_account_id,
            credit=amount,
            narration=f"Bank credit: {category}"
        ),
    ]

    # Generate idempotency key
    account_hash = hashlib.md5(account_number.encode()).hexdigest()[:8]
    file_hash = generate_file_hash(source_file)
    idempotency_key = f"bank:{file_hash}:{row_idx}:{account_hash}:{txn_date.isoformat()}:{ref_no}:{amount}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"Bank Credit: {description[:50]}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="BANK_CREDIT",
    )

    return _convert_result(result)


def record_bank_debit(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    account_number: str,
    txn_date: date,
    amount: Decimal,
    description: str,
    category: str,
    ref_no: str,
    source_file: str,
    row_idx: int,
    source: TransactionSource = TransactionSource.PARSER_ICICI,
) -> LedgerRecordResult:
    """
    Record bank debit (withdrawal/expense) transaction.

    Dr Expense/Category Account | Amount
    Cr Bank Account (1101)      | Amount
    """
    bank_account_id = _get_or_create_account_id(conn, AccountCode.BANK_SAVINGS.value)

    # Map category to expense/asset account
    expense_account_code = _map_bank_category_to_account(category, is_credit=False)
    expense_account_id = _get_or_create_account_id(conn, expense_account_code)

    entries = [
        JournalEntry(
            account_id=expense_account_id,
            debit=amount,
            narration=f"Bank debit: {category}"
        ),
        JournalEntry(
            account_id=bank_account_id,
            credit=amount,
            narration=description[:100]
        ),
    ]

    # Generate idempotency key
    account_hash = hashlib.md5(account_number.encode()).hexdigest()[:8]
    file_hash = generate_file_hash(source_file)
    idempotency_key = f"bank:{file_hash}:{row_idx}:{account_hash}:{txn_date.isoformat()}:{ref_no}:{amount}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"Bank Debit: {description[:50]}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="BANK_DEBIT",
    )

    return _convert_result(result)


def _map_bank_category_to_account(category: str, is_credit: bool) -> str:
    """
    Map bank transaction category to account code.

    Args:
        category: Transaction category
        is_credit: True for credits, False for debits

    Returns:
        Account code
    """
    category_upper = category.upper() if category else ""

    if is_credit:
        # Income categories
        if "SALARY" in category_upper:
            return AccountCode.SALARY_INCOME.value
        elif "INTEREST" in category_upper:
            return AccountCode.BANK_INTEREST.value
        elif "DIVIDEND" in category_upper:
            return AccountCode.DIVIDEND_INDIAN.value
        elif "REFUND" in category_upper or "TDS" in category_upper:
            return AccountCode.TDS_RECEIVABLE.value
        else:
            # Default to Other Income (use salary as placeholder)
            return AccountCode.SALARY_INCOME.value
    else:
        # Expense/Transfer categories
        if "INVESTMENT" in category_upper or "MF" in category_upper:
            return AccountCode.MF_EQUITY.value
        elif "STOCK" in category_upper:
            return AccountCode.INDIAN_STOCKS.value
        elif "FD" in category_upper or "FIXED DEPOSIT" in category_upper:
            return AccountCode.BANK_FD.value
        elif "PPF" in category_upper:
            return AccountCode.PPF.value
        elif "TAX" in category_upper:
            return AccountCode.ADVANCE_TAX.value
        else:
            # Default to expense (use brokerage as general expense)
            return AccountCode.BROKERAGE.value


# =============================================================================
# Stock Transaction Recording
# =============================================================================

def record_stock_buy(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    symbol: str,
    txn_date: date,
    quantity: int,
    price: Decimal,
    amount: Decimal,
    brokerage: Decimal,
    source_file: str,
    row_idx: int,
    broker: str = "Generic",
    trade_id: str = "",
    source: TransactionSource = TransactionSource.PARSER_ZERODHA,
) -> LedgerRecordResult:
    """
    Record stock purchase transaction.

    Dr Indian Stocks (1203)  | Amount + Brokerage
    Cr Bank Account (1101)   | Amount + Brokerage
    """
    stock_account_id = _get_or_create_account_id(conn, AccountCode.INDIAN_STOCKS.value)
    bank_account_id = _get_or_create_account_id(conn, AccountCode.BANK_SAVINGS.value)

    total_amount = amount + brokerage

    entries = [
        JournalEntry(
            account_id=stock_account_id,
            debit=total_amount,
            narration=f"Buy: {symbol} x {quantity} @ {price}"
        ),
        JournalEntry(
            account_id=bank_account_id,
            credit=total_amount,
            narration=f"Payment for stock purchase: {symbol}"
        ),
    ]

    # Generate idempotency key
    file_hash = generate_file_hash(source_file)
    if trade_id:
        idempotency_key = f"stock:{broker}:{trade_id}"
    else:
        idempotency_key = f"stock:{file_hash}:{row_idx}:{symbol}:{txn_date.isoformat()}:{quantity}:BUY"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"Stock Buy: {symbol} x {quantity}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="STOCK_BUY",
    )

    return _convert_result(result)


def record_stock_sell(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    symbol: str,
    txn_date: date,
    quantity: int,
    price: Decimal,
    proceeds: Decimal,
    cost_basis: Decimal,
    brokerage: Decimal,
    stt: Decimal,
    is_long_term: bool,
    source_file: str,
    row_idx: int,
    broker: str = "Generic",
    trade_id: str = "",
    source: TransactionSource = TransactionSource.PARSER_ZERODHA,
) -> LedgerRecordResult:
    """
    Record stock sale transaction with capital gains.

    Dr Bank Account          | Proceeds
    Dr Brokerage             | Brokerage
    Dr STT Paid              | STT
    Cr Indian Stocks (1203)  | Cost Basis
    Cr Capital Gains         | Gain (if positive)
    -- or --
    Dr Capital Loss          | Loss (if negative)
    """
    stock_account_id = _get_or_create_account_id(conn, AccountCode.INDIAN_STOCKS.value)
    bank_account_id = _get_or_create_account_id(conn, AccountCode.BANK_SAVINGS.value)
    brokerage_account_id = _get_or_create_account_id(conn, AccountCode.BROKERAGE.value)
    stt_account_id = _get_or_create_account_id(conn, AccountCode.STT_PAID.value)

    cg_account_code = AccountCode.LTCG_EQUITY if is_long_term else AccountCode.STCG_EQUITY
    cg_account_id = _get_or_create_account_id(conn, cg_account_code.value)

    # Calculate net proceeds and gain/loss
    net_proceeds = proceeds - brokerage - stt
    gain_loss = net_proceeds - cost_basis

    entries = []

    # Debit: Bank receives proceeds
    entries.append(JournalEntry(
        account_id=bank_account_id,
        debit=proceeds,
        narration=f"Proceeds from stock sale: {symbol}"
    ))

    # Debit: Brokerage
    if brokerage > 0:
        entries.append(JournalEntry(
            account_id=brokerage_account_id,
            debit=brokerage,
            narration=f"Brokerage on {symbol} sale"
        ))

    # Debit: STT
    if stt > 0:
        entries.append(JournalEntry(
            account_id=stt_account_id,
            debit=stt,
            narration=f"STT on {symbol} sale"
        ))

    # Credit: Stock asset (cost basis)
    entries.append(JournalEntry(
        account_id=stock_account_id,
        credit=cost_basis,
        narration=f"Sell: {symbol} x {quantity} @ cost"
    ))

    # Handle capital gain/loss
    if gain_loss > 0:
        entries.append(JournalEntry(
            account_id=cg_account_id,
            credit=gain_loss,
            narration=f"{'LTCG' if is_long_term else 'STCG'} on {symbol}"
        ))
    elif gain_loss < 0:
        entries.append(JournalEntry(
            account_id=cg_account_id,
            debit=abs(gain_loss),
            narration=f"Capital loss on {symbol}"
        ))

    # Generate idempotency key
    file_hash = generate_file_hash(source_file)
    if trade_id:
        idempotency_key = f"stock:{broker}:{trade_id}"
    else:
        idempotency_key = f"stock:{file_hash}:{row_idx}:{symbol}:{txn_date.isoformat()}:{quantity}:SELL"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"Stock Sell: {symbol} x {quantity}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="STOCK_SELL",
    )

    return _convert_result(result)


# =============================================================================
# Salary Transaction Recording
# =============================================================================

def record_salary(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    employer: str,
    pay_period: str,
    gross_salary: Decimal,
    net_salary: Decimal,
    tds_deducted: Decimal,
    epf_employee: Decimal,
    txn_date: date,
    source_file: str,
    row_idx: int,
    source: TransactionSource = TransactionSource.PARSER_HDFC,
) -> LedgerRecordResult:
    """
    Record salary transaction with proper double-entry.

    Dr Bank Account (Net)      | Net Amount
    Dr TDS Receivable (1601)   | TDS Amount
    Dr EPF Asset (1301)        | Employee EPF
    Cr Salary Income (4100)    | Gross Amount
    """
    bank_account_id = _get_or_create_account_id(conn, AccountCode.BANK_SAVINGS.value)
    tds_account_id = _get_or_create_account_id(conn, AccountCode.TDS_RECEIVABLE.value)
    epf_account_id = _get_or_create_account_id(conn, AccountCode.EPF_EMPLOYEE.value)
    salary_income_id = _get_or_create_account_id(conn, AccountCode.SALARY_INCOME.value)

    entries = []

    # Debit: Bank receives net salary
    entries.append(JournalEntry(
        account_id=bank_account_id,
        debit=net_salary,
        narration=f"Net salary for {pay_period}"
    ))

    # Debit: TDS receivable
    if tds_deducted > 0:
        entries.append(JournalEntry(
            account_id=tds_account_id,
            debit=tds_deducted,
            narration=f"TDS deducted for {pay_period}"
        ))

    # Debit: EPF employee contribution
    if epf_employee > 0:
        entries.append(JournalEntry(
            account_id=epf_account_id,
            debit=epf_employee,
            narration=f"EPF contribution for {pay_period}"
        ))

    # Credit: Salary income (gross)
    entries.append(JournalEntry(
        account_id=salary_income_id,
        credit=gross_salary,
        narration=f"Gross salary from {employer}"
    ))

    # Generate idempotency key
    idempotency_key = f"salary:{employer}:{pay_period}:{gross_salary}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"Salary: {employer} - {pay_period}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="SALARY",
    )

    return _convert_result(result)


# =============================================================================
# EPF Transaction Recording
# =============================================================================

def record_epf_contribution(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    uan: str,
    wage_month: str,
    employee_contribution: Decimal,
    employer_contribution: Decimal,
    txn_date: date,
    source_file: str,
    row_idx: int,
    source: TransactionSource = TransactionSource.PARSER_EPF,
) -> LedgerRecordResult:
    """
    Record EPF monthly contribution (CR type).

    Dr EPF Asset - Employee (1301) | Employee Amount
    Dr EPF Asset - Employer (1302) | Employer Amount
    Cr Bank/Salary (1101)          | Total

    Note: In practice, EPF is deducted from salary, so we credit bank.
    """
    epf_ee_account_id = _get_or_create_account_id(conn, AccountCode.EPF_EMPLOYEE.value)
    epf_er_account_id = _get_or_create_account_id(conn, AccountCode.EPF_EMPLOYER.value)
    bank_account_id = _get_or_create_account_id(conn, AccountCode.BANK_SAVINGS.value)

    total = employee_contribution + employer_contribution

    entries = []

    # Debit: EPF Employee contribution
    if employee_contribution > 0:
        entries.append(JournalEntry(
            account_id=epf_ee_account_id,
            debit=employee_contribution,
            narration=f"EPF EE contribution for {wage_month}"
        ))

    # Debit: EPF Employer contribution
    if employer_contribution > 0:
        entries.append(JournalEntry(
            account_id=epf_er_account_id,
            debit=employer_contribution,
            narration=f"EPF ER contribution for {wage_month}"
        ))

    # Credit: Bank (representing deduction from salary)
    entries.append(JournalEntry(
        account_id=bank_account_id,
        credit=total,
        narration=f"EPF contribution deducted for {wage_month}"
    ))

    # Generate idempotency key
    uan_hash = hashlib.md5(uan.encode()).hexdigest()[:8]
    idempotency_key = f"epf:{uan_hash}:{wage_month}:{employee_contribution}:{employer_contribution}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"EPF Contribution: {wage_month}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="EPF_CONTRIBUTION",
    )

    return _convert_result(result)


def record_epf_interest(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    uan: str,
    financial_year: str,
    employee_interest: Decimal,
    employer_interest: Decimal,
    txn_date: date,
    source_file: str,
    source: TransactionSource = TransactionSource.PARSER_EPF,
) -> LedgerRecordResult:
    """
    Record EPF interest accrual (INT type).

    Dr EPF Asset - Employee (1301) | Employee Interest
    Dr EPF Asset - Employer (1302) | Employer Interest
    Cr Interest Income (4205)      | Total Interest
    """
    epf_ee_account_id = _get_or_create_account_id(conn, AccountCode.EPF_EMPLOYEE.value)
    epf_er_account_id = _get_or_create_account_id(conn, AccountCode.EPF_EMPLOYER.value)
    interest_income_id = _get_or_create_account_id(conn, AccountCode.EPF_INTEREST.value)

    total_interest = employee_interest + employer_interest

    entries = []

    # Debit: EPF Employee interest
    if employee_interest > 0:
        entries.append(JournalEntry(
            account_id=epf_ee_account_id,
            debit=employee_interest,
            narration=f"EPF EE interest for FY {financial_year}"
        ))

    # Debit: EPF Employer interest
    if employer_interest > 0:
        entries.append(JournalEntry(
            account_id=epf_er_account_id,
            debit=employer_interest,
            narration=f"EPF ER interest for FY {financial_year}"
        ))

    # Credit: Interest Income
    entries.append(JournalEntry(
        account_id=interest_income_id,
        credit=total_interest,
        narration=f"EPF interest income for FY {financial_year}"
    ))

    # Generate idempotency key
    uan_hash = hashlib.md5(uan.encode()).hexdigest()[:8]
    idempotency_key = f"epf:int:{uan_hash}:{financial_year}:{total_interest}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"EPF Interest: FY {financial_year}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="EPF_INTEREST",
    )

    return _convert_result(result)


# =============================================================================
# PPF Transaction Recording
# =============================================================================

def record_ppf_deposit(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    account_number: str,
    txn_date: date,
    amount: Decimal,
    source_file: str,
    row_idx: int,
    source: TransactionSource = TransactionSource.PARSER_PPF,
) -> LedgerRecordResult:
    """
    Record PPF deposit transaction.

    Dr PPF Asset (1303)      | Amount
    Cr Bank Account (1101)   | Amount
    """
    ppf_account_id = _get_or_create_account_id(conn, AccountCode.PPF.value)
    bank_account_id = _get_or_create_account_id(conn, AccountCode.BANK_SAVINGS.value)

    entries = [
        JournalEntry(
            account_id=ppf_account_id,
            debit=amount,
            narration=f"PPF deposit on {txn_date.isoformat()}"
        ),
        JournalEntry(
            account_id=bank_account_id,
            credit=amount,
            narration=f"Payment for PPF deposit"
        ),
    ]

    # Generate idempotency key
    account_hash = hashlib.md5(account_number.encode()).hexdigest()[:8]
    file_hash = generate_file_hash(source_file)
    idempotency_key = f"ppf:{file_hash}:{row_idx}:{account_hash}:{txn_date.isoformat()}:DEPOSIT:{amount}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"PPF Deposit: {amount}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="PPF_DEPOSIT",
    )

    return _convert_result(result)


def record_ppf_interest(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    account_number: str,
    txn_date: date,
    amount: Decimal,
    financial_year: str,
    source_file: str,
    row_idx: int,
    source: TransactionSource = TransactionSource.PARSER_PPF,
) -> LedgerRecordResult:
    """
    Record PPF interest credit.

    Dr PPF Asset (1303)       | Interest
    Cr Interest Income (4206) | Interest
    """
    ppf_account_id = _get_or_create_account_id(conn, AccountCode.PPF.value)
    interest_income_id = _get_or_create_account_id(conn, AccountCode.PPF_INTEREST.value)

    entries = [
        JournalEntry(
            account_id=ppf_account_id,
            debit=amount,
            narration=f"PPF interest for FY {financial_year}"
        ),
        JournalEntry(
            account_id=interest_income_id,
            credit=amount,
            narration=f"PPF interest income for FY {financial_year}"
        ),
    ]

    # Generate idempotency key
    account_hash = hashlib.md5(account_number.encode()).hexdigest()[:8]
    file_hash = generate_file_hash(source_file)
    idempotency_key = f"ppf:{file_hash}:{row_idx}:{account_hash}:{txn_date.isoformat()}:INT:{amount}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"PPF Interest: FY {financial_year}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="PPF_INTEREST",
    )

    return _convert_result(result)


def record_ppf_withdrawal(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    account_number: str,
    txn_date: date,
    amount: Decimal,
    source_file: str,
    row_idx: int,
    source: TransactionSource = TransactionSource.PARSER_PPF,
) -> LedgerRecordResult:
    """
    Record PPF withdrawal (after lock-in).

    Dr Bank Account (1101)   | Amount
    Cr PPF Asset (1303)      | Amount
    """
    ppf_account_id = _get_or_create_account_id(conn, AccountCode.PPF.value)
    bank_account_id = _get_or_create_account_id(conn, AccountCode.BANK_SAVINGS.value)

    entries = [
        JournalEntry(
            account_id=bank_account_id,
            debit=amount,
            narration=f"PPF withdrawal on {txn_date.isoformat()}"
        ),
        JournalEntry(
            account_id=ppf_account_id,
            credit=amount,
            narration=f"PPF withdrawal"
        ),
    ]

    # Generate idempotency key
    account_hash = hashlib.md5(account_number.encode()).hexdigest()[:8]
    file_hash = generate_file_hash(source_file)
    idempotency_key = f"ppf:{file_hash}:{row_idx}:{account_hash}:{txn_date.isoformat()}:WD:{amount}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"PPF Withdrawal: {amount}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="PPF_WITHDRAWAL",
    )

    return _convert_result(result)


# =============================================================================
# Helper Functions
# =============================================================================

def _convert_result(record: TransactionRecord) -> LedgerRecordResult:
    """Convert TransactionRecord to LedgerRecordResult."""
    return LedgerRecordResult(
        success=record.result == TransactionResult.SUCCESS,
        is_duplicate=record.result == TransactionResult.DUPLICATE,
        journal_id=record.journal_id,
        idempotency_key=record.idempotency_key,
        error_message=record.error_message,
    )


# =============================================================================
# Complex Salary Mapping (Multi-Legged Journal Entry)
# =============================================================================

def validate_salary_components(
    gross_salary: Decimal,
    net_salary: Decimal,
    tds_deducted: Decimal,
    epf_employee: Decimal,
    professional_tax: Decimal = Decimal("0"),
    other_deductions: Decimal = Decimal("0"),
    tolerance: Decimal = Decimal("1.00")
) -> bool:
    """
    Validate that salary components sum to gross salary.

    The accounting equation: Gross = Net + TDS + EPF + Professional Tax + Other

    Args:
        gross_salary: Gross salary
        net_salary: Net salary (take-home)
        tds_deducted: TDS deducted
        epf_employee: Employee EPF contribution
        professional_tax: Professional tax deducted
        other_deductions: Any other deductions
        tolerance: Acceptable rounding difference

    Returns:
        True if valid

    Raises:
        AccountingBalanceError: If components don't balance
    """
    calculated_gross = net_salary + tds_deducted + epf_employee + professional_tax + other_deductions
    difference = abs(gross_salary - calculated_gross)

    if difference > tolerance:
        raise AccountingBalanceError(
            message="Salary components do not sum to Gross Salary",
            expected=str(gross_salary),
            actual=str(calculated_gross),
            difference=str(difference),
        )

    return True


def record_salary_multi_leg(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    employer: str,
    pay_period: str,
    gross_salary: Decimal,
    net_salary: Decimal,
    tds_deducted: Decimal,
    epf_employee: Decimal,
    txn_date: date,
    source_file: str,
    row_idx: int,
    professional_tax: Decimal = Decimal("0"),
    other_deductions: Decimal = Decimal("0"),
    validate: bool = True,
    source: TransactionSource = TransactionSource.PARSER_HDFC,
) -> LedgerRecordResult:
    """
    Record salary transaction with complex multi-legged journal entry.

    Creates a single journal entry with four lines:
    - Debit (1101): Net Salary to Bank
    - Debit (1601): TDS Receivable
    - Debit (1301): EPF Employee Asset
    - Debit (5301): Professional Tax Expense (if any)
    - Credit (4100): Gross Salary Income

    Args:
        txn_service: TransactionService instance
        conn: Database connection
        user_id: User ID
        employer: Employer name
        pay_period: Pay period (e.g., "March 2024")
        gross_salary: Gross salary
        net_salary: Net salary (take-home)
        tds_deducted: TDS deducted
        epf_employee: Employee EPF contribution
        txn_date: Transaction date
        source_file: Source file path
        row_idx: Row index
        professional_tax: Professional tax deducted
        other_deductions: Other deductions
        validate: Whether to validate component balance
        source: Transaction source

    Returns:
        LedgerRecordResult

    Raises:
        AccountingBalanceError: If validate=True and components don't balance
    """
    # Validate salary components if requested
    if validate:
        validate_salary_components(
            gross_salary, net_salary, tds_deducted, epf_employee,
            professional_tax, other_deductions
        )

    # Get account IDs
    bank_account_id = _get_or_create_account_id(conn, AccountCode.BANK_SAVINGS.value)
    tds_account_id = _get_or_create_account_id(conn, AccountCode.TDS_RECEIVABLE.value)
    epf_account_id = _get_or_create_account_id(conn, AccountCode.EPF_EMPLOYEE.value)
    salary_income_id = _get_or_create_account_id(conn, AccountCode.SALARY_INCOME.value)
    prof_tax_id = _get_or_create_account_id(conn, AccountCode.PROFESSIONAL_TAX_PAID.value)
    other_ded_id = _get_or_create_account_id(conn, AccountCode.OTHER_SALARY_DEDUCTIONS.value)

    entries = []

    # Debit: Net salary to Bank
    entries.append(JournalEntry(
        account_id=bank_account_id,
        debit=net_salary,
        narration=f"Net salary for {pay_period}"
    ))

    # Debit: TDS receivable
    if tds_deducted > 0:
        entries.append(JournalEntry(
            account_id=tds_account_id,
            debit=tds_deducted,
            narration=f"TDS deducted for {pay_period}"
        ))

    # Debit: EPF employee contribution
    if epf_employee > 0:
        entries.append(JournalEntry(
            account_id=epf_account_id,
            debit=epf_employee,
            narration=f"EPF contribution for {pay_period}"
        ))

    # Debit: Professional tax
    if professional_tax > 0:
        entries.append(JournalEntry(
            account_id=prof_tax_id,
            debit=professional_tax,
            narration=f"Professional tax for {pay_period}"
        ))

    # Debit: Other deductions
    if other_deductions > 0:
        entries.append(JournalEntry(
            account_id=other_ded_id,
            debit=other_deductions,
            narration=f"Other deductions for {pay_period}"
        ))

    # Credit: Gross salary income
    entries.append(JournalEntry(
        account_id=salary_income_id,
        credit=gross_salary,
        narration=f"Gross salary from {employer}"
    ))

    # Generate idempotency key
    idempotency_key = f"salary:multi:{employer}:{pay_period}:{gross_salary}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"Salary: {employer} - {pay_period}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="SALARY_MULTI_LEG",
    )

    return _convert_result(result)


def record_employer_pf_contribution(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    employer: str,
    pay_period: str,
    employer_contribution: Decimal,
    txn_date: date,
    source_file: str,
    row_idx: int,
    source: TransactionSource = TransactionSource.PARSER_HDFC,
) -> LedgerRecordResult:
    """
    Record employer PF contribution as separate entry.

    Debit (1302): EPF Employer Asset
    Credit (4107): Employer PF Contribution Income

    Args:
        txn_service: TransactionService instance
        conn: Database connection
        user_id: User ID
        employer: Employer name
        pay_period: Pay period
        employer_contribution: Employer PF amount
        txn_date: Transaction date
        source_file: Source file path
        row_idx: Row index
        source: Transaction source

    Returns:
        LedgerRecordResult
    """
    epf_er_account_id = _get_or_create_account_id(conn, AccountCode.EPF_EMPLOYER.value)
    employer_pf_income_id = _get_or_create_account_id(conn, AccountCode.EMPLOYER_PF_CONTRIBUTION.value)

    entries = [
        JournalEntry(
            account_id=epf_er_account_id,
            debit=employer_contribution,
            narration=f"Employer PF contribution for {pay_period}"
        ),
        JournalEntry(
            account_id=employer_pf_income_id,
            credit=employer_contribution,
            narration=f"Employer PF contribution from {employer}"
        ),
    ]

    idempotency_key = f"salary:erpf:{employer}:{pay_period}:{employer_contribution}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"Employer PF: {employer} - {pay_period}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=txn_date,
        reference_type="EMPLOYER_PF",
    )

    return _convert_result(result)


# =============================================================================
# Equity & MF Inventory Accounting (with Cost Basis)
# =============================================================================

def record_mf_purchase_with_cost_basis(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    folio_number: str,
    scheme_name: str,
    isin: str,
    txn_date: date,
    amount: Decimal,
    units: Decimal,
    is_equity: bool,
    source_file: str,
    row_idx: int,
    source: TransactionSource = TransactionSource.PARSER_CAMS,
) -> LedgerRecordResult:
    """
    Record MF purchase with cost basis tracking.

    Creates:
    - Journal entry: Dr MF Asset | Cr Bank
    - Cost basis lot for FIFO tracking

    Args:
        txn_service: TransactionService instance
        conn: Database connection
        user_id: User ID
        folio_number: Folio number
        scheme_name: Scheme name
        isin: ISIN code (used for lot tracking)
        txn_date: Transaction date
        amount: Purchase amount
        units: Units purchased
        is_equity: True for equity, False for debt
        source_file: Source file path
        row_idx: Row index
        source: Transaction source

    Returns:
        LedgerRecordResult
    """
    from pfas.services.cost_basis_tracker import CostBasisTracker

    # Record to ledger
    ledger_result = record_mf_purchase(
        txn_service=txn_service,
        conn=conn,
        user_id=user_id,
        folio_number=folio_number,
        scheme_name=scheme_name,
        txn_date=txn_date,
        amount=amount,
        units=units,
        is_equity=is_equity,
        source_file=source_file,
        row_idx=row_idx,
        source=source,
    )

    # If ledger entry succeeded, create cost basis lot
    if ledger_result.success and not ledger_result.is_duplicate:
        cost_tracker = CostBasisTracker(conn)
        asset_type = "MF_EQUITY" if is_equity else "MF_DEBT"

        cost_tracker.record_purchase(
            user_id=user_id,
            asset_type=asset_type,
            symbol=isin or folio_number,
            purchase_date=txn_date,
            units=units,
            total_cost=amount,
            reference=folio_number,
        )

    return ledger_result


def record_mf_redemption_with_cost_basis(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    folio_number: str,
    scheme_name: str,
    isin: str,
    txn_date: date,
    proceeds: Decimal,
    units: Decimal,
    is_equity: bool,
    source_file: str,
    row_idx: int,
    stt: Decimal = Decimal("0"),
    source: TransactionSource = TransactionSource.PARSER_CAMS,
) -> Tuple[LedgerRecordResult, Optional[Decimal]]:
    """
    Record MF redemption with automatic cost basis calculation.

    Uses FIFO to determine cost basis and holding period.
    Automatically calculates realized capital gain/loss.

    Args:
        txn_service: TransactionService instance
        conn: Database connection
        user_id: User ID
        folio_number: Folio number
        scheme_name: Scheme name
        isin: ISIN code
        txn_date: Transaction date
        proceeds: Sale proceeds
        units: Units sold
        is_equity: True for equity, False for debt
        source_file: Source file path
        row_idx: Row index
        stt: STT paid
        source: Transaction source

    Returns:
        Tuple of (LedgerRecordResult, realized_gain)
    """
    from pfas.services.cost_basis_tracker import CostBasisTracker, CostMethod

    cost_tracker = CostBasisTracker(conn, cost_method=CostMethod.FIFO)
    asset_type = "MF_EQUITY" if is_equity else "MF_DEBT"

    # Calculate cost basis
    cost_result = cost_tracker.calculate_cost_basis(
        user_id=user_id,
        asset_type=asset_type,
        symbol=isin or folio_number,
        units_to_sell=units,
        sell_date=txn_date,
        sale_proceeds=proceeds - stt,
    )

    # Record to ledger with calculated cost basis
    ledger_result = record_mf_redemption(
        txn_service=txn_service,
        conn=conn,
        user_id=user_id,
        folio_number=folio_number,
        scheme_name=scheme_name,
        txn_date=txn_date,
        proceeds=proceeds,
        cost_basis=cost_result.total_cost_basis,
        units=units,
        is_equity=is_equity,
        is_long_term=cost_result.is_long_term,
        source_file=source_file,
        row_idx=row_idx,
        stt=stt,
        source=source,
    )

    # If successful, deplete the lots
    if ledger_result.success and not ledger_result.is_duplicate:
        cost_tracker.deplete_lots(
            user_id=user_id,
            asset_type=asset_type,
            symbol=isin or folio_number,
            cost_result=cost_result,
        )

    return ledger_result, cost_result.realized_gain


def record_stock_buy_with_cost_basis(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    symbol: str,
    txn_date: date,
    quantity: int,
    price: Decimal,
    amount: Decimal,
    brokerage: Decimal,
    source_file: str,
    row_idx: int,
    broker: str = "Generic",
    trade_id: str = "",
    source: TransactionSource = TransactionSource.PARSER_ZERODHA,
) -> LedgerRecordResult:
    """
    Record stock purchase with cost basis tracking.

    Args:
        All args same as record_stock_buy

    Returns:
        LedgerRecordResult
    """
    from pfas.services.cost_basis_tracker import CostBasisTracker

    # Record to ledger
    ledger_result = record_stock_buy(
        txn_service=txn_service,
        conn=conn,
        user_id=user_id,
        symbol=symbol,
        txn_date=txn_date,
        quantity=quantity,
        price=price,
        amount=amount,
        brokerage=brokerage,
        source_file=source_file,
        row_idx=row_idx,
        broker=broker,
        trade_id=trade_id,
        source=source,
    )

    # Create cost basis lot if successful
    if ledger_result.success and not ledger_result.is_duplicate:
        cost_tracker = CostBasisTracker(conn)
        total_cost = amount + brokerage

        cost_tracker.record_purchase(
            user_id=user_id,
            asset_type="STOCK",
            symbol=symbol,
            purchase_date=txn_date,
            units=Decimal(quantity),
            total_cost=total_cost,
            reference=trade_id or f"{broker}:{txn_date.isoformat()}",
        )

    return ledger_result


def record_stock_sell_with_cost_basis(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    symbol: str,
    txn_date: date,
    quantity: int,
    price: Decimal,
    proceeds: Decimal,
    brokerage: Decimal,
    stt: Decimal,
    source_file: str,
    row_idx: int,
    broker: str = "Generic",
    trade_id: str = "",
    source: TransactionSource = TransactionSource.PARSER_ZERODHA,
) -> Tuple[LedgerRecordResult, Optional[Decimal]]:
    """
    Record stock sale with automatic cost basis calculation (FIFO).

    Args:
        All args same as record_stock_sell, without cost_basis and is_long_term
        (these are calculated automatically)

    Returns:
        Tuple of (LedgerRecordResult, realized_gain)
    """
    from pfas.services.cost_basis_tracker import CostBasisTracker, CostMethod

    cost_tracker = CostBasisTracker(conn, cost_method=CostMethod.FIFO)
    net_proceeds = proceeds - brokerage - stt

    # Calculate cost basis
    cost_result = cost_tracker.calculate_cost_basis(
        user_id=user_id,
        asset_type="STOCK",
        symbol=symbol,
        units_to_sell=Decimal(quantity),
        sell_date=txn_date,
        sale_proceeds=net_proceeds,
    )

    # Record to ledger
    ledger_result = record_stock_sell(
        txn_service=txn_service,
        conn=conn,
        user_id=user_id,
        symbol=symbol,
        txn_date=txn_date,
        quantity=quantity,
        price=price,
        proceeds=proceeds,
        cost_basis=cost_result.total_cost_basis,
        brokerage=brokerage,
        stt=stt,
        is_long_term=cost_result.is_long_term,
        source_file=source_file,
        row_idx=row_idx,
        broker=broker,
        trade_id=trade_id,
        source=source,
    )

    # Deplete lots if successful
    if ledger_result.success and not ledger_result.is_duplicate:
        cost_tracker.deplete_lots(
            user_id=user_id,
            asset_type="STOCK",
            symbol=symbol,
            cost_result=cost_result,
        )

    return ledger_result, cost_result.realized_gain


# =============================================================================
# USA RSU & Foreign Asset Mapping
# =============================================================================

def get_sbi_tt_rate(
    conn: sqlite3.Connection,
    rate_date: date,
    from_currency: str = "USD"
) -> Decimal:
    """
    Get SBI TT Buying Rate for currency conversion.

    Args:
        conn: Database connection
        rate_date: Date for rate lookup
        from_currency: Source currency (default USD)

    Returns:
        Exchange rate

    Raises:
        ForexRateNotFoundError: If rate not available
    """
    from pfas.services.currency import SBITTRateProvider

    try:
        rate_provider = SBITTRateProvider(conn)
        return rate_provider.get_rate(rate_date, from_currency)
    except (ValueError, Exception) as e:
        raise ForexRateNotFoundError(
            rate_date=rate_date.isoformat(),
            from_currency=from_currency,
        )


def record_rsu_vest(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    grant_number: str,
    symbol: str,
    vest_date: date,
    shares_vested: Decimal,
    fmv_usd: Decimal,
    shares_withheld_for_tax: Decimal,
    source_file: str,
    row_idx: int,
    tt_rate: Optional[Decimal] = None,
    source: TransactionSource = TransactionSource.PARSER_MORGAN_STANLEY,
) -> LedgerRecordResult:
    """
    Record RSU vesting transaction.

    Converts USD FMV to INR using SBI TT Buying Rate.

    Dr US Stock Asset (1401) | Perquisite value in INR
    Cr Foreign Salary Income (4108) | Perquisite value in INR

    Args:
        txn_service: TransactionService instance
        conn: Database connection
        user_id: User ID
        grant_number: Grant number
        symbol: Stock symbol
        vest_date: Vest date
        shares_vested: Total shares vested
        fmv_usd: Fair Market Value per share in USD
        shares_withheld_for_tax: Shares withheld for tax
        source_file: Source file path
        row_idx: Row index
        tt_rate: Optional TT rate (if not provided, will lookup)
        source: Transaction source

    Returns:
        LedgerRecordResult
    """
    from pfas.services.cost_basis_tracker import CostBasisTracker

    # Get TT rate if not provided
    if tt_rate is None:
        tt_rate = get_sbi_tt_rate(conn, vest_date)

    # Calculate perquisite in INR (total vested shares × FMV × TT Rate)
    perquisite_usd = shares_vested * fmv_usd
    perquisite_inr = (perquisite_usd * tt_rate).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    # Net shares after tax withholding
    net_shares = shares_vested - shares_withheld_for_tax

    # Cost basis per share in INR = FMV × TT Rate
    cost_basis_inr = (fmv_usd * tt_rate).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    # Get account IDs
    us_stock_account_id = _get_or_create_account_id(conn, AccountCode.US_STOCKS_RSU.value)
    foreign_salary_income_id = _get_or_create_account_id(conn, AccountCode.FOREIGN_SALARY_INCOME.value)

    entries = [
        JournalEntry(
            account_id=us_stock_account_id,
            debit=perquisite_inr,
            narration=f"RSU Vest: {symbol} x {shares_vested} @ ${fmv_usd} (TT: {tt_rate})"
        ),
        JournalEntry(
            account_id=foreign_salary_income_id,
            credit=perquisite_inr,
            narration=f"RSU Perquisite from {symbol} vest ({grant_number})"
        ),
    ]

    # Generate idempotency key
    file_hash = generate_file_hash(source_file)
    idempotency_key = f"rsu:vest:{file_hash}:{row_idx}:{grant_number}:{vest_date.isoformat()}:{shares_vested}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"RSU Vest: {symbol} x {shares_vested}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=vest_date,
        reference_type="RSU_VEST",
    )

    # Create cost basis lot for net shares (after tax withholding)
    if result.result == TransactionResult.SUCCESS:
        cost_tracker = CostBasisTracker(conn)
        cost_tracker.record_purchase(
            user_id=user_id,
            asset_type="RSU",
            symbol=symbol,
            purchase_date=vest_date,
            units=net_shares,
            total_cost=net_shares * cost_basis_inr,
            reference=grant_number,
            currency="INR",  # Store cost in INR
        )

    return _convert_result(result)


def record_rsu_sale(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    symbol: str,
    sell_date: date,
    shares_sold: Decimal,
    sell_price_usd: Decimal,
    fees_usd: Decimal,
    source_file: str,
    row_idx: int,
    tt_rate_at_sale: Optional[Decimal] = None,
    source: TransactionSource = TransactionSource.PARSER_MORGAN_STANLEY,
) -> Tuple[LedgerRecordResult, Optional[Decimal], Optional[Decimal]]:
    """
    Record RSU sale with forex gain/loss calculation.

    Handles:
    - FIFO cost basis retrieval
    - Capital gain/loss in INR
    - Forex gain/loss from exchange rate fluctuation

    The forex gain/loss arises from the difference between:
    - Cost basis at vest TT rate
    - Sale proceeds at current TT rate

    Args:
        txn_service: TransactionService instance
        conn: Database connection
        user_id: User ID
        symbol: Stock symbol
        sell_date: Sale date
        shares_sold: Shares sold
        sell_price_usd: Sale price per share in USD
        fees_usd: Fees in USD
        source_file: Source file path
        row_idx: Row index
        tt_rate_at_sale: Optional TT rate at sale
        source: Transaction source

    Returns:
        Tuple of (LedgerRecordResult, capital_gain_inr, forex_gain_inr)
    """
    from pfas.services.cost_basis_tracker import CostBasisTracker, CostMethod

    # Get TT rate at sale if not provided
    if tt_rate_at_sale is None:
        tt_rate_at_sale = get_sbi_tt_rate(conn, sell_date)

    # Calculate proceeds in USD and INR
    gross_proceeds_usd = shares_sold * sell_price_usd
    net_proceeds_usd = gross_proceeds_usd - fees_usd
    net_proceeds_inr = (net_proceeds_usd * tt_rate_at_sale).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    # Get cost basis using FIFO
    cost_tracker = CostBasisTracker(conn, cost_method=CostMethod.FIFO)
    cost_result = cost_tracker.calculate_cost_basis(
        user_id=user_id,
        asset_type="RSU",
        symbol=symbol,
        units_to_sell=shares_sold,
        sell_date=sell_date,
        sale_proceeds=net_proceeds_inr,
    )

    # Cost basis was stored in INR at vest TT rate
    cost_basis_inr = cost_result.total_cost_basis

    # Capital gain in INR
    capital_gain_inr = net_proceeds_inr - cost_basis_inr

    # For detailed forex tracking, we'd need to compare USD-based cost with INR-based cost
    # Simplified: Forex gain = difference between sale conversion and cost conversion
    # This is embedded in the capital gain when cost basis is stored in INR

    # Determine accounts
    bank_account_id = _get_or_create_account_id(conn, AccountCode.BANK_SAVINGS.value)
    us_stock_account_id = _get_or_create_account_id(conn, AccountCode.US_STOCKS_RSU.value)

    # Use appropriate capital gains account
    if cost_result.is_long_term:
        cg_account_id = _get_or_create_account_id(conn, AccountCode.LTCG_FOREIGN.value)
        cg_type = "LTCG"
    else:
        cg_account_id = _get_or_create_account_id(conn, AccountCode.STCG_FOREIGN.value)
        cg_type = "STCG"

    entries = []

    # Debit: Bank receives INR proceeds
    entries.append(JournalEntry(
        account_id=bank_account_id,
        debit=net_proceeds_inr,
        narration=f"RSU Sale: {symbol} x {shares_sold} @ ${sell_price_usd} (TT: {tt_rate_at_sale})"
    ))

    # Credit: RSU Asset at cost basis
    entries.append(JournalEntry(
        account_id=us_stock_account_id,
        credit=cost_basis_inr,
        narration=f"RSU Sale: {symbol} x {shares_sold} @ cost"
    ))

    # Handle capital gain/loss
    if capital_gain_inr > 0:
        entries.append(JournalEntry(
            account_id=cg_account_id,
            credit=capital_gain_inr,
            narration=f"{cg_type} on RSU {symbol}: {cost_result.holding_period_days} days"
        ))
    elif capital_gain_inr < 0:
        entries.append(JournalEntry(
            account_id=cg_account_id,
            debit=abs(capital_gain_inr),
            narration=f"Capital loss on RSU {symbol}"
        ))

    # Generate idempotency key
    file_hash = generate_file_hash(source_file)
    idempotency_key = f"rsu:sale:{file_hash}:{row_idx}:{symbol}:{sell_date.isoformat()}:{shares_sold}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"RSU Sale: {symbol} x {shares_sold}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=sell_date,
        reference_type="RSU_SALE",
    )

    # Deplete lots if successful
    if result.result == TransactionResult.SUCCESS:
        cost_tracker.deplete_lots(
            user_id=user_id,
            asset_type="RSU",
            symbol=symbol,
            cost_result=cost_result,
        )

    return _convert_result(result), capital_gain_inr, None  # forex_gain tracked via cost basis


def record_espp_purchase(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    symbol: str,
    purchase_date: date,
    shares_purchased: Decimal,
    purchase_price_usd: Decimal,
    market_price_usd: Decimal,
    source_file: str,
    row_idx: int,
    tt_rate: Optional[Decimal] = None,
    source: TransactionSource = TransactionSource.PARSER_MORGAN_STANLEY,
) -> LedgerRecordResult:
    """
    Record ESPP purchase transaction.

    ESPP discount is taxable as perquisite.

    Dr US Stock - ESPP (1402) | Purchase value in INR
    Cr Bank (1101) | Purchase amount paid in INR
    And separately:
    Dr US Stock - ESPP (1402) | Perquisite value (discount)
    Cr ESPP Perquisite Income (4105) | Perquisite value

    Args:
        txn_service: TransactionService instance
        conn: Database connection
        user_id: User ID
        symbol: Stock symbol
        purchase_date: Purchase date
        shares_purchased: Shares purchased
        purchase_price_usd: Discounted purchase price per share
        market_price_usd: FMV at purchase
        source_file: Source file path
        row_idx: Row index
        tt_rate: Optional TT rate
        source: Transaction source

    Returns:
        LedgerRecordResult
    """
    from pfas.services.cost_basis_tracker import CostBasisTracker

    # Get TT rate if not provided
    if tt_rate is None:
        tt_rate = get_sbi_tt_rate(conn, purchase_date)

    # Calculate values in INR
    purchase_value_inr = (shares_purchased * purchase_price_usd * tt_rate).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    market_value_inr = (shares_purchased * market_price_usd * tt_rate).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    perquisite_inr = market_value_inr - purchase_value_inr

    # Get account IDs
    espp_account_id = _get_or_create_account_id(conn, AccountCode.US_STOCKS_ESPP.value)
    bank_account_id = _get_or_create_account_id(conn, AccountCode.BANK_SAVINGS.value)
    espp_perquisite_id = _get_or_create_account_id(conn, AccountCode.ESPP_PERQUISITE.value)

    entries = [
        # Purchase at discounted price
        JournalEntry(
            account_id=espp_account_id,
            debit=market_value_inr,  # Record at FMV for cost basis
            narration=f"ESPP Purchase: {symbol} x {shares_purchased} @ FMV ${market_price_usd}"
        ),
        JournalEntry(
            account_id=bank_account_id,
            credit=purchase_value_inr,
            narration=f"Payment for ESPP: {symbol}"
        ),
        JournalEntry(
            account_id=espp_perquisite_id,
            credit=perquisite_inr,
            narration=f"ESPP Discount perquisite: {symbol}"
        ),
    ]

    # Generate idempotency key
    file_hash = generate_file_hash(source_file)
    idempotency_key = f"espp:buy:{file_hash}:{row_idx}:{symbol}:{purchase_date.isoformat()}:{shares_purchased}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"ESPP Purchase: {symbol} x {shares_purchased}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=purchase_date,
        reference_type="ESPP_PURCHASE",
    )

    # Create cost basis lot at FMV (market price)
    if result.result == TransactionResult.SUCCESS:
        cost_tracker = CostBasisTracker(conn)
        cost_tracker.record_purchase(
            user_id=user_id,
            asset_type="ESPP",
            symbol=symbol,
            purchase_date=purchase_date,
            units=shares_purchased,
            total_cost=market_value_inr,  # Cost basis = FMV
            reference=f"ESPP:{purchase_date.isoformat()}",
            currency="INR",
        )

    return _convert_result(result)


def record_foreign_dividend(
    txn_service: TransactionService,
    conn: sqlite3.Connection,
    user_id: int,
    symbol: str,
    dividend_date: date,
    gross_dividend_usd: Decimal,
    withholding_tax_usd: Decimal,
    source_file: str,
    row_idx: int,
    tt_rate: Optional[Decimal] = None,
    source: TransactionSource = TransactionSource.PARSER_MORGAN_STANLEY,
) -> LedgerRecordResult:
    """
    Record foreign dividend with US withholding tax (DTAA credit).

    Dr Bank (1101) | Net dividend in INR
    Dr Foreign Tax Credit (1604) | Withholding in INR (DTAA credit)
    Cr Dividend - Foreign (4204) | Gross dividend in INR

    Args:
        txn_service: TransactionService instance
        conn: Database connection
        user_id: User ID
        symbol: Stock symbol
        dividend_date: Dividend date
        gross_dividend_usd: Gross dividend in USD
        withholding_tax_usd: US withholding tax (typically 25%)
        source_file: Source file path
        row_idx: Row index
        tt_rate: Optional TT rate
        source: Transaction source

    Returns:
        LedgerRecordResult
    """
    # Get TT rate if not provided
    if tt_rate is None:
        tt_rate = get_sbi_tt_rate(conn, dividend_date)

    # Calculate values in INR
    gross_inr = (gross_dividend_usd * tt_rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    withholding_inr = (withholding_tax_usd * tt_rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    net_inr = gross_inr - withholding_inr

    # Get account IDs
    bank_account_id = _get_or_create_account_id(conn, AccountCode.BANK_SAVINGS.value)
    foreign_tax_credit_id = _get_or_create_account_id(conn, AccountCode.FOREIGN_TAX_CREDIT.value)
    foreign_dividend_id = _get_or_create_account_id(conn, AccountCode.DIVIDEND_FOREIGN.value)

    entries = [
        JournalEntry(
            account_id=bank_account_id,
            debit=net_inr,
            narration=f"Net dividend from {symbol} (after US withholding)"
        ),
        JournalEntry(
            account_id=foreign_tax_credit_id,
            debit=withholding_inr,
            narration=f"US withholding tax on {symbol} dividend (DTAA credit)"
        ),
        JournalEntry(
            account_id=foreign_dividend_id,
            credit=gross_inr,
            narration=f"Gross dividend from {symbol}"
        ),
    ]

    # Generate idempotency key
    file_hash = generate_file_hash(source_file)
    idempotency_key = f"div:foreign:{file_hash}:{row_idx}:{symbol}:{dividend_date.isoformat()}:{gross_dividend_usd}"

    result = txn_service.record(
        user_id=user_id,
        entries=entries,
        description=f"Foreign Dividend: {symbol}",
        source=source,
        idempotency_key=idempotency_key,
        txn_date=dividend_date,
        reference_type="FOREIGN_DIVIDEND",
    )

    return _convert_result(result)
