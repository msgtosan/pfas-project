"""PPF Statement parser."""

import logging
import pandas as pd
from pathlib import Path
from datetime import date as date_type, timedelta
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass, field
from typing import List, Optional
import sqlite3

# Ledger integration imports
from pfas.core.transaction_service import TransactionService, TransactionSource
from pfas.parsers.ledger_integration import (
    record_ppf_deposit,
    record_ppf_interest,
    record_ppf_withdrawal,
)

logger = logging.getLogger(__name__)


@dataclass
class PPFAccount:
    """PPF account information."""
    account_number: str
    bank_name: str
    branch: str = ""
    opening_date: Optional[date_type] = None
    maturity_date: Optional[date_type] = None

    def calculate_maturity_date(self):
        """Calculate maturity date (15 years from opening)."""
        if self.opening_date:
            # PPF matures after 15 years
            self.maturity_date = date_type(
                self.opening_date.year + 15,
                self.opening_date.month,
                self.opening_date.day
            )


@dataclass
class PPFTransaction:
    """PPF transaction (deposit, interest, or withdrawal)."""
    date: date_type
    transaction_type: str  # DEPOSIT, INTEREST, WITHDRAWAL
    amount: Decimal
    balance: Decimal = Decimal("0")
    interest_rate: Optional[Decimal] = None
    financial_year: str = ""


@dataclass
class ParseResult:
    """Result of parsing PPF statement."""
    success: bool
    account: Optional[PPFAccount] = None
    transactions: List[PPFTransaction] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    source_file: str = ""

    def add_error(self, error: str):
        """Add an error message."""
        self.errors.append(error)
        self.success = False

    def add_warning(self, warning: str):
        """Add a warning message."""
        self.warnings.append(warning)


class PPFParser:
    """
    Parser for PPF bank statements.

    Parses Excel/CSV statements from banks for PPF accounts.
    Tracks deposits, interest credits, and withdrawals.

    Tax benefits:
    - Deposits: 80C eligible (max ₹1.5L per FY)
    - Interest: Tax-free
    - Maturity: 15 years, can be extended in 5-year blocks
    """

    # Current PPF interest rate (subject to change quarterly)
    PPF_INTEREST_RATE = Decimal("7.1")

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize PPF parser.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection

    def parse(self, file_path: Path, account_number: str = "",
              bank_name: str = "", branch: str = "") -> ParseResult:
        """
        Parse PPF statement Excel/CSV.

        Args:
            file_path: Path to PPF statement file
            account_number: PPF account number
            bank_name: Bank name
            branch: Branch name

        Returns:
            ParseResult with account and transactions

        Examples:
            >>> parser = PPFParser(conn)
            >>> result = parser.parse(Path("ppf.xlsx"), account_number="12345")
            >>> print(f"Transactions: {len(result.transactions)}")
        """
        file_path = Path(file_path)
        result = ParseResult(success=True, source_file=str(file_path))

        if not file_path.exists():
            result.add_error(f"File not found: {file_path}")
            return result

        try:
            # Read Excel/CSV
            if file_path.suffix.lower() in ['.xlsx', '.xls']:
                try:
                    df = pd.read_excel(file_path)
                except ValueError as ve:
                    if "Worksheet index" in str(ve) or "0 worksheets found" in str(ve):
                        result.add_error(f"Excel file has no worksheets (corrupted or empty): {file_path.name}")
                        return result
                    raise
            elif file_path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path)
            else:
                result.add_error(f"Unsupported format: {file_path.suffix}")
                return result

            # Validate we have data
            if len(df) == 0:
                result.add_error("File is empty (no data rows)")
                return result

            # Validate required columns exist
            df.columns = df.columns.str.strip().str.upper()
            date_cols = {'DATE', 'TRANSACTION DATE', 'TXN DATE', 'VALUE DATE'}
            if not any(col in df.columns for col in date_cols):
                result.add_error(f"Date column not found. Found columns: {list(df.columns)}")
                return result

            # Parse transactions
            transactions = self._parse_transactions(df, result)
            result.transactions = transactions

            # Create account info
            opening_date = transactions[0].date if transactions else None
            result.account = PPFAccount(
                account_number=account_number,
                bank_name=bank_name,
                branch=branch,
                opening_date=opening_date
            )
            if opening_date:
                result.account.calculate_maturity_date()

            if len(transactions) == 0:
                result.add_warning("No transactions found")

        except Exception as e:
            result.add_error(f"Failed to parse file: {str(e)}")

        return result

    def _parse_transactions(self, df: pd.DataFrame, result: ParseResult) -> List[PPFTransaction]:
        """
        Parse transactions from DataFrame.

        Expected columns (flexible):
        - Date / Transaction Date
        - Description / Narration / Type
        - Deposit / Credit / Debit / Amount
        - Balance / Closing Balance

        Args:
            df: DataFrame from Excel/CSV (columns already normalized to uppercase)
            result: ParseResult to add warnings to

        Returns:
            List of PPFTransaction objects
        """
        transactions = []

        # Find date column (columns already normalized to uppercase)
        date_col = None
        for col in ['DATE', 'TRANSACTION DATE', 'TXN DATE', 'VALUE DATE']:
            if col in df.columns:
                date_col = col
                break

        if not date_col:
            # This shouldn't happen since we validated in parse(), but handle it anyway
            result.add_error("Date column not found")
            return transactions

        for idx, row in df.iterrows():
            try:
                # Parse date
                txn_date = pd.to_datetime(row[date_col]).date()

                # Determine transaction type and amount
                txn_type, amount = self._determine_type_and_amount(row)

                if not txn_type or amount == Decimal("0"):
                    continue

                # Get balance
                balance = self._extract_balance(row)

                # Determine financial year
                fy = self._get_financial_year(txn_date)

                # Create transaction
                txn = PPFTransaction(
                    date=txn_date,
                    transaction_type=txn_type,
                    amount=amount,
                    balance=balance,
                    interest_rate=self.PPF_INTEREST_RATE if txn_type == 'INTEREST' else None,
                    financial_year=fy
                )
                transactions.append(txn)

            except Exception as e:
                result.add_warning(f"Row {idx}: {str(e)}")
                continue

        return transactions

    def _determine_type_and_amount(self, row: pd.Series) -> tuple[str, Decimal]:
        """
        Determine transaction type and amount from row.

        Args:
            row: DataFrame row

        Returns:
            (transaction_type, amount)
        """
        # Check description/narration
        desc = ""
        for col in ['DESCRIPTION', 'NARRATION', 'PARTICULARS', 'TYPE']:
            if col in row.index and not pd.isna(row[col]):
                desc = str(row[col]).upper()
                break

        # Check amount columns
        amount = Decimal("0")

        # Look for credit/deposit
        for col in ['DEPOSIT', 'CREDIT', 'CR', 'AMOUNT']:
            if col in row.index and not pd.isna(row[col]):
                val = self._to_decimal(row[col])
                if val > 0:
                    amount = val
                    break

        # Look for debit/withdrawal
        if amount == Decimal("0"):
            for col in ['WITHDRAWAL', 'DEBIT', 'DR']:
                if col in row.index and not pd.isna(row[col]):
                    val = self._to_decimal(row[col])
                    if val > 0:
                        return ('WITHDRAWAL', val)

        # Determine type from description
        if amount > 0:
            if any(keyword in desc for keyword in ['INTEREST', 'INT', 'CREDIT']):
                return ('INTEREST', amount)
            else:
                return ('DEPOSIT', amount)

        return ('', Decimal("0"))

    def _extract_balance(self, row: pd.Series) -> Decimal:
        """Extract balance from row."""
        for col in ['BALANCE', 'CLOSING BALANCE', 'BAL', 'CL BAL']:
            if col in row.index and not pd.isna(row[col]):
                return self._to_decimal(row[col])
        return Decimal("0")

    def _get_financial_year(self, txn_date: date_type) -> str:
        """
        Get financial year for a transaction date.

        Args:
            txn_date: Transaction date

        Returns:
            Financial year string (e.g., "2024-25")
        """
        if txn_date.month >= 4:  # Apr-Mar
            return f"{txn_date.year}-{str(txn_date.year + 1)[-2:]}"
        else:
            return f"{txn_date.year - 1}-{str(txn_date.year)[-2:]}"

    def _to_decimal(self, value) -> Decimal:
        """Convert value to Decimal safely."""
        if pd.isna(value) or value == '' or value is None:
            return Decimal("0")

        try:
            # Remove commas and convert
            clean_value = str(value).replace(",", "").strip()
            return Decimal(clean_value)
        except (ValueError, InvalidOperation) as e:
            logger.debug(f"Could not convert '{value}' to Decimal: {e}")
            return Decimal("0")

    def calculate_80c_eligible(self, transactions: List[PPFTransaction], fy: str) -> Decimal:
        """
        Calculate 80C eligible deposits for a financial year.

        Args:
            transactions: List of PPF transactions
            fy: Financial year (e.g., "2024-25")

        Returns:
            Total 80C eligible amount (capped at ₹1.5L)

        Examples:
            >>> eligible = parser.calculate_80c_eligible(result.transactions, "2024-25")
            >>> print(f"80C deduction: ₹{eligible:,.2f}")
        """
        # Sum all deposits in the FY
        deposits = sum(
            txn.amount for txn in transactions
            if txn.transaction_type == 'DEPOSIT' and txn.financial_year == fy
        )

        # Cap at ₹1.5L (80C limit)
        return min(deposits, Decimal("150000"))

    def save_to_db(self, result: ParseResult, user_id: Optional[int] = None) -> int:
        """
        Save parsed PPF data to database with double-entry ledger.

        Args:
            result: ParseResult from parsing
            user_id: User ID (optional)

        Returns:
            Number of transactions saved

        Examples:
            >>> result = parser.parse(Path("ppf.xlsx"), account_number="12345")
            >>> count = parser.save_to_db(result, user_id=1)
            >>> print(f"Saved {count} transactions")
        """
        if not result.success or not result.account:
            return 0

        cursor = self.conn.cursor()
        in_transaction = self.conn.in_transaction

        # Initialize TransactionService for ledger entries
        txn_service = TransactionService(self.conn)

        try:
            if not in_transaction:
                cursor.execute("BEGIN IMMEDIATE")

            # Get or create PPF account
            ppf_account_id = self._get_or_create_account(result.account, user_id)

            # Insert transactions with ledger entries
            count = 0
            for row_idx, txn in enumerate(result.transactions):
                # Record to double-entry ledger based on transaction type
                ledger_result = self._record_to_ledger(
                    txn_service, user_id, result.account, txn, result.source_file, row_idx
                )

                # Skip if duplicate
                if ledger_result.is_duplicate:
                    logger.debug(f"Duplicate PPF transaction skipped: {txn.date}")
                    continue

                # Insert to ppf_transactions for backward compatibility
                if self._insert_transaction(ppf_account_id, txn, result.source_file, user_id):
                    count += 1

            if not in_transaction:
                self.conn.commit()

            return count

        except Exception as e:
            if not in_transaction:
                self.conn.rollback()
            raise Exception(f"Failed to save PPF data: {e}") from e

    def _record_to_ledger(
        self,
        txn_service: TransactionService,
        user_id: int,
        account: PPFAccount,
        txn: PPFTransaction,
        source_file: str,
        row_idx: int
    ):
        """
        Record PPF transaction to double-entry ledger.

        Args:
            txn_service: TransactionService instance
            user_id: User ID
            account: PPFAccount
            txn: PPFTransaction to record
            source_file: Source file path
            row_idx: Row index

        Returns:
            LedgerRecordResult
        """
        if txn.transaction_type == 'DEPOSIT':
            return record_ppf_deposit(
                txn_service=txn_service,
                conn=self.conn,
                user_id=user_id,
                account_number=account.account_number,
                txn_date=txn.date,
                amount=txn.amount,
                source_file=source_file,
                row_idx=row_idx,
                source=TransactionSource.PARSER_PPF,
            )
        elif txn.transaction_type == 'INTEREST':
            return record_ppf_interest(
                txn_service=txn_service,
                conn=self.conn,
                user_id=user_id,
                account_number=account.account_number,
                txn_date=txn.date,
                amount=txn.amount,
                financial_year=txn.financial_year,
                source_file=source_file,
                row_idx=row_idx,
                source=TransactionSource.PARSER_PPF,
            )
        elif txn.transaction_type == 'WITHDRAWAL':
            return record_ppf_withdrawal(
                txn_service=txn_service,
                conn=self.conn,
                user_id=user_id,
                account_number=account.account_number,
                txn_date=txn.date,
                amount=txn.amount,
                source_file=source_file,
                row_idx=row_idx,
                source=TransactionSource.PARSER_PPF,
            )
        else:
            # Unknown transaction type
            from pfas.parsers.ledger_integration import LedgerRecordResult
            logger.warning(f"Unknown PPF transaction type: {txn.transaction_type}")
            return LedgerRecordResult(success=True, is_duplicate=False)

    def _get_or_create_account(self, account: PPFAccount, user_id: Optional[int]) -> int:
        """Get or create PPF account and return ID."""
        cursor = self.conn.execute(
            "SELECT id FROM ppf_accounts WHERE account_number = ?",
            (account.account_number,)
        )
        row = cursor.fetchone()

        if row:
            return row['id']

        cursor = self.conn.execute(
            """INSERT INTO ppf_accounts
            (user_id, account_number, bank_name, branch, opening_date, maturity_date)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (
                user_id,
                account.account_number,
                account.bank_name,
                account.branch,
                account.opening_date.isoformat() if account.opening_date else None,
                account.maturity_date.isoformat() if account.maturity_date else None
            )
        )
        return cursor.lastrowid

    def _insert_transaction(self, ppf_account_id: int, txn: PPFTransaction,
                           source_file: str, user_id: Optional[int]) -> bool:
        """Insert PPF transaction into database."""
        try:
            self.conn.execute(
                """INSERT INTO ppf_transactions
                (ppf_account_id, transaction_date, transaction_type, amount,
                 balance, interest_rate, financial_year, source_file, user_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ppf_account_id,
                    txn.date.isoformat(),
                    txn.transaction_type,
                    str(txn.amount),
                    str(txn.balance),
                    str(txn.interest_rate) if txn.interest_rate else None,
                    txn.financial_year,
                    source_file,
                    user_id
                )
            )
            return True
        except sqlite3.IntegrityError:
            # Duplicate transaction
            return False
