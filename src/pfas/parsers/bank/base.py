"""
Base class for bank statement parsers.

Provides common functionality for parsing different bank statement formats.
Uses TransactionService for all database operations.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, List
from decimal import Decimal
import sqlite3
import hashlib

try:
    import keyring
    HAS_KEYRING = True
except ImportError:
    HAS_KEYRING = False

import pdfplumber
import pandas as pd

from pfas.core.encryption import encrypt_field
from pfas.core.transaction_service import (
    TransactionService,
    TransactionSource,
    AssetRecord,
    IdempotencyKeyGenerator,
)
from pfas.core.journal import JournalEntry
from pfas.core.accounts import get_account_by_code
from pfas.parsers.bank.models import ParseResult, BankTransaction, BankAccount


class BankStatementParser(ABC):
    """Abstract base class for bank statement parsers."""

    BANK_NAME: str = ""  # Override in subclass

    def __init__(self, db_connection: sqlite3.Connection, master_key: bytes = None):
        """
        Initialize parser.

        Args:
            db_connection: Database connection
            master_key: Master encryption key for account numbers
        """
        self.conn = db_connection
        self.master_key = master_key or b"default_key_32_bytes_long_here"

    def parse(self, file_path: Path, password: Optional[str] = None) -> ParseResult:
        """
        Parse bank statement file.

        Args:
            file_path: Path to statement file (PDF or Excel)
            password: Optional password for encrypted PDFs

        Returns:
            ParseResult with transactions and account info
        """
        file_path = Path(file_path)

        if not file_path.exists():
            return ParseResult(
                success=False,
                errors=[f"File not found: {file_path}"],
                source_file=str(file_path)
            )

        try:
            # Handle password-protected PDFs
            if file_path.suffix.lower() == '.pdf':
                password = password or self._get_stored_password(file_path.name)
                content = self._read_pdf(file_path, password)
            elif file_path.suffix.lower() in ['.xlsx', '.xls']:
                content = self._read_excel(file_path)
            else:
                return ParseResult(
                    success=False,
                    errors=[f"Unsupported format: {file_path.suffix}"],
                    source_file=str(file_path)
                )

            return self._parse_content(content, str(file_path))

        except Exception as e:
            return ParseResult(
                success=False,
                errors=[f"Error parsing file: {str(e)}"],
                source_file=str(file_path)
            )

    def _get_stored_password(self, filename: str) -> Optional[str]:
        """Retrieve password from system keyring."""
        if not HAS_KEYRING:
            return None
        try:
            return keyring.get_password("pfas_bank", filename)
        except Exception:
            return None

    def _store_password(self, filename: str, password: str) -> None:
        """Store password in system keyring."""
        if HAS_KEYRING:
            try:
                keyring.set_password("pfas_bank", filename, password)
            except Exception:
                pass

    def _read_pdf(self, file_path: Path, password: Optional[str] = None) -> dict:
        """
        Read PDF content, handling encryption.

        Returns dict with 'text' and 'tables' keys.
        """
        text_content = []
        tables = []

        with pdfplumber.open(file_path, password=password) as pdf:
            for page in pdf.pages:
                # Extract text
                page_text = page.extract_text() or ""
                text_content.append(page_text)

                # Extract tables
                page_tables = page.extract_tables()
                if page_tables:
                    tables.extend(page_tables)

        return {
            "text": "\n".join(text_content),
            "tables": tables
        }

    def _read_excel(self, file_path: Path) -> dict:
        """
        Read Excel file content.

        Returns dict with 'dataframe' key containing pandas DataFrame.
        """
        df = pd.read_excel(file_path)
        return {"dataframe": df}

    @abstractmethod
    def _parse_content(self, content: dict, source_file: str) -> ParseResult:
        """
        Parse extracted content. Override in subclass.

        Args:
            content: Extracted content (text/tables for PDF, dataframe for Excel)
            source_file: Source file path

        Returns:
            ParseResult
        """
        pass

    def save_to_db(self, result: ParseResult, user_id: int = None, coa_account_id: int = None) -> int:
        """
        Save parsed transactions to database via TransactionService.

        All inserts flow through TransactionService.record() ensuring:
        - Idempotency (duplicate prevention)
        - Audit logging
        - Atomic transactions

        Args:
            result: ParseResult from parsing
            user_id: User ID (required for multi-user support)
            coa_account_id: Chart of accounts account ID for this bank account

        Returns:
            Number of transactions saved

        Raises:
            Exception: If transaction save fails
        """
        if not result.success or not result.account:
            return 0

        if user_id is None:
            user_id = 1  # Default user

        # Initialize TransactionService
        txn_service = TransactionService(self.conn)

        # Create or get bank account (reference data, uses asset_only)
        bank_account_id = self._get_or_create_bank_account(
            txn_service, result.account, user_id, coa_account_id
        )

        # Insert transactions with journal entries
        count = 0
        for idx, txn in enumerate(result.transactions):
            if self._record_transaction(
                txn_service, bank_account_id, txn, result.source_file, idx, user_id
            ):
                count += 1

        return count

    def _get_or_create_bank_account(
        self,
        txn_service: TransactionService,
        account: BankAccount,
        user_id: int,
        coa_account_id: Optional[int]
    ) -> int:
        """Get existing bank account or create new one via TransactionService."""
        # Check if account exists
        cursor = self.conn.execute(
            "SELECT id FROM bank_accounts WHERE account_number_last4 = ? AND bank_name = ?",
            (account.last4, account.bank_name)
        )
        row = cursor.fetchone()

        if row:
            return row["id"]

        # Encrypt account number
        encrypted, salt = encrypt_field(account.account_number, self.master_key)

        # Create via TransactionService
        idempotency_key = f"bank_account:{account.bank_name}:{account.last4}"

        asset_record = AssetRecord(
            table_name="bank_accounts",
            data={
                "account_number_encrypted": encrypted,
                "account_number_salt": salt,
                "account_number_last4": account.last4,
                "bank_name": account.bank_name,
                "branch": account.branch,
                "ifsc_code": account.ifsc_code,
                "account_type": account.account_type,
                "opening_date": account.opening_date.isoformat() if account.opening_date else None,
                "user_id": user_id,
                "account_id": coa_account_id,
            },
            on_conflict="IGNORE"
        )

        result = txn_service.record_asset_only(
            user_id=user_id,
            asset_records=[asset_record],
            idempotency_key=idempotency_key,
            source=self._get_transaction_source(),
            description=f"Bank account: {account.bank_name} ****{account.last4}",
        )

        if result.asset_record_ids.get("bank_accounts"):
            return result.asset_record_ids["bank_accounts"]

        # If insert was ignored (duplicate), fetch the existing ID
        cursor = self.conn.execute(
            "SELECT id FROM bank_accounts WHERE account_number_last4 = ? AND bank_name = ?",
            (account.last4, account.bank_name)
        )
        row = cursor.fetchone()
        return row["id"] if row else 0

    def _record_transaction(
        self,
        txn_service: TransactionService,
        bank_account_id: int,
        txn: BankTransaction,
        source_file: str,
        row_idx: int,
        user_id: int
    ) -> bool:
        """
        Record bank transaction via TransactionService with journal entry.

        Args:
            txn_service: TransactionService instance
            bank_account_id: Bank account ID
            txn: Bank transaction
            source_file: Source file path
            row_idx: Row index for idempotency
            user_id: User ID

        Returns:
            True if recorded, False if duplicate.
        """
        # Generate idempotency key
        idempotency_key = IdempotencyKeyGenerator.bank_transaction(
            account_number=str(bank_account_id),
            txn_date=txn.date,
            ref_no=txn.reference_number or f"row_{row_idx}",
            amount=txn.debit if txn.debit > 0 else txn.credit,
        )

        # Create journal entries
        entries = self._create_journal_entries(txn, user_id)

        # Create asset record for bank_transactions table
        asset_record = AssetRecord(
            table_name="bank_transactions",
            data={
                "bank_account_id": bank_account_id,
                "date": txn.date.isoformat(),
                "value_date": txn.value_date.isoformat() if txn.value_date else None,
                "description": txn.description,
                "reference_number": txn.reference_number,
                "debit": str(txn.debit),
                "credit": str(txn.credit),
                "balance": str(txn.balance) if txn.balance else None,
                "category": txn.category.value,
                "is_interest": txn.is_interest,
                "source_file": source_file,
                "user_id": user_id,
            },
            on_conflict="IGNORE"
        )

        # Record via TransactionService
        result = txn_service.record(
            user_id=user_id,
            entries=entries,
            description=txn.description[:100] if txn.description else "Bank transaction",
            source=self._get_transaction_source(),
            idempotency_key=idempotency_key,
            txn_date=txn.date,
            reference_type="BANK_TRANSACTION",
            asset_records=[asset_record],
        )

        return result.result.value == "success"

    def _create_journal_entries(self, txn: BankTransaction, user_id: int) -> List[JournalEntry]:
        """Create journal entries for bank transaction."""
        entries = []

        # Get bank account ID from chart of accounts
        bank_account = get_account_by_code(self.conn, "1101")  # Bank - Savings
        if not bank_account:
            return entries

        bank_account_id = bank_account.id

        # Map category to account code
        category_to_account = {
            "SALARY": "4100",
            "INTEREST": "4201",
            "DIVIDEND": "4203",
            "REFUND": "1601",
            "RENT": "4401",
            "MF_INVESTMENT": "1201",
            "STOCK_INVESTMENT": "1203",
            "FD": "1103",
            "PPF": "1303",
            "TAX": "1603",
            "EXPENSE": "5202",
        }

        category_str = txn.category.value if hasattr(txn.category, 'value') else str(txn.category)
        counter_account_code = category_to_account.get(category_str.upper(), "3200")  # Retained Earnings default
        counter_account = get_account_by_code(self.conn, counter_account_code)

        if not counter_account:
            counter_account = get_account_by_code(self.conn, "3200")  # Default

        if txn.credit > 0:
            # Credit to bank (deposit)
            entries.append(JournalEntry(
                account_id=bank_account_id,
                debit=txn.credit,
                narration=txn.description[:100] if txn.description else "Deposit"
            ))
            entries.append(JournalEntry(
                account_id=counter_account.id,
                credit=txn.credit,
                narration=f"Credit: {category_str}"
            ))
        elif txn.debit > 0:
            # Debit from bank (withdrawal)
            entries.append(JournalEntry(
                account_id=counter_account.id,
                debit=txn.debit,
                narration=f"Debit: {category_str}"
            ))
            entries.append(JournalEntry(
                account_id=bank_account_id,
                credit=txn.debit,
                narration=txn.description[:100] if txn.description else "Withdrawal"
            ))

        return entries

    def _get_transaction_source(self) -> TransactionSource:
        """Get transaction source based on bank name. Override in subclass."""
        bank_name = self.BANK_NAME.upper()
        if "ICICI" in bank_name:
            return TransactionSource.PARSER_ICICI
        elif "HDFC" in bank_name:
            return TransactionSource.PARSER_HDFC
        else:
            return TransactionSource.PARSER_ICICI  # Default

    def _table_to_text(self, table: List[List]) -> str:
        """Convert table to text format."""
        if not table:
            return ""

        lines = []
        for row in table:
            if row:
                line = " | ".join(str(cell or "") for cell in row)
                lines.append(line)

        return "\n".join(lines)
