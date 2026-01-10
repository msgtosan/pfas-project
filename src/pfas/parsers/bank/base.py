"""
Base class for bank statement parsers.

Provides common functionality for parsing different bank statement formats.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, List
import sqlite3

try:
    import keyring
    HAS_KEYRING = True
except ImportError:
    HAS_KEYRING = False

import pdfplumber
import pandas as pd

from pfas.core.encryption import encrypt_field
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
        Save parsed transactions to database with transaction atomicity.

        Args:
            result: ParseResult from parsing
            user_id: User ID (optional, for multi-user support)
            coa_account_id: Chart of accounts account ID for this bank account

        Returns:
            Number of transactions saved

        Raises:
            Exception: If transaction save fails (with automatic rollback)
        """
        if not result.success or not result.account:
            return 0

        # Use transaction for atomicity
        cursor = self.conn.cursor()
        in_transaction = self.conn.in_transaction

        try:
            if not in_transaction:
                cursor.execute("BEGIN IMMEDIATE")

            # Create or get bank account
            bank_account_id = self._get_or_create_bank_account(
                result.account,
                user_id,
                coa_account_id
            )

            # Insert transactions (with duplicate prevention)
            count = 0
            for txn in result.transactions:
                if self._insert_transaction(bank_account_id, txn, result.source_file, user_id):
                    count += 1

            if not in_transaction:
                self.conn.commit()
            return count

        except Exception as e:
            if not in_transaction:
                self.conn.rollback()
            raise Exception(f"Failed to save transactions: {e}") from e

    def _get_or_create_bank_account(
        self,
        account: BankAccount,
        user_id: Optional[int],
        coa_account_id: Optional[int]
    ) -> int:
        """Get existing bank account or create new one."""
        # Encrypt account number
        encrypted, salt = encrypt_field(account.account_number, self.master_key)

        # Check if account exists
        cursor = self.conn.execute(
            "SELECT id FROM bank_accounts WHERE account_number_last4 = ? AND bank_name = ?",
            (account.last4, account.bank_name)
        )
        row = cursor.fetchone()

        if row:
            return row["id"]

        # Create new account
        cursor = self.conn.execute(
            """
            INSERT INTO bank_accounts
            (account_number_encrypted, account_number_salt, account_number_last4,
             bank_name, branch, ifsc_code, account_type, opening_date, user_id, account_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                encrypted,
                salt,
                account.last4,
                account.bank_name,
                account.branch,
                account.ifsc_code,
                account.account_type,
                account.opening_date.isoformat() if account.opening_date else None,
                user_id,
                coa_account_id
            )
        )

        return cursor.lastrowid

    def _insert_transaction(
        self,
        bank_account_id: int,
        txn: BankTransaction,
        source_file: str,
        user_id: Optional[int] = None
    ) -> bool:
        """
        Insert transaction with duplicate prevention.

        Args:
            bank_account_id: Bank account ID
            txn: Bank transaction
            source_file: Source file path
            user_id: User ID (optional, for multi-user support)

        Returns:
            True if inserted, False if duplicate.
        """
        try:
            self.conn.execute(
                """
                INSERT INTO bank_transactions
                (bank_account_id, date, value_date, description, reference_number,
                 debit, credit, balance, category, is_interest, source_file, user_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bank_account_id,
                    txn.date.isoformat(),
                    txn.value_date.isoformat() if txn.value_date else None,
                    txn.description,
                    txn.reference_number,
                    str(txn.debit),
                    str(txn.credit),
                    str(txn.balance) if txn.balance else None,
                    txn.category.value,
                    txn.is_interest,
                    source_file,
                    user_id
                )
            )
            return True
        except sqlite3.IntegrityError:
            # Duplicate transaction
            return False

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
