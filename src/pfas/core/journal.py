"""
Double-entry journal engine with balance validation.

Ensures all journal entries follow accounting principles:
- Sum of Debits = Sum of Credits
- Proper audit trail for all transactions
"""

from dataclasses import dataclass, field
from decimal import Decimal
from datetime import date
from typing import List, Optional
import sqlite3

from pfas.core.exceptions import UnbalancedJournalError, AccountNotFoundError


@dataclass
class JournalEntry:
    """Represents a single line item in a journal entry."""

    account_id: int
    debit: Decimal = field(default_factory=lambda: Decimal("0"))
    credit: Decimal = field(default_factory=lambda: Decimal("0"))
    currency: str = "INR"
    exchange_rate: Decimal = field(default_factory=lambda: Decimal("1"))
    narration: str = ""

    def __post_init__(self):
        """Convert numeric types to Decimal."""
        if not isinstance(self.debit, Decimal):
            self.debit = Decimal(str(self.debit))
        if not isinstance(self.credit, Decimal):
            self.credit = Decimal(str(self.credit))
        if not isinstance(self.exchange_rate, Decimal):
            self.exchange_rate = Decimal(str(self.exchange_rate))


@dataclass
class Journal:
    """Represents a complete journal with multiple entries."""

    id: int
    date: date
    description: str
    reference_type: Optional[str]
    reference_id: Optional[int]
    created_by: Optional[int]
    is_reversed: bool
    entries: List[JournalEntry] = field(default_factory=list)


class JournalEngine:
    """
    Engine for creating and managing double-entry journal entries.

    Usage:
        engine = JournalEngine(connection)

        entries = [
            JournalEntry(account_id=1, debit=Decimal("1000")),
            JournalEntry(account_id=2, credit=Decimal("1000")),
        ]

        journal_id = engine.create_journal(
            txn_date=date.today(),
            description="Salary received",
            entries=entries
        )
    """

    # Tolerance for balance validation (handles floating point issues)
    BALANCE_TOLERANCE = Decimal("0.01")

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize the journal engine.

        Args:
            db_connection: SQLite database connection
        """
        self.conn = db_connection

    def create_journal(
        self,
        txn_date: date,
        description: str,
        entries: List[JournalEntry],
        reference_type: str = None,
        reference_id: int = None,
        created_by: int = None,
    ) -> int:
        """
        Create a balanced journal entry.

        Args:
            txn_date: Transaction date
            description: Journal description
            entries: List of JournalEntry line items
            reference_type: Optional source type (e.g., 'SALARY', 'MF_REDEMPTION')
            reference_id: Optional FK to source table
            created_by: Optional user ID who created the entry

        Returns:
            Journal ID

        Raises:
            UnbalancedJournalError: If debits don't equal credits
            AccountNotFoundError: If an account doesn't exist
        """
        # Validate balance
        if not self._validate_balance(entries):
            total_debit = sum(e.debit * e.exchange_rate for e in entries)
            total_credit = sum(e.credit * e.exchange_rate for e in entries)
            raise UnbalancedJournalError(
                f"Debit ({total_debit}) does not equal Credit ({total_credit})"
            )

        # Validate accounts exist
        self._validate_accounts(entries)

        cursor = self.conn.cursor()

        # Check if we're already in a transaction by checking in_transaction attribute
        # SQLite autocommit mode means in_transaction is False when not in a transaction
        in_transaction = self.conn.in_transaction

        # Begin explicit transaction for atomicity (only if not already in one)
        try:
            if not in_transaction:
                cursor.execute("BEGIN IMMEDIATE")

            # Insert journal header
            cursor.execute(
                """
                INSERT INTO journals (date, description, reference_type, reference_id, created_by)
                VALUES (?, ?, ?, ?, ?)
                """,
                (txn_date.isoformat(), description, reference_type, reference_id, created_by),
            )
            journal_id = cursor.lastrowid

            # Insert journal entries
            for entry in entries:
                cursor.execute(
                    """
                    INSERT INTO journal_entries
                    (journal_id, account_id, debit, credit, currency, exchange_rate, narration)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        journal_id,
                        entry.account_id,
                        str(entry.debit),
                        str(entry.credit),
                        entry.currency,
                        str(entry.exchange_rate),
                        entry.narration,
                    ),
                )

            # Commit transaction (only if we started it)
            if not in_transaction:
                self.conn.commit()
            return journal_id

        except Exception as e:
            # Rollback on any error to maintain data integrity (only if we started the transaction)
            if not in_transaction:
                self.conn.rollback()
            raise Exception(f"Failed to create journal: {e}") from e

    def _validate_balance(self, entries: List[JournalEntry]) -> bool:
        """
        Validate that sum of debits equals sum of credits.

        All amounts are converted to base currency (INR) using exchange rates.
        """
        total_debit = sum(e.debit * e.exchange_rate for e in entries)
        total_credit = sum(e.credit * e.exchange_rate for e in entries)

        return abs(total_debit - total_credit) < self.BALANCE_TOLERANCE

    def _validate_accounts(self, entries: List[JournalEntry]) -> None:
        """Validate that all accounts in entries exist."""
        for entry in entries:
            cursor = self.conn.execute(
                "SELECT id FROM accounts WHERE id = ?", (entry.account_id,)
            )
            if not cursor.fetchone():
                raise AccountNotFoundError(str(entry.account_id))

    def get_journal(self, journal_id: int) -> Optional[Journal]:
        """
        Get a journal with all its entries.

        Args:
            journal_id: Journal ID

        Returns:
            Journal object or None if not found
        """
        cursor = self.conn.execute(
            "SELECT * FROM journals WHERE id = ?", (journal_id,)
        )
        row = cursor.fetchone()

        if not row:
            return None

        # Get entries
        entries_cursor = self.conn.execute(
            "SELECT * FROM journal_entries WHERE journal_id = ?", (journal_id,)
        )
        entries = [
            JournalEntry(
                account_id=e["account_id"],
                debit=Decimal(str(e["debit"])),
                credit=Decimal(str(e["credit"])),
                currency=e["currency"],
                exchange_rate=Decimal(str(e["exchange_rate"])),
                narration=e["narration"] or "",
            )
            for e in entries_cursor.fetchall()
        ]

        return Journal(
            id=row["id"],
            date=date.fromisoformat(row["date"]),
            description=row["description"],
            reference_type=row["reference_type"],
            reference_id=row["reference_id"],
            created_by=row["created_by"],
            is_reversed=bool(row["is_reversed"]),
            entries=entries,
        )

    def reverse_journal(
        self, journal_id: int, reversal_date: date = None, description: str = None
    ) -> int:
        """
        Create a reversal entry for a journal.

        Args:
            journal_id: ID of journal to reverse
            reversal_date: Date for reversal (defaults to today)
            description: Reversal description (defaults to "Reversal of Journal #X")

        Returns:
            ID of the reversal journal
        """
        original = self.get_journal(journal_id)
        if not original:
            raise ValueError(f"Journal {journal_id} not found")

        if reversal_date is None:
            reversal_date = date.today()

        if description is None:
            description = f"Reversal of Journal #{journal_id}: {original.description}"

        # Create reversed entries (swap debit/credit)
        reversed_entries = [
            JournalEntry(
                account_id=e.account_id,
                debit=e.credit,  # Swap
                credit=e.debit,  # Swap
                currency=e.currency,
                exchange_rate=e.exchange_rate,
                narration=f"Reversal: {e.narration}",
            )
            for e in original.entries
        ]

        # Wrap the entire reversal in a transaction for atomicity
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")

            # Mark original as reversed
            cursor.execute(
                "UPDATE journals SET is_reversed = 1 WHERE id = ?", (journal_id,)
            )

            # Create reversal journal (this will not start a new transaction since we're already in one)
            reversal_journal_id = self.create_journal(
                txn_date=reversal_date,
                description=description,
                entries=reversed_entries,
                reference_type="REVERSAL",
                reference_id=journal_id,
            )

            self.conn.commit()
            return reversal_journal_id

        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to reverse journal: {e}") from e

    def get_account_balance(
        self, account_id: int, as_of_date: date = None
    ) -> Decimal:
        """
        Calculate account balance as of a given date.

        For ASSET and EXPENSE accounts: Balance = Debits - Credits
        For LIABILITY, EQUITY, and INCOME accounts: Balance = Credits - Debits

        Args:
            account_id: Account ID
            as_of_date: Calculate balance as of this date (defaults to today)

        Returns:
            Account balance in base currency (INR)
        """
        if as_of_date is None:
            as_of_date = date.today()

        # Get account type
        cursor = self.conn.execute(
            "SELECT account_type FROM accounts WHERE id = ?", (account_id,)
        )
        row = cursor.fetchone()
        if not row:
            raise AccountNotFoundError(str(account_id))

        account_type = row["account_type"]

        # Sum debits and credits (include both original and reversal entries)
        cursor = self.conn.execute(
            """
            SELECT
                COALESCE(SUM(je.debit * je.exchange_rate), 0) as total_debit,
                COALESCE(SUM(je.credit * je.exchange_rate), 0) as total_credit
            FROM journal_entries je
            JOIN journals j ON je.journal_id = j.id
            WHERE je.account_id = ? AND j.date <= ?
            """,
            (account_id, as_of_date.isoformat()),
        )
        row = cursor.fetchone()
        total_debit = Decimal(str(row["total_debit"]))
        total_credit = Decimal(str(row["total_credit"]))

        # Calculate balance based on account type
        if account_type in ("ASSET", "EXPENSE"):
            return total_debit - total_credit
        else:  # LIABILITY, EQUITY, INCOME
            return total_credit - total_debit

    def get_account_ledger(
        self,
        account_id: int,
        start_date: date = None,
        end_date: date = None,
    ) -> List[dict]:
        """
        Get ledger entries for an account within a date range.

        Args:
            account_id: Account ID
            start_date: Start of date range (optional)
            end_date: End of date range (optional)

        Returns:
            List of ledger entries with running balance
        """
        query = """
            SELECT
                j.id as journal_id,
                j.date,
                j.description,
                je.debit,
                je.credit,
                je.currency,
                je.exchange_rate,
                je.narration
            FROM journal_entries je
            JOIN journals j ON je.journal_id = j.id
            WHERE je.account_id = ? AND j.is_reversed = 0
        """
        params = [account_id]

        if start_date:
            query += " AND j.date >= ?"
            params.append(start_date.isoformat())

        if end_date:
            query += " AND j.date <= ?"
            params.append(end_date.isoformat())

        query += " ORDER BY j.date, j.id"

        cursor = self.conn.execute(query, params)
        entries = []
        running_balance = Decimal("0")

        # Get account type for balance calculation
        type_cursor = self.conn.execute(
            "SELECT account_type FROM accounts WHERE id = ?", (account_id,)
        )
        account_type = type_cursor.fetchone()["account_type"]
        is_debit_account = account_type in ("ASSET", "EXPENSE")

        for row in cursor.fetchall():
            debit = Decimal(str(row["debit"]))
            credit = Decimal(str(row["credit"]))
            rate = Decimal(str(row["exchange_rate"]))

            if is_debit_account:
                running_balance += (debit - credit) * rate
            else:
                running_balance += (credit - debit) * rate

            entries.append({
                "journal_id": row["journal_id"],
                "date": row["date"],
                "description": row["description"],
                "debit": debit,
                "credit": credit,
                "currency": row["currency"],
                "exchange_rate": rate,
                "narration": row["narration"],
                "balance": running_balance,
            })

        return entries
