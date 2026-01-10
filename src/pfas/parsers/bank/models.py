"""
Bank transaction and account data models.

Dataclasses for representing parsed bank statements.
"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional, List
from enum import Enum


class TransactionCategory(Enum):
    """Categories for auto-classifying bank transactions."""

    SALARY = "SALARY"
    INTEREST = "INTEREST"
    TRANSFER = "TRANSFER"
    UPI = "UPI"
    NEFT = "NEFT"
    RTGS = "RTGS"
    IMPS = "IMPS"
    ATM = "ATM"
    CARD = "CARD"
    CHEQUE = "CHEQUE"
    CASH_DEPOSIT = "CASH_DEPOSIT"
    CASH_WITHDRAWAL = "CASH_WITHDRAWAL"
    OTHER = "OTHER"


@dataclass
class BankTransaction:
    """Represents a single bank transaction."""

    date: date
    description: str
    debit: Decimal = field(default_factory=lambda: Decimal("0"))
    credit: Decimal = field(default_factory=lambda: Decimal("0"))
    balance: Optional[Decimal] = None
    value_date: Optional[date] = None
    reference_number: Optional[str] = None
    category: TransactionCategory = TransactionCategory.OTHER
    is_interest: bool = False

    def __post_init__(self):
        """Auto-detect interest transactions and categorize."""
        # Convert numeric types to Decimal
        if not isinstance(self.debit, Decimal):
            self.debit = Decimal(str(self.debit))
        if not isinstance(self.credit, Decimal):
            self.credit = Decimal(str(self.credit))
        if self.balance is not None and not isinstance(self.balance, Decimal):
            self.balance = Decimal(str(self.balance))

        # Auto-detect interest
        desc_upper = self.description.upper()
        interest_keywords = ['INT PD', 'INTEREST', 'INT.PD', 'INT PAID', 'INT.CREDIT', 'INT CR']

        if any(kw in desc_upper for kw in interest_keywords):
            self.is_interest = True
            self.category = TransactionCategory.INTEREST
        elif self._auto_categorize():
            pass  # Category set by _auto_categorize

    def _auto_categorize(self) -> bool:
        """Auto-categorize transaction based on description."""
        desc_upper = self.description.upper()

        # Salary detection
        salary_keywords = ['SALARY', 'SAL CREDIT', 'PAYROLL', 'QUALCOMM', 'MICROSOFT', 'GOOGLE', 'AMAZON']
        if any(kw in desc_upper for kw in salary_keywords):
            self.category = TransactionCategory.SALARY
            return True

        # Transfer types
        if 'UPI/' in desc_upper or 'UPI-' in desc_upper:
            self.category = TransactionCategory.UPI
            return True
        if 'NEFT' in desc_upper:
            self.category = TransactionCategory.NEFT
            return True
        if 'RTGS' in desc_upper:
            self.category = TransactionCategory.RTGS
            return True
        if 'IMPS' in desc_upper:
            self.category = TransactionCategory.IMPS
            return True

        # Payment methods
        if 'ATM' in desc_upper:
            self.category = TransactionCategory.ATM
            return True
        if any(kw in desc_upper for kw in ['CARD', 'DEBIT CARD', 'POS']):
            self.category = TransactionCategory.CARD
            return True
        if any(kw in desc_upper for kw in ['CHQ', 'CHEQUE', 'CHECK']):
            self.category = TransactionCategory.CHEQUE
            return True

        # Cash transactions
        if 'CASH DEP' in desc_upper or 'CDM' in desc_upper:
            self.category = TransactionCategory.CASH_DEPOSIT
            return True
        if 'CASH WD' in desc_upper or 'CWD' in desc_upper:
            self.category = TransactionCategory.CASH_WITHDRAWAL
            return True

        return False

    @property
    def amount(self) -> Decimal:
        """Get transaction amount (credit - debit)."""
        return self.credit - self.debit

    @property
    def is_debit(self) -> bool:
        """Check if transaction is a debit."""
        return self.debit > Decimal("0")

    @property
    def is_credit(self) -> bool:
        """Check if transaction is a credit."""
        return self.credit > Decimal("0")


@dataclass
class BankAccount:
    """Represents a bank account."""

    account_number: str  # Full number (will be encrypted in DB)
    bank_name: str
    account_type: str = "SAVINGS"
    branch: Optional[str] = None
    ifsc_code: Optional[str] = None
    opening_date: Optional[date] = None

    @property
    def masked_number(self) -> str:
        """Return masked account number: ****1234"""
        if len(self.account_number) < 4:
            return "*" * len(self.account_number)
        return f"****{self.account_number[-4:]}"

    @property
    def last4(self) -> str:
        """Return last 4 digits of account number."""
        return self.account_number[-4:] if len(self.account_number) >= 4 else self.account_number


@dataclass
class ParseResult:
    """Result of parsing a bank statement."""

    success: bool
    transactions: List[BankTransaction] = field(default_factory=list)
    account: Optional[BankAccount] = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    source_file: str = ""
    statement_period_start: Optional[date] = None
    statement_period_end: Optional[date] = None

    def add_error(self, error: str) -> None:
        """Add an error message."""
        self.errors.append(error)

    def add_warning(self, warning: str) -> None:
        """Add a warning message."""
        self.warnings.append(warning)

    @property
    def transaction_count(self) -> int:
        """Get number of transactions parsed."""
        return len(self.transactions)

    @property
    def total_debits(self) -> Decimal:
        """Calculate total debits."""
        return sum(t.debit for t in self.transactions)

    @property
    def total_credits(self) -> Decimal:
        """Calculate total credits."""
        return sum(t.credit for t in self.transactions)

    @property
    def interest_total(self) -> Decimal:
        """Calculate total interest credits."""
        return sum(t.credit for t in self.transactions if t.is_interest)
