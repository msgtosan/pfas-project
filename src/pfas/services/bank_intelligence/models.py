"""
Data models for Bank Intelligence Service.

Provides dataclasses for bank transactions and configuration.
"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional, List, Dict
from enum import Enum
import hashlib
import json


class TransactionType(Enum):
    """Transaction type enum."""
    CREDIT = "CREDIT"
    DEBIT = "DEBIT"


class IncomeCategory(Enum):
    """Income categories for asset class extraction."""
    RENT_INCOME = "RENT_INCOME"
    SGB_INTEREST = "SGB_INTEREST"
    DIVIDEND = "DIVIDEND"
    SAVINGS_INTEREST = "SAVINGS_INTEREST"
    SALARY = "SALARY"
    MF_REDEMPTION = "MF_REDEMPTION"
    TRANSFER = "TRANSFER"
    UPI = "UPI"
    NEFT = "NEFT"
    RTGS = "RTGS"
    IMPS = "IMPS"
    ATM = "ATM"
    CARD = "CARD"
    CASH = "CASH"
    OTHER = "OTHER"


@dataclass
class BankTransactionIntel:
    """
    Represents a bank transaction with intelligence metadata.

    Stores full transaction details including original base_string
    for audit purposes and category classification for asset extraction.
    """
    user_name: str
    bank_name: str
    txn_date: date
    base_string: str  # Original unaltered text
    amount: Decimal
    txn_type: TransactionType
    remarks: Optional[str] = None  # Cleaned/normalized remarks
    value_date: Optional[date] = None
    balance: Optional[Decimal] = None
    category: Optional[str] = None
    sub_category: Optional[str] = None
    fiscal_year: Optional[str] = None
    source_file: Optional[str] = None
    uid: Optional[str] = None

    def __post_init__(self):
        """Generate UID and fiscal year if not provided."""
        if not isinstance(self.amount, Decimal):
            self.amount = Decimal(str(self.amount))
        if self.balance is not None and not isinstance(self.balance, Decimal):
            self.balance = Decimal(str(self.balance))
        if self.uid is None:
            self.uid = self.generate_uid()
        if self.fiscal_year is None:
            self.fiscal_year = self.get_fiscal_year()
        if self.remarks is None:
            self.remarks = self.base_string

    def generate_uid(self) -> str:
        """Generate SHA256 hash for transaction uniqueness."""
        data = f"{self.user_name}|{self.bank_name}|{self.txn_date}|{self.base_string}|{self.amount}"
        return hashlib.sha256(data.encode()).hexdigest()

    def get_fiscal_year(self) -> str:
        """
        Calculate Indian Fiscal Year (April 1 - March 31).

        Examples:
            2024-03-15 -> "FY 2023-24"
            2024-04-01 -> "FY 2024-25"
            2024-12-25 -> "FY 2024-25"
        """
        if self.txn_date.month >= 4:  # April onwards
            return f"FY {self.txn_date.year}-{str(self.txn_date.year + 1)[-2:]}"
        else:  # Jan-March
            return f"FY {self.txn_date.year - 1}-{str(self.txn_date.year)[-2:]}"

    @property
    def signed_amount(self) -> Decimal:
        """Return amount with sign (positive for credit, negative for debit)."""
        if self.txn_type == TransactionType.DEBIT:
            return -abs(self.amount)
        return abs(self.amount)

    def to_dict(self) -> dict:
        """Convert to dictionary for database storage."""
        return {
            "uid": self.uid,
            "user_name": self.user_name,
            "bank_name": self.bank_name,
            "txn_date": self.txn_date.isoformat(),
            "value_date": self.value_date.isoformat() if self.value_date else None,
            "remarks": self.remarks,
            "base_string": self.base_string,
            "amount": str(self.signed_amount),
            "txn_type": self.txn_type.value,
            "balance": str(self.balance) if self.balance else None,
            "category": self.category,
            "sub_category": self.sub_category,
            "fiscal_year": self.fiscal_year,
            "source_file": self.source_file,
        }


@dataclass
class UserBankConfig:
    """
    User-specific bank statement configuration.

    Loaded from user_bank_config.json in the bank directory.
    """
    user_name: str
    bank_name: str
    account_type: str = "SAVINGS"
    statement_format: str = "XLS"
    header_search_keywords: List[str] = field(default_factory=lambda: [
        "REMARK", "WITHDRAWAL/DR", "DEPOSIT/CR"
    ])
    date_column_names: List[str] = field(default_factory=lambda: [
        "Date", "Txn Date", "Transaction Date", "VALUE DATE"
    ])
    amount_column_patterns: Dict[str, List[str]] = field(default_factory=lambda: {
        "debit": ["WITHDRAWAL", "DR", "Debit", "WITHDRAWAL/DR"],
        "credit": ["DEPOSIT", "CR", "Credit", "DEPOSIT/CR"]
    })
    skip_rows_top: int = 0
    date_format: str = "%d/%m/%Y"
    category_overrides: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_json(cls, json_path: str) -> "UserBankConfig":
        """Load configuration from JSON file."""
        with open(json_path, 'r') as f:
            data = json.load(f)
        return cls(**data)

    @classmethod
    def default_for_bank(cls, user_name: str, bank_name: str) -> "UserBankConfig":
        """Create default configuration for a bank."""
        return cls(user_name=user_name, bank_name=bank_name)

    def to_json(self, json_path: str) -> None:
        """Save configuration to JSON file."""
        with open(json_path, 'w') as f:
            json.dump(self.__dict__, f, indent=4)


@dataclass
class CategoryRule:
    """Rule for category classification."""
    category: str
    keywords: List[str]
    is_income: bool = False
    asset_class: Optional[str] = None
    pfas_table: Optional[str] = None
    priority: int = 0  # Higher priority rules are checked first


@dataclass
class IngestionResult:
    """Result of bank statement ingestion."""
    success: bool
    transactions_processed: int = 0
    transactions_inserted: int = 0
    transactions_skipped: int = 0  # Duplicates
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    source_files: List[str] = field(default_factory=list)

    def add_error(self, error: str) -> None:
        """Add an error message."""
        self.errors.append(error)

    def add_warning(self, warning: str) -> None:
        """Add a warning message."""
        self.warnings.append(warning)

    def __str__(self) -> str:
        """String representation of result."""
        status = "SUCCESS" if self.success else "FAILED"
        return (
            f"{status}: {self.transactions_processed} processed, "
            f"{self.transactions_inserted} inserted, "
            f"{self.transactions_skipped} skipped (duplicates)"
        )
