"""
Core module - Foundation components for PFAS.

Provides:
- DatabaseManager: SQLCipher encrypted database management
- JournalEngine: Double-entry accounting journal
- CurrencyConverter: Multi-currency support with exchange rates
- AuditLogger: Compliance audit logging
- SessionManager: User session management with timeout
- Field encryption utilities
- Core models: NormalizedTransaction, CashFlow, BalanceSheetSnapshot, etc.
"""

from pfas.core.database import DatabaseManager
from pfas.core.encryption import encrypt_field, decrypt_field, derive_key
from pfas.core.accounts import setup_chart_of_accounts, get_account_by_code, CHART_OF_ACCOUNTS
from pfas.core.journal import JournalEngine, JournalEntry
from pfas.core.currency import CurrencyConverter
from pfas.core.audit import AuditLogger
from pfas.core.session import SessionManager
from pfas.core.exceptions import (
    PFASError,
    DatabaseError,
    EncryptionError,
    UnbalancedJournalError,
    SessionExpiredError,
    AccountNotFoundError,
    ExchangeRateNotFoundError,
)
from pfas.core.models import (
    ActivityType,
    FlowDirection,
    AssetCategory,
    LiabilityType,
    CashFlowCategory,
    NormalizedTransaction,
    CashFlow,
    AssetHolding,
    Liability,
    LiabilityTransaction,
    BalanceSheetSnapshot,
    CashFlowStatement,
    get_financial_year,
    get_fy_dates,
)

__all__ = [
    # Database & Infrastructure
    "DatabaseManager",
    "encrypt_field",
    "decrypt_field",
    "derive_key",
    "setup_chart_of_accounts",
    "get_account_by_code",
    "CHART_OF_ACCOUNTS",
    "JournalEngine",
    "JournalEntry",
    "CurrencyConverter",
    "AuditLogger",
    "SessionManager",
    # Exceptions
    "PFASError",
    "DatabaseError",
    "EncryptionError",
    "UnbalancedJournalError",
    "SessionExpiredError",
    "AccountNotFoundError",
    "ExchangeRateNotFoundError",
    # Core Models - Enums
    "ActivityType",
    "FlowDirection",
    "AssetCategory",
    "LiabilityType",
    "CashFlowCategory",
    # Core Models - Dataclasses
    "NormalizedTransaction",
    "CashFlow",
    "AssetHolding",
    "Liability",
    "LiabilityTransaction",
    "BalanceSheetSnapshot",
    "CashFlowStatement",
    # Utility Functions
    "get_financial_year",
    "get_fy_dates",
]
