"""
Core module - Foundation components for PFAS.

Provides:
- DatabaseManager: SQLCipher encrypted database management
- JournalEngine: Double-entry accounting journal
- CurrencyConverter: Multi-currency support with exchange rates
- AuditLogger: Compliance audit logging
- SessionManager: User session management with timeout
- Field encryption utilities
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

__all__ = [
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
    "PFASError",
    "DatabaseError",
    "EncryptionError",
    "UnbalancedJournalError",
    "SessionExpiredError",
    "AccountNotFoundError",
    "ExchangeRateNotFoundError",
]
