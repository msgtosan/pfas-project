"""
Core module - Foundation components for PFAS.

Provides:
- DatabaseManager: SQLCipher encrypted database management
- JournalEngine: Double-entry accounting journal
- TransactionService: Unified transaction recording with idempotency
- LedgerMapper: Automatic journal entry generation from normalized records
- CurrencyConverter: Multi-currency support with exchange rates
- AuditLogger: Compliance audit logging
- SessionManager: User session management with timeout
- Security: User context management and validation
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
from pfas.core.security import (
    UserContext,
    UserContextError,
    require_user_context,
    validate_user_owns_record,
)
from pfas.core.transaction_service import (
    TransactionService,
    TransactionResult,
    TransactionSource,
    TransactionRecord,
    AssetRecord,
    IdempotencyKeyGenerator,
)
from pfas.core.ledger_mapper import (
    map_to_journal,
    get_supported_transaction_types,
    register_mapper,
    AccountCode as LedgerAccountCode,
    TransactionType as LedgerTransactionType,
    AssetCategory as LedgerAssetCategory,
)
from pfas.core.exceptions import (
    PFASError,
    DatabaseError,
    EncryptionError,
    UnbalancedJournalError,
    SessionExpiredError,
    AccountNotFoundError,
    ExchangeRateNotFoundError,
    UserContextError,
    IdempotencyError,
    BatchIngestionError,
    AccountingBalanceError,
    InsufficientSharesError,
    ForexRateNotFoundError,
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
    # Security & User Context
    "UserContext",
    "UserContextError",
    "require_user_context",
    "validate_user_owns_record",
    # Transaction Service
    "TransactionService",
    "TransactionResult",
    "TransactionSource",
    "TransactionRecord",
    "AssetRecord",
    "IdempotencyKeyGenerator",
    # Ledger Mapper
    "map_to_journal",
    "get_supported_transaction_types",
    "register_mapper",
    "LedgerAccountCode",
    "LedgerTransactionType",
    "LedgerAssetCategory",
    # Exceptions
    "PFASError",
    "DatabaseError",
    "EncryptionError",
    "UnbalancedJournalError",
    "SessionExpiredError",
    "AccountNotFoundError",
    "ExchangeRateNotFoundError",
    "IdempotencyError",
    "BatchIngestionError",
    "AccountingBalanceError",
    "InsufficientSharesError",
    "ForexRateNotFoundError",
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
