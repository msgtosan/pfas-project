"""
Custom exceptions for PFAS core module.

All PFAS-specific exceptions inherit from PFASError for easy catching.
"""


class PFASError(Exception):
    """Base exception for all PFAS errors."""

    def __init__(self, message: str, code: str = None):
        super().__init__(message)
        self.message = message
        self.code = code


class DatabaseError(PFASError):
    """Database operation errors."""

    def __init__(self, message: str, code: str = "DB_ERROR"):
        super().__init__(message, code)


class EncryptionError(PFASError):
    """Encryption/decryption errors."""

    def __init__(self, message: str, code: str = "ENCRYPT_ERROR"):
        super().__init__(message, code)


class UnbalancedJournalError(PFASError):
    """Raised when journal entries don't balance (Debit != Credit)."""

    def __init__(self, message: str = "Debit does not equal Credit", code: str = "JOURNAL_UNBALANCED"):
        super().__init__(message, code)


class SessionExpiredError(PFASError):
    """Raised when session has expired or is invalid."""

    def __init__(self, message: str = "Session has expired", code: str = "SESSION_EXPIRED"):
        super().__init__(message, code)


class AccountNotFoundError(PFASError):
    """Raised when an account is not found."""

    def __init__(self, account_code: str, code: str = "ACCOUNT_NOT_FOUND"):
        super().__init__(f"Account not found: {account_code}", code)
        self.account_code = account_code


class ExchangeRateNotFoundError(PFASError):
    """Raised when exchange rate is not found for date/currency."""

    def __init__(self, currency: str, date: str, code: str = "RATE_NOT_FOUND"):
        super().__init__(f"Exchange rate not found for {currency} on {date}", code)
        self.currency = currency
        self.date = date


class ValidationError(PFASError):
    """Data validation errors."""

    def __init__(self, message: str, field: str = None, code: str = "VALIDATION_ERROR"):
        super().__init__(message, code)
        self.field = field


class AuthenticationError(PFASError):
    """Authentication failures."""

    def __init__(self, message: str = "Authentication failed", code: str = "AUTH_ERROR"):
        super().__init__(message, code)


class UserContextError(PFASError):
    """Raised when user context is missing or invalid."""

    def __init__(self, message: str = "User context required", code: str = "USER_CONTEXT_ERROR"):
        super().__init__(message, code)


class IdempotencyError(PFASError):
    """Raised when a duplicate transaction is detected."""

    def __init__(self, idempotency_key: str, code: str = "DUPLICATE_TRANSACTION"):
        super().__init__(f"Duplicate transaction detected: {idempotency_key}", code)
        self.idempotency_key = idempotency_key


class BatchIngestionError(PFASError):
    """Raised when batch ingestion fails."""

    def __init__(self, message: str, failed_files: list = None, code: str = "BATCH_ERROR"):
        super().__init__(message, code)
        self.failed_files = failed_files or []


class AccountingBalanceError(PFASError):
    """
    Raised when accounting components do not balance.

    This includes:
    - Salary components not summing to gross salary
    - Journal entries not balancing (debits != credits)
    - Cost basis mismatches in inventory accounting
    """

    def __init__(
        self,
        message: str,
        expected: str = None,
        actual: str = None,
        difference: str = None,
        code: str = "ACCOUNTING_BALANCE_ERROR"
    ):
        super().__init__(message, code)
        self.expected = expected
        self.actual = actual
        self.difference = difference


class InsufficientSharesError(PFASError):
    """Raised when attempting to sell more shares than available."""

    def __init__(
        self,
        symbol: str,
        requested: str,
        available: str,
        code: str = "INSUFFICIENT_SHARES"
    ):
        super().__init__(
            f"Insufficient shares for {symbol}: requested {requested}, available {available}",
            code
        )
        self.symbol = symbol
        self.requested = requested
        self.available = available


class ForexRateNotFoundError(PFASError):
    """Raised when forex rate is not available for conversion."""

    def __init__(
        self,
        rate_date: str,
        from_currency: str = "USD",
        to_currency: str = "INR",
        code: str = "FOREX_RATE_NOT_FOUND"
    ):
        super().__init__(
            f"Exchange rate not found for {from_currency}/{to_currency} on {rate_date}",
            code
        )
        self.rate_date = rate_date
        self.from_currency = from_currency
        self.to_currency = to_currency
