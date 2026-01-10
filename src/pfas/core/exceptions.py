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
