"""
MF Parser Exceptions - Hierarchical exception system for MF parsing.

Exception hierarchy:
    MFParserError (base)
    ├── CASParseError
    │   ├── HeaderParseError
    │   ├── IncorrectPasswordError
    │   └── UnsupportedFormatError
    ├── IntegrityError
    │   └── BalanceMismatchError
    ├── IncompleteDataError
    └── GainsCalculationError
        └── FIFOMismatchError
"""


class MFParserError(Exception):
    """
    Base exception for all MF parsing errors.

    All MF-specific exceptions inherit from this class,
    allowing callers to catch all MF errors with a single handler.
    """

    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


# ============================================================================
# Parsing Errors
# ============================================================================

class CASParseError(MFParserError):
    """
    Error while parsing CAS PDF file.

    Raised when the parser encounters invalid or unexpected content
    in a CAS statement.
    """
    pass


class HeaderParseError(CASParseError):
    """
    Error parsing CAS statement header.

    Raised when the statement period or investor info cannot be
    extracted from the header section.
    """
    pass


class IncorrectPasswordError(CASParseError):
    """
    Incorrect password for encrypted PDF.

    Raised when the provided password cannot decrypt the PDF file.
    """

    def __init__(self, file_path: str):
        super().__init__(
            f"Incorrect password for PDF: {file_path}",
            details={"file": file_path}
        )
        self.file_path = file_path


class UnsupportedFormatError(CASParseError):
    """
    Unsupported CAS file format.

    Raised when the parser encounters a CAS format it cannot handle.
    """

    def __init__(self, format_type: str, file_path: str):
        super().__init__(
            f"Unsupported CAS format: {format_type}",
            details={"format": format_type, "file": file_path}
        )
        self.format_type = format_type
        self.file_path = file_path


# ============================================================================
# Data Integrity Errors
# ============================================================================

class IntegrityError(MFParserError):
    """
    Data integrity check failed.

    Raised when parsed data fails validation checks, such as
    balance reconciliation or transaction verification.
    """
    pass


class BalanceMismatchError(IntegrityError):
    """
    Unit balance mismatch detected.

    Raised when calculated closing balance doesn't match
    the stated closing balance in the CAS.
    """

    def __init__(
        self,
        scheme_name: str,
        folio: str,
        stated_balance: str,
        calculated_balance: str
    ):
        super().__init__(
            f"Balance mismatch for {scheme_name} (Folio: {folio}): "
            f"stated={stated_balance}, calculated={calculated_balance}",
            details={
                "scheme": scheme_name,
                "folio": folio,
                "stated": stated_balance,
                "calculated": calculated_balance,
            }
        )
        self.scheme_name = scheme_name
        self.folio = folio
        self.stated_balance = stated_balance
        self.calculated_balance = calculated_balance


# ============================================================================
# Data Completeness Errors
# ============================================================================

class IncompleteDataError(MFParserError):
    """
    CAS data incomplete for analysis.

    Raised when attempting analysis on a CAS that lacks
    required data (e.g., running capital gains on a summary CAS).
    """

    def __init__(self, required: str, available: str):
        super().__init__(
            f"Incomplete CAS data: requires {required}, but only {available} available",
            details={"required": required, "available": available}
        )
        self.required = required
        self.available = available


# ============================================================================
# Capital Gains Errors
# ============================================================================

class GainsCalculationError(MFParserError):
    """
    Error computing capital gains.

    Raised when capital gains calculation encounters an error,
    such as insufficient purchase history or invalid data.
    """
    pass


class FIFOMismatchError(GainsCalculationError):
    """
    FIFO unit matching failed.

    Raised when redemption units cannot be matched with
    purchase units using FIFO logic.
    """

    def __init__(
        self,
        scheme_name: str,
        folio: str,
        redemption_units: str,
        available_units: str
    ):
        super().__init__(
            f"FIFO mismatch for {scheme_name} (Folio: {folio}): "
            f"redemption={redemption_units} units, available={available_units} units",
            details={
                "scheme": scheme_name,
                "folio": folio,
                "redemption_units": redemption_units,
                "available_units": available_units,
            }
        )
        self.scheme_name = scheme_name
        self.folio = folio
        self.redemption_units = redemption_units
        self.available_units = available_units


class GrandfatheringError(GainsCalculationError):
    """
    Error applying grandfathering rules.

    Raised when grandfathering calculation fails due to
    missing FMV data or invalid dates.
    """

    def __init__(self, scheme_name: str, purchase_date: str, reason: str):
        super().__init__(
            f"Grandfathering error for {scheme_name}: {reason}",
            details={
                "scheme": scheme_name,
                "purchase_date": purchase_date,
                "reason": reason,
            }
        )
        self.scheme_name = scheme_name
        self.purchase_date = purchase_date
        self.reason = reason
