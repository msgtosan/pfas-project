"""
Security utilities for PFAS.

Provides user context validation, isolation enforcement, and security decorators.
"""

import functools
import inspect
from typing import Callable, TypeVar, ParamSpec, Any

from pfas.core.exceptions import PFASError


class UserContextError(PFASError):
    """Raised when user context is missing or invalid."""

    def __init__(self, message: str = "User context required", code: str = "USER_CONTEXT_ERROR"):
        super().__init__(message, code)


class UserContext:
    """
    Thread-local user context for request-scoped user isolation.

    Usage:
        with UserContext.set(user_id=123):
            # All operations in this block have user context
            service.get_balance_sheet(...)

        # Or set globally for CLI operations
        UserContext.set_current(user_id=123)
    """

    _current_user_id: int | None = None

    @classmethod
    def set_current(cls, user_id: int) -> None:
        """Set the current user context globally."""
        if user_id is None:
            raise UserContextError("user_id cannot be None")
        cls._current_user_id = user_id

    @classmethod
    def get_current(cls) -> int | None:
        """Get the current user ID."""
        return cls._current_user_id

    @classmethod
    def clear(cls) -> None:
        """Clear the current user context."""
        cls._current_user_id = None

    @classmethod
    def set(cls, user_id: int) -> "UserContextManager":
        """Context manager for scoped user context."""
        return UserContextManager(user_id)


class UserContextManager:
    """Context manager for scoped user context."""

    def __init__(self, user_id: int):
        self.user_id = user_id
        self.previous_user_id: int | None = None

    def __enter__(self) -> "UserContextManager":
        self.previous_user_id = UserContext._current_user_id
        UserContext._current_user_id = self.user_id
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        UserContext._current_user_id = self.previous_user_id


P = ParamSpec("P")
T = TypeVar("T")


def require_user_context(func: Callable[P, T]) -> Callable[P, T]:
    """
    Decorator that enforces user_id parameter is present and non-None.

    Checks for user_id in:
    1. Function parameters (keyword or positional)
    2. Global UserContext if not in parameters

    Usage:
        @require_user_context
        def get_balance_sheet(self, user_id: int, as_of: date):
            ...

        # Will raise UserContextError if user_id is None
        service.get_balance_sheet(user_id=None, as_of=today)  # Raises!

        # Will use global context if user_id not provided
        UserContext.set_current(user_id=123)
        service.get_balance_sheet(as_of=today)  # OK, uses context
    """

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        # Get function signature to find user_id parameter
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())

        user_id = None
        user_id_source = None

        # Check if user_id is in kwargs
        if "user_id" in kwargs:
            user_id = kwargs["user_id"]
            user_id_source = "kwargs"

        # Check if user_id is in positional args
        elif "user_id" in params:
            user_id_index = params.index("user_id")
            # Account for 'self' in methods
            if params[0] == "self":
                user_id_index -= 1
                if user_id_index >= 0 and len(args) > user_id_index + 1:
                    user_id = args[user_id_index + 1]
                    user_id_source = "positional"
            elif len(args) > user_id_index:
                user_id = args[user_id_index]
                user_id_source = "positional"

        # Fall back to global context
        if user_id is None:
            user_id = UserContext.get_current()
            user_id_source = "context"

            # Inject user_id into kwargs if parameter exists
            if user_id is not None and "user_id" in params:
                kwargs["user_id"] = user_id

        # Validate user_id
        if user_id is None:
            func_name = func.__qualname__
            raise UserContextError(
                f"user_id is required for {func_name}. "
                "Pass user_id parameter or set UserContext.set_current(user_id)"
            )

        if not isinstance(user_id, int):
            raise UserContextError(
                f"user_id must be an integer, got {type(user_id).__name__}"
            )

        if user_id <= 0:
            raise UserContextError(f"user_id must be positive, got {user_id}")

        return func(*args, **kwargs)

    return wrapper


def validate_user_owns_record(
    conn,
    user_id: int,
    table: str,
    record_id: int,
    user_column: str = "user_id"
) -> bool:
    """
    Validate that a user owns a specific record.

    Args:
        conn: Database connection
        user_id: User ID to validate
        table: Table name
        record_id: Record ID to check
        user_column: Column name containing user_id (default: user_id)

    Returns:
        True if user owns record

    Raises:
        UserContextError: If user does not own record
    """
    # Use parameterized query for record_id, but table/column must be validated
    allowed_tables = {
        "mf_folios", "mf_transactions", "bank_accounts", "bank_transactions",
        "stock_trades", "epf_transactions", "ppf_transactions", "nps_transactions",
        "salary_records", "sgb_holdings", "rental_income", "rsu_vests", "espp_purchases"
    }

    if table not in allowed_tables:
        raise ValueError(f"Invalid table: {table}")

    if user_column not in ("user_id", "owner_id"):
        raise ValueError(f"Invalid user column: {user_column}")

    cursor = conn.execute(
        f"SELECT {user_column} FROM {table} WHERE id = ?",
        (record_id,)
    )
    row = cursor.fetchone()

    if row is None:
        raise UserContextError(f"Record {record_id} not found in {table}")

    record_user_id = row[0]
    if record_user_id != user_id:
        raise UserContextError(
            f"User {user_id} does not have access to record {record_id} in {table}"
        )

    return True


def get_user_filter_clause(user_id: int, alias: str = None) -> tuple[str, list]:
    """
    Generate SQL WHERE clause for user filtering.

    Args:
        user_id: User ID to filter by
        alias: Optional table alias (e.g., 'u' for 'users u')

    Returns:
        Tuple of (clause_string, params_list)

    Example:
        clause, params = get_user_filter_clause(123, alias='mf')
        query = f"SELECT * FROM mf_folios mf WHERE {clause}"
        cursor.execute(query, params)
    """
    prefix = f"{alias}." if alias else ""
    return f"{prefix}user_id = ?", [user_id]
