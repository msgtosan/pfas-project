"""
Multi-currency support with exchange rate management.

For Phase 1: Manual rate entry and lookup.
For Phase 2: SBI TT Buying Rate automatic lookup.
"""

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from datetime import date, timedelta
from typing import Optional, List
import sqlite3

from pfas.core.exceptions import ExchangeRateNotFoundError


@dataclass
class ExchangeRate:
    """Represents an exchange rate record."""

    id: int
    date: date
    from_currency: str
    to_currency: str
    rate: Decimal
    source: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "ExchangeRate":
        """Create ExchangeRate from database row."""
        return cls(
            id=row["id"],
            date=date.fromisoformat(row["date"]) if isinstance(row["date"], str) else row["date"],
            from_currency=row["from_currency"],
            to_currency=row["to_currency"],
            rate=Decimal(str(row["rate"])),
            source=row["source"],
        )


class CurrencyConverter:
    """
    Currency conversion using stored exchange rates.

    Usage:
        converter = CurrencyConverter(connection)

        # Add a rate
        converter.add_rate(date(2024, 6, 15), "USD", Decimal("83.50"))

        # Convert
        inr = converter.convert(Decimal("100"), "USD", date(2024, 6, 15))
        # Returns Decimal("8350.00")
    """

    # Rounding precision for currency amounts
    PRECISION = Decimal("0.01")

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize the currency converter.

        Args:
            db_connection: SQLite database connection
        """
        self.conn = db_connection

    def add_rate(
        self,
        rate_date: date,
        from_currency: str,
        rate: Decimal,
        to_currency: str = "INR",
        source: str = "SBI_TT_BUYING",
    ) -> int:
        """
        Add or update an exchange rate.

        Args:
            rate_date: Date of the rate
            from_currency: Source currency (e.g., "USD")
            rate: Exchange rate (units of to_currency per 1 from_currency)
            to_currency: Target currency (defaults to "INR")
            source: Rate source (defaults to "SBI_TT_BUYING")

        Returns:
            Exchange rate record ID
        """
        if not isinstance(rate, Decimal):
            rate = Decimal(str(rate))

        cursor = self.conn.cursor()

        # Use INSERT OR REPLACE for upsert behavior
        cursor.execute(
            """
            INSERT OR REPLACE INTO exchange_rates
            (date, from_currency, to_currency, rate, source)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                rate_date.isoformat(),
                from_currency.upper(),
                to_currency.upper(),
                str(rate),
                source,
            ),
        )

        self.conn.commit()
        return cursor.lastrowid

    def get_rate(
        self,
        from_currency: str,
        as_of_date: date,
        to_currency: str = "INR",
    ) -> Optional[ExchangeRate]:
        """
        Get exchange rate for a specific date.

        Args:
            from_currency: Source currency
            as_of_date: Date to look up
            to_currency: Target currency (defaults to "INR")

        Returns:
            ExchangeRate object or None if not found
        """
        cursor = self.conn.execute(
            """
            SELECT * FROM exchange_rates
            WHERE from_currency = ? AND to_currency = ? AND date = ?
            """,
            (from_currency.upper(), to_currency.upper(), as_of_date.isoformat()),
        )
        row = cursor.fetchone()
        if row:
            return ExchangeRate.from_row(row)
        return None

    def get_rate_or_nearest(
        self,
        from_currency: str,
        as_of_date: date,
        to_currency: str = "INR",
        max_days_back: int = 7,
    ) -> Optional[ExchangeRate]:
        """
        Get exchange rate for a date, falling back to nearest previous rate.

        Args:
            from_currency: Source currency
            as_of_date: Date to look up
            to_currency: Target currency (defaults to "INR")
            max_days_back: Maximum days to look back for a rate

        Returns:
            ExchangeRate object or None if not found within range
        """
        # Try exact date first
        rate = self.get_rate(from_currency, as_of_date, to_currency)
        if rate:
            return rate

        # Look for nearest previous rate
        earliest_date = as_of_date - timedelta(days=max_days_back)
        cursor = self.conn.execute(
            """
            SELECT * FROM exchange_rates
            WHERE from_currency = ? AND to_currency = ?
            AND date <= ? AND date >= ?
            ORDER BY date DESC
            LIMIT 1
            """,
            (
                from_currency.upper(),
                to_currency.upper(),
                as_of_date.isoformat(),
                earliest_date.isoformat(),
            ),
        )
        row = cursor.fetchone()
        if row:
            return ExchangeRate.from_row(row)
        return None

    def convert(
        self,
        amount: Decimal,
        from_currency: str,
        as_of_date: date,
        to_currency: str = "INR",
        use_nearest: bool = True,
    ) -> Decimal:
        """
        Convert amount from one currency to another.

        Args:
            amount: Amount to convert
            from_currency: Source currency
            as_of_date: Date for exchange rate lookup
            to_currency: Target currency (defaults to "INR")
            use_nearest: Fall back to nearest rate if exact date not found

        Returns:
            Converted amount rounded to 2 decimal places

        Raises:
            ExchangeRateNotFoundError: If no rate found for the date
        """
        if not isinstance(amount, Decimal):
            amount = Decimal(str(amount))

        # Same currency - no conversion needed
        if from_currency.upper() == to_currency.upper():
            return amount.quantize(self.PRECISION, rounding=ROUND_HALF_UP)

        # Get exchange rate
        if use_nearest:
            rate_record = self.get_rate_or_nearest(from_currency, as_of_date, to_currency)
        else:
            rate_record = self.get_rate(from_currency, as_of_date, to_currency)

        if not rate_record:
            raise ExchangeRateNotFoundError(from_currency, as_of_date.isoformat())

        # Convert
        result = amount * rate_record.rate
        return result.quantize(self.PRECISION, rounding=ROUND_HALF_UP)

    def get_rates_for_period(
        self,
        from_currency: str,
        start_date: date,
        end_date: date,
        to_currency: str = "INR",
    ) -> List[ExchangeRate]:
        """
        Get all exchange rates for a currency within a date range.

        Args:
            from_currency: Source currency
            start_date: Start of date range
            end_date: End of date range
            to_currency: Target currency (defaults to "INR")

        Returns:
            List of ExchangeRate objects
        """
        cursor = self.conn.execute(
            """
            SELECT * FROM exchange_rates
            WHERE from_currency = ? AND to_currency = ?
            AND date >= ? AND date <= ?
            ORDER BY date
            """,
            (
                from_currency.upper(),
                to_currency.upper(),
                start_date.isoformat(),
                end_date.isoformat(),
            ),
        )
        return [ExchangeRate.from_row(row) for row in cursor.fetchall()]

    def bulk_add_rates(
        self,
        rates: List[tuple],
        source: str = "SBI_TT_BUYING",
    ) -> int:
        """
        Add multiple exchange rates at once.

        Args:
            rates: List of (date, from_currency, rate) tuples
            source: Rate source

        Returns:
            Number of rates added
        """
        cursor = self.conn.cursor()
        count = 0

        for rate_date, from_currency, rate in rates:
            # Ensure rate is stored as string to preserve precision
            if isinstance(rate, Decimal):
                rate_str = str(rate)
            else:
                rate_str = str(Decimal(str(rate)))

            cursor.execute(
                """
                INSERT OR REPLACE INTO exchange_rates
                (date, from_currency, to_currency, rate, source)
                VALUES (?, ?, 'INR', ?, ?)
                """,
                (
                    rate_date.isoformat() if isinstance(rate_date, date) else rate_date,
                    from_currency.upper(),
                    rate_str,
                    source,
                ),
            )
            count += 1

        self.conn.commit()
        return count


def get_sbi_tt_buying_rate(currency: str, rate_date: date) -> Optional[Decimal]:
    """
    Fetch SBI TT Buying Rate for a currency.

    Note: This is a placeholder for Phase 2 implementation.
    In Phase 2, this will make HTTP requests to SBI's rate page.

    Args:
        currency: Currency code (e.g., "USD")
        rate_date: Date for the rate

    Returns:
        Exchange rate or None if not available
    """
    # Phase 2: Implement SBI rate scraping
    # For now, return None to indicate rate not available
    return None
