"""SBI TT Buying Rate provider for currency conversion.

This module provides exchange rates for converting foreign currency
amounts to INR as required for:
- RSU perquisite valuation
- ESPP perquisite valuation
- Foreign dividend conversion
- Capital gains calculation
- Schedule FA valuations

As per RBI/CBDT guidelines, SBI TT Buying Rate should be used
for all income tax related conversions.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional, List
import sqlite3


@dataclass
class ExchangeRate:
    """Exchange rate record."""

    rate_date: date
    from_currency: str
    to_currency: str
    rate: Decimal
    source: str  # 'SBI', 'RBI', 'MANUAL'


class SBITTRateProvider:
    """
    Provides SBI TT Buying Rate for currency conversions.

    Rate lookup priority:
    1. Exact date match in cache
    2. Nearest available rate (within 7 days)
    3. Manual fallback entry

    Note: For holidays/weekends, use the last available business day rate.
    """

    MAX_LOOKBACK_DAYS = 7  # Max days to look back for rate

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize rate provider.

        Args:
            db_connection: Database connection
        """
        self.conn = db_connection

    def get_rate(
        self,
        rate_date: date,
        from_currency: str = "USD",
        to_currency: str = "INR"
    ) -> Decimal:
        """
        Get exchange rate for a specific date.

        Args:
            rate_date: Date for which rate is needed
            from_currency: Source currency (default: USD)
            to_currency: Target currency (default: INR)

        Returns:
            Exchange rate as Decimal

        Raises:
            ValueError: If no rate is available
        """
        # Check exact date first
        cached = self._get_cached_rate(rate_date, from_currency, to_currency)
        if cached:
            return cached.rate

        # Try nearest available rate
        nearest = self._get_nearest_rate(rate_date, from_currency, to_currency)
        if nearest:
            return nearest.rate

        raise ValueError(
            f"No exchange rate available for {from_currency}/{to_currency} "
            f"on or near {rate_date}"
        )

    def get_rate_record(
        self,
        rate_date: date,
        from_currency: str = "USD",
        to_currency: str = "INR"
    ) -> Optional[ExchangeRate]:
        """
        Get full exchange rate record including source.

        Args:
            rate_date: Date for which rate is needed
            from_currency: Source currency
            to_currency: Target currency

        Returns:
            ExchangeRate record or None
        """
        cached = self._get_cached_rate(rate_date, from_currency, to_currency)
        if cached:
            return cached

        return self._get_nearest_rate(rate_date, from_currency, to_currency)

    def add_rate(
        self,
        rate_date: date,
        from_currency: str,
        to_currency: str,
        rate: Decimal,
        source: str = "MANUAL"
    ) -> None:
        """
        Add or update an exchange rate.

        Args:
            rate_date: Rate date
            from_currency: Source currency
            to_currency: Target currency
            rate: Exchange rate
            source: Rate source ('SBI', 'RBI', 'MANUAL')
        """
        self.conn.execute(
            """INSERT OR REPLACE INTO exchange_rates
            (date, from_currency, to_currency, rate, source)
            VALUES (?, ?, ?, ?, ?)""",
            (rate_date.isoformat(), from_currency, to_currency, str(rate), source)
        )
        self.conn.commit()

    def add_manual_rate(
        self,
        rate_date: date,
        rate: Decimal,
        from_currency: str = "USD"
    ) -> None:
        """
        Add manual rate entry (convenience method).

        Args:
            rate_date: Rate date
            rate: Exchange rate
            from_currency: Source currency (default: USD)
        """
        self.add_rate(rate_date, from_currency, "INR", rate, "MANUAL")

    def bulk_add_rates(self, rates: List[ExchangeRate]) -> int:
        """
        Bulk add exchange rates.

        Args:
            rates: List of ExchangeRate records

        Returns:
            Number of rates added
        """
        count = 0
        for rate in rates:
            try:
                self.conn.execute(
                    """INSERT OR REPLACE INTO exchange_rates
                    (date, from_currency, to_currency, rate, source)
                    VALUES (?, ?, ?, ?, ?)""",
                    (
                        rate.rate_date.isoformat(),
                        rate.from_currency,
                        rate.to_currency,
                        str(rate.rate),
                        rate.source
                    )
                )
                count += 1
            except Exception:
                continue

        self.conn.commit()
        return count

    def get_rates_for_period(
        self,
        start_date: date,
        end_date: date,
        from_currency: str = "USD"
    ) -> List[ExchangeRate]:
        """
        Get all rates for a date range.

        Args:
            start_date: Period start
            end_date: Period end
            from_currency: Source currency

        Returns:
            List of ExchangeRate records
        """
        cursor = self.conn.execute(
            """SELECT date, from_currency, to_currency, rate, source
            FROM exchange_rates
            WHERE from_currency = ?
                AND to_currency = 'INR'
                AND date >= ?
                AND date <= ?
            ORDER BY date""",
            (from_currency, start_date.isoformat(), end_date.isoformat())
        )

        rates = []
        for row in cursor.fetchall():
            rates.append(ExchangeRate(
                rate_date=date.fromisoformat(row['date'])
                if isinstance(row['date'], str) else row['date'],
                from_currency=row['from_currency'],
                to_currency=row['to_currency'],
                rate=Decimal(str(row['rate'])),
                source=row['source']
            ))

        return rates

    def get_fy_end_rate(
        self,
        financial_year: str,
        from_currency: str = "USD"
    ) -> Decimal:
        """
        Get rate for FY end date (March 31).

        Used for Schedule FA closing valuations.

        Args:
            financial_year: FY in format '2024-25'
            from_currency: Source currency

        Returns:
            Exchange rate for March 31
        """
        start_year = int(financial_year.split('-')[0])
        fy_end = date(start_year + 1, 3, 31)

        return self.get_rate(fy_end, from_currency)

    def convert(
        self,
        amount: Decimal,
        rate_date: date,
        from_currency: str = "USD",
        to_currency: str = "INR"
    ) -> Decimal:
        """
        Convert amount using rate for specified date.

        Args:
            amount: Amount in source currency
            rate_date: Date for rate lookup
            from_currency: Source currency
            to_currency: Target currency

        Returns:
            Converted amount
        """
        rate = self.get_rate(rate_date, from_currency, to_currency)
        return amount * rate

    def _get_cached_rate(
        self,
        rate_date: date,
        from_currency: str,
        to_currency: str
    ) -> Optional[ExchangeRate]:
        """Get exact date match from cache."""
        cursor = self.conn.execute(
            """SELECT date, from_currency, to_currency, rate, source
            FROM exchange_rates
            WHERE date = ?
                AND from_currency = ?
                AND to_currency = ?""",
            (rate_date.isoformat(), from_currency, to_currency)
        )

        row = cursor.fetchone()
        if row:
            return ExchangeRate(
                rate_date=date.fromisoformat(row['date'])
                if isinstance(row['date'], str) else row['date'],
                from_currency=row['from_currency'],
                to_currency=row['to_currency'],
                rate=Decimal(str(row['rate'])),
                source=row['source']
            )

        return None

    def _get_nearest_rate(
        self,
        rate_date: date,
        from_currency: str,
        to_currency: str
    ) -> Optional[ExchangeRate]:
        """
        Get nearest available rate within lookback window.

        For weekends/holidays, returns last business day rate.
        """
        # Look for rate on or before the date (within window)
        min_date = rate_date - timedelta(days=self.MAX_LOOKBACK_DAYS)

        cursor = self.conn.execute(
            """SELECT date, from_currency, to_currency, rate, source
            FROM exchange_rates
            WHERE from_currency = ?
                AND to_currency = ?
                AND date <= ?
                AND date >= ?
            ORDER BY date DESC
            LIMIT 1""",
            (from_currency, to_currency, rate_date.isoformat(), min_date.isoformat())
        )

        row = cursor.fetchone()
        if row:
            return ExchangeRate(
                rate_date=date.fromisoformat(row['date'])
                if isinstance(row['date'], str) else row['date'],
                from_currency=row['from_currency'],
                to_currency=row['to_currency'],
                rate=Decimal(str(row['rate'])),
                source=row['source']
            )

        return None
