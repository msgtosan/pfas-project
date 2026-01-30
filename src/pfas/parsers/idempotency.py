"""
Idempotency support for PFAS parsers.

Provides standardized idempotency key generation and deduplication checking
that can be mixed into any parser class.
"""

import hashlib
import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional, Dict, Any, List
import sqlite3

logger = logging.getLogger(__name__)


@dataclass
class IdempotencyResult:
    """Result of an idempotency check."""

    is_duplicate: bool
    existing_record_id: Optional[int] = None
    idempotency_key: str = ""


class IdempotencyMixin:
    """
    Mixin class providing idempotency support for parsers.

    Add this mixin to any parser class to enable:
    - Standardized idempotency key generation
    - Duplicate detection before insert
    - Automatic recording of processed keys

    Usage:
        class MyParser(IdempotencyMixin):
            def __init__(self, conn):
                self.conn = conn
                self._init_idempotency()

            def parse_record(self, record):
                key = self.generate_key("mytype", record["id"], record["date"])
                if self.is_duplicate(key):
                    return None  # Skip duplicate
                # ... process record ...
                self.record_processed(key, record_id)
    """

    def _init_idempotency(self) -> None:
        """Initialize idempotency support. Call from __init__."""
        self._ensure_idempotency_table()

    def _ensure_idempotency_table(self) -> None:
        """Ensure idempotency tracking table exists."""
        if not hasattr(self, "conn"):
            raise AttributeError("Parser must have 'conn' attribute for idempotency support")

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS parser_idempotency (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                idempotency_key TEXT UNIQUE NOT NULL,
                parser_type TEXT NOT NULL,
                record_table TEXT,
                record_id INTEGER,
                source_file TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_parser_idempotency_key
            ON parser_idempotency(idempotency_key)
        """)

        self.conn.commit()

    def generate_key(self, prefix: str, *parts: Any) -> str:
        """
        Generate a standardized idempotency key.

        Args:
            prefix: Key type prefix (e.g., 'mf', 'stock', 'bank')
            *parts: Variable parts to include in key

        Returns:
            Formatted idempotency key

        Example:
            key = self.generate_key("mf", folio, date, amount, units)
            # Returns: "mf:12345:2024-03-15:10000.00:100.000"
        """
        formatted_parts = []
        for part in parts:
            if isinstance(part, date):
                formatted_parts.append(part.isoformat())
            elif isinstance(part, Decimal):
                formatted_parts.append(str(part))
            elif part is None:
                formatted_parts.append("")
            else:
                formatted_parts.append(str(part))

        return f"{prefix}:{':'.join(formatted_parts)}"

    def generate_key_hashed(self, prefix: str, *parts: Any) -> str:
        """
        Generate idempotency key with hashed sensitive parts.

        Use this when parts contain sensitive data like account numbers.

        Args:
            prefix: Key type prefix
            *parts: Variable parts (will be hashed together)

        Returns:
            Formatted key with hashed content
        """
        content = ":".join(str(p) for p in parts)
        content_hash = hashlib.md5(content.encode()).hexdigest()[:16]
        return f"{prefix}:{content_hash}"

    def is_duplicate(self, idempotency_key: str) -> bool:
        """
        Check if an idempotency key already exists.

        Args:
            idempotency_key: Key to check

        Returns:
            True if duplicate, False if new
        """
        cursor = self.conn.execute(
            "SELECT 1 FROM parser_idempotency WHERE idempotency_key = ?",
            (idempotency_key,)
        )
        return cursor.fetchone() is not None

    def check_idempotency(self, idempotency_key: str) -> IdempotencyResult:
        """
        Check idempotency with full result details.

        Args:
            idempotency_key: Key to check

        Returns:
            IdempotencyResult with duplicate status and existing record info
        """
        cursor = self.conn.execute(
            "SELECT record_id FROM parser_idempotency WHERE idempotency_key = ?",
            (idempotency_key,)
        )
        row = cursor.fetchone()

        if row:
            return IdempotencyResult(
                is_duplicate=True,
                existing_record_id=row[0],
                idempotency_key=idempotency_key
            )

        return IdempotencyResult(
            is_duplicate=False,
            idempotency_key=idempotency_key
        )

    def record_processed(
        self,
        idempotency_key: str,
        record_id: int = None,
        record_table: str = None,
        source_file: str = None,
        parser_type: str = None
    ) -> None:
        """
        Record that a key has been processed.

        Args:
            idempotency_key: The processed key
            record_id: ID of the created record (optional)
            record_table: Table where record was inserted (optional)
            source_file: Source file path (optional)
            parser_type: Type of parser (defaults to class name)
        """
        if parser_type is None:
            parser_type = self.__class__.__name__

        self.conn.execute("""
            INSERT OR IGNORE INTO parser_idempotency
            (idempotency_key, parser_type, record_table, record_id, source_file)
            VALUES (?, ?, ?, ?, ?)
        """, (idempotency_key, parser_type, record_table, record_id, source_file))

        self.conn.commit()

    def get_processed_count(self, parser_type: str = None) -> int:
        """
        Get count of processed records.

        Args:
            parser_type: Filter by parser type (optional)

        Returns:
            Number of processed records
        """
        if parser_type:
            cursor = self.conn.execute(
                "SELECT COUNT(*) FROM parser_idempotency WHERE parser_type = ?",
                (parser_type,)
            )
        else:
            cursor = self.conn.execute("SELECT COUNT(*) FROM parser_idempotency")

        return cursor.fetchone()[0]


# Standalone key generators for use outside parser classes

def mf_idempotency_key(
    folio_number: str,
    txn_date: date,
    amount: Decimal,
    units: Decimal,
    txn_type: str = "PURCHASE"
) -> str:
    """
    Generate idempotency key for MF transaction.

    Format: mf:{folio}:{date}:{amount}:{units}:{type}
    """
    return f"mf:{folio_number}:{txn_date.isoformat()}:{amount}:{units}:{txn_type}"


def stock_idempotency_key(broker: str, trade_id: str) -> str:
    """
    Generate idempotency key for stock trade.

    Format: stock:{broker}:{trade_id}
    """
    return f"stock:{broker}:{trade_id}"


def bank_idempotency_key(
    account_number: str,
    txn_date: date,
    ref_no: str,
    amount: Decimal,
) -> str:
    """
    Generate idempotency key for bank transaction.

    Format: bank:{account_hash}:{date}:{ref_no}:{amount}
    """
    account_hash = hashlib.md5(account_number.encode()).hexdigest()[:8]
    return f"bank:{account_hash}:{txn_date.isoformat()}:{ref_no}:{amount}"


def salary_idempotency_key(
    employer: str,
    pay_period: str,
    gross_amount: Decimal,
) -> str:
    """
    Generate idempotency key for salary record.

    Format: salary:{employer}:{period}:{gross}
    """
    return f"salary:{employer}:{pay_period}:{gross_amount}"


def epf_idempotency_key(
    uan: str,
    month: str,
    employee_amount: Decimal,
    employer_amount: Decimal,
) -> str:
    """
    Generate idempotency key for EPF contribution.

    Format: epf:{uan_hash}:{month}:{ee}:{er}
    """
    uan_hash = hashlib.md5(uan.encode()).hexdigest()[:8]
    return f"epf:{uan_hash}:{month}:{employee_amount}:{employer_amount}"


def dividend_idempotency_key(
    source_type: str,
    symbol_or_scheme: str,
    record_date: date,
    amount: Decimal,
) -> str:
    """
    Generate idempotency key for dividend.

    Format: dividend:{type}:{symbol}:{date}:{amount}
    """
    return f"dividend:{source_type}:{symbol_or_scheme}:{record_date.isoformat()}:{amount}"
