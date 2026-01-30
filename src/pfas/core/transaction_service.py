"""
Unified Transaction Service for PFAS.

All asset changes flow through this service, ensuring:
- Double-entry accounting via JournalEngine
- Idempotency via processed_transactions table
- User isolation and validation
- Comprehensive audit trail
- Automatic journal entry generation via ledger_mapper
"""

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Dict, Any
import sqlite3

from pfas.core.journal import JournalEngine, JournalEntry
from pfas.core.audit import AuditLogger
from pfas.core.security import require_user_context, validate_user_owns_record
from pfas.core.exceptions import (
    PFASError,
    IdempotencyError,
    UnbalancedJournalError,
    UserContextError,
)

# Lazy import to avoid circular dependency
_ledger_mapper = None

def _get_ledger_mapper():
    """Lazy load ledger_mapper to avoid circular imports."""
    global _ledger_mapper
    if _ledger_mapper is None:
        from pfas.core import ledger_mapper
        _ledger_mapper = ledger_mapper
    return _ledger_mapper

logger = logging.getLogger(__name__)


class TransactionResult(Enum):
    """Result of a transaction recording attempt."""

    SUCCESS = "success"
    DUPLICATE = "duplicate"
    VALIDATION_ERROR = "validation_error"
    JOURNAL_ERROR = "journal_error"


class TransactionSource(Enum):
    """Source types for transactions."""

    PARSER_CAMS = "parser:cams"
    PARSER_KARVY = "parser:karvy"
    PARSER_ZERODHA = "parser:zerodha"
    PARSER_ICICI = "parser:icici"
    PARSER_HDFC = "parser:hdfc"
    PARSER_MORGAN_STANLEY = "parser:morgan_stanley"
    PARSER_EPF = "parser:epf"
    PARSER_PPF = "parser:ppf"
    PARSER_NPS = "parser:nps"
    BATCH_INGESTER = "batch_ingester"
    MANUAL = "manual"
    API = "api"
    MIGRATION = "migration"


@dataclass
class AssetRecord:
    """
    Represents an asset table insert to be executed alongside journal entry.

    This enables atomic dual-write: journal entry + asset table insert.
    """
    table_name: str
    data: Dict[str, Any]
    on_conflict: str = "IGNORE"  # IGNORE, REPLACE, or FAIL

    def get_insert_sql(self) -> tuple:
        """Generate INSERT SQL and parameters."""
        columns = list(self.data.keys())
        placeholders = ["?" for _ in columns]
        values = [self.data[col] for col in columns]

        conflict_clause = ""
        if self.on_conflict == "IGNORE":
            conflict_clause = "OR IGNORE"
        elif self.on_conflict == "REPLACE":
            conflict_clause = "OR REPLACE"

        sql = f"""
            INSERT {conflict_clause} INTO {self.table_name}
            ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """
        return sql, values


@dataclass
class TransactionRecord:
    """
    Result of a transaction recording.

    Contains the journal ID, idempotency key, and any affected record IDs.
    """

    result: TransactionResult
    journal_id: Optional[int] = None
    idempotency_key: Optional[str] = None
    affected_records: List[Dict[str, Any]] = field(default_factory=list)
    error_message: Optional[str] = None
    asset_record_ids: Dict[str, int] = field(default_factory=dict)


class TransactionService:
    """
    Unified service for recording all financial transactions.

    All asset changes go through this service to ensure:
    1. Double-entry accounting (debits == credits)
    2. Idempotency (duplicate transactions detected and rejected)
    3. User isolation (user_id validated on all operations)
    4. Audit trail (all changes logged)

    Usage:
        service = TransactionService(conn)

        entries = [
            JournalEntry(account_id=1101, debit=Decimal("10000")),  # Bank +
            JournalEntry(account_id=4101, credit=Decimal("10000")), # Salary Income
        ]

        result = service.record(
            user_id=1,
            entries=entries,
            description="March 2024 Salary",
            source=TransactionSource.PARSER_HDFC,
            idempotency_key="salary:2024-03:hdfc:10000"
        )

        if result.result == TransactionResult.DUPLICATE:
            print("Transaction already recorded")
    """

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize transaction service.

        Args:
            db_connection: SQLite database connection
        """
        self.conn = db_connection
        # Note: Don't set row_factory here as it may not be compatible with sqlcipher3
        self.journal_engine = JournalEngine(db_connection)
        self._ensure_tables_exist()

    def _ensure_tables_exist(self) -> None:
        """Ensure required tables exist."""
        # Processed transactions for idempotency
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                idempotency_key TEXT UNIQUE NOT NULL,
                user_id INTEGER NOT NULL,
                journal_id INTEGER,
                source TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata JSON,
                FOREIGN KEY (journal_id) REFERENCES journals(id)
            )
        """)

        # Index for fast idempotency lookups
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_txn_key
            ON processed_transactions(idempotency_key)
        """)

        # Audit log table (if not exists)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                record_id INTEGER,
                action TEXT CHECK(action IN ('INSERT','UPDATE','DELETE')),
                old_values JSON,
                new_values JSON,
                user_id INTEGER,
                ip_address TEXT,
                source TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.commit()

    @require_user_context
    def record(
        self,
        user_id: int,
        entries: List[JournalEntry],
        description: str,
        source: TransactionSource,
        idempotency_key: str,
        txn_date: date = None,
        reference_type: str = None,
        reference_id: int = None,
        metadata: Dict[str, Any] = None,
        asset_records: List[AssetRecord] = None,
        normalized_record: Dict[str, Any] = None,
    ) -> TransactionRecord:
        """
        Record a transaction with full validation and optional asset table inserts.

        This method performs atomic dual-write:
        1. Creates journal entries for double-entry accounting
        2. Inserts records into asset tables (mf_transactions, bank_transactions, etc.)

        If no entries are provided but normalized_record is, the ledger_mapper
        will automatically generate appropriate journal entries based on the
        transaction_type and asset_category.

        Args:
            user_id: User ID (validated by decorator)
            entries: List of JournalEntry (must balance). If empty/None and
                    normalized_record is provided, entries will be auto-generated.
            description: Transaction description
            source: Source of transaction
            idempotency_key: Unique key for deduplication
            txn_date: Transaction date (default: today)
            reference_type: Optional type reference (e.g., 'MF_PURCHASE')
            reference_id: Optional FK to source record
            metadata: Optional metadata to store
            asset_records: Optional list of AssetRecord for asset table inserts
            normalized_record: Optional normalized transaction data for auto-generating
                             journal entries via ledger_mapper

        Returns:
            TransactionRecord with result status and asset_record_ids

        Raises:
            UnbalancedJournalError: If entries don't balance
            UserContextError: If user validation fails
        """
        if txn_date is None:
            txn_date = date.today()

        if asset_records is None:
            asset_records = []

        if entries is None:
            entries = []

        # 1. Check idempotency
        if self._is_duplicate(idempotency_key):
            logger.info(f"Duplicate transaction detected: {idempotency_key}")
            return TransactionRecord(
                result=TransactionResult.DUPLICATE,
                idempotency_key=idempotency_key,
                error_message="Transaction already processed"
            )

        # 2. Auto-generate entries from normalized_record if none provided
        if not entries and normalized_record:
            try:
                ledger_mapper = _get_ledger_mapper()
                entries = ledger_mapper.map_to_journal(normalized_record, self.conn)
                if entries:
                    logger.debug(f"Auto-generated {len(entries)} journal entries from normalized_record")
            except Exception as e:
                logger.warning(f"Failed to auto-generate journal entries: {e}")
                # Continue without entries - asset records can still be created

        # 3. Validate entries balance (only if we have entries)
        if entries:
            total_debit = sum(e.debit * e.exchange_rate for e in entries)
            total_credit = sum(e.credit * e.exchange_rate for e in entries)

            if abs(total_debit - total_credit) >= Decimal("0.01"):
                return TransactionRecord(
                    result=TransactionResult.VALIDATION_ERROR,
                    error_message=f"Entries don't balance: debit={total_debit}, credit={total_credit}"
                )
        else:
            total_debit = Decimal("0")

        # 3. Begin atomic transaction
        cursor = self.conn.cursor()
        asset_record_ids = {}

        try:
            cursor.execute("BEGIN IMMEDIATE")

            # 4. Create journal entry (only if we have entries)
            journal_id = None
            if entries:
                journal_id = self.journal_engine.create_journal(
                    txn_date=txn_date,
                    description=description,
                    entries=entries,
                    reference_type=reference_type,
                    reference_id=reference_id,
                    created_by=user_id,
                )

            # 5. Insert asset records
            for asset_record in asset_records:
                sql, values = asset_record.get_insert_sql()
                cursor.execute(sql, values)
                record_id = cursor.lastrowid

                # Store the record ID
                table_key = asset_record.table_name
                if table_key in asset_record_ids:
                    # Handle multiple inserts to same table
                    if isinstance(asset_record_ids[table_key], list):
                        asset_record_ids[table_key].append(record_id)
                    else:
                        asset_record_ids[table_key] = [asset_record_ids[table_key], record_id]
                else:
                    asset_record_ids[table_key] = record_id

            # 6. Record idempotency key
            cursor.execute("""
                INSERT INTO processed_transactions
                (idempotency_key, user_id, journal_id, source, metadata)
                VALUES (?, ?, ?, ?, ?)
            """, (
                idempotency_key,
                user_id,
                journal_id,
                source.value if isinstance(source, TransactionSource) else source,
                str(metadata) if metadata else None,
            ))

            # 7. Log audit entry
            audit_logger = AuditLogger(self.conn, user_id=user_id, source=source.value)

            if journal_id:
                audit_logger.log_insert(
                    table_name="journals",
                    record_id=journal_id,
                    new_values={
                        "date": txn_date.isoformat(),
                        "description": description,
                        "total_amount": str(total_debit),
                        "entries_count": len(entries),
                    },
                )

            # Log asset table inserts
            for table_name, record_id in asset_record_ids.items():
                if isinstance(record_id, list):
                    for rid in record_id:
                        audit_logger.log_insert(table_name=table_name, record_id=rid, new_values={})
                else:
                    audit_logger.log_insert(table_name=table_name, record_id=record_id, new_values={})

            self.conn.commit()

            logger.info(
                f"Transaction recorded: journal_id={journal_id}, "
                f"key={idempotency_key}, user={user_id}, "
                f"asset_records={list(asset_record_ids.keys())}"
            )

            return TransactionRecord(
                result=TransactionResult.SUCCESS,
                journal_id=journal_id,
                idempotency_key=idempotency_key,
                asset_record_ids=asset_record_ids,
            )

        except UnbalancedJournalError as e:
            self.conn.rollback()
            logger.error(f"Journal balance error: {e}")
            return TransactionRecord(
                result=TransactionResult.JOURNAL_ERROR,
                error_message=str(e)
            )

        except Exception as e:
            self.conn.rollback()
            logger.exception(f"Transaction failed: {e}")
            return TransactionRecord(
                result=TransactionResult.JOURNAL_ERROR,
                error_message=str(e)
            )

    def record_asset_only(
        self,
        user_id: int,
        asset_records: List[AssetRecord],
        idempotency_key: str,
        source: TransactionSource,
        description: str = "",
        metadata: Dict[str, Any] = None,
    ) -> TransactionRecord:
        """
        Record asset table inserts without journal entries.

        Use this for reference data (AMCs, brokers) or non-financial records.

        Args:
            user_id: User ID
            asset_records: List of AssetRecord to insert
            idempotency_key: Unique key for deduplication
            source: Source of transaction
            description: Optional description
            metadata: Optional metadata

        Returns:
            TransactionRecord with asset_record_ids
        """
        return self.record(
            user_id=user_id,
            entries=[],  # No journal entries
            description=description,
            source=source,
            idempotency_key=idempotency_key,
            asset_records=asset_records,
            metadata=metadata,
        )

    def _is_duplicate(self, idempotency_key: str) -> bool:
        """Check if idempotency key already exists."""
        cursor = self.conn.execute(
            "SELECT 1 FROM processed_transactions WHERE idempotency_key = ?",
            (idempotency_key,)
        )
        return cursor.fetchone() is not None

    def get_transaction_by_key(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        """
        Get transaction details by idempotency key.

        Args:
            idempotency_key: The idempotency key

        Returns:
            Transaction details or None
        """
        cursor = self.conn.execute("""
            SELECT pt.*, j.date, j.description
            FROM processed_transactions pt
            LEFT JOIN journals j ON pt.journal_id = j.id
            WHERE pt.idempotency_key = ?
        """, (idempotency_key,))

        row = cursor.fetchone()
        if row:
            return dict(row)
        return None

    @require_user_context
    def reverse_transaction(
        self,
        user_id: int,
        idempotency_key: str,
        reason: str,
    ) -> TransactionRecord:
        """
        Reverse a previously recorded transaction.

        Args:
            user_id: User ID
            idempotency_key: Key of transaction to reverse
            reason: Reason for reversal

        Returns:
            TransactionRecord with reversal journal ID
        """
        # Get original transaction
        original = self.get_transaction_by_key(idempotency_key)
        if not original:
            return TransactionRecord(
                result=TransactionResult.VALIDATION_ERROR,
                error_message=f"Transaction not found: {idempotency_key}"
            )

        if original["user_id"] != user_id:
            raise UserContextError(
                f"User {user_id} cannot reverse transaction owned by user {original['user_id']}"
            )

        journal_id = original["journal_id"]
        if not journal_id:
            return TransactionRecord(
                result=TransactionResult.VALIDATION_ERROR,
                error_message="Original transaction has no journal entry"
            )

        # Create reversal
        try:
            reversal_id = self.journal_engine.reverse_journal(
                journal_id=journal_id,
                reversal_date=date.today(),
                description=f"Reversal: {reason}"
            )

            # Record reversal in processed_transactions
            reversal_key = f"reversal:{idempotency_key}:{date.today().isoformat()}"
            self.conn.execute("""
                INSERT INTO processed_transactions
                (idempotency_key, user_id, journal_id, source, metadata)
                VALUES (?, ?, ?, ?, ?)
            """, (
                reversal_key,
                user_id,
                reversal_id,
                "reversal",
                f'{{"original_key": "{idempotency_key}", "reason": "{reason}"}}'
            ))

            self.conn.commit()

            return TransactionRecord(
                result=TransactionResult.SUCCESS,
                journal_id=reversal_id,
                idempotency_key=reversal_key,
            )

        except Exception as e:
            self.conn.rollback()
            return TransactionRecord(
                result=TransactionResult.JOURNAL_ERROR,
                error_message=str(e)
            )


class IdempotencyKeyGenerator:
    """
    Generates deterministic idempotency keys for various transaction types.

    Keys are designed to be unique within a transaction type but consistent
    for the same logical transaction (enabling deduplication).
    """

    @staticmethod
    def mf_transaction(
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

    @staticmethod
    def stock_trade(
        broker: str,
        trade_id: str,
    ) -> str:
        """
        Generate idempotency key for stock trade.

        Format: stock:{broker}:{trade_id}
        """
        return f"stock:{broker}:{trade_id}"

    @staticmethod
    def bank_transaction(
        account_number: str,
        txn_date: date,
        ref_no: str,
        amount: Decimal,
    ) -> str:
        """
        Generate idempotency key for bank transaction.

        Format: bank:{account}:{date}:{ref_no}:{amount}
        """
        # Hash account number for privacy
        account_hash = hashlib.md5(account_number.encode()).hexdigest()[:8]
        return f"bank:{account_hash}:{txn_date.isoformat()}:{ref_no}:{amount}"

    @staticmethod
    def salary_record(
        employer: str,
        pay_period: str,  # e.g., "2024-03"
        gross_amount: Decimal,
    ) -> str:
        """
        Generate idempotency key for salary record.

        Format: salary:{employer}:{period}:{gross}
        """
        return f"salary:{employer}:{pay_period}:{gross_amount}"

    @staticmethod
    def epf_contribution(
        uan: str,
        month: str,  # e.g., "2024-03"
        employee_amount: Decimal,
        employer_amount: Decimal,
    ) -> str:
        """
        Generate idempotency key for EPF contribution.

        Format: epf:{uan_hash}:{month}:{ee}:{er}
        """
        uan_hash = hashlib.md5(uan.encode()).hexdigest()[:8]
        return f"epf:{uan_hash}:{month}:{employee_amount}:{employer_amount}"

    @staticmethod
    def dividend(
        source_type: str,  # 'stock' or 'mf'
        symbol_or_scheme: str,
        record_date: date,
        amount: Decimal,
    ) -> str:
        """
        Generate idempotency key for dividend.

        Format: dividend:{type}:{symbol}:{date}:{amount}
        """
        return f"dividend:{source_type}:{symbol_or_scheme}:{record_date.isoformat()}:{amount}"

    @staticmethod
    def custom(prefix: str, *parts: Any) -> str:
        """
        Generate custom idempotency key.

        Args:
            prefix: Key prefix (e.g., 'rsu', 'espp')
            *parts: Variable parts to include in key

        Returns:
            Formatted idempotency key
        """
        parts_str = ":".join(str(p) for p in parts)
        return f"{prefix}:{parts_str}"

    @staticmethod
    def from_checksum(
        source_type: str,
        checksum: str,
        row_index: int = 0
    ) -> str:
        """
        Generate idempotency key from row checksum.

        Used by BaseParser for normalized records where the raw data
        checksum (MD5 hash) uniquely identifies the record.

        Args:
            source_type: Parser source type (e.g., 'CAMS', 'ZERODHA')
            checksum: MD5 hash of the raw row data
            row_index: Row index in source file

        Returns:
            Formatted idempotency key

        Format: normalized:{source}:{checksum}:{row}
        """
        return f"normalized:{source_type.lower()}:{checksum}:{row_index}"
