"""
Batch Ingester for PFAS.

Provides atomic batch ingestion with rollback on partial failure.
All files in a batch are processed as a single unit - either all succeed or all fail.
"""

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional, Dict, Any, Callable, Type
import sqlite3

from pfas.core.exceptions import BatchIngestionError, PFASError
from pfas.core.audit import AuditLogger
from pfas.core.security import require_user_context

logger = logging.getLogger(__name__)


class FileStatus(Enum):
    """Status of individual file processing."""

    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"  # e.g., duplicate file


@dataclass
class FileResult:
    """Result of processing a single file."""

    file_path: Path
    status: FileStatus
    records_processed: int = 0
    records_skipped: int = 0
    error_message: Optional[str] = None
    file_hash: Optional[str] = None
    processing_time_ms: int = 0


@dataclass
class BatchResult:
    """Result of batch ingestion."""

    success: bool
    total_files: int
    files_processed: int
    files_failed: int
    files_skipped: int
    total_records: int
    file_results: List[FileResult] = field(default_factory=list)
    error_message: Optional[str] = None
    batch_id: Optional[str] = None
    processing_time_ms: int = 0

    def add_file_result(self, result: FileResult) -> None:
        """Add a file result to the batch."""
        self.file_results.append(result)
        if result.status == FileStatus.SUCCESS:
            self.files_processed += 1
            self.total_records += result.records_processed
        elif result.status == FileStatus.FAILED:
            self.files_failed += 1
        elif result.status == FileStatus.SKIPPED:
            self.files_skipped += 1


class BatchIngester:
    """
    Batch file ingester with atomic transaction support.

    All files in a batch are processed within a single database transaction.
    If any file fails, the entire batch is rolled back.

    Features:
    - Atomic batch processing (all or nothing)
    - File-level deduplication via MD5 hash
    - Progress tracking and detailed results
    - Audit logging for compliance

    Usage:
        ingester = BatchIngester(conn, user_id=1)

        # Register parsers for file types
        ingester.register_parser(".xlsx", CAMSParser)
        ingester.register_parser(".pdf", CAMSParser)
        ingester.register_parser(".csv", ZerodhaParser)

        # Process batch
        files = list(Path("inbox").glob("*"))
        result = ingester.ingest_batch(files)

        if result.success:
            print(f"Processed {result.total_records} records from {result.files_processed} files")
        else:
            print(f"Batch failed: {result.error_message}")
    """

    def __init__(
        self,
        db_connection: sqlite3.Connection,
        user_id: int,
        audit_source: str = "batch_ingester"
    ):
        """
        Initialize batch ingester.

        Args:
            db_connection: Database connection
            user_id: User ID for all operations
            audit_source: Source identifier for audit logs
        """
        self.conn = db_connection
        self.user_id = user_id
        self.audit_source = audit_source
        self._parsers: Dict[str, Type] = {}
        self._ensure_tables_exist()

    def _ensure_tables_exist(self) -> None:
        """Ensure required tables exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_hash TEXT UNIQUE NOT NULL,
                file_name TEXT NOT NULL,
                file_path TEXT,
                file_size INTEGER,
                user_id INTEGER NOT NULL,
                batch_id TEXT,
                parser_type TEXT,
                records_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'success',
                error_message TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_files_hash
            ON processed_files(file_hash)
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS batch_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT UNIQUE NOT NULL,
                user_id INTEGER NOT NULL,
                files_count INTEGER,
                records_count INTEGER,
                status TEXT DEFAULT 'pending',
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT
            )
        """)

        self.conn.commit()

    def register_parser(
        self,
        extension: str,
        parser_class: Type,
        parser_kwargs: Dict[str, Any] = None
    ) -> None:
        """
        Register a parser for a file extension.

        Args:
            extension: File extension (e.g., '.xlsx', '.pdf')
            parser_class: Parser class to use
            parser_kwargs: Additional kwargs to pass to parser constructor
        """
        ext = extension.lower() if extension.startswith(".") else f".{extension.lower()}"
        self._parsers[ext] = (parser_class, parser_kwargs or {})
        logger.debug(f"Registered parser {parser_class.__name__} for {ext}")

    def get_parser(self, file_path: Path) -> Optional[Any]:
        """
        Get parser instance for a file.

        Args:
            file_path: Path to file

        Returns:
            Parser instance or None if no parser registered
        """
        ext = file_path.suffix.lower()
        if ext not in self._parsers:
            return None

        parser_class, kwargs = self._parsers[ext]
        return parser_class(self.conn, **kwargs)

    def calculate_file_hash(self, file_path: Path) -> str:
        """
        Calculate MD5 hash of file contents.

        Args:
            file_path: Path to file

        Returns:
            MD5 hash string
        """
        hasher = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def is_file_processed(self, file_hash: str) -> bool:
        """
        Check if file has already been processed.

        Args:
            file_hash: MD5 hash of file

        Returns:
            True if already processed
        """
        cursor = self.conn.execute(
            "SELECT 1 FROM processed_files WHERE file_hash = ? AND status = 'success'",
            (file_hash,)
        )
        return cursor.fetchone() is not None

    @require_user_context
    def ingest_batch(
        self,
        files: List[Path],
        user_id: int = None,
        stop_on_error: bool = True,
        dry_run: bool = False
    ) -> BatchResult:
        """
        Ingest a batch of files atomically.

        All files are processed within a single transaction. If any file fails
        and stop_on_error is True, the entire batch is rolled back.

        Args:
            files: List of file paths to process
            user_id: User ID (validated by decorator, uses self.user_id if not provided)
            stop_on_error: Stop and rollback on first error (default: True)
            dry_run: Validate without committing (default: False)

        Returns:
            BatchResult with processing details

        Raises:
            BatchIngestionError: If batch fails and stop_on_error is True
        """
        if user_id is None:
            user_id = self.user_id

        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{user_id}"
        start_time = datetime.now()

        result = BatchResult(
            success=False,
            total_files=len(files),
            files_processed=0,
            files_failed=0,
            files_skipped=0,
            total_records=0,
            batch_id=batch_id
        )

        if not files:
            result.success = True
            result.error_message = "No files to process"
            return result

        # Record batch start
        self.conn.execute("""
            INSERT INTO batch_runs (batch_id, user_id, files_count, status)
            VALUES (?, ?, ?, 'processing')
        """, (batch_id, user_id, len(files)))

        cursor = self.conn.cursor()

        try:
            # Begin atomic transaction
            cursor.execute("BEGIN IMMEDIATE")

            for file_path in files:
                file_start = datetime.now()
                file_result = self._process_single_file(
                    file_path=file_path,
                    user_id=user_id,
                    batch_id=batch_id
                )

                file_result.processing_time_ms = int(
                    (datetime.now() - file_start).total_seconds() * 1000
                )
                result.add_file_result(file_result)

                if file_result.status == FileStatus.FAILED and stop_on_error:
                    raise BatchIngestionError(
                        f"File processing failed: {file_path}",
                        failed_files=[str(file_path)]
                    )

            # All files processed successfully
            if dry_run:
                self.conn.rollback()
                result.success = True
                result.error_message = "Dry run - changes rolled back"
            else:
                self.conn.commit()
                result.success = True

            # Update batch status
            self.conn.execute("""
                UPDATE batch_runs
                SET status = ?, completed_at = CURRENT_TIMESTAMP, records_count = ?
                WHERE batch_id = ?
            """, ("success" if result.success else "failed", result.total_records, batch_id))
            self.conn.commit()

        except BatchIngestionError as e:
            self.conn.rollback()
            result.success = False
            result.error_message = str(e)

            # Update batch status
            self.conn.execute("""
                UPDATE batch_runs
                SET status = 'failed', completed_at = CURRENT_TIMESTAMP, error_message = ?
                WHERE batch_id = ?
            """, (str(e), batch_id))
            self.conn.commit()

            logger.error(f"Batch {batch_id} failed: {e}")

        except Exception as e:
            self.conn.rollback()
            result.success = False
            result.error_message = f"Unexpected error: {e}"

            self.conn.execute("""
                UPDATE batch_runs
                SET status = 'failed', completed_at = CURRENT_TIMESTAMP, error_message = ?
                WHERE batch_id = ?
            """, (str(e), batch_id))
            self.conn.commit()

            logger.exception(f"Batch {batch_id} failed with unexpected error")

        result.processing_time_ms = int(
            (datetime.now() - start_time).total_seconds() * 1000
        )

        return result

    def _process_single_file(
        self,
        file_path: Path,
        user_id: int,
        batch_id: str
    ) -> FileResult:
        """
        Process a single file within the batch transaction.

        Args:
            file_path: Path to file
            user_id: User ID
            batch_id: Batch identifier

        Returns:
            FileResult with processing details
        """
        file_path = Path(file_path)

        if not file_path.exists():
            return FileResult(
                file_path=file_path,
                status=FileStatus.FAILED,
                error_message=f"File not found: {file_path}"
            )

        # Calculate hash for deduplication
        try:
            file_hash = self.calculate_file_hash(file_path)
        except Exception as e:
            return FileResult(
                file_path=file_path,
                status=FileStatus.FAILED,
                error_message=f"Failed to hash file: {e}"
            )

        # Check if already processed
        if self.is_file_processed(file_hash):
            logger.info(f"Skipping duplicate file: {file_path.name}")
            return FileResult(
                file_path=file_path,
                status=FileStatus.SKIPPED,
                file_hash=file_hash,
                error_message="File already processed (duplicate hash)"
            )

        # Get parser
        parser = self.get_parser(file_path)
        if parser is None:
            return FileResult(
                file_path=file_path,
                status=FileStatus.FAILED,
                file_hash=file_hash,
                error_message=f"No parser registered for {file_path.suffix}"
            )

        # Parse file
        try:
            parse_result = parser.parse(file_path)

            if not parse_result.success:
                return FileResult(
                    file_path=file_path,
                    status=FileStatus.FAILED,
                    file_hash=file_hash,
                    error_message="; ".join(parse_result.errors) if hasattr(parse_result, 'errors') else "Parse failed"
                )

            records_count = len(parse_result.transactions) if hasattr(parse_result, 'transactions') else 0

            # Record successful processing
            self.conn.execute("""
                INSERT INTO processed_files
                (file_hash, file_name, file_path, file_size, user_id, batch_id, parser_type, records_count, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'success')
            """, (
                file_hash,
                file_path.name,
                str(file_path),
                file_path.stat().st_size,
                user_id,
                batch_id,
                parser.__class__.__name__,
                records_count
            ))

            # Log audit entry
            audit_logger = AuditLogger(self.conn, user_id=user_id, source=self.audit_source)
            audit_logger.log_insert(
                table_name="processed_files",
                record_id=0,  # Will be set by database
                new_values={
                    "file_name": file_path.name,
                    "file_hash": file_hash,
                    "records_count": records_count,
                    "batch_id": batch_id
                }
            )

            return FileResult(
                file_path=file_path,
                status=FileStatus.SUCCESS,
                records_processed=records_count,
                file_hash=file_hash
            )

        except Exception as e:
            logger.exception(f"Failed to parse {file_path}: {e}")
            return FileResult(
                file_path=file_path,
                status=FileStatus.FAILED,
                file_hash=file_hash,
                error_message=str(e)
            )

    def get_batch_history(
        self,
        user_id: int = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get batch processing history.

        Args:
            user_id: Filter by user (optional)
            limit: Maximum records to return

        Returns:
            List of batch run records
        """
        query = "SELECT * FROM batch_runs"
        params = []

        if user_id:
            query += " WHERE user_id = ?"
            params.append(user_id)

        query += " ORDER BY started_at DESC LIMIT ?"
        params.append(limit)

        cursor = self.conn.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_processed_files(
        self,
        batch_id: str = None,
        user_id: int = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get processed files history.

        Args:
            batch_id: Filter by batch (optional)
            user_id: Filter by user (optional)
            limit: Maximum records to return

        Returns:
            List of processed file records
        """
        query = "SELECT * FROM processed_files WHERE 1=1"
        params = []

        if batch_id:
            query += " AND batch_id = ?"
            params.append(batch_id)

        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)

        query += " ORDER BY processed_at DESC LIMIT ?"
        params.append(limit)

        cursor = self.conn.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
