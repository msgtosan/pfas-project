"""
MF Statement Ingester - Idempotent ingestion of MF statements.

Orchestrates scanning, parsing, and database insertion with:
- File hash-based deduplication
- Transaction-level idempotency
- Ingestion logging for audit trail
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Callable

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

from .scanner import MFStatementScanner, ScannedFile, ScanResult, RTA, FileType
from .cams import CAMSParser
from .karvy import KarvyParser
from .models import ParseResult
from pfas.services.encrypted_file_handler import EncryptedFileHandler

logger = logging.getLogger(__name__)


@dataclass
class IngestionResult:
    """Result of ingestion process."""
    success: bool = True
    files_processed: int = 0
    files_skipped: int = 0
    records_inserted: int = 0
    records_skipped: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    processed_files: List[Path] = field(default_factory=list)

    def add_error(self, msg: str):
        """Add error and mark as failed."""
        self.errors.append(msg)
        self.success = False

    def add_warning(self, msg: str):
        """Add warning."""
        self.warnings.append(msg)


class MFIngester:
    """
    Orchestrates MF statement ingestion with idempotency.

    Process:
    1. Scan inbox for new files
    2. Check ingestion_log for already processed files (by hash)
    3. Parse new files using appropriate parser (CAMS/Karvy)
    4. Insert records with duplicate detection
    5. Log ingestion status

    Usage:
        ingester = MFIngester(conn, user_id, inbox_path)
        result = ingester.ingest()

        print(f"Inserted {result.records_inserted} records")
        for file in result.processed_files:
            print(f"Processed: {file}")
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        user_id: int,
        inbox_path: Path,
        password_callback: Optional[Callable[[Path], str]] = None,
        encrypted_file_handler: Optional[EncryptedFileHandler] = None
    ):
        """
        Initialize ingester.

        Args:
            conn: Database connection
            user_id: User ID for records
            inbox_path: Path to inbox/Mutual-Fund/ folder
            password_callback: Optional callback for PDF passwords (deprecated, use encrypted_file_handler)
            encrypted_file_handler: Optional EncryptedFileHandler for password management
        """
        self.conn = conn
        self.user_id = user_id
        self.inbox_path = Path(inbox_path)
        self.password_callback = password_callback
        self.encrypted_file_handler = encrypted_file_handler

        # Initialize parsers
        self.cams_parser = CAMSParser(conn)
        self.karvy_parser = KarvyParser(conn)

    def ingest(self, force: bool = False) -> IngestionResult:
        """
        Run full ingestion pipeline.

        Args:
            force: If True, reprocess files even if already ingested

        Returns:
            IngestionResult with processing statistics
        """
        result = IngestionResult()

        # Step 1: Scan inbox
        scanner = MFStatementScanner(self.inbox_path, self.password_callback)
        scan_result = scanner.scan()

        if not scan_result.success:
            result.errors.extend(scan_result.errors)
            result.success = False
            return result

        if not scan_result.files:
            result.warnings.append("No files found in inbox")
            return result

        logger.info(f"Found {len(scan_result.files)} files to process")

        # Step 2: Process each file
        for scanned_file in scan_result.files:
            try:
                file_result = self._process_file(scanned_file, force)

                if file_result.get('skipped'):
                    result.files_skipped += 1
                else:
                    result.files_processed += 1
                    result.records_inserted += file_result.get('inserted', 0)
                    result.records_skipped += file_result.get('duplicates', 0)
                    result.processed_files.append(scanned_file.path)

                if file_result.get('errors'):
                    result.warnings.extend(file_result['errors'])

            except Exception as e:
                result.add_error(f"Error processing {scanned_file.path.name}: {str(e)}")
                logger.exception(f"Error processing {scanned_file.path}")

        logger.info(
            f"Ingestion complete: {result.files_processed} files, "
            f"{result.records_inserted} records inserted"
        )

        return result

    def _process_file(self, scanned_file: ScannedFile, force: bool) -> dict:
        """
        Process a single scanned file.

        Args:
            scanned_file: ScannedFile object
            force: If True, reprocess even if already ingested

        Returns:
            Dict with processing statistics
        """
        result = {
            'skipped': False,
            'inserted': 0,
            'duplicates': 0,
            'errors': []
        }

        # Check if already processed (by file hash)
        if not force and self._is_already_processed(scanned_file.file_hash):
            logger.debug(f"Skipping already processed file: {scanned_file.path.name}")
            result['skipped'] = True
            return result

        # Create ingestion log entry
        log_id = self._create_ingestion_log(scanned_file)

        try:
            # Update status to PROCESSING
            self._update_ingestion_status(log_id, 'PROCESSING')

            # Get password if needed
            password = None
            if scanned_file.password_protected:
                # Try encrypted_file_handler first, then fall back to password_callback
                if self.encrypted_file_handler:
                    try:
                        password = self.encrypted_file_handler.get_password(scanned_file.path)
                        logger.debug(f"Retrieved password for {scanned_file.path.name} from encrypted file handler")
                    except Exception as e:
                        logger.warning(f"Failed to get password from handler: {e}")
                        password = None

                if password is None and self.password_callback:
                    password = self.password_callback(scanned_file.path)

                # Note: We don't fail if no password is found - the parser will try without password
                # and fail gracefully if the PDF is actually encrypted

            # Parse file using appropriate parser
            parse_result = self._parse_file(scanned_file, password)

            if not parse_result.success:
                result['errors'].extend(parse_result.errors)
                self._update_ingestion_status(
                    log_id, 'FAILED', '; '.join(parse_result.errors)
                )
                return result

            # Save to database
            if scanned_file.rta == RTA.CAMS:
                inserted = self.cams_parser.save_to_db(parse_result, self.user_id)
                duplicates = self.cams_parser.get_duplicate_count()
                self.cams_parser.reset_duplicate_count()
            else:
                inserted = self.karvy_parser.save_to_db(parse_result, self.user_id)
                duplicates = self.karvy_parser.get_duplicate_count()
                self.karvy_parser.reset_duplicate_count()

            result['inserted'] = inserted
            result['duplicates'] = duplicates
            result['errors'].extend(parse_result.warnings)

            # Update ingestion log
            self._update_ingestion_status(
                log_id, 'COMPLETED',
                records_processed=inserted,
                records_skipped=duplicates
            )

            logger.info(
                f"Processed {scanned_file.path.name}: "
                f"{inserted} inserted, {duplicates} duplicates"
            )

        except Exception as e:
            self._update_ingestion_status(log_id, 'FAILED', str(e))
            raise

        return result

    def _parse_file(self, scanned_file: ScannedFile, password: Optional[str]) -> ParseResult:
        """
        Parse file using appropriate parser.

        Args:
            scanned_file: ScannedFile object
            password: Optional password for PDFs

        Returns:
            ParseResult
        """
        if scanned_file.rta == RTA.CAMS:
            return self.cams_parser.parse(scanned_file.path, password)
        elif scanned_file.rta == RTA.KARVY:
            return self.karvy_parser.parse(scanned_file.path, password)
        else:
            # Try CAMS first, then Karvy
            result = self.cams_parser.parse(scanned_file.path, password)
            if result.success and result.transactions:
                return result

            result = self.karvy_parser.parse(scanned_file.path, password)
            if result.success and result.transactions:
                return result

            # Return empty result if neither worked
            return ParseResult(
                success=False,
                source_file=str(scanned_file.path),
                errors=[f"Could not parse file with either CAMS or Karvy parser"]
            )

    def _is_already_processed(self, file_hash: str) -> bool:
        """
        Check if file has already been processed.

        Args:
            file_hash: SHA256 hash of file

        Returns:
            True if already processed successfully
        """
        cursor = self.conn.execute(
            """
            SELECT id FROM ingestion_log
            WHERE user_id = ? AND file_hash = ? AND status = 'COMPLETED'
            """,
            (self.user_id, file_hash)
        )
        return cursor.fetchone() is not None

    def _create_ingestion_log(self, scanned_file: ScannedFile) -> int:
        """
        Create ingestion log entry.

        Args:
            scanned_file: ScannedFile object

        Returns:
            Log entry ID
        """
        cursor = self.conn.execute(
            """
            INSERT INTO ingestion_log
            (user_id, source_file, file_hash, asset_type, rta_source, status)
            VALUES (?, ?, ?, ?, ?, 'PENDING')
            ON CONFLICT(user_id, file_hash) DO UPDATE SET
                source_file = excluded.source_file,
                status = 'PENDING',
                created_at = CURRENT_TIMESTAMP
            RETURNING id
            """,
            (
                self.user_id,
                str(scanned_file.path),
                scanned_file.file_hash,
                'Mutual-Fund',
                scanned_file.rta.value
            )
        )
        row = cursor.fetchone()
        self.conn.commit()
        return row[0] if row else 0

    def _update_ingestion_status(
        self,
        log_id: int,
        status: str,
        error_message: Optional[str] = None,
        records_processed: int = 0,
        records_skipped: int = 0
    ):
        """
        Update ingestion log status.

        Args:
            log_id: Log entry ID
            status: New status
            error_message: Optional error message
            records_processed: Number of records processed
            records_skipped: Number of records skipped
        """
        completed_at = datetime.now().isoformat() if status in ('COMPLETED', 'FAILED') else None

        self.conn.execute(
            """
            UPDATE ingestion_log SET
                status = ?,
                error_message = ?,
                records_processed = ?,
                records_skipped = ?,
                completed_at = ?
            WHERE id = ?
            """,
            (status, error_message, records_processed, records_skipped, completed_at, log_id)
        )
        self.conn.commit()

    def get_ingestion_history(self, limit: int = 20) -> List[dict]:
        """
        Get recent ingestion history for user.

        Args:
            limit: Maximum number of entries

        Returns:
            List of ingestion log entries as dicts
        """
        cursor = self.conn.execute(
            """
            SELECT
                id, source_file, file_hash, asset_type, rta_source,
                records_processed, records_skipped, status, error_message,
                archive_path, created_at, completed_at
            FROM ingestion_log
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (self.user_id, limit)
        )

        return [dict(row) for row in cursor.fetchall()]

    def get_pending_files(self) -> List[dict]:
        """
        Get files that failed or are pending reprocessing.

        Returns:
            List of pending/failed ingestion entries
        """
        cursor = self.conn.execute(
            """
            SELECT
                id, source_file, file_hash, rta_source, status, error_message
            FROM ingestion_log
            WHERE user_id = ? AND status IN ('PENDING', 'FAILED')
            ORDER BY created_at DESC
            """,
            (self.user_id,)
        )

        return [dict(row) for row in cursor.fetchall()]


def ingest_mf_statements(
    conn: sqlite3.Connection,
    user_id: int,
    inbox_path: Path,
    password_callback: Optional[Callable[[Path], str]] = None,
    encrypted_file_handler: Optional[EncryptedFileHandler] = None,
    force: bool = False
) -> IngestionResult:
    """
    Convenience function to ingest MF statements.

    Args:
        conn: Database connection
        user_id: User ID
        inbox_path: Path to inbox/Mutual-Fund/
        password_callback: Optional callback for passwords (deprecated, use encrypted_file_handler)
        encrypted_file_handler: Optional EncryptedFileHandler for password management
        force: If True, reprocess all files

    Returns:
        IngestionResult

    Example:
        from pfas.core.paths import PathResolver
        from pfas.services.encrypted_file_handler import create_encrypted_file_handler

        resolver = PathResolver(root_path, "Sanjay")
        handler = create_encrypted_file_handler(resolver)
        result = ingest_mf_statements(conn, 1, resolver.inbox() / "Mutual-Fund", encrypted_file_handler=handler)
        print(f"Inserted {result.records_inserted} records")
    """
    ingester = MFIngester(conn, user_id, inbox_path, password_callback, encrypted_file_handler)
    return ingester.ingest(force)
