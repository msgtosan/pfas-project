"""
Generic Asset Ingester - Common ingestion logic for all asset types.

Provides a base class that all asset-specific ingesters can inherit from.
"""

import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any, Callable

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

logger = logging.getLogger(__name__)


@dataclass
class FileProcessingError:
    """Details of a file processing failure."""
    file_path: Path
    file_name: str
    error_message: str
    error_type: str  # 'PARSE', 'VALIDATION', 'INGESTION', 'EXCEPTION'
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class GenericIngestionResult:
    """Result of generic ingestion process."""
    success: bool = True
    asset_type: str = ""
    files_processed: int = 0
    files_succeeded: int = 0
    files_failed: int = 0
    files_skipped: int = 0
    records_inserted: int = 0
    records_skipped: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    succeeded_files: List[Path] = field(default_factory=list)
    failed_files: List[FileProcessingError] = field(default_factory=list)
    skipped_files: List[Path] = field(default_factory=list)

    def add_error(self, msg: str):
        """Add error and mark as failed."""
        self.errors.append(msg)
        self.success = False

    def add_warning(self, msg: str):
        """Add warning."""
        self.warnings.append(msg)

    def add_failed_file(self, file_path: Path, error_message: str, error_type: str = 'EXCEPTION'):
        """Add a failed file with details."""
        self.failed_files.append(FileProcessingError(
            file_path=file_path,
            file_name=file_path.name,
            error_message=error_message,
            error_type=error_type
        ))
        self.files_failed += 1

    def print_summary(self):
        """Print detailed summary to console."""
        print(f"\n{'='*70}")
        print(f"  {self.asset_type} Ingestion Summary")
        print(f"{'='*70}")
        print(f"  ✓ Success: {self.files_succeeded} files archived")
        if self.files_failed > 0:
            print(f"  ✗ Failed:  {self.files_failed} files (kept in inbox/failed/)")
            for failed in self.failed_files:
                print(f"     - {failed.file_name}: {failed.error_message}")
        if self.files_skipped > 0:
            print(f"  ⊘ Skipped: {self.files_skipped} files (already processed)")
        print(f"\n  Records: {self.records_inserted} inserted, {self.records_skipped} duplicates")

        if self.failed_files:
            print(f"\n  💡 Re-run after fixing issues:")
            print(f"     pfas --user <username> ingest --asset {self.asset_type} --force")
        print(f"{'='*70}\n")


class GenericAssetIngester(ABC):
    """
    Base class for asset-specific ingesters.

    Subclasses implement:
    - get_supported_extensions()
    - detect_source_from_path()
    - parse_file()
    - save_to_db()
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        user_id: int,
        inbox_path: Path,
        asset_type: str
    ):
        """
        Initialize generic ingester.

        Args:
            conn: Database connection
            user_id: User ID
            inbox_path: Path to inbox folder for this asset
            asset_type: Asset type name (e.g., "Bank", "Indian-Stocks")
        """
        self.conn = conn
        self.user_id = user_id
        self.inbox_path = Path(inbox_path)
        self.asset_type = asset_type

    @abstractmethod
    def get_supported_extensions(self) -> List[str]:
        """Return list of supported file extensions (e.g., ['.xls', '.xlsx', '.pdf'])."""
        pass

    @abstractmethod
    def detect_source_from_path(self, file_path: Path) -> Optional[str]:
        """
        Detect source/institution from file path.

        Args:
            file_path: Path to file

        Returns:
            Source name (e.g., "ICICI", "Zerodha") or None
        """
        pass

    @abstractmethod
    def parse_file(self, file_path: Path, source: Optional[str]) -> Dict[str, Any]:
        """
        Parse a single file.

        Args:
            file_path: Path to file
            source: Detected source

        Returns:
            Dict with 'success', 'records', 'errors' keys
        """
        pass

    @abstractmethod
    def save_to_db(self, records: List[Any]) -> int:
        """
        Save parsed records to database.

        Args:
            records: List of parsed records

        Returns:
            Number of records inserted
        """
        pass

    def ingest(self, force: bool = False, move_failed: bool = True) -> GenericIngestionResult:
        """
        Run full ingestion pipeline with failure-safe handling.

        Files are only archived on COMPLETE SUCCESS. On any failure:
        - File stays in inbox (or moves to inbox/failed/)
        - Error is logged with details
        - Summary shows what failed and why

        Args:
            force: If True, reprocess files even if already ingested
            move_failed: If True, move failed files to inbox/failed/ subdirectory

        Returns:
            GenericIngestionResult
        """
        result = GenericIngestionResult(asset_type=self.asset_type)

        if not self.inbox_path.exists():
            result.add_error(f"Inbox path does not exist: {self.inbox_path}")
            return result

        # Find files
        files = self._find_files()
        if not files:
            result.add_warning("No files found in inbox")
            return result

        logger.info(f"Found {len(files)} files to process in {self.inbox_path}")

        # Process each file
        for file_path in files:
            try:
                file_result = self._process_file(file_path, force)

                if file_result.get('skipped'):
                    result.files_skipped += 1
                    result.skipped_files.append(file_path)
                elif file_result.get('success'):
                    # SUCCESS - Only mark as succeeded if parsing and ingestion both worked
                    result.files_processed += 1
                    result.files_succeeded += 1
                    result.records_inserted += file_result.get('inserted', 0)
                    result.records_skipped += file_result.get('duplicates', 0)
                    result.succeeded_files.append(file_path)
                    logger.info(f"✓ Successfully processed: {file_path.name}")
                else:
                    # FAILURE - Parse error, validation error, or ingestion failed
                    result.files_processed += 1
                    error_msg = '; '.join(file_result.get('errors', ['Unknown error']))
                    error_type = file_result.get('error_type', 'EXCEPTION')
                    result.add_failed_file(file_path, error_msg, error_type)

                    # Move to failed subfolder if requested
                    if move_failed:
                        self._move_to_failed(file_path)

                    logger.error(f"✗ Failed to process: {file_path.name} - {error_msg}")

            except Exception as e:
                # EXCEPTION - Unexpected error during processing
                result.files_processed += 1
                error_msg = f"{type(e).__name__}: {str(e)}"
                result.add_failed_file(file_path, error_msg, 'EXCEPTION')

                # Move to failed subfolder if requested
                if move_failed:
                    self._move_to_failed(file_path)

                logger.exception(f"✗ Exception processing {file_path.name}")

        logger.info(
            f"Ingestion complete: {result.files_succeeded} succeeded, "
            f"{result.files_failed} failed, {result.files_skipped} skipped, "
            f"{result.records_inserted} records inserted"
        )

        return result

    def _find_files(self) -> List[Path]:
        """Find all supported files in inbox, excluding failed/ subdirectory."""
        files = []
        extensions = self.get_supported_extensions()

        for ext in extensions:
            all_files = self.inbox_path.rglob(f"*{ext}")
            # Exclude files in failed/ subdirectory
            files.extend([f for f in all_files if 'failed' not in f.parts])

        # Sort by modification time (newest first)
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

        return files

    def _process_file(self, file_path: Path, force: bool) -> Dict[str, Any]:
        """
        Process a single file.

        Returns dict with:
            - 'success': True if fully successful, False if any error
            - 'skipped': True if file was skipped (already processed)
            - 'inserted': Number of records inserted
            - 'duplicates': Number of duplicate records
            - 'errors': List of error messages
            - 'error_type': Type of error (PARSE, VALIDATION, INGESTION, EXCEPTION)
        """
        result = {
            'success': False,
            'skipped': False,
            'inserted': 0,
            'duplicates': 0,
            'errors': [],
            'error_type': None
        }

        # Calculate file hash
        file_hash = self._calculate_file_hash(file_path)

        # Check if already processed
        if not force and self._is_already_processed(file_hash):
            logger.debug(f"Skipping already processed file: {file_path.name}")
            result['skipped'] = True
            result['success'] = False  # Skipped is not success
            return result

        # Create ingestion log entry
        log_id = self._create_ingestion_log(file_path, file_hash)

        try:
            # Update status to PROCESSING
            self._update_ingestion_status(log_id, 'PROCESSING')

            # Detect source
            source = self.detect_source_from_path(file_path)

            # Parse file
            parse_result = self.parse_file(file_path, source)

            if not parse_result.get('success'):
                # Parse failure
                result['errors'].extend(parse_result.get('errors', ['Parse failed']))
                result['error_type'] = 'PARSE'
                result['success'] = False
                self._update_ingestion_status(
                    log_id, 'FAILED', '; '.join(result['errors'])
                )
                return result

            # Save to database
            records = parse_result.get('records', [])
            if records:
                try:
                    inserted = self.save_to_db(records)
                    result['inserted'] = inserted
                except Exception as e:
                    # Ingestion failure
                    result['errors'].append(f"Database ingestion failed: {str(e)}")
                    result['error_type'] = 'INGESTION'
                    result['success'] = False
                    self._update_ingestion_status(log_id, 'FAILED', str(e))
                    return result

            # Update ingestion log to COMPLETED
            self._update_ingestion_status(
                log_id, 'COMPLETED',
                records_processed=result['inserted'],
                records_skipped=result['duplicates']
            )

            # Mark as SUCCESS
            result['success'] = True

            logger.debug(
                f"Processed {file_path.name}: "
                f"{result['inserted']} inserted, {result['duplicates']} duplicates"
            )

        except Exception as e:
            # Unexpected exception
            result['errors'].append(str(e))
            result['error_type'] = 'EXCEPTION'
            result['success'] = False
            self._update_ingestion_status(log_id, 'FAILED', str(e))
            raise

        return result

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file."""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _is_already_processed(self, file_hash: str) -> bool:
        """Check if file has already been processed."""
        cursor = self.conn.execute(
            """
            SELECT id FROM ingestion_log
            WHERE user_id = ? AND file_hash = ? AND status = 'COMPLETED'
            """,
            (self.user_id, file_hash)
        )
        return cursor.fetchone() is not None

    def _create_ingestion_log(self, file_path: Path, file_hash: str) -> int:
        """Create ingestion log entry."""
        source = self.detect_source_from_path(file_path)

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
                str(file_path),
                file_hash,
                self.asset_type,
                source
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
        """Update ingestion log status."""
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

    def _move_to_failed(self, file_path: Path) -> Optional[Path]:
        """
        Move failed file to inbox/failed/ subdirectory with timestamp.

        Args:
            file_path: Path to failed file

        Returns:
            New path or None if move failed
        """
        try:
            # Create failed directory
            failed_dir = self.inbox_path / "failed"
            failed_dir.mkdir(exist_ok=True)

            # Generate timestamped filename
            from datetime import date
            today = date.today().isoformat()
            new_filename = f"{today}_{file_path.name}"
            new_path = failed_dir / new_filename

            # Handle duplicate names
            counter = 1
            while new_path.exists():
                stem = file_path.stem
                ext = file_path.suffix
                new_filename = f"{today}_{stem}_{counter}{ext}"
                new_path = failed_dir / new_filename
                counter += 1

            # Move file
            import shutil
            shutil.move(str(file_path), str(new_path))
            logger.info(f"Moved failed file to: {new_path.relative_to(self.inbox_path)}")

            return new_path

        except Exception as e:
            logger.error(f"Failed to move {file_path.name} to failed directory: {e}")
            return None
