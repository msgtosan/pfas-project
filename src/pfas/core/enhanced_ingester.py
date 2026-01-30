"""
Enhanced Ingester - Statement-aware ingestion with hybrid detection.

Integrates:
- Universal Asset Scanner for file discovery
- Statement Type Detector for transactions/holdings routing
- Multi-file processing with FY detection and deduplication

Usage:
    from pfas.core.enhanced_ingester import EnhancedIngester
    from pfas.core.paths import PathResolver

    resolver = PathResolver(project_root, "Sanjay")
    ingester = EnhancedIngester(conn, resolver)

    # Ingest all MF statements (auto-detects type)
    result = ingester.ingest_asset("Mutual-Fund")

    print(f"Transactions: {result.transactions_processed}")
    print(f"Holdings: {result.holdings_processed}")
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

from pfas.core.asset_scanner import AssetScanner, AssetScanResult, ScannedAssetFile, AssetType
from pfas.core.statement_detector import StatementType, StatementTypeDetector
from pfas.core.file_processor import MultiFileProcessor

logger = logging.getLogger(__name__)


@dataclass
class EnhancedIngestionResult:
    """Result of enhanced ingestion with statement type breakdown."""
    success: bool = True
    asset_type: str = ""

    # File counts
    files_scanned: int = 0
    files_processed: int = 0
    files_skipped: int = 0
    files_failed: int = 0

    # Record counts by statement type
    transactions_processed: int = 0
    holdings_processed: int = 0
    duplicates_skipped: int = 0

    # Detailed tracking
    processed_files: List[Path] = field(default_factory=list)
    failed_files: List[Path] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    # By financial year
    by_fy: Dict[str, Dict[str, int]] = field(default_factory=dict)

    def add_error(self, msg: str):
        self.errors.append(msg)
        self.success = False

    def add_warning(self, msg: str):
        self.warnings.append(msg)

    def add_fy_stats(self, fy: str, txn_count: int, holdings_count: int):
        if fy not in self.by_fy:
            self.by_fy[fy] = {"transactions": 0, "holdings": 0}
        self.by_fy[fy]["transactions"] += txn_count
        self.by_fy[fy]["holdings"] += holdings_count


class EnhancedIngester:
    """
    Enhanced ingester with statement type awareness.

    Features:
    - Automatic transactions/holdings detection
    - Routes to appropriate database tables
    - Multi-file FY grouping and deduplication
    - Failed file handling
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        path_resolver,
        password_callback: Optional[Callable[[Path], str]] = None
    ):
        """
        Initialize enhanced ingester.

        Args:
            conn: Database connection
            path_resolver: PathResolver instance
            password_callback: Optional callback for PDF passwords
        """
        self.conn = conn
        self.path_resolver = path_resolver
        self.password_callback = password_callback

        # Initialize scanner with statement detection
        self.scanner = AssetScanner(path_resolver)
        self.statement_detector = StatementTypeDetector(path_resolver)

        # Get or create user ID
        self.user_id = self._get_or_create_user()

    def _get_or_create_user(self) -> int:
        """Get or create user record."""
        user_name = self.path_resolver.user_name

        cursor = self.conn.execute(
            "SELECT id FROM users WHERE name = ?",
            (user_name,)
        )
        row = cursor.fetchone()

        if row:
            return row[0] if isinstance(row, tuple) else row["id"]

        # Create user
        cursor = self.conn.execute(
            "INSERT INTO users (name) VALUES (?) RETURNING id",
            (user_name,)
        )
        row = cursor.fetchone()
        self.conn.commit()
        return row[0] if row else 0

    def ingest_asset(
        self,
        asset_type: str | AssetType,
        force: bool = False,
        transactions_only: bool = False,
        holdings_only: bool = False
    ) -> EnhancedIngestionResult:
        """
        Ingest all statements for an asset type.

        Args:
            asset_type: Asset type to ingest
            force: Re-process already ingested files
            transactions_only: Only process transaction statements
            holdings_only: Only process holdings statements

        Returns:
            EnhancedIngestionResult with detailed statistics
        """
        result = EnhancedIngestionResult(asset_type=str(asset_type))

        # Scan for files
        scan_result = self.scanner.scan_asset(asset_type)
        result.files_scanned = scan_result.total_scanned

        if not scan_result.success:
            result.errors.extend(scan_result.errors)
            result.success = False
            return result

        if not scan_result.files:
            result.add_warning(f"No files found for {asset_type}")
            return result

        # Filter by statement type if requested
        files_to_process = scan_result.files
        if transactions_only:
            files_to_process = scan_result.transaction_files
        elif holdings_only:
            files_to_process = scan_result.holdings_files

        # Sort by date for chronological processing
        files_to_process = sorted(
            files_to_process,
            key=lambda f: MultiFileProcessor.detect_financial_year(f.path)
        )

        logger.info(
            f"Processing {len(files_to_process)} files for {asset_type} "
            f"({len(scan_result.transaction_files)} txn, {len(scan_result.holdings_files)} holdings)"
        )

        # Process each file
        for scanned_file in files_to_process:
            try:
                file_result = self._process_file(scanned_file, force)

                if file_result.get("skipped"):
                    result.files_skipped += 1
                elif file_result.get("failed"):
                    result.files_failed += 1
                    result.failed_files.append(scanned_file.path)
                else:
                    result.files_processed += 1
                    result.processed_files.append(scanned_file.path)

                    # Track by statement type
                    if scanned_file.is_transaction:
                        result.transactions_processed += file_result.get("records", 0)
                    else:
                        result.holdings_processed += file_result.get("records", 0)

                    result.duplicates_skipped += file_result.get("duplicates", 0)

                    # Track by FY
                    fy = scanned_file.financial_year or "UNKNOWN"
                    txn_count = file_result.get("records", 0) if scanned_file.is_transaction else 0
                    holdings_count = file_result.get("records", 0) if scanned_file.is_holding else 0
                    result.add_fy_stats(fy, txn_count, holdings_count)

                if file_result.get("warnings"):
                    result.warnings.extend(file_result["warnings"])

            except Exception as e:
                result.add_error(f"Error processing {scanned_file.path.name}: {str(e)}")
                result.failed_files.append(scanned_file.path)
                logger.exception(f"Error processing {scanned_file.path}")

        # Log summary
        logger.info(
            f"Ingestion complete for {asset_type}: "
            f"{result.files_processed} files, "
            f"{result.transactions_processed} txns, "
            f"{result.holdings_processed} holdings"
        )

        return result

    def _process_file(
        self,
        scanned_file: ScannedAssetFile,
        force: bool
    ) -> Dict[str, Any]:
        """
        Process a single scanned file.

        Routes to appropriate handler based on statement type.
        """
        result = {
            "skipped": False,
            "failed": False,
            "records": 0,
            "duplicates": 0,
            "warnings": []
        }

        # Check if already processed
        if not force and self._is_already_processed(scanned_file.file_hash):
            logger.debug(f"Skipping already processed: {scanned_file.path.name}")
            result["skipped"] = True
            return result

        # Create ingestion log
        log_id = self._create_ingestion_log(scanned_file)

        try:
            self._update_ingestion_status(log_id, "PROCESSING")

            # Route based on asset type and statement type
            if scanned_file.is_transaction:
                records, duplicates = self._process_transactions(scanned_file)
            else:
                records, duplicates = self._process_holdings(scanned_file)

            result["records"] = records
            result["duplicates"] = duplicates

            self._update_ingestion_status(
                log_id, "COMPLETED",
                records_processed=records,
                records_skipped=duplicates
            )

            logger.info(
                f"Processed {scanned_file.path.name}: "
                f"{records} records ({scanned_file.statement_type.value})"
            )

        except Exception as e:
            self._update_ingestion_status(log_id, "FAILED", error_message=str(e))
            result["failed"] = True
            result["warnings"].append(str(e))

            # Move to failed folder
            self.scanner.move_to_failed(scanned_file.path)

        return result

    def _process_transactions(self, scanned_file: ScannedAssetFile) -> tuple:
        """Process a transaction statement file."""
        # Import appropriate parser based on asset type
        asset_type = scanned_file.asset_type

        if asset_type == AssetType.MUTUAL_FUND:
            return self._process_mf_transactions(scanned_file)
        elif asset_type in (AssetType.INDIAN_STOCKS, AssetType.USA_STOCKS):
            return self._process_stock_transactions(scanned_file)
        elif asset_type == AssetType.BANK:
            return self._process_bank_transactions(scanned_file)
        else:
            # Generic processing
            return self._process_generic_transactions(scanned_file)

    def _process_holdings(self, scanned_file: ScannedAssetFile) -> tuple:
        """Process a holdings statement file."""
        asset_type = scanned_file.asset_type

        if asset_type == AssetType.MUTUAL_FUND:
            return self._process_mf_holdings(scanned_file)
        elif asset_type in (AssetType.INDIAN_STOCKS, AssetType.USA_STOCKS):
            return self._process_stock_holdings(scanned_file)
        else:
            return self._process_generic_holdings(scanned_file)

    def _process_mf_transactions(self, scanned_file: ScannedAssetFile) -> tuple:
        """Process MF transaction statement."""
        from pfas.parsers.mf.cams import CAMSParser
        from pfas.parsers.mf.karvy import KarvyParser

        # Detect RTA from path/content
        path_parts = [p.upper() for p in scanned_file.path.parts]

        if "CAMS" in path_parts:
            parser = CAMSParser(self.conn)
        elif "KARVY" in path_parts or "KFINTECH" in path_parts:
            parser = KarvyParser(self.conn)
        else:
            # Try CAMS first
            parser = CAMSParser(self.conn)

        result = parser.parse(scanned_file.path)

        if result.success:
            records = parser.save_to_db(result, self.user_id)
            duplicates = getattr(parser, 'get_duplicate_count', lambda: 0)()
            return records, duplicates

        return 0, 0

    def _process_mf_holdings(self, scanned_file: ScannedAssetFile) -> tuple:
        """Process MF holdings statement."""
        # Holdings go to mf_holdings table
        # Implementation depends on your holdings parser
        logger.info(f"Processing MF holdings: {scanned_file.path.name}")
        return 0, 0  # Placeholder

    def _process_stock_transactions(self, scanned_file: ScannedAssetFile) -> tuple:
        """Process stock transaction statement."""
        logger.info(f"Processing stock transactions: {scanned_file.path.name}")
        return 0, 0  # Placeholder - implement with stock parser

    def _process_stock_holdings(self, scanned_file: ScannedAssetFile) -> tuple:
        """Process stock holdings statement."""
        logger.info(f"Processing stock holdings: {scanned_file.path.name}")
        return 0, 0  # Placeholder

    def _process_bank_transactions(self, scanned_file: ScannedAssetFile) -> tuple:
        """Process bank statement transactions."""
        logger.info(f"Processing bank transactions: {scanned_file.path.name}")
        return 0, 0  # Placeholder

    def _process_generic_transactions(self, scanned_file: ScannedAssetFile) -> tuple:
        """Generic transaction processing."""
        logger.info(f"Processing generic transactions: {scanned_file.path.name}")
        return 0, 0

    def _process_generic_holdings(self, scanned_file: ScannedAssetFile) -> tuple:
        """Generic holdings processing."""
        logger.info(f"Processing generic holdings: {scanned_file.path.name}")
        return 0, 0

    def _is_already_processed(self, file_hash: str) -> bool:
        """Check if file was already processed successfully."""
        cursor = self.conn.execute(
            """
            SELECT id FROM ingestion_log
            WHERE user_id = ? AND file_hash = ? AND status = 'COMPLETED'
            """,
            (self.user_id, file_hash)
        )
        return cursor.fetchone() is not None

    def _create_ingestion_log(self, scanned_file: ScannedAssetFile) -> int:
        """Create ingestion log entry."""
        cursor = self.conn.execute(
            """
            INSERT INTO ingestion_log
            (user_id, source_file, file_hash, asset_type, statement_type, status)
            VALUES (?, ?, ?, ?, ?, 'PENDING')
            ON CONFLICT(user_id, file_hash) DO UPDATE SET
                source_file = excluded.source_file,
                statement_type = excluded.statement_type,
                status = 'PENDING',
                created_at = CURRENT_TIMESTAMP
            RETURNING id
            """,
            (
                self.user_id,
                str(scanned_file.path),
                scanned_file.file_hash,
                scanned_file.asset_type.value,
                scanned_file.statement_type.value
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
        completed_at = datetime.now().isoformat() if status in ("COMPLETED", "FAILED") else None

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


def ingest_with_detection(
    conn: sqlite3.Connection,
    path_resolver,
    asset_type: str,
    force: bool = False
) -> EnhancedIngestionResult:
    """
    Convenience function for statement-aware ingestion.

    Args:
        conn: Database connection
        path_resolver: PathResolver instance
        asset_type: Asset type to ingest
        force: Re-process all files

    Returns:
        EnhancedIngestionResult
    """
    ingester = EnhancedIngester(conn, path_resolver)
    return ingester.ingest_asset(asset_type, force=force)
