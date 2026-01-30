"""
File Archiver - Moves processed files to archive with standardized naming.

Preserves directory structure and adds date prefix for organization.
Maintains temporal versioning manifest for audit and supersession tracking.
"""

import hashlib
import logging
import shutil
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

from pfas.core.manifest import (
    CategoryManifest,
    ManifestEntry,
    ExtractionMetadata,
    calculate_file_hash,
)
from pfas.core.uri_resolver import PFASURIResolver, create_uri

logger = logging.getLogger(__name__)


@dataclass
class ArchiveResult:
    """Result of archiving operation."""
    success: bool = True
    files_archived: int = 0
    files_failed: int = 0
    archived_paths: List[Path] = field(default_factory=list)
    archived_uris: List[str] = field(default_factory=list)
    manifest_entries: List[ManifestEntry] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def add_error(self, msg: str):
        """Add error message."""
        self.errors.append(msg)


class FileArchiver:
    """
    Archives processed files from inbox to archive folder.

    Naming convention:
    - inbox/Mutual-Fund/CAMS/cas.pdf
    - → archive/Mutual-Fund/CAMS/2026-01-17_Sanjay_CAMS_cas.pdf

    The archiver:
    1. Preserves the sub-folder structure (CAMS/KARVY/etc.)
    2. Adds date prefix (YYYY-MM-DD)
    3. Adds user name and source identifier
    4. Updates ingestion_log with archive_path
    5. Maintains manifest.json for temporal versioning (NEW)
    6. Generates portable URIs for database storage (NEW)

    Usage:
        archiver = FileArchiver(inbox_path, archive_path, "Sanjay")
        result = archiver.archive_file(Path("inbox/Mutual-Fund/CAMS/cas.pdf"))
    """

    def __init__(
        self,
        inbox_base: Path,
        archive_base: Path,
        user_name: str,
        conn: Optional[sqlite3.Connection] = None,
        data_root: Optional[Path] = None,
    ):
        """
        Initialize archiver.

        Args:
            inbox_base: Base path for inbox (e.g., Users/Sanjay/inbox)
            archive_base: Base path for archive (e.g., Users/Sanjay/archive)
            user_name: User name for file naming
            conn: Optional database connection for updating ingestion_log
            data_root: Optional data root for URI resolution
        """
        self.inbox_base = Path(inbox_base)
        self.archive_base = Path(archive_base)
        self.user_name = user_name
        self.conn = conn
        self.data_root = data_root

        # Cache for category manifests
        self._manifests: Dict[str, CategoryManifest] = {}

        # URI resolver (lazy init)
        self._uri_resolver: Optional[PFASURIResolver] = None

    def _get_manifest(self, category_path: Path) -> CategoryManifest:
        """Get or create manifest for a category."""
        category_key = str(category_path)

        if category_key not in self._manifests:
            self._manifests[category_key] = CategoryManifest(category_path)

        return self._manifests[category_key]

    def _get_uri_resolver(self) -> Optional[PFASURIResolver]:
        """Get URI resolver if data_root is set."""
        if self._uri_resolver is None and self.data_root:
            self._uri_resolver = PFASURIResolver(self.data_root)
        return self._uri_resolver

    def archive_file(
        self,
        source_path: Path,
        source_type: Optional[str] = None,
        file_hash: Optional[str] = None,
        statement_period: Optional[Tuple[str, str]] = None,
        extraction_metadata: Optional[Dict[str, Any]] = None,
        supersedes_hash: Optional[str] = None,
    ) -> Optional[Tuple[Path, Optional[str], Optional[ManifestEntry]]]:
        """
        Archive a single file with manifest tracking.

        Args:
            source_path: Path to file in inbox
            source_type: Optional source identifier (e.g., "CAMS", "Zerodha")
            file_hash: Optional file hash for updating ingestion_log
            statement_period: Optional tuple of (from_date, to_date)
            extraction_metadata: Optional dict with parser_version, records, etc.
            supersedes_hash: Optional hash of file this supersedes

        Returns:
            Tuple of (archive_path, uri, manifest_entry) or None if failed
        """
        source_path = Path(source_path)

        if not source_path.exists():
            logger.error(f"Source file does not exist: {source_path}")
            return None

        try:
            # Calculate file hash if not provided
            if not file_hash:
                file_hash = calculate_file_hash(source_path)

            # Get file size
            file_size = source_path.stat().st_size

            # Determine relative path from inbox
            try:
                relative_path = source_path.relative_to(self.inbox_base)
            except ValueError:
                relative_path = Path(source_path.name)

            # Build archive path preserving structure
            if isinstance(relative_path, Path) and relative_path.parent != Path("."):
                archive_dir = self.archive_base / relative_path.parent
                category = relative_path.parts[0] if relative_path.parts else None
            else:
                archive_dir = self.archive_base
                category = None

            # Ensure archive directory exists
            archive_dir.mkdir(parents=True, exist_ok=True)

            # Generate archive filename
            archive_name = self._generate_archive_name(source_path, source_type)
            archive_path = archive_dir / archive_name

            # Handle existing file with same name
            archive_path = self._get_unique_path(archive_path)

            # Move file
            shutil.move(str(source_path), str(archive_path))

            logger.info(f"Archived: {source_path.name} -> {archive_path}")

            # Update manifest
            manifest_entry = None
            if category:
                category_path = self.archive_base / category
                manifest = self._get_manifest(category_path)
                manifest_entry = manifest.add_entry(
                    file_hash=file_hash,
                    original_name=source_path.name,
                    archived_name=archive_path.name,
                    file_size=file_size,
                    statement_period=statement_period,
                    extraction_metadata=extraction_metadata,
                    source_type=source_type,
                    supersedes=supersedes_hash,
                )

            # Generate URI
            uri = None
            resolver = self._get_uri_resolver()
            if resolver:
                try:
                    uri = resolver.to_uri(archive_path)
                except Exception as e:
                    logger.warning(f"Could not generate URI: {e}")

            # Update ingestion_log if connection provided
            if self.conn:
                self._update_ingestion_log(file_hash, archive_path, uri)

            return (archive_path, uri, manifest_entry)

        except Exception as e:
            logger.exception(f"Failed to archive {source_path}: {e}")
            return None

    def archive_files(
        self,
        file_paths: List[Path],
        extraction_metadata: Optional[Dict[str, Any]] = None,
    ) -> ArchiveResult:
        """
        Archive multiple files.

        Args:
            file_paths: List of file paths to archive
            extraction_metadata: Optional metadata to apply to all files

        Returns:
            ArchiveResult with statistics
        """
        result = ArchiveResult()

        for path in file_paths:
            archive_result = self.archive_file(
                path,
                extraction_metadata=extraction_metadata
            )

            if archive_result:
                archived_path, uri, manifest_entry = archive_result
                result.files_archived += 1
                result.archived_paths.append(archived_path)
                if uri:
                    result.archived_uris.append(uri)
                if manifest_entry:
                    result.manifest_entries.append(manifest_entry)
            else:
                result.files_failed += 1
                result.add_error(f"Failed to archive: {path.name}")

        if result.files_failed > 0:
            result.success = result.files_archived > 0

        return result

    def _generate_archive_name(
        self,
        source_path: Path,
        source_type: Optional[str] = None
    ) -> str:
        """
        Generate standardized archive filename.

        Format: YYYY-MM-DD_User_Source_OriginalName.ext
        Example: 2026-01-17_Sanjay_CAMS_cas.pdf

        Args:
            source_path: Original file path
            source_type: Optional source identifier

        Returns:
            Archive filename
        """
        today = date.today().isoformat()
        stem = source_path.stem
        ext = source_path.suffix

        # Try to detect source from path if not provided
        if not source_type:
            source_type = self._detect_source_from_path(source_path)

        if source_type:
            return f"{today}_{self.user_name}_{source_type}_{stem}{ext}"
        else:
            return f"{today}_{self.user_name}_{stem}{ext}"

    def _detect_source_from_path(self, file_path: Path) -> Optional[str]:
        """
        Detect source type from file path.

        Args:
            file_path: File path

        Returns:
            Source type string or None
        """
        path_parts = [p.upper() for p in file_path.parts]

        # RTA detection
        if 'CAMS' in path_parts:
            return 'CAMS'
        elif 'KARVY' in path_parts or 'KFINTECH' in path_parts:
            return 'KARVY'

        # Broker detection
        if 'ZERODHA' in path_parts:
            return 'Zerodha'
        elif 'ICICIDIRECT' in path_parts:
            return 'ICICIDirect'
        elif 'ETRADE' in path_parts:
            return 'ETrade'

        # Bank detection
        if 'ICICI' in path_parts:
            return 'ICICI'
        elif 'HDFC' in path_parts:
            return 'HDFC'
        elif 'SBI' in path_parts:
            return 'SBI'

        return None

    def _get_unique_path(self, path: Path) -> Path:
        """
        Get unique path by appending counter if file exists.

        Args:
            path: Original path

        Returns:
            Unique path
        """
        if not path.exists():
            return path

        stem = path.stem
        ext = path.suffix
        parent = path.parent

        counter = 1
        while True:
            new_path = parent / f"{stem}_{counter}{ext}"
            if not new_path.exists():
                return new_path
            counter += 1

    def _update_ingestion_log(
        self,
        file_hash: str,
        archive_path: Path,
        file_uri: Optional[str] = None
    ):
        """
        Update ingestion_log with archive path and URI.

        Args:
            file_hash: File hash
            archive_path: Path to archived file
            file_uri: Optional PFAS URI
        """
        try:
            # Normalize hash format
            if file_hash.startswith("sha256:"):
                file_hash = file_hash[7:]

            # Try to update with URI if available
            if file_uri:
                self.conn.execute(
                    """
                    UPDATE ingestion_log
                    SET archive_path = ?, file_uri = ?, status = 'ARCHIVED'
                    WHERE file_hash = ?
                    """,
                    (str(archive_path), file_uri, file_hash)
                )
            else:
                self.conn.execute(
                    """
                    UPDATE ingestion_log
                    SET archive_path = ?, status = 'ARCHIVED'
                    WHERE file_hash = ?
                    """,
                    (str(archive_path), file_hash)
                )
            self.conn.commit()
        except Exception as e:
            logger.warning(f"Failed to update ingestion_log: {e}")

    def cleanup_empty_dirs(self) -> int:
        """
        Remove empty directories in inbox after archiving.

        Returns:
            Number of directories removed
        """
        removed = 0

        # Walk bottom-up to remove empty directories
        for dirpath in sorted(
            self.inbox_base.rglob('*'),
            key=lambda p: len(p.parts),
            reverse=True
        ):
            if dirpath.is_dir() and not any(dirpath.iterdir()):
                try:
                    dirpath.rmdir()
                    removed += 1
                    logger.debug(f"Removed empty directory: {dirpath}")
                except Exception as e:
                    logger.warning(f"Could not remove directory {dirpath}: {e}")

        return removed


def archive_processed_files(
    processed_files: List[Path],
    inbox_base: Path,
    archive_base: Path,
    user_name: str,
    conn: Optional[sqlite3.Connection] = None,
    data_root: Optional[Path] = None,
    extraction_metadata: Optional[Dict[str, Any]] = None,
) -> ArchiveResult:
    """
    Archive successfully processed files.

    IMPORTANT: Only archive files that were FULLY SUCCESSFULLY processed.
    Do NOT pass failed files to this function.

    Args:
        processed_files: List of file paths that were SUCCESSFULLY processed
        inbox_base: Base inbox path
        archive_base: Base archive path
        user_name: User name
        conn: Optional database connection
        data_root: Optional data root for URI resolution
        extraction_metadata: Optional metadata to store with archived files

    Returns:
        ArchiveResult with paths, URIs, and manifest entries

    Example:
        # Only archive files that succeeded
        result = archive_processed_files(
            processed_files=ingestion_result.succeeded_files,  # NOT all processed_files!
            inbox_base=Path("inbox"),
            archive_base=Path("archive"),
            user_name="Sanjay",
            data_root=Path("Data"),
            extraction_metadata={"parser_version": "1.0.0", "records_extracted": 145}
        )
    """
    if not processed_files:
        logger.info("No files to archive")
        return ArchiveResult(success=True)

    archiver = FileArchiver(
        inbox_base, archive_base, user_name, conn, data_root
    )
    return archiver.archive_files(processed_files, extraction_metadata)
