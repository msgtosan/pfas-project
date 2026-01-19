"""
File Archiver - Moves processed files to archive with standardized naming.

Preserves directory structure and adds date prefix for organization.
"""

import logging
import shutil
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import List, Optional

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

logger = logging.getLogger(__name__)


@dataclass
class ArchiveResult:
    """Result of archiving operation."""
    success: bool = True
    files_archived: int = 0
    files_failed: int = 0
    archived_paths: List[Path] = field(default_factory=list)
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

    Usage:
        archiver = FileArchiver(inbox_path, archive_path, "Sanjay")
        result = archiver.archive_file(Path("inbox/Mutual-Fund/CAMS/cas.pdf"))
    """

    def __init__(
        self,
        inbox_base: Path,
        archive_base: Path,
        user_name: str,
        conn: Optional[sqlite3.Connection] = None
    ):
        """
        Initialize archiver.

        Args:
            inbox_base: Base path for inbox (e.g., Users/Sanjay/inbox)
            archive_base: Base path for archive (e.g., Users/Sanjay/archive)
            user_name: User name for file naming
            conn: Optional database connection for updating ingestion_log
        """
        self.inbox_base = Path(inbox_base)
        self.archive_base = Path(archive_base)
        self.user_name = user_name
        self.conn = conn

    def archive_file(
        self,
        source_path: Path,
        source_type: Optional[str] = None,
        file_hash: Optional[str] = None
    ) -> Optional[Path]:
        """
        Archive a single file.

        Args:
            source_path: Path to file in inbox
            source_type: Optional source identifier (e.g., "CAMS", "Zerodha")
            file_hash: Optional file hash for updating ingestion_log

        Returns:
            Path to archived file, or None if failed
        """
        source_path = Path(source_path)

        if not source_path.exists():
            logger.error(f"Source file does not exist: {source_path}")
            return None

        try:
            # Determine relative path from inbox
            # e.g., if source is inbox/Mutual-Fund/CAMS/cas.pdf
            # and inbox_base is inbox/
            # then relative_path is Mutual-Fund/CAMS/cas.pdf
            try:
                relative_path = source_path.relative_to(self.inbox_base)
            except ValueError:
                # File not under inbox_base, use just the filename
                relative_path = source_path.name

            # Build archive path preserving structure
            # archive/Mutual-Fund/CAMS/
            if isinstance(relative_path, Path):
                archive_dir = self.archive_base / relative_path.parent
            else:
                archive_dir = self.archive_base

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

            # Update ingestion_log if connection provided
            if self.conn and file_hash:
                self._update_ingestion_log(file_hash, archive_path)

            return archive_path

        except Exception as e:
            logger.exception(f"Failed to archive {source_path}: {e}")
            return None

    def archive_files(self, file_paths: List[Path]) -> ArchiveResult:
        """
        Archive multiple files.

        Args:
            file_paths: List of file paths to archive

        Returns:
            ArchiveResult with statistics
        """
        result = ArchiveResult()

        for path in file_paths:
            archived_path = self.archive_file(path)

            if archived_path:
                result.files_archived += 1
                result.archived_paths.append(archived_path)
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

    def _update_ingestion_log(self, file_hash: str, archive_path: Path):
        """
        Update ingestion_log with archive path.

        Args:
            file_hash: File hash
            archive_path: Path to archived file
        """
        try:
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
    conn: Optional[sqlite3.Connection] = None
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

    Returns:
        ArchiveResult

    Example:
        # Only archive files that succeeded
        result = archive_processed_files(
            processed_files=ingestion_result.succeeded_files,  # NOT all processed_files!
            inbox_base=Path("inbox"),
            archive_base=Path("archive"),
            user_name="Sanjay"
        )
    """
    if not processed_files:
        logger.info("No files to archive")
        return ArchiveResult(success=True)

    archiver = FileArchiver(inbox_base, archive_base, user_name, conn)
    return archiver.archive_files(processed_files)
