"""
Multi-File Processor - Handles multiple statement files with FY detection.

Provides utilities for:
- Chronological sorting of statement files
- Financial year detection from filename or content
- Deduplication of records across files
- Merge strategies for overlapping data

Usage:
    from pfas.core.file_processor import MultiFileProcessor

    files = list(Path("inbox/EPF").glob("*.pdf"))
    sorted_files = MultiFileProcessor.sort_by_date(files)
    fy = MultiFileProcessor.detect_financial_year(sorted_files[0])
"""

import re
from datetime import date
from pathlib import Path
from typing import List, Optional, TypeVar, Callable
import logging

logger = logging.getLogger(__name__)

T = TypeVar('T')


class MultiFileProcessor:
    """
    Generic processor for multiple statement files.
    Handles chronological sorting, FY detection, and deduplication.
    """

    # Patterns for financial year detection
    FY_PATTERNS = [
        # FY2024-25, FY24-25, FY2425
        r'FY[_-]?(\d{4})[_-]?(\d{2,4})',
        r'FY[_-]?(\d{2})[_-]?(\d{2})',
        # 2024-25, 2024-2025
        r'(\d{4})[_-](\d{2,4})',
        # Passbook_2024, Statement_2025
        r'[_-](\d{4})(?:[_\.]|$)',
    ]

    # Date patterns in filenames
    DATE_PATTERNS = [
        # 2024-01-15, 2024_01_15
        r'(\d{4})[_-](\d{2})[_-](\d{2})',
        # 15-01-2024, 15_01_2024
        r'(\d{2})[_-](\d{2})[_-](\d{4})',
    ]

    @classmethod
    def sort_by_date(cls, files: List[Path], reverse: bool = False) -> List[Path]:
        """
        Sort files by date extracted from filename or modification time.

        Args:
            files: List of file paths to sort
            reverse: If True, sort newest first (default: oldest first)

        Returns:
            Sorted list of file paths
        """
        def get_date(file_path: Path) -> date:
            # Try FY pattern first
            fy = cls.detect_financial_year(file_path)
            if fy and fy != "UNKNOWN":
                # Extract start year from FY string like "2024-25"
                try:
                    year = int(fy.split('-')[0])
                    return date(year, 4, 1)  # Start of FY
                except (ValueError, IndexError):
                    pass

            # Try date pattern in filename
            stem = file_path.stem
            for pattern in cls.DATE_PATTERNS:
                match = re.search(pattern, stem)
                if match:
                    groups = match.groups()
                    try:
                        if len(groups[0]) == 4:  # YYYY-MM-DD format
                            return date(int(groups[0]), int(groups[1]), int(groups[2]))
                        else:  # DD-MM-YYYY format
                            return date(int(groups[2]), int(groups[1]), int(groups[0]))
                    except ValueError:
                        continue

            # Fallback to modification time
            try:
                return date.fromtimestamp(file_path.stat().st_mtime)
            except Exception:
                return date.today()

        return sorted(files, key=get_date, reverse=reverse)

    @classmethod
    def detect_financial_year(cls, file_path: Path) -> str:
        """
        Detect financial year from filename patterns.

        Supports patterns:
        - FY2024-25, FY24-25, FY2425
        - 2024-25, 2024-2025
        - EPF_2024.pdf (assumes FY starting that year)

        Args:
            file_path: Path to the file

        Returns:
            Financial year string (e.g., "2024-25") or "UNKNOWN"
        """
        stem = file_path.stem.upper()

        for pattern in cls.FY_PATTERNS:
            match = re.search(pattern, stem, re.IGNORECASE)
            if match:
                groups = match.groups()

                if len(groups) == 2:
                    start_year = int(groups[0])
                    end_part = groups[1]

                    # Handle 2-digit vs 4-digit start year
                    if start_year < 100:
                        start_year += 2000

                    # Handle end year
                    if len(end_part) == 4:
                        end_year = int(end_part)
                    elif len(end_part) == 2:
                        end_year = start_year + 1
                        # Validate the end_part matches expected
                        expected_end = str(end_year)[-2:]
                        if end_part != expected_end:
                            # Might be a different pattern, skip
                            continue
                    else:
                        continue

                    return f"{start_year}-{str(end_year)[-2:]}"

                elif len(groups) == 1:
                    # Single year like "2024" - assume FY starting that year
                    year = int(groups[0])
                    if year < 100:
                        year += 2000
                    return f"{year}-{str(year + 1)[-2:]}"

        return "UNKNOWN"

    @classmethod
    def detect_fy_from_date(cls, txn_date: date) -> str:
        """
        Get financial year for a given date.

        Indian FY runs April to March.

        Args:
            txn_date: Transaction date

        Returns:
            Financial year string (e.g., "2024-25")
        """
        if txn_date.month >= 4:  # Apr-Dec: same year starts FY
            start_year = txn_date.year
        else:  # Jan-Mar: previous year starts FY
            start_year = txn_date.year - 1

        return f"{start_year}-{str(start_year + 1)[-2:]}"

    @classmethod
    def group_by_fy(cls, files: List[Path]) -> dict:
        """
        Group files by detected financial year.

        Args:
            files: List of file paths

        Returns:
            Dict mapping FY string to list of files
        """
        grouped = {}
        for file_path in files:
            fy = cls.detect_financial_year(file_path)
            if fy not in grouped:
                grouped[fy] = []
            grouped[fy].append(file_path)
        return grouped

    @classmethod
    def deduplicate_records(
        cls,
        records: List[T],
        key_func: Callable[[T], tuple]
    ) -> List[T]:
        """
        Remove duplicate records based on a key function.

        Args:
            records: List of records (any type)
            key_func: Function that returns a tuple key for deduplication

        Returns:
            List with duplicates removed (first occurrence kept)
        """
        seen = set()
        unique = []
        for record in records:
            key = key_func(record)
            if key not in seen:
                seen.add(key)
                unique.append(record)
        return unique

    @classmethod
    def merge_sorted(
        cls,
        records: List[T],
        date_func: Callable[[T], date]
    ) -> List[T]:
        """
        Merge and sort records by date.

        Args:
            records: List of records
            date_func: Function to extract date from record

        Returns:
            Sorted list of records (oldest first)
        """
        return sorted(records, key=date_func)

    @classmethod
    def get_latest_file(
        cls,
        directory: Path,
        extensions: List[str],
        pattern: str = "*"
    ) -> Optional[Path]:
        """
        Get the most recent file from a directory.

        Args:
            directory: Directory to search
            extensions: List of file extensions (e.g., ['.pdf', '.xlsx'])
            pattern: Glob pattern to match (default: all files)

        Returns:
            Path to most recent file, or None if not found
        """
        if not directory.exists():
            return None

        files = []
        for ext in extensions:
            files.extend(directory.glob(f"{pattern}{ext}"))

        if not files:
            return None

        # Sort by modification time (newest first) and return first
        sorted_files = cls.sort_by_date(files, reverse=True)
        return sorted_files[0] if sorted_files else None


# Convenience functions
def sort_files_by_date(files: List[Path], reverse: bool = False) -> List[Path]:
    """Sort files by date (convenience function)."""
    return MultiFileProcessor.sort_by_date(files, reverse)


def detect_fy(file_path: Path) -> str:
    """Detect financial year from file (convenience function)."""
    return MultiFileProcessor.detect_financial_year(file_path)


def get_fy_from_date(txn_date: date) -> str:
    """Get FY from date (convenience function)."""
    return MultiFileProcessor.detect_fy_from_date(txn_date)
