"""
Universal Asset Scanner - Scans inbox for all asset types with hybrid statement detection.

Supports:
- Mutual-Fund, Indian-Stocks, USA-Stocks, EPF, PPF, NPS, FD-Bonds, SGB, Bank
- Hybrid detection: Folder hints > Filename > Content > Default
- Multi-user isolation via PathResolver
- Configurable rules with user overrides

Usage:
    from pfas.core.asset_scanner import AssetScanner
    from pfas.core.paths import PathResolver

    resolver = PathResolver(project_root, "Sanjay")
    scanner = AssetScanner(resolver)

    # Scan specific asset
    result = scanner.scan_asset("Mutual-Fund")

    # Get only transaction files
    txn_files = result.transaction_files

    # Get only holdings files
    holdings_files = result.holdings_files
"""

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from pathlib import Path
from typing import List, Optional, Dict, Callable, Set

from pfas.core.statement_detector import (
    StatementTypeDetector,
    StatementType,
    DetectionResult,
    StatementRulesConfig
)
from pfas.core.file_processor import MultiFileProcessor

logger = logging.getLogger(__name__)


class AssetType(Enum):
    """Supported asset types."""
    MUTUAL_FUND = "Mutual-Fund"
    INDIAN_STOCKS = "Indian-Stocks"
    USA_STOCKS = "USA-Stocks"
    EPF = "EPF"
    PPF = "PPF"
    NPS = "NPS"
    FD_BONDS = "FD-Bonds"
    SGB = "SGB"
    BANK = "Bank"
    SALARY = "Salary"
    RENTAL = "Rental"
    DIVIDENDS = "Dividends"


# Assets that typically have both transaction and holdings statements
DUAL_STATEMENT_ASSETS = {
    AssetType.MUTUAL_FUND,
    AssetType.INDIAN_STOCKS,
    AssetType.USA_STOCKS,
}

# Assets that are transaction-only (contributions, withdrawals)
TRANSACTION_ONLY_ASSETS = {
    AssetType.EPF,
    AssetType.PPF,
    AssetType.NPS,
    AssetType.BANK,
    AssetType.SALARY,
}

# Supported file extensions per asset type
ASSET_EXTENSIONS = {
    AssetType.MUTUAL_FUND: {'.pdf', '.xlsx', '.xls'},
    AssetType.INDIAN_STOCKS: {'.csv', '.xlsx', '.xls'},
    AssetType.USA_STOCKS: {'.csv', '.xlsx', '.pdf'},
    AssetType.EPF: {'.pdf'},
    AssetType.PPF: {'.pdf', '.xlsx'},
    AssetType.NPS: {'.pdf', '.csv', '.xlsx'},
    AssetType.FD_BONDS: {'.pdf', '.xlsx'},
    AssetType.SGB: {'.pdf', '.xlsx'},
    AssetType.BANK: {'.csv', '.xlsx', '.xls', '.pdf'},
    AssetType.SALARY: {'.pdf', '.xlsx'},
    AssetType.RENTAL: {'.xlsx', '.csv'},
    AssetType.DIVIDENDS: {'.xlsx', '.csv'},
}


@dataclass
class ScannedAssetFile:
    """Represents a scanned asset file with statement type detection."""
    path: Path
    asset_type: AssetType
    statement_type: StatementType
    detection_result: DetectionResult
    file_hash: str = ""
    size_bytes: int = 0
    financial_year: str = ""

    def __post_init__(self):
        """Calculate file hash and detect FY if not provided."""
        if not self.file_hash and self.path.exists():
            self.file_hash = self._calculate_hash()
            self.size_bytes = self.path.stat().st_size
        if not self.financial_year:
            self.financial_year = MultiFileProcessor.detect_financial_year(self.path)

    def _calculate_hash(self) -> str:
        """Calculate SHA256 hash of file."""
        sha256 = hashlib.sha256()
        with open(self.path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    @property
    def is_transaction(self) -> bool:
        return self.statement_type == StatementType.TRANSACTIONS

    @property
    def is_holding(self) -> bool:
        return self.statement_type == StatementType.HOLDINGS


@dataclass
class AssetScanResult:
    """Result of scanning an asset folder."""
    asset_type: AssetType
    files: List[ScannedAssetFile] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    total_scanned: int = 0

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    @property
    def transaction_files(self) -> List[ScannedAssetFile]:
        """Return only transaction statement files."""
        return [f for f in self.files if f.is_transaction]

    @property
    def holdings_files(self) -> List[ScannedAssetFile]:
        """Return only holdings statement files."""
        return [f for f in self.files if f.is_holding]

    @property
    def by_financial_year(self) -> Dict[str, List[ScannedAssetFile]]:
        """Group files by financial year."""
        grouped = {}
        for f in self.files:
            fy = f.financial_year or "UNKNOWN"
            if fy not in grouped:
                grouped[fy] = []
            grouped[fy].append(f)
        return grouped

    def get_sorted_files(self, reverse: bool = False) -> List[ScannedAssetFile]:
        """Get files sorted by date (oldest first by default)."""
        paths = [f.path for f in self.files]
        sorted_paths = MultiFileProcessor.sort_by_date(paths, reverse=reverse)
        path_to_file = {f.path: f for f in self.files}
        return [path_to_file[p] for p in sorted_paths]


class AssetScanner:
    """
    Universal scanner for all asset types with hybrid statement detection.

    Scanning priority for statement files:
    1. inbox/<asset>/transactions/ - explicitly transaction files
    2. inbox/<asset>/holdings/ - explicitly holdings files
    3. inbox/<asset>/ (flat) - auto-detect using filename/content

    Also checks archive/ with same structure if configured.
    """

    def __init__(
        self,
        path_resolver,
        statement_config: Optional[StatementRulesConfig] = None,
        include_archive: bool = False
    ):
        """
        Initialize scanner.

        Args:
            path_resolver: PathResolver instance for the user
            statement_config: Optional pre-loaded statement rules config
            include_archive: Whether to also scan archive folder
        """
        self.path_resolver = path_resolver
        self.include_archive = include_archive
        self.statement_detector = StatementTypeDetector(
            path_resolver=path_resolver,
            config=statement_config
        )

    def scan_asset(
        self,
        asset_type: str | AssetType,
        subfolders: Optional[List[str]] = None
    ) -> AssetScanResult:
        """
        Scan inbox for a specific asset type.

        Args:
            asset_type: Asset type string or enum
            subfolders: Optional list of subfolders within asset folder (e.g., ["CAMS", "KARVY"])

        Returns:
            AssetScanResult with all found files
        """
        if isinstance(asset_type, str):
            try:
                asset_type = AssetType(asset_type)
            except ValueError:
                # Try to match by name
                for at in AssetType:
                    if at.value.lower() == asset_type.lower():
                        asset_type = at
                        break
                else:
                    return AssetScanResult(
                        asset_type=AssetType.MUTUAL_FUND,
                        errors=[f"Unknown asset type: {asset_type}"]
                    )

        result = AssetScanResult(asset_type=asset_type)
        extensions = ASSET_EXTENSIONS.get(asset_type, {'.pdf', '.xlsx', '.csv'})

        # Determine base paths to scan
        inbox = self.path_resolver.inbox()
        archive = self.path_resolver.archive() if self.include_archive else None

        base_paths = []

        # Add inbox paths
        asset_inbox = inbox / asset_type.value
        if asset_inbox.exists():
            base_paths.append(("inbox", asset_inbox))

        # Add archive paths if configured
        if archive:
            asset_archive = archive / asset_type.value
            if asset_archive.exists():
                base_paths.append(("archive", asset_archive))

        if not base_paths:
            result.warnings.append(f"No folders found for {asset_type.value}")
            return result

        # Scan each base path
        for source, base_path in base_paths:
            self._scan_path(
                base_path=base_path,
                asset_type=asset_type,
                extensions=extensions,
                result=result,
                subfolders=subfolders,
                source=source
            )

        # Sort files by date
        result.files = result.get_sorted_files()

        logger.info(
            f"Scanned {asset_type.value}: {result.total_scanned} files, "
            f"{len(result.transaction_files)} transactions, "
            f"{len(result.holdings_files)} holdings"
        )

        return result

    def _scan_path(
        self,
        base_path: Path,
        asset_type: AssetType,
        extensions: Set[str],
        result: AssetScanResult,
        subfolders: Optional[List[str]],
        source: str
    ):
        """Scan a single base path for files."""

        # Priority 1: Check transactions/ subfolder
        txn_folder = base_path / "transactions"
        if txn_folder.exists():
            self._scan_folder(
                folder=txn_folder,
                asset_type=asset_type,
                extensions=extensions,
                result=result,
                force_type=StatementType.TRANSACTIONS,
                subfolders=subfolders
            )

        # Priority 2: Check holdings/ subfolder
        holdings_folder = base_path / "holdings"
        if holdings_folder.exists():
            self._scan_folder(
                folder=holdings_folder,
                asset_type=asset_type,
                extensions=extensions,
                result=result,
                force_type=StatementType.HOLDINGS,
                subfolders=subfolders
            )

        # Priority 3: Scan flat structure (auto-detect)
        self._scan_folder(
            folder=base_path,
            asset_type=asset_type,
            extensions=extensions,
            result=result,
            force_type=None,  # Auto-detect
            subfolders=subfolders,
            exclude_folders={"transactions", "holdings", "failed"}
        )

    def _scan_folder(
        self,
        folder: Path,
        asset_type: AssetType,
        extensions: Set[str],
        result: AssetScanResult,
        force_type: Optional[StatementType],
        subfolders: Optional[List[str]] = None,
        exclude_folders: Optional[Set[str]] = None
    ):
        """Scan a folder for statement files."""
        if not folder.exists():
            return

        exclude_folders = exclude_folders or set()

        # Find all matching files
        files_to_scan = []

        if subfolders:
            # Scan specific subfolders
            for subfolder in subfolders:
                subfolder_path = folder / subfolder
                if subfolder_path.exists():
                    for ext in extensions:
                        files_to_scan.extend(subfolder_path.rglob(f"*{ext}"))
        else:
            # Scan all files recursively
            for ext in extensions:
                for file_path in folder.rglob(f"*{ext}"):
                    # Skip excluded folders
                    relative = file_path.relative_to(folder)
                    if relative.parts and relative.parts[0].lower() in exclude_folders:
                        continue
                    files_to_scan.append(file_path)

        # Process each file
        for file_path in files_to_scan:
            result.total_scanned += 1

            try:
                # Detect statement type
                if force_type:
                    detection = DetectionResult(
                        statement_type=force_type,
                        detection_method=DetectionResult.__dataclass_fields__['detection_method'].default,
                        confidence=1.0,
                        matched_keywords=[f"folder:{folder.name}"]
                    )
                    # Fix: properly set detection method
                    from pfas.core.statement_detector import DetectionMethod
                    detection = DetectionResult(
                        statement_type=force_type,
                        detection_method=DetectionMethod.FOLDER,
                        confidence=1.0,
                        matched_keywords=[f"folder:{folder.name}"]
                    )
                else:
                    detection = self.statement_detector.detect(file_path)

                scanned_file = ScannedAssetFile(
                    path=file_path,
                    asset_type=asset_type,
                    statement_type=detection.statement_type,
                    detection_result=detection
                )

                # Check for duplicates by hash
                existing_hashes = {f.file_hash for f in result.files}
                if scanned_file.file_hash in existing_hashes:
                    result.warnings.append(f"Duplicate file skipped: {file_path.name}")
                    continue

                result.files.append(scanned_file)

                if detection.warnings:
                    result.warnings.extend(detection.warnings)

            except Exception as e:
                result.warnings.append(f"Error processing {file_path.name}: {str(e)}")
                logger.warning(f"Error processing {file_path}: {e}")

    def scan_all_assets(self) -> Dict[AssetType, AssetScanResult]:
        """
        Scan all asset types in inbox.

        Returns:
            Dict mapping asset type to scan result
        """
        results = {}
        for asset_type in AssetType:
            result = self.scan_asset(asset_type)
            if result.files or result.errors:
                results[asset_type] = result
        return results

    def move_to_failed(self, file_path: Path) -> Optional[Path]:
        """
        Move a file to the failed/ subfolder.

        Args:
            file_path: Path to the file

        Returns:
            New path if moved, None if failed
        """
        try:
            # Determine the failed folder (sibling to current location)
            parent = file_path.parent
            failed_folder = parent / "failed"
            failed_folder.mkdir(exist_ok=True)

            new_path = failed_folder / file_path.name
            file_path.rename(new_path)

            logger.info(f"Moved to failed: {file_path.name}")
            return new_path

        except Exception as e:
            logger.error(f"Failed to move {file_path.name} to failed/: {e}")
            return None


def scan_asset_inbox(
    path_resolver,
    asset_type: str | AssetType,
    include_archive: bool = False
) -> AssetScanResult:
    """
    Convenience function to scan an asset inbox.

    Args:
        path_resolver: PathResolver instance
        asset_type: Asset type to scan
        include_archive: Whether to include archive folder

    Returns:
        AssetScanResult
    """
    scanner = AssetScanner(path_resolver, include_archive=include_archive)
    return scanner.scan_asset(asset_type)
