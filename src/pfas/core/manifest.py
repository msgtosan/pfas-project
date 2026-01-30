"""
Temporal Versioning Manifest for PFAS Archive.

Provides point-in-time audit capabilities and file supersession tracking:
- ManifestEntry: Individual file metadata with versioning
- CategoryManifest: Per-category manifest.json management
- Lineage tracking for file supersession chains

Usage:
    manifest = CategoryManifest(archive_path / "Mutual-Fund")

    # Add a new file entry
    manifest.add_entry(
        file_hash="sha256:abc123...",
        original_name="CAMS_CAS_FY24-25.pdf",
        archived_name="2026-01-17_Sanjay_CAMS_CAS_FY24-25.pdf",
        statement_period=("2024-04-01", "2025-03-31"),
        extraction_metadata={"parser_version": "1.0.0", "records": 145}
    )

    # Mark supersession
    manifest.mark_superseded("sha256:abc123...", by="sha256:def456...")

    # Query historical state
    entries = manifest.get_entries_as_of(date(2026, 1, 15))
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

logger = logging.getLogger(__name__)


@dataclass
class ExtractionMetadata:
    """Metadata about file extraction/parsing."""

    parser_version: str = "1.0.0"
    records_extracted: int = 0
    page_count: int = 0
    encrypted: bool = False
    password_hint: Optional[str] = None  # e.g., "PAN", "DOB"
    extraction_timestamp: Optional[str] = None
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "parser_version": self.parser_version,
            "records_extracted": self.records_extracted,
            "page_count": self.page_count,
            "encrypted": self.encrypted,
            "password_hint": self.password_hint,
            "extraction_timestamp": self.extraction_timestamp,
            "warnings": self.warnings,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExtractionMetadata":
        """Create from dictionary."""
        return cls(
            parser_version=data.get("parser_version", "1.0.0"),
            records_extracted=data.get("records_extracted", 0),
            page_count=data.get("page_count", 0),
            encrypted=data.get("encrypted", False),
            password_hint=data.get("password_hint"),
            extraction_timestamp=data.get("extraction_timestamp"),
            warnings=data.get("warnings", []),
        )


@dataclass
class StatementPeriod:
    """Statement coverage period."""

    from_date: Optional[str] = None  # ISO format YYYY-MM-DD
    to_date: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "from": self.from_date,
            "to": self.to_date,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StatementPeriod":
        """Create from dictionary."""
        if data is None:
            return cls()
        return cls(
            from_date=data.get("from"),
            to_date=data.get("to"),
        )


@dataclass
class ManifestEntry:
    """
    Individual file entry in the manifest.

    Tracks file metadata, versioning, and supersession relationships.
    """

    file_hash: str  # sha256:hexdigest
    original_name: str
    archived_name: str
    archived_at: str  # ISO timestamp
    file_size: int = 0
    statement_period: StatementPeriod = field(default_factory=StatementPeriod)
    extraction_metadata: ExtractionMetadata = field(default_factory=ExtractionMetadata)
    supersedes: Optional[str] = None  # hash of file this supersedes
    superseded_by: Optional[str] = None  # hash of file that superseded this
    superseded_at: Optional[str] = None  # when superseded
    source_type: Optional[str] = None  # e.g., "CAMS", "NSDL", "Zerodha"
    notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "file_hash": self.file_hash,
            "original_name": self.original_name,
            "archived_name": self.archived_name,
            "archived_at": self.archived_at,
            "file_size": self.file_size,
            "statement_period": self.statement_period.to_dict(),
            "supersedes": self.supersedes,
            "superseded_by": self.superseded_by,
            "superseded_at": self.superseded_at,
            "extraction_metadata": self.extraction_metadata.to_dict(),
            "source_type": self.source_type,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ManifestEntry":
        """Create from dictionary."""
        return cls(
            file_hash=data["file_hash"],
            original_name=data["original_name"],
            archived_name=data["archived_name"],
            archived_at=data["archived_at"],
            file_size=data.get("file_size", 0),
            statement_period=StatementPeriod.from_dict(data.get("statement_period")),
            supersedes=data.get("supersedes"),
            superseded_by=data.get("superseded_by"),
            superseded_at=data.get("superseded_at"),
            extraction_metadata=ExtractionMetadata.from_dict(
                data.get("extraction_metadata", {})
            ),
            source_type=data.get("source_type"),
            notes=data.get("notes"),
        )

    @property
    def is_superseded(self) -> bool:
        """Check if this entry has been superseded."""
        return self.superseded_by is not None

    @property
    def is_active(self) -> bool:
        """Check if this entry is the current active version."""
        return not self.is_superseded


class CategoryManifest:
    """
    Manages manifest.json for an archive category.

    The manifest provides:
    - Point-in-time audit capability
    - File supersession tracking
    - Lineage chains for file versions
    - Parser version tracking for re-extraction triggers

    Directory structure:
        archive/
        ├── Mutual-Fund/
        │   ├── manifest.json
        │   ├── 2026-01-17_Sanjay_CAMS_CAS.pdf
        │   └── ...
        ├── Indian-Stocks/
        │   ├── manifest.json
        │   └── ...
    """

    MANIFEST_VERSION = "1.0"

    def __init__(self, category_path: Path, user_id: Optional[int] = None):
        """
        Initialize manifest for a category.

        Args:
            category_path: Path to category directory (e.g., archive/Mutual-Fund)
            user_id: Optional user ID for multi-user tracking
        """
        self.category_path = Path(category_path)
        self.user_id = user_id
        self.manifest_file = self.category_path / "manifest.json"
        self._data: Dict[str, Any] = {}
        self._entries: Dict[str, ManifestEntry] = {}
        self._lineage: Dict[str, List[str]] = {}

        self._load()

    def _load(self) -> None:
        """Load manifest from file."""
        if self.manifest_file.exists():
            try:
                with open(self.manifest_file, encoding="utf-8") as f:
                    self._data = json.load(f)

                # Load entries
                for entry_data in self._data.get("versions", []):
                    entry = ManifestEntry.from_dict(entry_data)
                    self._entries[entry.file_hash] = entry

                # Load lineage
                self._lineage = self._data.get("lineage", {})

                logger.debug(f"Loaded manifest with {len(self._entries)} entries")

            except Exception as e:
                logger.warning(f"Failed to load manifest: {e}")
                self._init_empty()
        else:
            self._init_empty()

    def _init_empty(self) -> None:
        """Initialize empty manifest."""
        self._data = {
            "manifest_version": self.MANIFEST_VERSION,
            "category": self.category_path.name,
            "user_id": self.user_id,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "versions": [],
            "lineage": {},
        }
        self._entries = {}
        self._lineage = {}
        # Save immediately to create manifest.json
        self._save()

    def _save(self) -> None:
        """Save manifest to file."""
        self.category_path.mkdir(parents=True, exist_ok=True)

        # Update versions list
        self._data["versions"] = [
            entry.to_dict() for entry in self._entries.values()
        ]
        self._data["lineage"] = self._lineage
        self._data["updated_at"] = datetime.now().isoformat()

        with open(self.manifest_file, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)

        logger.debug(f"Saved manifest with {len(self._entries)} entries")

    def add_entry(
        self,
        file_hash: str,
        original_name: str,
        archived_name: str,
        file_size: int = 0,
        statement_period: Optional[Tuple[str, str]] = None,
        extraction_metadata: Optional[Dict[str, Any]] = None,
        source_type: Optional[str] = None,
        supersedes: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> ManifestEntry:
        """
        Add a new file entry to the manifest.

        Args:
            file_hash: SHA256 hash of file content (sha256:hexdigest format)
            original_name: Original filename before archiving
            archived_name: Archived filename
            file_size: File size in bytes
            statement_period: Tuple of (from_date, to_date) in ISO format
            extraction_metadata: Dict with parser_version, records, etc.
            source_type: Source identifier (e.g., "CAMS", "NSDL")
            supersedes: Hash of file this supersedes (if any)
            notes: Optional notes

        Returns:
            Created ManifestEntry
        """
        # Normalize hash format
        if not file_hash.startswith("sha256:"):
            file_hash = f"sha256:{file_hash}"

        # Check for duplicate
        if file_hash in self._entries:
            logger.warning(f"Entry already exists for hash: {file_hash}")
            return self._entries[file_hash]

        # Create period
        period = StatementPeriod()
        if statement_period:
            period.from_date = statement_period[0]
            period.to_date = statement_period[1]

        # Create extraction metadata
        ext_meta = ExtractionMetadata()
        if extraction_metadata:
            ext_meta = ExtractionMetadata.from_dict(extraction_metadata)
        ext_meta.extraction_timestamp = datetime.now().isoformat()

        # Create entry
        entry = ManifestEntry(
            file_hash=file_hash,
            original_name=original_name,
            archived_name=archived_name,
            archived_at=datetime.now().isoformat(),
            file_size=file_size,
            statement_period=period,
            extraction_metadata=ext_meta,
            source_type=source_type,
            supersedes=supersedes,
            notes=notes,
        )

        # Add to entries
        self._entries[file_hash] = entry

        # Update lineage
        if supersedes:
            # Normalize supersedes hash
            if not supersedes.startswith("sha256:"):
                supersedes = f"sha256:{supersedes}"

            # Update entry with normalized supersedes
            entry.supersedes = supersedes

            # Mark the old entry as superseded
            if supersedes in self._entries:
                old_entry = self._entries[supersedes]
                old_entry.superseded_by = file_hash
                old_entry.superseded_at = datetime.now().isoformat()

            # Update lineage chain
            if supersedes in self._lineage:
                self._lineage[supersedes].append(file_hash)
            else:
                self._lineage[supersedes] = [file_hash]

        # Initialize lineage for new entry
        if file_hash not in self._lineage:
            self._lineage[file_hash] = []

        self._save()

        logger.info(f"Added manifest entry: {archived_name} ({file_hash[:20]}...)")
        return entry

    def mark_superseded(
        self,
        old_hash: str,
        by_hash: str,
        notes: Optional[str] = None
    ) -> bool:
        """
        Mark an entry as superseded by another.

        Args:
            old_hash: Hash of entry to mark as superseded
            by_hash: Hash of superseding entry
            notes: Optional notes about supersession

        Returns:
            True if successful
        """
        # Normalize hashes
        if not old_hash.startswith("sha256:"):
            old_hash = f"sha256:{old_hash}"
        if not by_hash.startswith("sha256:"):
            by_hash = f"sha256:{by_hash}"

        if old_hash not in self._entries:
            logger.warning(f"Entry not found: {old_hash}")
            return False

        old_entry = self._entries[old_hash]
        old_entry.superseded_by = by_hash
        old_entry.superseded_at = datetime.now().isoformat()
        if notes:
            old_entry.notes = notes

        # Update superseding entry
        if by_hash in self._entries:
            self._entries[by_hash].supersedes = old_hash

        # Update lineage
        if old_hash in self._lineage:
            if by_hash not in self._lineage[old_hash]:
                self._lineage[old_hash].append(by_hash)
        else:
            self._lineage[old_hash] = [by_hash]

        self._save()

        logger.info(f"Marked {old_hash[:20]}... as superseded by {by_hash[:20]}...")
        return True

    def get_entry(self, file_hash: str) -> Optional[ManifestEntry]:
        """Get entry by hash."""
        if not file_hash.startswith("sha256:"):
            file_hash = f"sha256:{file_hash}"
        return self._entries.get(file_hash)

    def get_entry_by_name(self, archived_name: str) -> Optional[ManifestEntry]:
        """Get entry by archived filename."""
        for entry in self._entries.values():
            if entry.archived_name == archived_name:
                return entry
        return None

    def get_active_entries(self) -> List[ManifestEntry]:
        """Get all non-superseded entries."""
        return [e for e in self._entries.values() if e.is_active]

    def get_all_entries(self) -> List[ManifestEntry]:
        """Get all entries including superseded."""
        return list(self._entries.values())

    def get_entries_as_of(self, as_of_date: date) -> List[ManifestEntry]:
        """
        Get entries that were active as of a specific date.

        This enables point-in-time audit - "what was the state on Date X?"

        Args:
            as_of_date: Date to query

        Returns:
            List of entries that were active on that date
        """
        result = []
        as_of_str = as_of_date.isoformat()

        for entry in self._entries.values():
            # Entry must have been archived before the date
            archived_date = entry.archived_at[:10]  # Get date part
            if archived_date > as_of_str:
                continue

            # Entry must not have been superseded before the date
            if entry.superseded_at:
                superseded_date = entry.superseded_at[:10]
                if superseded_date <= as_of_str:
                    continue

            result.append(entry)

        return result

    def get_lineage(self, file_hash: str) -> List[str]:
        """
        Get supersession lineage for a file.

        Args:
            file_hash: Starting file hash

        Returns:
            List of hashes in supersession chain
        """
        if not file_hash.startswith("sha256:"):
            file_hash = f"sha256:{file_hash}"

        return self._lineage.get(file_hash, [])

    def get_full_lineage_chain(self, file_hash: str) -> List[str]:
        """
        Get complete lineage chain starting from earliest version.

        Args:
            file_hash: Any file hash in the chain

        Returns:
            Complete chain from oldest to newest
        """
        if not file_hash.startswith("sha256:"):
            file_hash = f"sha256:{file_hash}"

        # Find root (entry with no supersedes)
        current = file_hash
        visited = set()
        while True:
            if current in visited:
                break  # Prevent infinite loop
            visited.add(current)

            entry = self._entries.get(current)
            if not entry or not entry.supersedes:
                break
            current = entry.supersedes

        # Build chain from root by following superseded_by links
        chain = [current]
        visited = set([current])
        while True:
            entry = self._entries.get(current)
            if not entry or not entry.superseded_by:
                break
            next_hash = entry.superseded_by
            if next_hash in visited:
                break  # Prevent infinite loop
            visited.add(next_hash)
            chain.append(next_hash)
            current = next_hash

        return chain

    def needs_reextraction(self, parser_type: str, current_version: str) -> List[ManifestEntry]:
        """
        Find entries that need re-extraction due to parser update.

        Args:
            parser_type: Type of parser (e.g., "CAMS_CAS")
            current_version: Current parser version

        Returns:
            List of entries extracted with older parser version
        """
        result = []

        for entry in self.get_active_entries():
            if entry.source_type != parser_type:
                continue

            entry_version = entry.extraction_metadata.parser_version
            if self._version_less_than(entry_version, current_version):
                result.append(entry)

        return result

    def _version_less_than(self, v1: str, v2: str) -> bool:
        """Compare semantic versions."""
        try:
            parts1 = [int(x) for x in v1.split(".")]
            parts2 = [int(x) for x in v2.split(".")]
            return parts1 < parts2
        except (ValueError, AttributeError):
            return v1 < v2

    def export_summary(self) -> Dict[str, Any]:
        """
        Export manifest summary for reporting.

        Returns:
            Summary dictionary
        """
        active = self.get_active_entries()
        superseded = [e for e in self._entries.values() if e.is_superseded]

        return {
            "category": self.category_path.name,
            "total_entries": len(self._entries),
            "active_entries": len(active),
            "superseded_entries": len(superseded),
            "earliest_entry": min(
                (e.archived_at for e in self._entries.values()),
                default=None
            ),
            "latest_entry": max(
                (e.archived_at for e in self._entries.values()),
                default=None
            ),
            "total_size_bytes": sum(e.file_size for e in active),
            "sources": list(set(
                e.source_type for e in active if e.source_type
            )),
        }


def calculate_file_hash(file_path: Path) -> str:
    """
    Calculate SHA256 hash of file content.

    Args:
        file_path: Path to file

    Returns:
        Hash string in "sha256:hexdigest" format
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return f"sha256:{sha256.hexdigest()}"


def get_all_category_manifests(archive_base: Path) -> Dict[str, CategoryManifest]:
    """
    Load all category manifests under an archive directory.

    Args:
        archive_base: Base archive path (e.g., Users/Sanjay/archive)

    Returns:
        Dictionary of category name to CategoryManifest
    """
    manifests = {}

    for category_dir in archive_base.iterdir():
        if category_dir.is_dir():
            manifest = CategoryManifest(category_dir)
            manifests[category_dir.name] = manifest

    return manifests
