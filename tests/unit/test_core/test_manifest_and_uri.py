"""
Tests for Temporal Versioning Manifest and URI Resolver.

Tests cover:
- ManifestEntry creation and serialization
- CategoryManifest file operations
- Supersession tracking and lineage
- Point-in-time queries
- URI resolution and normalization
- Multi-user isolation
"""

import json
import pytest
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from decimal import Decimal

from pfas.core.manifest import (
    ManifestEntry,
    CategoryManifest,
    ExtractionMetadata,
    StatementPeriod,
    calculate_file_hash,
    get_all_category_manifests,
)
from pfas.core.uri_resolver import (
    PFASURIResolver,
    ParsedURI,
    create_uri,
    URIResolutionError,
    PFAS_SCHEME,
)


class TestExtractionMetadata:
    """Tests for ExtractionMetadata dataclass."""

    def test_default_values(self):
        """Test default metadata values."""
        meta = ExtractionMetadata()
        assert meta.parser_version == "1.0.0"
        assert meta.records_extracted == 0
        assert meta.encrypted is False
        assert meta.warnings == []

    def test_to_dict(self):
        """Test serialization to dict."""
        meta = ExtractionMetadata(
            parser_version="2.0.0",
            records_extracted=145,
            page_count=12,
            encrypted=True,
            password_hint="PAN",
        )
        d = meta.to_dict()
        assert d["parser_version"] == "2.0.0"
        assert d["records_extracted"] == 145
        assert d["encrypted"] is True

    def test_from_dict(self):
        """Test deserialization from dict."""
        d = {
            "parser_version": "1.5.0",
            "records_extracted": 50,
            "encrypted": False,
        }
        meta = ExtractionMetadata.from_dict(d)
        assert meta.parser_version == "1.5.0"
        assert meta.records_extracted == 50


class TestStatementPeriod:
    """Tests for StatementPeriod dataclass."""

    def test_to_dict(self):
        """Test serialization."""
        period = StatementPeriod(
            from_date="2024-04-01",
            to_date="2025-03-31"
        )
        d = period.to_dict()
        assert d["from"] == "2024-04-01"
        assert d["to"] == "2025-03-31"

    def test_from_dict(self):
        """Test deserialization."""
        d = {"from": "2023-01-01", "to": "2023-12-31"}
        period = StatementPeriod.from_dict(d)
        assert period.from_date == "2023-01-01"
        assert period.to_date == "2023-12-31"

    def test_from_none(self):
        """Test handling of None input."""
        period = StatementPeriod.from_dict(None)
        assert period.from_date is None
        assert period.to_date is None


class TestManifestEntry:
    """Tests for ManifestEntry dataclass."""

    def test_creation(self):
        """Test basic entry creation."""
        entry = ManifestEntry(
            file_hash="sha256:abc123",
            original_name="test.pdf",
            archived_name="2026-01-17_Sanjay_CAMS_test.pdf",
            archived_at="2026-01-17T10:30:00",
        )
        assert entry.file_hash == "sha256:abc123"
        assert entry.is_active is True
        assert entry.is_superseded is False

    def test_superseded_entry(self):
        """Test superseded entry detection."""
        entry = ManifestEntry(
            file_hash="sha256:abc123",
            original_name="test.pdf",
            archived_name="2026-01-17_Sanjay_CAMS_test.pdf",
            archived_at="2026-01-17T10:30:00",
            superseded_by="sha256:def456",
            superseded_at="2026-01-18T10:30:00",
        )
        assert entry.is_superseded is True
        assert entry.is_active is False

    def test_round_trip_serialization(self):
        """Test to_dict and from_dict round trip."""
        entry = ManifestEntry(
            file_hash="sha256:abc123",
            original_name="test.pdf",
            archived_name="archived_test.pdf",
            archived_at="2026-01-17T10:30:00",
            file_size=12345,
            statement_period=StatementPeriod("2024-04-01", "2025-03-31"),
            extraction_metadata=ExtractionMetadata(parser_version="2.0.0"),
            source_type="CAMS",
        )

        d = entry.to_dict()
        restored = ManifestEntry.from_dict(d)

        assert restored.file_hash == entry.file_hash
        assert restored.file_size == entry.file_size
        assert restored.statement_period.from_date == "2024-04-01"
        assert restored.extraction_metadata.parser_version == "2.0.0"


class TestCategoryManifest:
    """Tests for CategoryManifest class."""

    @pytest.fixture
    def temp_category(self, tmp_path):
        """Create temporary category directory."""
        category_path = tmp_path / "Mutual-Fund"
        category_path.mkdir()
        return category_path

    def test_init_empty(self, temp_category):
        """Test initialization with empty manifest."""
        manifest = CategoryManifest(temp_category)
        assert len(manifest.get_all_entries()) == 0
        assert (temp_category / "manifest.json").exists()

    def test_add_entry(self, temp_category):
        """Test adding entries."""
        manifest = CategoryManifest(temp_category)

        entry = manifest.add_entry(
            file_hash="abc123",
            original_name="cas.pdf",
            archived_name="2026-01-17_cas.pdf",
            file_size=10000,
            statement_period=("2024-04-01", "2025-03-31"),
            source_type="CAMS",
        )

        assert entry.file_hash == "sha256:abc123"
        assert len(manifest.get_all_entries()) == 1

        # Verify persistence
        manifest2 = CategoryManifest(temp_category)
        assert len(manifest2.get_all_entries()) == 1

    def test_supersession(self, temp_category):
        """Test file supersession tracking."""
        manifest = CategoryManifest(temp_category)

        # Add original
        manifest.add_entry(
            file_hash="hash1",
            original_name="cas_v1.pdf",
            archived_name="archived_v1.pdf",
        )

        # Add replacement
        manifest.add_entry(
            file_hash="hash2",
            original_name="cas_v2.pdf",
            archived_name="archived_v2.pdf",
            supersedes="hash1",
        )

        # Check supersession
        old = manifest.get_entry("sha256:hash1")
        assert old.is_superseded is True
        assert old.superseded_by == "sha256:hash2"

        new = manifest.get_entry("sha256:hash2")
        assert new.is_active is True
        assert new.supersedes == "sha256:hash1"

        # Check active entries
        active = manifest.get_active_entries()
        assert len(active) == 1
        assert active[0].file_hash == "sha256:hash2"

    def test_lineage_chain(self, temp_category):
        """Test lineage chain retrieval."""
        manifest = CategoryManifest(temp_category)

        # Create chain: v1 -> v2 -> v3
        manifest.add_entry(file_hash="v1", original_name="v1.pdf", archived_name="a_v1.pdf")
        manifest.add_entry(file_hash="v2", original_name="v2.pdf", archived_name="a_v2.pdf", supersedes="v1")
        manifest.add_entry(file_hash="v3", original_name="v3.pdf", archived_name="a_v3.pdf", supersedes="v2")

        chain = manifest.get_full_lineage_chain("sha256:v3")
        assert len(chain) == 3
        assert chain[0] == "sha256:v1"
        assert chain[-1] == "sha256:v3"

    def test_point_in_time_query(self, temp_category):
        """Test point-in-time audit capability."""
        manifest = CategoryManifest(temp_category)

        # Add entry on day 1
        manifest.add_entry(
            file_hash="old_hash",
            original_name="old.pdf",
            archived_name="archived_old.pdf",
        )

        # Manually set archived_at for testing
        old_entry = manifest.get_entry("sha256:old_hash")
        old_entry.archived_at = "2026-01-10T10:00:00"

        # Add newer entry and mark old as superseded
        manifest.add_entry(
            file_hash="new_hash",
            original_name="new.pdf",
            archived_name="archived_new.pdf",
            supersedes="old_hash",
        )

        # Query as of day before supersession
        manifest._entries["sha256:old_hash"].superseded_at = "2026-01-15T10:00:00"
        manifest._entries["sha256:new_hash"].archived_at = "2026-01-15T10:00:00"

        entries_jan12 = manifest.get_entries_as_of(date(2026, 1, 12))
        assert len(entries_jan12) == 1
        assert entries_jan12[0].file_hash == "sha256:old_hash"

    def test_needs_reextraction(self, temp_category):
        """Test parser version checking for re-extraction."""
        manifest = CategoryManifest(temp_category)

        manifest.add_entry(
            file_hash="hash1",
            original_name="file1.pdf",
            archived_name="archived1.pdf",
            source_type="CAMS",
            extraction_metadata={"parser_version": "1.0.0"},
        )
        manifest.add_entry(
            file_hash="hash2",
            original_name="file2.pdf",
            archived_name="archived2.pdf",
            source_type="CAMS",
            extraction_metadata={"parser_version": "2.0.0"},
        )

        # Check for parser 2.0.0 - only v1 needs reextraction
        needs_reextract = manifest.needs_reextraction("CAMS", "2.0.0")
        assert len(needs_reextract) == 1
        assert needs_reextract[0].file_hash == "sha256:hash1"


class TestPFASURIResolver:
    """Tests for URI Resolver."""

    @pytest.fixture
    def data_root(self, tmp_path):
        """Create mock data root structure."""
        users_path = tmp_path / "Users"
        users_path.mkdir()

        sanjay_path = users_path / "Sanjay"
        sanjay_path.mkdir()

        # Create subdirectories
        (sanjay_path / "archive").mkdir()
        (sanjay_path / "archive" / "Mutual-Fund").mkdir()
        (sanjay_path / "inbox").mkdir()
        (sanjay_path / "config").mkdir()

        # Create a test file
        test_file = sanjay_path / "archive" / "Mutual-Fund" / "test.pdf"
        test_file.write_text("test content")

        return tmp_path

    def test_to_uri(self, data_root):
        """Test path to URI conversion."""
        resolver = PFASURIResolver(data_root)

        path = data_root / "Users" / "Sanjay" / "archive" / "Mutual-Fund" / "test.pdf"
        uri = resolver.to_uri(path)

        assert uri.startswith("pfas://users/sanjay/archive/")
        assert "mutual-fund" in uri or "Mutual-Fund" in uri
        assert "test.pdf" in uri

    def test_resolve(self, data_root):
        """Test URI to path resolution."""
        resolver = PFASURIResolver(data_root)

        uri = "pfas://users/sanjay/archive/mutual-fund/test.pdf"
        path = resolver.resolve(uri)

        assert path.exists()
        assert path.name == "test.pdf"

    def test_round_trip(self, data_root):
        """Test path -> URI -> path round trip."""
        resolver = PFASURIResolver(data_root)

        original_path = data_root / "Users" / "Sanjay" / "archive" / "Mutual-Fund" / "test.pdf"
        uri = resolver.to_uri(original_path)
        resolved_path = resolver.resolve(uri)

        assert resolved_path.exists()
        assert resolved_path.name == original_path.name

    def test_parse_uri(self, data_root):
        """Test URI parsing."""
        resolver = PFASURIResolver(data_root)

        parsed = resolver.parse("pfas://users/sanjay/archive/mutual-fund/cas.pdf")

        assert parsed.scheme == "pfas"
        assert parsed.user_namespace == "sanjay"
        assert parsed.area == "archive"
        assert parsed.category == "mutual-fund"
        assert parsed.filename == "cas.pdf"
        assert parsed.is_valid

    def test_invalid_uri(self, data_root):
        """Test invalid URI handling."""
        resolver = PFASURIResolver(data_root)

        parsed = resolver.parse("http://example.com/file.pdf")
        assert not parsed.is_valid

    def test_normalize_uri(self, data_root):
        """Test URI normalization."""
        resolver = PFASURIResolver(data_root)

        uri = "pfas://users/SANJAY/Archive/Mutual-Fund/CAS.PDF"
        normalized = resolver.normalize_uri(uri)

        assert "sanjay" in normalized
        assert "archive" in normalized
        assert "CAS.PDF" in normalized  # Filename preserved

    def test_get_user_namespace(self, data_root):
        """Test user namespace extraction."""
        resolver = PFASURIResolver(data_root)

        path = data_root / "Users" / "Sanjay" / "archive" / "test.pdf"
        namespace = resolver.get_user_namespace(path)

        assert namespace == "sanjay"

    def test_uri_exists(self, data_root):
        """Test URI existence check."""
        resolver = PFASURIResolver(data_root)

        # Existing file
        assert resolver.uri_exists("pfas://users/sanjay/archive/mutual-fund/test.pdf")

        # Non-existing file
        assert not resolver.uri_exists("pfas://users/sanjay/archive/mutual-fund/nonexistent.pdf")

    def test_list_user_namespaces(self, data_root):
        """Test listing user namespaces."""
        resolver = PFASURIResolver(data_root)
        namespaces = resolver.list_user_namespaces()

        assert "sanjay" in namespaces


class TestCreateURI:
    """Tests for create_uri helper function."""

    def test_basic_uri(self):
        """Test basic URI creation."""
        uri = create_uri("Sanjay", "archive")
        assert uri == "pfas://users/sanjay/archive"

    def test_uri_with_category(self):
        """Test URI with category."""
        uri = create_uri("Sanjay", "archive", category="Mutual-Fund")
        assert uri == "pfas://users/sanjay/archive/mutual-fund"

    def test_uri_with_filename(self):
        """Test URI with filename."""
        uri = create_uri("Sanjay", "archive", category="Mutual-Fund", filename="cas.pdf")
        assert uri == "pfas://users/sanjay/archive/mutual-fund/cas.pdf"


class TestCalculateFileHash:
    """Tests for file hash calculation."""

    def test_calculate_hash(self, tmp_path):
        """Test file hash calculation."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello, World!")

        hash_result = calculate_file_hash(test_file)

        assert hash_result.startswith("sha256:")
        assert len(hash_result) == 7 + 64  # "sha256:" + 64 hex chars

    def test_same_content_same_hash(self, tmp_path):
        """Test that identical content produces same hash."""
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"

        file1.write_text("Same content")
        file2.write_text("Same content")

        assert calculate_file_hash(file1) == calculate_file_hash(file2)

    def test_different_content_different_hash(self, tmp_path):
        """Test that different content produces different hash."""
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"

        file1.write_text("Content A")
        file2.write_text("Content B")

        assert calculate_file_hash(file1) != calculate_file_hash(file2)


class TestIntegration:
    """Integration tests for manifest and URI resolver together."""

    @pytest.fixture
    def data_setup(self, tmp_path):
        """Set up test data structure."""
        users = tmp_path / "Users" / "TestUser"
        archive = users / "archive" / "Mutual-Fund"
        archive.mkdir(parents=True)

        # Create test file
        test_file = archive / "cas.pdf"
        test_file.write_bytes(b"PDF content here")

        return tmp_path

    def test_archive_with_manifest_and_uri(self, data_setup):
        """Test full workflow: archive file, update manifest, generate URI."""
        archive_path = data_setup / "Users" / "TestUser" / "archive" / "Mutual-Fund"
        file_path = archive_path / "cas.pdf"

        # Calculate hash
        file_hash = calculate_file_hash(file_path)

        # Create manifest entry
        manifest = CategoryManifest(archive_path)
        entry = manifest.add_entry(
            file_hash=file_hash,
            original_name="original_cas.pdf",
            archived_name="cas.pdf",
            file_size=file_path.stat().st_size,
            statement_period=("2024-04-01", "2025-03-31"),
            extraction_metadata={
                "parser_version": "1.0.0",
                "records_extracted": 145,
                "page_count": 12,
            },
            source_type="CAMS",
        )

        # Generate URI
        resolver = PFASURIResolver(data_setup)
        uri = resolver.to_uri(file_path)

        # Verify everything works together
        assert entry.file_hash == file_hash
        assert "testuser" in uri.lower()
        assert resolver.uri_exists(uri)

        # Verify manifest persistence
        manifest2 = CategoryManifest(archive_path)
        loaded_entry = manifest2.get_entry(file_hash)
        assert loaded_entry is not None
        assert loaded_entry.extraction_metadata.records_extracted == 145
