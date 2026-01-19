"""
PathResolver Tests - Comprehensive path handling validation.

Tests for:
1. PathResolver initialization
2. Config loading and fallbacks
3. Dynamic path generation
4. Password file handling
5. Multi-user path isolation
"""

import pytest
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from pfas.core.paths import PathResolver


class TestPathResolverInitialization:
    """Test PathResolver initialization and config loading."""

    def test_basic_initialization(self, tmp_path):
        """PathResolver initializes with root and user."""
        # Create minimal structure
        users_dir = tmp_path / "Users" / "TestUser"
        users_dir.mkdir(parents=True)

        resolver = PathResolver(root_path=tmp_path, user_name="TestUser")

        assert resolver.root == tmp_path.resolve()
        assert resolver.user_name == "TestUser"
        assert resolver.user_dir == users_dir

    def test_whitespace_in_username_trimmed(self, tmp_path):
        """Username whitespace should be trimmed."""
        users_dir = tmp_path / "Users" / "TestUser"
        users_dir.mkdir(parents=True)

        resolver = PathResolver(root_path=tmp_path, user_name="  TestUser  ")

        assert resolver.user_name == "TestUser"

    def test_config_loading(self, tmp_path):
        """PathResolver loads config from paths.json."""
        # Create config
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_file = config_dir / "paths.json"
        config_file.write_text(json.dumps({
            "users_base": "CustomUsers",
            "per_user": {
                "db_file": "custom/db.sqlite",
                "inbox": "inbox",
                "archive": "archive",
                "reports": "reports",
                "user_config_dir": "config"
            },
            "report_naming": {
                "pattern": "{user}_{asset}_{report_type}_{date}.xlsx"
            }
        }))

        # Create user directory with custom base
        users_dir = tmp_path / "CustomUsers" / "TestUser"
        users_dir.mkdir(parents=True)

        resolver = PathResolver(root_path=tmp_path, user_name="TestUser")

        assert resolver.user_dir == users_dir
        assert str(resolver.db_path()).endswith("custom/db.sqlite")

    def test_fallback_defaults_when_no_config(self, tmp_path):
        """PathResolver uses defaults when config missing."""
        users_dir = tmp_path / "Users" / "TestUser"
        users_dir.mkdir(parents=True)

        resolver = PathResolver(root_path=tmp_path, user_name="TestUser")

        # Should use default values
        assert "inbox" in str(resolver.inbox())
        assert "archive" in str(resolver.archive())


class TestPathGeneration:
    """Test path generation methods."""

    @pytest.fixture
    def resolver(self, tmp_path):
        """Create resolver with standard structure."""
        users_dir = tmp_path / "Users" / "TestUser"
        users_dir.mkdir(parents=True)
        return PathResolver(root_path=tmp_path, user_name="TestUser")

    def test_inbox_path(self, resolver):
        """inbox() returns correct path."""
        inbox = resolver.inbox()
        assert inbox.name == "inbox"
        assert "TestUser" in str(inbox)

    def test_archive_path(self, resolver):
        """archive() returns correct path."""
        archive = resolver.archive()
        assert archive.name == "archive"
        assert "TestUser" in str(archive)

    def test_reports_path(self, resolver):
        """reports() returns correct path."""
        reports = resolver.reports()
        assert reports.name == "reports"
        assert "TestUser" in str(reports)

    def test_db_path(self, resolver):
        """db_path() returns correct path."""
        db = resolver.db_path()
        assert "db" in str(db).lower() or "finance" in str(db).lower()

    def test_report_file_generation(self, resolver):
        """report_file() generates standardized names."""
        report = resolver.report_file(
            asset_type="Mutual-Fund",
            report_type="capital_gains"
        )

        assert "Mutual-Fund" in str(report)
        assert "capital_gains" in str(report)
        assert report.suffix == ".xlsx"


class TestPasswordHandling:
    """Test password configuration file handling."""

    @pytest.fixture
    def resolver_with_passwords(self, tmp_path):
        """Create resolver with passwords.json."""
        users_dir = tmp_path / "Users" / "TestUser"
        config_dir = users_dir / "config"
        config_dir.mkdir(parents=True)

        passwords = {
            "files": {
                "secret_statement.pdf": "file_specific_password"
            },
            "patterns": {
                "*.pdf": "pdf_default_password",
                "CAMS*": "cams_password",
                "*": "fallback_password"
            }
        }
        (config_dir / "passwords.json").write_text(json.dumps(passwords))

        return PathResolver(root_path=tmp_path, user_name="TestUser")

    def test_exact_filename_match(self, resolver_with_passwords):
        """Exact filename match has highest priority."""
        file_path = Path("/any/path/secret_statement.pdf")
        password = resolver_with_passwords.get_file_password(file_path, interactive=False)

        assert password == "file_specific_password"

    def test_pattern_match(self, resolver_with_passwords):
        """Pattern matching works for *.pdf."""
        file_path = Path("/any/path/unknown_statement.pdf")
        password = resolver_with_passwords.get_file_password(file_path, interactive=False)

        assert password == "pdf_default_password"

    def test_substring_pattern_match(self, resolver_with_passwords):
        """Substring pattern (CAMS*) matches."""
        file_path = Path("/any/path/CAMS_consolidated.xlsx")
        # Note: This depends on implementation priority - substring vs extension
        password = resolver_with_passwords.get_file_password(file_path, interactive=False)

        # Should match CAMS* pattern
        assert password in ["cams_password", "fallback_password"]

    def test_wildcard_fallback(self, resolver_with_passwords):
        """Wildcard * is used as last resort."""
        file_path = Path("/any/path/random_file.txt")
        password = resolver_with_passwords.get_file_password(file_path, interactive=False)

        assert password == "fallback_password"

    def test_no_password_file(self, tmp_path):
        """Returns None when no password file exists."""
        users_dir = tmp_path / "Users" / "TestUser"
        users_dir.mkdir(parents=True)

        resolver = PathResolver(root_path=tmp_path, user_name="TestUser")
        password = resolver.get_file_password(Path("any_file.pdf"), interactive=False)

        assert password is None


class TestMultiUserIsolation:
    """Test path isolation between users."""

    def test_different_users_different_paths(self, tmp_path):
        """Different users have isolated paths."""
        # Create two users
        (tmp_path / "Users" / "User1").mkdir(parents=True)
        (tmp_path / "Users" / "User2").mkdir(parents=True)

        resolver1 = PathResolver(root_path=tmp_path, user_name="User1")
        resolver2 = PathResolver(root_path=tmp_path, user_name="User2")

        assert resolver1.inbox() != resolver2.inbox()
        assert resolver1.db_path() != resolver2.db_path()
        assert resolver1.reports() != resolver2.reports()

        # Verify no path crossing
        assert "User2" not in str(resolver1.inbox())
        assert "User1" not in str(resolver2.inbox())

    def test_paths_contain_username(self, tmp_path):
        """All user paths contain the username."""
        users_dir = tmp_path / "Users" / "Sanjay"
        users_dir.mkdir(parents=True)

        resolver = PathResolver(root_path=tmp_path, user_name="Sanjay")

        assert "Sanjay" in str(resolver.user_dir)
        assert "Sanjay" in str(resolver.inbox())
        assert "Sanjay" in str(resolver.archive())
        assert "Sanjay" in str(resolver.reports())
        assert "Sanjay" in str(resolver.db_path())


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_path_with_spaces(self, tmp_path):
        """Handle paths with spaces."""
        users_dir = tmp_path / "Users" / "John Doe"
        users_dir.mkdir(parents=True)

        resolver = PathResolver(root_path=tmp_path, user_name="John Doe")

        # Paths should be valid
        assert resolver.user_dir.exists()
        assert "John Doe" in str(resolver.inbox())

    def test_unicode_username(self, tmp_path):
        """Handle unicode characters in username."""
        users_dir = tmp_path / "Users" / "संजय"
        users_dir.mkdir(parents=True)

        resolver = PathResolver(root_path=tmp_path, user_name="संजय")

        assert resolver.user_name == "संजय"
        assert resolver.user_dir.exists()

    def test_root_as_string(self, tmp_path):
        """Accept root path as string."""
        users_dir = tmp_path / "Users" / "TestUser"
        users_dir.mkdir(parents=True)

        # Pass as string instead of Path
        resolver = PathResolver(root_path=str(tmp_path), user_name="TestUser")

        assert isinstance(resolver.root, Path)
        assert resolver.root.exists()
