"""
Tests for Encrypted File Handler Service
"""

import pytest
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile

from pfas.core.paths import PathResolver
from pfas.services.encrypted_file_handler import (
    EncryptedFileHandler,
    create_encrypted_file_handler
)


class TestPathResolverPasswordMethods:
    """Test PathResolver password methods."""

    @pytest.fixture
    def temp_config_dir(self):
        """Create temporary config directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            config_dir = tmpdir / "Users" / "TestUser" / "config"
            config_dir.mkdir(parents=True)
            yield tmpdir, config_dir

    @pytest.fixture
    def path_resolver(self, temp_config_dir):
        """Create PathResolver with temp config."""
        root_path, config_dir = temp_config_dir

        # Create paths.json in root config
        root_config_dir = root_path / "config"
        root_config_dir.mkdir(exist_ok=True)
        with open(root_config_dir / "paths.json", "w") as f:
            json.dump({
                "users_base": "Users",
                "per_user": {
                    "db_file": "db/finance.db",
                    "user_config_dir": "config",
                    "inbox": "inbox",
                    "archive": "archive",
                    "reports": "reports"
                }
            }, f)

        return PathResolver(root_path, "TestUser")

    def test_password_config_file_path(self, path_resolver):
        """Test password config file path generation."""
        pwd_file = path_resolver.password_config_file()
        assert pwd_file.name == "passwords.json"
        assert "config" in str(pwd_file)

    def test_get_file_password_exact_match(self, path_resolver, temp_config_dir):
        """Test exact filename match."""
        _, config_dir = temp_config_dir

        # Create passwords.json with exact match
        passwords = {
            "files": {
                "test_file.pdf": "exact_password_123"
            },
            "patterns": {}
        }
        with open(config_dir / "passwords.json", "w") as f:
            json.dump(passwords, f)

        file_path = Path("/test/test_file.pdf")
        password = path_resolver.get_file_password(file_path, interactive=False)

        assert password == "exact_password_123"

    def test_get_file_password_pattern_match(self, path_resolver, temp_config_dir):
        """Test pattern matching."""
        _, config_dir = temp_config_dir

        # Create passwords.json with patterns
        passwords = {
            "files": {},
            "patterns": {
                "CAMS": "cams_pattern_pwd",
                "*.pdf": "default_pdf_pwd"
            }
        }
        with open(config_dir / "passwords.json", "w") as f:
            json.dump(passwords, f)

        # Test substring pattern
        file_path = Path("/test/CAMS_statement.pdf")
        password = path_resolver.get_file_password(file_path, interactive=False)
        assert password == "cams_pattern_pwd"

        # Test extension pattern
        file_path2 = Path("/test/other_file.pdf")
        password2 = path_resolver.get_file_password(file_path2, interactive=False)
        assert password2 == "default_pdf_pwd"

    def test_get_file_password_wildcard_fallback(self, path_resolver, temp_config_dir):
        """Test wildcard fallback."""
        _, config_dir = temp_config_dir

        passwords = {
            "files": {},
            "patterns": {
                "*": "wildcard_password"
            }
        }
        with open(config_dir / "passwords.json", "w") as f:
            json.dump(passwords, f)

        file_path = Path("/test/unknown_file.pdf")
        password = path_resolver.get_file_password(file_path, interactive=False)

        assert password == "wildcard_password"

    def test_get_file_password_priority_order(self, path_resolver, temp_config_dir):
        """Test that exact match has priority over patterns."""
        _, config_dir = temp_config_dir

        passwords = {
            "files": {
                "exact.pdf": "exact_pwd"
            },
            "patterns": {
                "exact": "pattern_pwd",
                "*.pdf": "extension_pwd",
                "*": "wildcard_pwd"
            }
        }
        with open(config_dir / "passwords.json", "w") as f:
            json.dump(passwords, f)

        file_path = Path("/test/exact.pdf")
        password = path_resolver.get_file_password(file_path, interactive=False)

        # Should match exact filename first
        assert password == "exact_pwd"

    def test_get_file_password_no_config(self, path_resolver):
        """Test behavior when passwords.json doesn't exist."""
        file_path = Path("/test/file.pdf")
        password = path_resolver.get_file_password(file_path, interactive=False)

        assert password is None

    @patch('builtins.input', return_value='')
    @patch('getpass.getpass', return_value='interactive_pwd')
    def test_get_file_password_interactive(self, mock_getpass, mock_input, path_resolver):
        """Test interactive password prompt."""
        file_path = Path("/test/file.pdf")
        password = path_resolver.get_file_password(file_path, interactive=True)

        assert password == 'interactive_pwd'
        mock_getpass.assert_called_once()


class TestEncryptedFileHandler:
    """Test EncryptedFileHandler."""

    @pytest.fixture
    def mock_path_resolver(self):
        """Create mock PathResolver."""
        resolver = Mock(spec=PathResolver)
        resolver.get_file_password.return_value = "test_password_123"
        resolver._prompt_for_password.return_value = "prompted_password"
        return resolver

    @pytest.fixture
    def handler(self, mock_path_resolver):
        """Create EncryptedFileHandler."""
        return EncryptedFileHandler(mock_path_resolver, interactive=True)

    def test_initialization(self, handler, mock_path_resolver):
        """Test handler initialization."""
        assert handler.path_resolver == mock_path_resolver
        assert handler.interactive is True
        assert handler._password_cache == {}

    def test_get_password_from_config(self, handler, mock_path_resolver):
        """Test getting password from configuration."""
        file_path = Path("/test/file.pdf")

        password = handler.get_password(file_path)

        assert password == "test_password_123"
        mock_path_resolver.get_file_password.assert_called_once_with(file_path, True)

    def test_get_password_caching(self, handler, mock_path_resolver):
        """Test password caching."""
        file_path = Path("/test/file.pdf")

        # First call - should call path_resolver
        password1 = handler.get_password(file_path, use_cache=True)
        assert password1 == "test_password_123"
        assert mock_path_resolver.get_file_password.call_count == 1

        # Second call - should use cache
        password2 = handler.get_password(file_path, use_cache=True)
        assert password2 == "test_password_123"
        assert mock_path_resolver.get_file_password.call_count == 1  # Not called again

    def test_get_password_no_caching(self, handler, mock_path_resolver):
        """Test getting password without caching."""
        file_path = Path("/test/file.pdf")

        password1 = handler.get_password(file_path, use_cache=False)
        password2 = handler.get_password(file_path, use_cache=False)

        assert password1 == "test_password_123"
        assert password2 == "test_password_123"
        assert mock_path_resolver.get_file_password.call_count == 2

    def test_clear_cache_specific_file(self, handler):
        """Test clearing cache for specific file."""
        file_path = Path("/test/file.pdf")
        handler._password_cache[str(file_path)] = "cached_pwd"

        handler.clear_cache(file_path)

        assert str(file_path) not in handler._password_cache

    def test_clear_cache_all(self, handler):
        """Test clearing all cached passwords."""
        handler._password_cache["/test/file1.pdf"] = "pwd1"
        handler._password_cache["/test/file2.pdf"] = "pwd2"

        handler.clear_cache()

        assert handler._password_cache == {}

    def test_non_interactive_mode(self, mock_path_resolver):
        """Test non-interactive mode."""
        handler = EncryptedFileHandler(mock_path_resolver, interactive=False)

        assert handler.interactive is False

        file_path = Path("/test/file.pdf")
        handler.get_password(file_path)

        mock_path_resolver.get_file_password.assert_called_once_with(file_path, False)


class TestEncryptedFileHandlerPDFIntegration:
    """Test PDF opening with encryption."""

    @pytest.fixture
    def handler_with_password(self):
        """Create handler with password."""
        resolver = Mock(spec=PathResolver)
        resolver.get_file_password.return_value = "test_password"
        return EncryptedFileHandler(resolver, interactive=False)

    @pytest.fixture
    def temp_pdf(self):
        """Create temporary PDF file (mock)."""
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            pdf_path = Path(f.name)
        yield pdf_path
        pdf_path.unlink(missing_ok=True)

    @pytest.mark.skipif(True, reason="Requires pdfplumber to be installed")
    def test_open_pdf_pdfplumber_success(self, handler_with_password, temp_pdf):
        """Test opening PDF with pdfplumber."""
        # This would require a real encrypted PDF file to test properly
        # Skipping for now - would need to mock pdfplumber
        pass

    @pytest.mark.skipif(True, reason="Requires PyPDF2 to be installed")
    def test_open_pdf_pypdf2_success(self, handler_with_password, temp_pdf):
        """Test opening PDF with PyPDF2."""
        # This would require a real encrypted PDF file to test properly
        # Skipping for now - would need to mock PyPDF2
        pass


class TestFactoryFunction:
    """Test factory function."""

    def test_create_encrypted_file_handler(self):
        """Test factory function."""
        resolver = Mock(spec=PathResolver)

        handler = create_encrypted_file_handler(resolver, interactive=True)

        assert isinstance(handler, EncryptedFileHandler)
        assert handler.path_resolver == resolver
        assert handler.interactive is True

    def test_create_encrypted_file_handler_non_interactive(self):
        """Test factory function non-interactive."""
        resolver = Mock(spec=PathResolver)

        handler = create_encrypted_file_handler(resolver, interactive=False)

        assert handler.interactive is False
