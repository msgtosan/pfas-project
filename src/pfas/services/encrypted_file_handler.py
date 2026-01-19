"""
Encrypted File Handler Service

Provides centralized password management and decryption for encrypted PDFs and other files.
Integrates with user password configuration files.
"""

import logging
from pathlib import Path
from typing import Optional, Callable, Any
from contextlib import contextmanager

try:
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    pdfplumber = None

try:
    import PyPDF2
    PYPDF2_SUPPORT = True
except ImportError:
    PYPDF2_SUPPORT = False
    PyPDF2 = None

from pfas.core.paths import PathResolver

logger = logging.getLogger(__name__)


class EncryptedFileHandler:
    """
    Centralized handler for encrypted files.

    Manages password lookup and provides unified interface for opening
    encrypted PDFs and other protected files.
    """

    def __init__(self, path_resolver: PathResolver, interactive: bool = True):
        """
        Initialize encrypted file handler.

        Args:
            path_resolver: PathResolver instance for password lookup
            interactive: Allow interactive password prompts
        """
        self.path_resolver = path_resolver
        self.interactive = interactive
        self._password_cache = {}  # Cache passwords per session

    def get_password(self, file_path: Path, use_cache: bool = True) -> Optional[str]:
        """
        Get password for a file.

        Args:
            file_path: Path to the encrypted file
            use_cache: Use cached password if available

        Returns:
            Password string or None
        """
        # Check cache first
        if use_cache and str(file_path) in self._password_cache:
            return self._password_cache[str(file_path)]

        # Get password from config or prompt
        password = self.path_resolver.get_file_password(file_path, self.interactive)

        # Cache successful password
        if password and use_cache:
            self._password_cache[str(file_path)] = password

        return password

    @contextmanager
    def open_pdf_pdfplumber(self, file_path: Path, password: Optional[str] = None):
        """
        Open PDF with pdfplumber, handling encryption automatically.

        Args:
            file_path: Path to PDF file
            password: Optional password override

        Yields:
            pdfplumber.PDF object

        Raises:
            ImportError: If pdfplumber not installed
            Exception: If PDF cannot be opened
        """
        if not PDF_SUPPORT:
            raise ImportError(
                "pdfplumber is required for PDF support. "
                "Install it with: pip install pdfplumber"
            )

        # Get password if not provided
        if password is None:
            password = self.get_password(file_path)

        # Try opening with password
        try:
            with pdfplumber.open(str(file_path), password=password or "") as pdf:
                yield pdf
        except Exception as e:
            error_msg = str(e).lower()
            if "password" in error_msg or "encrypted" in error_msg:
                # If configured password failed, try interactive prompt once
                if not self.interactive or password is not None:
                    raise Exception(f"PDF password incorrect or missing for: {file_path.name}") from e

                logger.warning(f"Configured password failed for {file_path.name}, prompting user")
                password = self.path_resolver._prompt_for_password(file_path)

                if password:
                    with pdfplumber.open(str(file_path), password=password) as pdf:
                        # Cache successful password
                        self._password_cache[str(file_path)] = password
                        yield pdf
                else:
                    raise Exception(f"No password provided for: {file_path.name}") from e
            else:
                raise

    @contextmanager
    def open_pdf_pypdf2(self, file_path: Path, password: Optional[str] = None):
        """
        Open PDF with PyPDF2, handling encryption automatically.

        Args:
            file_path: Path to PDF file
            password: Optional password override

        Yields:
            PyPDF2.PdfReader object

        Raises:
            ImportError: If PyPDF2 not installed
            Exception: If PDF cannot be opened
        """
        if not PYPDF2_SUPPORT:
            raise ImportError(
                "PyPDF2 is required. Install it with: pip install PyPDF2"
            )

        # Get password if not provided
        if password is None:
            password = self.get_password(file_path)

        # Try opening with password
        try:
            with open(file_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)

                # Check if encrypted
                if reader.is_encrypted:
                    if not password:
                        raise Exception(f"PDF is encrypted but no password available: {file_path.name}")

                    # Try decrypting
                    if not reader.decrypt(password):
                        # Try interactive prompt once
                        if self.interactive:
                            logger.warning(f"Configured password failed for {file_path.name}, prompting user")
                            password = self.path_resolver._prompt_for_password(file_path)

                            if password and reader.decrypt(password):
                                # Cache successful password
                                self._password_cache[str(file_path)] = password
                            else:
                                raise Exception(f"PDF password incorrect: {file_path.name}")
                        else:
                            raise Exception(f"PDF password incorrect: {file_path.name}")

                yield reader
        except Exception as e:
            error_msg = str(e).lower()
            if "password" in error_msg or "encrypted" in error_msg:
                raise Exception(f"Failed to decrypt PDF: {file_path.name}") from e
            else:
                raise

    def check_pdf_encrypted(self, file_path: Path) -> bool:
        """
        Check if a PDF is encrypted.

        Args:
            file_path: Path to PDF file

        Returns:
            True if encrypted, False otherwise
        """
        if not PYPDF2_SUPPORT:
            logger.warning("PyPDF2 not available, cannot check encryption status")
            return False

        try:
            with open(file_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                return reader.is_encrypted
        except Exception as e:
            logger.warning(f"Failed to check encryption status for {file_path}: {e}")
            return False

    def try_pdf_passwords(
        self,
        file_path: Path,
        passwords: list[str],
        library: str = "pdfplumber"
    ) -> Optional[str]:
        """
        Try multiple passwords on a PDF and return the working one.

        Args:
            file_path: Path to PDF file
            passwords: List of passwords to try
            library: PDF library to use ('pdfplumber' or 'pypdf2')

        Returns:
            Working password or None
        """
        for password in passwords:
            try:
                if library == "pdfplumber" and PDF_SUPPORT:
                    with pdfplumber.open(str(file_path), password=password) as pdf:
                        # Try to access first page to verify password works
                        if len(pdf.pages) > 0:
                            return password
                elif library == "pypdf2" and PYPDF2_SUPPORT:
                    with open(file_path, 'rb') as f:
                        reader = PyPDF2.PdfReader(f)
                        if reader.is_encrypted:
                            if reader.decrypt(password):
                                return password
            except:
                continue

        return None

    def clear_cache(self, file_path: Optional[Path] = None):
        """
        Clear cached passwords.

        Args:
            file_path: If provided, clear only this file's password
        """
        if file_path:
            self._password_cache.pop(str(file_path), None)
        else:
            self._password_cache.clear()


def create_encrypted_file_handler(
    path_resolver: PathResolver,
    interactive: bool = True
) -> EncryptedFileHandler:
    """
    Factory function to create EncryptedFileHandler.

    Args:
        path_resolver: PathResolver instance
        interactive: Allow interactive password prompts

    Returns:
        EncryptedFileHandler instance
    """
    return EncryptedFileHandler(path_resolver, interactive)
