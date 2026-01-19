"""
MF Statement Scanner - Recursively scans inbox for MF statements.

Detects RTA (CAMS/KARVY) from folder structure or file content.
Handles password-protected PDFs with user prompt.
"""

import hashlib
import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Optional, Callable

logger = logging.getLogger(__name__)


class RTA(Enum):
    """Registrar and Transfer Agent."""
    CAMS = "CAMS"
    KARVY = "KARVY"  # Also known as KFintech
    UNKNOWN = "UNKNOWN"


class FileType(Enum):
    """Supported file types."""
    PDF = "pdf"
    XLSX = "xlsx"
    XLS = "xls"


@dataclass
class ScannedFile:
    """
    Represents a scanned statement file.

    Attributes:
        path: Absolute path to the file
        rta: Detected RTA (CAMS/KARVY/UNKNOWN)
        file_type: File extension type
        password_protected: Whether PDF is password protected
        detected_from: How RTA was detected ("path" or "content")
        file_hash: SHA256 hash for deduplication
        size_bytes: File size in bytes
    """
    path: Path
    rta: RTA
    file_type: FileType
    password_protected: bool = False
    detected_from: str = "path"
    file_hash: str = ""
    size_bytes: int = 0

    def __post_init__(self):
        """Calculate file hash if not provided."""
        if not self.file_hash and self.path.exists():
            self.file_hash = self._calculate_hash()
            self.size_bytes = self.path.stat().st_size

    def _calculate_hash(self) -> str:
        """Calculate SHA256 hash of file."""
        sha256 = hashlib.sha256()
        with open(self.path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()


@dataclass
class ScanResult:
    """Result of scanning inbox folder."""
    files: List[ScannedFile] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    total_scanned: int = 0

    @property
    def success(self) -> bool:
        """Return True if no errors."""
        return len(self.errors) == 0

    @property
    def cams_files(self) -> List[ScannedFile]:
        """Return only CAMS files."""
        return [f for f in self.files if f.rta == RTA.CAMS]

    @property
    def karvy_files(self) -> List[ScannedFile]:
        """Return only KARVY files."""
        return [f for f in self.files if f.rta == RTA.KARVY]

    @property
    def pdf_files(self) -> List[ScannedFile]:
        """Return only PDF files."""
        return [f for f in self.files if f.file_type == FileType.PDF]

    @property
    def excel_files(self) -> List[ScannedFile]:
        """Return only Excel files."""
        return [f for f in self.files if f.file_type in (FileType.XLSX, FileType.XLS)]


class MFStatementScanner:
    """
    Scanner for Mutual Fund statement files.

    Recursively scans inbox/Mutual-Fund/ folder structure:
    - inbox/Mutual-Fund/CAMS/*.pdf, *.xlsx
    - inbox/Mutual-Fund/KARVY/*.pdf, *.xlsx

    Detects RTA from:
    1. Folder name (CAMS/KARVY)
    2. File content (fallback)

    Usage:
        scanner = MFStatementScanner(inbox_path)
        result = scanner.scan()

        for file in result.cams_files:
            print(f"CAMS file: {file.path}")
    """

    # File extensions to scan
    SUPPORTED_EXTENSIONS = {'.pdf', '.xlsx', '.xls'}

    # RTA detection patterns in file content
    CAMS_PATTERNS = [
        b'Computer Age Management Services',
        b'CAMS',
        b'www.camsonline.com',
        b'MyCams',
    ]

    KARVY_PATTERNS = [
        b'KFin Technologies',
        b'Karvy',
        b'KFINTECH',
        b'www.kfintech.com',
        b'mfs.kfintech.com',
    ]

    def __init__(
        self,
        inbox_path: Path,
        password_callback: Optional[Callable[[Path], str]] = None
    ):
        """
        Initialize scanner.

        Args:
            inbox_path: Path to inbox/Mutual-Fund/ folder
            password_callback: Optional callback for password-protected PDFs
                              Signature: callback(file_path) -> password
        """
        self.inbox_path = Path(inbox_path)
        self.password_callback = password_callback
        self._pdfplumber = None

    def scan(self) -> ScanResult:
        """
        Scan inbox folder for MF statements.

        Returns:
            ScanResult with list of ScannedFile objects
        """
        result = ScanResult()

        if not self.inbox_path.exists():
            result.errors.append(f"Inbox path does not exist: {self.inbox_path}")
            return result

        if not self.inbox_path.is_dir():
            result.errors.append(f"Inbox path is not a directory: {self.inbox_path}")
            return result

        # Scan recursively
        for file_path in self._find_files():
            result.total_scanned += 1

            try:
                scanned = self._process_file(file_path)
                if scanned:
                    result.files.append(scanned)
            except Exception as e:
                result.warnings.append(f"Error processing {file_path.name}: {str(e)}")
                logger.warning(f"Error processing {file_path}: {e}")

        logger.info(
            f"Scan complete: {result.total_scanned} files scanned, "
            f"{len(result.files)} valid files found"
        )

        return result

    def _find_files(self) -> List[Path]:
        """
        Find all supported files in inbox folder.

        Returns:
            List of file paths
        """
        files = []

        for ext in self.SUPPORTED_EXTENSIONS:
            # Use rglob for recursive search
            files.extend(self.inbox_path.rglob(f"*{ext}"))

        # Sort by modification time (newest first)
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

        return files

    def _process_file(self, file_path: Path) -> Optional[ScannedFile]:
        """
        Process a single file and create ScannedFile.

        Args:
            file_path: Path to file

        Returns:
            ScannedFile or None if file should be skipped
        """
        # Determine file type
        ext = file_path.suffix.lower()
        if ext == '.pdf':
            file_type = FileType.PDF
        elif ext == '.xlsx':
            file_type = FileType.XLSX
        elif ext == '.xls':
            file_type = FileType.XLS
        else:
            return None

        # Detect RTA from folder structure first
        rta, detected_from = self._detect_rta_from_path(file_path)

        # Check if PDF is password protected
        password_protected = False
        if file_type == FileType.PDF:
            password_protected = self._is_pdf_password_protected(file_path)

        # If RTA still unknown, try content detection (only for unprotected files)
        if rta == RTA.UNKNOWN and not password_protected:
            content_rta = self._detect_rta_from_content(file_path, file_type)
            if content_rta != RTA.UNKNOWN:
                rta = content_rta
                detected_from = "content"

        return ScannedFile(
            path=file_path,
            rta=rta,
            file_type=file_type,
            password_protected=password_protected,
            detected_from=detected_from
        )

    def _detect_rta_from_path(self, file_path: Path) -> tuple[RTA, str]:
        """
        Detect RTA from folder path.

        Checks if any parent folder is named CAMS or KARVY.

        Args:
            file_path: Path to file

        Returns:
            Tuple of (RTA, detection_method)
        """
        path_parts = [p.upper() for p in file_path.parts]

        if 'CAMS' in path_parts:
            return RTA.CAMS, "path"
        elif 'KARVY' in path_parts or 'KFINTECH' in path_parts:
            return RTA.KARVY, "path"

        return RTA.UNKNOWN, "path"

    def _detect_rta_from_content(self, file_path: Path, file_type: FileType) -> RTA:
        """
        Detect RTA from file content.

        Args:
            file_path: Path to file
            file_type: Type of file

        Returns:
            Detected RTA
        """
        try:
            if file_type == FileType.PDF:
                return self._detect_rta_from_pdf(file_path)
            else:
                return self._detect_rta_from_excel(file_path)
        except Exception as e:
            logger.debug(f"Could not detect RTA from content: {e}")
            return RTA.UNKNOWN

    def _detect_rta_from_pdf(self, file_path: Path) -> RTA:
        """Detect RTA from PDF content."""
        try:
            # Read first few KB for pattern matching
            with open(file_path, 'rb') as f:
                content = f.read(50000)  # First 50KB

            # Check CAMS patterns
            for pattern in self.CAMS_PATTERNS:
                if pattern in content:
                    return RTA.CAMS

            # Check KARVY patterns
            for pattern in self.KARVY_PATTERNS:
                if pattern in content:
                    return RTA.KARVY

        except Exception as e:
            logger.debug(f"PDF content detection failed: {e}")

        return RTA.UNKNOWN

    def _detect_rta_from_excel(self, file_path: Path) -> RTA:
        """Detect RTA from Excel content."""
        try:
            import pandas as pd

            # Read first sheet, first few rows
            df = pd.read_excel(file_path, sheet_name=0, nrows=20, header=None)

            # Convert to string for pattern matching
            content = df.to_string().upper()

            if 'CAMS' in content or 'COMPUTER AGE' in content:
                return RTA.CAMS
            elif 'KARVY' in content or 'KFINTECH' in content or 'KFIN' in content:
                return RTA.KARVY

        except Exception as e:
            logger.debug(f"Excel content detection failed: {e}")

        return RTA.UNKNOWN

    def _is_pdf_password_protected(self, file_path: Path) -> bool:
        """
        Check if PDF is password protected.

        Args:
            file_path: Path to PDF

        Returns:
            True if password protected
        """
        try:
            # Lazy load pdfplumber
            if self._pdfplumber is None:
                try:
                    import pdfplumber
                    self._pdfplumber = pdfplumber
                except ImportError:
                    logger.warning("pdfplumber not installed, cannot check PDF protection")
                    return False

            # Try to open without password
            try:
                with self._pdfplumber.open(file_path) as pdf:
                    # If we can open and read first page, not protected
                    if pdf.pages:
                        _ = pdf.pages[0].chars
                    return False
            except Exception as e:
                error_msg = str(e).lower()
                if 'password' in error_msg or 'encrypted' in error_msg:
                    return True
                # Other errors - assume not password protected
                return False

        except Exception as e:
            logger.debug(f"Error checking PDF protection: {e}")
            return False

    def get_password_for_file(self, file_path: Path) -> Optional[str]:
        """
        Get password for a protected file.

        Uses callback if provided, otherwise prompts user.

        Args:
            file_path: Path to password-protected file

        Returns:
            Password string or None
        """
        if self.password_callback:
            return self.password_callback(file_path)

        # Default: prompt user
        try:
            import getpass
            print(f"\nPassword required for: {file_path.name}")
            print("(Common passwords: PAN number in uppercase, e.g., ABCDE1234F)")
            return getpass.getpass("Enter password: ")
        except Exception:
            return None


def scan_mf_inbox(
    inbox_path: Path,
    password_callback: Optional[Callable[[Path], str]] = None
) -> ScanResult:
    """
    Convenience function to scan MF inbox.

    Args:
        inbox_path: Path to inbox/Mutual-Fund/ folder
        password_callback: Optional callback for passwords

    Returns:
        ScanResult

    Example:
        result = scan_mf_inbox(Path("Data/Users/Sanjay/inbox/Mutual-Fund"))
        print(f"Found {len(result.cams_files)} CAMS files")
    """
    scanner = MFStatementScanner(inbox_path, password_callback)
    return scanner.scan()
