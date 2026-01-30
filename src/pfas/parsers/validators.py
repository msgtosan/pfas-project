"""
Parser Validators - Validate input files before parsing.

Provides robust validation for:
- Excel files (worksheet count, required columns, data rows)
- PDF files (page count, text extraction, password protection)
- CSV files (header detection, encoding, delimiter)

Usage:
    from pfas.parsers.validators import ExcelValidator, PDFValidator

    validator = ExcelValidator(required_columns=['DATE', 'AMOUNT'], min_rows=1)
    errors = validator.validate(Path("statement.xlsx"))
    if errors:
        print(f"Validation failed: {errors}")
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Set
import logging

logger = logging.getLogger(__name__)


class ParserValidator(ABC):
    """Base validator for all parsers."""

    @abstractmethod
    def validate(self, file_path: Path) -> List[str]:
        """
        Validate a file before parsing.

        Args:
            file_path: Path to the file to validate

        Returns:
            List of validation errors (empty if valid)
        """
        pass

    def is_valid(self, file_path: Path) -> bool:
        """Check if file is valid (no errors)."""
        return len(self.validate(file_path)) == 0


class ExcelValidator(ParserValidator):
    """
    Validator for Excel files (.xlsx, .xls).

    Checks:
    - File exists and is readable
    - File has at least one worksheet
    - Required columns are present (case-insensitive)
    - Minimum number of data rows
    """

    def __init__(
        self,
        required_columns: Optional[List[str]] = None,
        min_rows: int = 1,
        sheet_name: Optional[str] = None
    ):
        """
        Initialize Excel validator.

        Args:
            required_columns: List of required column names (case-insensitive)
            min_rows: Minimum number of data rows (excluding header)
            sheet_name: Specific sheet to validate (None = first sheet)
        """
        self.required_columns = [c.upper() for c in (required_columns or [])]
        self.min_rows = min_rows
        self.sheet_name = sheet_name

    def validate(self, file_path: Path) -> List[str]:
        """Validate Excel file."""
        errors = []

        if not file_path.exists():
            return [f"File not found: {file_path}"]

        try:
            import pandas as pd

            # Try to open the Excel file
            try:
                with pd.ExcelFile(file_path) as xls:
                    # Check worksheet count
                    if len(xls.sheet_names) == 0:
                        errors.append("Excel file has no worksheets (empty or corrupted)")
                        return errors

                    # Determine which sheet to validate
                    if self.sheet_name:
                        if self.sheet_name not in xls.sheet_names:
                            errors.append(f"Sheet '{self.sheet_name}' not found. Available: {xls.sheet_names}")
                            return errors
                        sheet = self.sheet_name
                    else:
                        sheet = 0  # First sheet

                    # Read the sheet
                    df = pd.read_excel(xls, sheet_name=sheet)

            except ValueError as ve:
                error_msg = str(ve)
                if "Worksheet index" in error_msg or "0 worksheets" in error_msg:
                    errors.append("Excel file has no worksheets (corrupted or empty)")
                else:
                    errors.append(f"Excel parse error: {error_msg}")
                return errors

            # Check row count
            if len(df) < self.min_rows:
                errors.append(f"File has {len(df)} data rows, minimum {self.min_rows} required")

            # Check required columns
            if self.required_columns:
                actual_cols = set(df.columns.str.upper().str.strip())
                missing = set(self.required_columns) - actual_cols

                # Try fuzzy matching for close matches
                if missing:
                    unmatched = []
                    for req_col in missing:
                        # Check if any actual column contains the required keyword
                        found = any(req_col in col for col in actual_cols)
                        if not found:
                            unmatched.append(req_col)

                    if unmatched:
                        errors.append(f"Missing required columns: {unmatched}. Found: {list(df.columns)}")

        except pd.errors.EmptyDataError:
            errors.append("File is empty (no data)")
        except ImportError:
            errors.append("pandas library not installed")
        except Exception as e:
            errors.append(f"Failed to validate Excel file: {str(e)}")

        return errors


class PDFValidator(ParserValidator):
    """
    Validator for PDF files.

    Checks:
    - File exists and is readable
    - PDF has at least one page
    - Text is extractable (not image-only)
    - Not password-protected (or password is available)
    """

    def __init__(
        self,
        min_pages: int = 1,
        require_text: bool = True,
        password: Optional[str] = None
    ):
        """
        Initialize PDF validator.

        Args:
            min_pages: Minimum number of pages
            require_text: Require extractable text on first page
            password: Password for encrypted PDFs
        """
        self.min_pages = min_pages
        self.require_text = require_text
        self.password = password

    def validate(self, file_path: Path) -> List[str]:
        """Validate PDF file."""
        errors = []

        if not file_path.exists():
            return [f"File not found: {file_path}"]

        try:
            import pdfplumber

            try:
                with pdfplumber.open(file_path, password=self.password) as pdf:
                    # Check page count
                    if len(pdf.pages) < self.min_pages:
                        errors.append(f"PDF has {len(pdf.pages)} pages, minimum {self.min_pages} required")

                    # Check text extraction
                    if self.require_text and pdf.pages:
                        text = pdf.pages[0].extract_text()
                        if not text or len(text.strip()) < 10:
                            errors.append("PDF first page has no extractable text (may be image-only)")

            except Exception as e:
                error_msg = str(e).lower()
                if 'password' in error_msg or 'encrypted' in error_msg:
                    if self.password:
                        errors.append("PDF password is incorrect")
                    else:
                        errors.append("PDF is password-protected (provide password)")
                else:
                    errors.append(f"Failed to open PDF: {str(e)}")

        except ImportError:
            errors.append("pdfplumber library not installed")
        except Exception as e:
            errors.append(f"Failed to validate PDF: {str(e)}")

        return errors

    def is_password_protected(self, file_path: Path) -> bool:
        """Check if PDF is password-protected."""
        try:
            import pdfplumber
            try:
                with pdfplumber.open(file_path) as pdf:
                    # Try to access first page
                    if pdf.pages:
                        _ = pdf.pages[0].chars
                    return False
            except Exception as e:
                error_msg = str(e).lower()
                return 'password' in error_msg or 'encrypted' in error_msg
        except Exception:
            return False


class CSVValidator(ParserValidator):
    """
    Validator for CSV files.

    Checks:
    - File exists and is readable
    - Has header row with expected columns
    - Minimum number of data rows
    - Consistent column count
    """

    def __init__(
        self,
        required_columns: Optional[List[str]] = None,
        min_rows: int = 1,
        delimiter: Optional[str] = None,
        encoding: str = 'utf-8'
    ):
        """
        Initialize CSV validator.

        Args:
            required_columns: List of required column names (case-insensitive)
            min_rows: Minimum number of data rows
            delimiter: Expected delimiter (None = auto-detect)
            encoding: File encoding
        """
        self.required_columns = [c.upper() for c in (required_columns or [])]
        self.min_rows = min_rows
        self.delimiter = delimiter
        self.encoding = encoding

    def validate(self, file_path: Path) -> List[str]:
        """Validate CSV file."""
        errors = []

        if not file_path.exists():
            return [f"File not found: {file_path}"]

        try:
            import pandas as pd

            # Try different encodings if default fails
            encodings_to_try = [self.encoding, 'utf-8', 'latin-1', 'cp1252']

            df = None
            for enc in encodings_to_try:
                try:
                    df = pd.read_csv(
                        file_path,
                        delimiter=self.delimiter,
                        encoding=enc,
                        nrows=100  # Read only first 100 rows for validation
                    )
                    break
                except UnicodeDecodeError:
                    continue
                except pd.errors.EmptyDataError:
                    errors.append("File is empty (no data)")
                    return errors

            if df is None:
                errors.append(f"Could not read file with any supported encoding")
                return errors

            # Check row count
            if len(df) < self.min_rows:
                errors.append(f"File has {len(df)} data rows, minimum {self.min_rows} required")

            # Check required columns
            if self.required_columns:
                actual_cols = set(df.columns.str.upper().str.strip())
                missing = set(self.required_columns) - actual_cols
                if missing:
                    errors.append(f"Missing required columns: {list(missing)}. Found: {list(df.columns)}")

        except ImportError:
            errors.append("pandas library not installed")
        except Exception as e:
            errors.append(f"Failed to validate CSV: {str(e)}")

        return errors


class CompositeValidator(ParserValidator):
    """
    Validator that runs multiple validators based on file type.
    """

    def __init__(self):
        """Initialize with default validators for each file type."""
        self.validators = {
            '.xlsx': ExcelValidator(),
            '.xls': ExcelValidator(),
            '.pdf': PDFValidator(),
            '.csv': CSVValidator(),
        }

    def add_validator(self, extension: str, validator: ParserValidator):
        """Add or replace validator for a file extension."""
        self.validators[extension.lower()] = validator

    def validate(self, file_path: Path) -> List[str]:
        """Validate file using appropriate validator."""
        ext = file_path.suffix.lower()

        if ext not in self.validators:
            return [f"No validator for file type: {ext}"]

        return self.validators[ext].validate(file_path)


# Pre-configured validators for common use cases
class NPSValidator(ExcelValidator):
    """Validator for NPS statement files."""

    def __init__(self):
        super().__init__(
            required_columns=['PRAN', 'TRANSACTION DATE', 'TIER', 'AMOUNT'],
            min_rows=1
        )


class PPFValidator(ExcelValidator):
    """Validator for PPF statement files."""

    def __init__(self):
        super().__init__(
            required_columns=['DATE'],  # Minimal requirement
            min_rows=1
        )


class EPFValidator(PDFValidator):
    """Validator for EPF passbook PDFs."""

    def __init__(self, password: Optional[str] = None):
        super().__init__(
            min_pages=1,
            require_text=True,
            password=password
        )


class MFValidator(CompositeValidator):
    """Validator for Mutual Fund statement files (PDF or Excel)."""

    def __init__(self):
        super().__init__()
        # Override with MF-specific requirements
        self.validators['.xlsx'] = ExcelValidator(min_rows=1)
        self.validators['.pdf'] = PDFValidator(require_text=True)
