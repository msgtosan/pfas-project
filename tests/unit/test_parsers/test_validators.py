"""
Tests for Parser Validators.
"""

import pytest
import pandas as pd
from pathlib import Path
from pfas.parsers.validators import (
    ExcelValidator,
    PDFValidator,
    CSVValidator,
    CompositeValidator,
    NPSValidator,
    PPFValidator
)


class TestExcelValidator:
    """Test Excel file validation."""

    @pytest.fixture
    def valid_xlsx(self, tmp_path) -> Path:
        """Create valid Excel file with data."""
        file_path = tmp_path / "valid.xlsx"
        df = pd.DataFrame({
            'DATE': ['2024-01-01', '2024-01-02'],
            'AMOUNT': [100, 200],
            'TYPE': ['DEPOSIT', 'WITHDRAWAL']
        })
        df.to_excel(file_path, index=False)
        return file_path

    @pytest.fixture
    def empty_xlsx(self, tmp_path) -> Path:
        """Create empty Excel file (headers only)."""
        file_path = tmp_path / "empty.xlsx"
        df = pd.DataFrame(columns=['DATE', 'AMOUNT'])
        df.to_excel(file_path, index=False)
        return file_path

    def test_valid_file(self, valid_xlsx):
        """Test validation passes for valid file."""
        validator = ExcelValidator(min_rows=1)
        errors = validator.validate(valid_xlsx)
        assert len(errors) == 0

    def test_file_not_found(self, tmp_path):
        """Test validation fails for missing file."""
        validator = ExcelValidator()
        errors = validator.validate(tmp_path / "nonexistent.xlsx")
        assert len(errors) == 1
        assert "not found" in errors[0].lower()

    def test_min_rows_check(self, empty_xlsx):
        """Test validation fails when below min rows."""
        validator = ExcelValidator(min_rows=1)
        errors = validator.validate(empty_xlsx)
        assert len(errors) == 1
        assert "0 data rows" in errors[0]

    def test_required_columns(self, valid_xlsx):
        """Test validation with required columns."""
        validator = ExcelValidator(required_columns=['DATE', 'AMOUNT'])
        errors = validator.validate(valid_xlsx)
        assert len(errors) == 0

    def test_missing_columns(self, valid_xlsx):
        """Test validation fails for missing columns."""
        validator = ExcelValidator(required_columns=['DATE', 'AMOUNT', 'MISSING'])
        errors = validator.validate(valid_xlsx)
        assert len(errors) == 1
        assert "MISSING" in errors[0]

    def test_is_valid_helper(self, valid_xlsx, tmp_path):
        """Test is_valid() helper method."""
        validator = ExcelValidator()
        assert validator.is_valid(valid_xlsx) is True
        assert validator.is_valid(tmp_path / "nonexistent.xlsx") is False


class TestCSVValidator:
    """Test CSV file validation."""

    @pytest.fixture
    def valid_csv(self, tmp_path) -> Path:
        """Create valid CSV file."""
        file_path = tmp_path / "valid.csv"
        df = pd.DataFrame({
            'PRAN': ['123456789012'],
            'TRANSACTION DATE': ['2024-01-15'],
            'TIER': ['I'],
            'AMOUNT': [50000]
        })
        df.to_csv(file_path, index=False)
        return file_path

    @pytest.fixture
    def empty_csv(self, tmp_path) -> Path:
        """Create empty CSV file."""
        file_path = tmp_path / "empty.csv"
        file_path.write_text("")
        return file_path

    def test_valid_file(self, valid_csv):
        """Test validation passes for valid CSV."""
        validator = CSVValidator(min_rows=1)
        errors = validator.validate(valid_csv)
        assert len(errors) == 0

    def test_empty_file(self, empty_csv):
        """Test validation fails for empty CSV."""
        validator = CSVValidator()
        errors = validator.validate(empty_csv)
        assert len(errors) >= 1
        assert any("empty" in e.lower() for e in errors)

    def test_required_columns(self, valid_csv):
        """Test validation with required columns."""
        validator = CSVValidator(required_columns=['PRAN', 'AMOUNT'])
        errors = validator.validate(valid_csv)
        assert len(errors) == 0

    def test_missing_columns(self, valid_csv):
        """Test validation fails for missing columns."""
        validator = CSVValidator(required_columns=['PRAN', 'MISSING_COL'])
        errors = validator.validate(valid_csv)
        assert len(errors) == 1
        assert "MISSING_COL" in errors[0]


class TestCompositeValidator:
    """Test composite validator that handles multiple file types."""

    def test_validates_xlsx(self, tmp_path):
        """Test composite validator uses Excel validator for xlsx."""
        file_path = tmp_path / "test.xlsx"
        df = pd.DataFrame({'A': [1, 2, 3]})
        df.to_excel(file_path, index=False)

        validator = CompositeValidator()
        errors = validator.validate(file_path)
        assert len(errors) == 0

    def test_validates_csv(self, tmp_path):
        """Test composite validator uses CSV validator for csv."""
        file_path = tmp_path / "test.csv"
        df = pd.DataFrame({'A': [1, 2, 3]})
        df.to_csv(file_path, index=False)

        validator = CompositeValidator()
        errors = validator.validate(file_path)
        assert len(errors) == 0

    def test_unsupported_extension(self, tmp_path):
        """Test composite validator rejects unsupported extensions."""
        file_path = tmp_path / "test.xyz"
        file_path.write_text("test")

        validator = CompositeValidator()
        errors = validator.validate(file_path)
        assert len(errors) == 1
        assert "No validator" in errors[0]


class TestNPSValidator:
    """Test NPS-specific validator."""

    def test_valid_nps_file(self, tmp_path):
        """Test validation of valid NPS file."""
        file_path = tmp_path / "nps.xlsx"
        df = pd.DataFrame({
            'PRAN': ['123456789012'],
            'Transaction Date': ['2024-01-15'],
            'Tier': ['I'],
            'Amount': [50000]
        })
        df.to_excel(file_path, index=False)

        validator = NPSValidator()
        errors = validator.validate(file_path)
        assert len(errors) == 0

    def test_missing_nps_columns(self, tmp_path):
        """Test validation fails for missing NPS columns."""
        file_path = tmp_path / "incomplete_nps.xlsx"
        df = pd.DataFrame({
            'PRAN': ['123456789012'],
            # Missing: TRANSACTION DATE, TIER, AMOUNT
        })
        df.to_excel(file_path, index=False)

        validator = NPSValidator()
        errors = validator.validate(file_path)
        assert len(errors) >= 1


class TestPPFValidator:
    """Test PPF-specific validator."""

    def test_valid_ppf_file(self, tmp_path):
        """Test validation of valid PPF file."""
        file_path = tmp_path / "ppf.xlsx"
        df = pd.DataFrame({
            'Date': ['2024-01-15', '2024-02-15'],
            'Description': ['DEPOSIT', 'INTEREST'],
            'Amount': [50000, 3500]
        })
        df.to_excel(file_path, index=False)

        validator = PPFValidator()
        errors = validator.validate(file_path)
        assert len(errors) == 0
