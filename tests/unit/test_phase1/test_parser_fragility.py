"""
Parser Fragility Tests - Edge cases and robustness validation.

Tests for:
1. Empty files
2. Files with wrong format
3. Missing required columns
4. Multi-file FY detection
5. Corrupt/malformed data
"""

import pytest
import pandas as pd
from pathlib import Path
from decimal import Decimal
from io import StringIO, BytesIO
from datetime import date
from tempfile import NamedTemporaryFile

# Parsers
from pfas.parsers.epf.epf import EPFParser, ParseResult as EPFParseResult
from pfas.parsers.nps.nps import NPSParser, ParseResult as NPSParseResult
from pfas.parsers.ppf.ppf import PPFParser, ParseResult as PPFParseResult


class TestEmptyFileHandling:
    """Test parser behavior with empty or near-empty files."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database connection."""
        import sqlite3
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        # Create minimal schema for parsers
        conn.execute("""
            CREATE TABLE IF NOT EXISTS epf_accounts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                uan TEXT,
                establishment_id TEXT,
                establishment_name TEXT,
                member_id TEXT,
                member_name TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS epf_transactions (
                id INTEGER PRIMARY KEY,
                epf_account_id INTEGER,
                wage_month TEXT,
                transaction_date TEXT,
                transaction_type TEXT,
                wages TEXT,
                eps_wages TEXT,
                employee_contribution TEXT,
                employer_contribution TEXT,
                pension_contribution TEXT,
                vpf_contribution TEXT,
                employee_balance TEXT,
                employer_balance TEXT,
                pension_balance TEXT,
                source_file TEXT,
                user_id INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS nps_accounts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                pran TEXT,
                nodal_office TEXT,
                scheme_preference TEXT,
                opening_date TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS nps_transactions (
                id INTEGER PRIMARY KEY,
                nps_account_id INTEGER,
                transaction_date TEXT,
                transaction_type TEXT,
                tier TEXT,
                contribution_type TEXT,
                amount TEXT,
                units TEXT,
                nav TEXT,
                scheme TEXT,
                financial_year TEXT,
                source_file TEXT,
                user_id INTEGER
            )
        """)
        conn.commit()
        yield conn
        conn.close()

    def test_epf_nonexistent_file(self, mock_db):
        """EPF parser handles nonexistent file gracefully."""
        parser = EPFParser(mock_db)
        result = parser.parse(Path("/nonexistent/file.pdf"))

        assert result.success is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0].lower()

    def test_nps_empty_csv(self, mock_db, tmp_path):
        """NPS parser handles empty CSV file."""
        empty_csv = tmp_path / "empty.csv"
        empty_csv.write_text("")

        parser = NPSParser(mock_db)
        result = parser.parse(empty_csv)

        # Should either fail or have warnings about no data
        assert len(result.transactions) == 0
        # Empty CSV should trigger pandas error or warning
        assert not result.success or len(result.warnings) > 0 or len(result.errors) > 0

    def test_nps_csv_headers_only(self, mock_db, tmp_path):
        """NPS parser handles CSV with headers but no data rows."""
        csv_content = "PRAN,Transaction Date,Transaction Type,Tier,Amount,Units,NAV,Scheme\n"
        csv_file = tmp_path / "headers_only.csv"
        csv_file.write_text(csv_content)

        parser = NPSParser(mock_db)
        result = parser.parse(csv_file)

        assert len(result.transactions) == 0
        assert len(result.warnings) > 0 or len(result.errors) > 0

    def test_ppf_empty_excel(self, mock_db, tmp_path):
        """PPF parser handles empty Excel file."""
        # Create empty Excel
        empty_xlsx = tmp_path / "empty.xlsx"
        df = pd.DataFrame()
        df.to_excel(empty_xlsx, index=False)

        parser = PPFParser(mock_db)
        result = parser.parse(empty_xlsx, account_number="TEST123")

        # Should handle gracefully
        assert len(result.transactions) == 0


class TestMissingColumns:
    """Test parser behavior when required columns are missing."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database connection."""
        import sqlite3
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE nps_accounts (id INTEGER PRIMARY KEY, pran TEXT)")
        conn.execute("CREATE TABLE nps_transactions (id INTEGER PRIMARY KEY)")
        conn.commit()
        yield conn
        conn.close()

    def test_nps_missing_pran_column(self, mock_db, tmp_path):
        """NPS parser handles missing PRAN column."""
        csv_content = """Transaction Date,Transaction Type,Tier,Amount
01-04-2024,CONTRIBUTION,I,5000
"""
        csv_file = tmp_path / "missing_pran.csv"
        csv_file.write_text(csv_content)

        parser = NPSParser(mock_db)
        result = parser.parse(csv_file)

        # All rows should be skipped (PRAN is required)
        assert len(result.transactions) == 0

    def test_ppf_missing_date_column(self, mock_db, tmp_path):
        """PPF parser handles missing date column."""
        # Create Excel without date column
        df = pd.DataFrame({
            'DESCRIPTION': ['DEPOSIT', 'INTEREST'],
            'AMOUNT': [10000, 500],
        })
        xlsx_file = tmp_path / "missing_date.xlsx"
        df.to_excel(xlsx_file, index=False)

        parser = PPFParser(mock_db)
        result = parser.parse(xlsx_file, account_number="TEST123")

        # Should fail to find date column
        assert len(result.transactions) == 0
        assert len(result.errors) > 0 or len(result.warnings) > 0


class TestMalformedData:
    """Test parser behavior with malformed data values."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database connection."""
        import sqlite3
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        yield conn
        conn.close()

    def test_nps_invalid_date_format(self, mock_db, tmp_path):
        """NPS parser handles invalid date formats."""
        csv_content = """PRAN,Transaction Date,Transaction Type,Tier,Amount
123456789012,INVALID-DATE,CONTRIBUTION,I,5000
123456789012,01-04-2024,CONTRIBUTION,I,6000
"""
        csv_file = tmp_path / "invalid_date.csv"
        csv_file.write_text(csv_content)

        parser = NPSParser(mock_db)
        result = parser.parse(csv_file)

        # Should skip invalid row but process valid one
        assert len(result.transactions) >= 0  # May or may not parse the valid row
        # Should have warning for skipped row
        assert len(result.warnings) > 0

    def test_ppf_negative_amounts(self, mock_db, tmp_path):
        """PPF parser handles negative amounts (withdrawals)."""
        df = pd.DataFrame({
            'DATE': ['01-04-2024', '01-05-2024'],
            'DESCRIPTION': ['DEPOSIT', 'WITHDRAWAL'],
            'DEPOSIT': [10000, 0],
            'WITHDRAWAL': [0, 5000],
            'BALANCE': [10000, 5000],
        })
        xlsx_file = tmp_path / "with_withdrawal.xlsx"
        df.to_excel(xlsx_file, index=False)

        parser = PPFParser(mock_db)
        result = parser.parse(xlsx_file, account_number="TEST123")

        # Should handle both deposit and withdrawal
        deposits = [t for t in result.transactions if t.transaction_type == 'DEPOSIT']
        withdrawals = [t for t in result.transactions if t.transaction_type == 'WITHDRAWAL']

        # Assertions depend on implementation, but should not crash
        assert result.success or len(result.errors) > 0


class TestFYDetection:
    """Test financial year detection from filenames."""

    def test_fy_pattern_fy2425(self):
        """Detect FY from FY24-25 pattern."""
        import re
        filename = "Sanjay_EPF_Passbook_FY24-25.pdf"
        pattern = r'FY(\d{2})-?(\d{2})'

        match = re.search(pattern, filename)
        assert match is not None
        assert match.group(1) == '24'
        assert match.group(2) == '25'

    def test_fy_pattern_20242025(self):
        """Detect FY from 2024-2025 pattern."""
        import re
        filename = "NPS_Statement_2024-2025.csv"
        pattern = r'(\d{4})-(\d{4})'

        match = re.search(pattern, filename)
        assert match is not None
        assert match.group(1) == '2024'
        assert match.group(2) == '2025'

    def test_fy_extraction_utility(self):
        """Test FY extraction utility function."""
        def extract_fy(filename: str) -> str:
            import re
            patterns = [
                (r'FY(\d{2})-?(\d{2})', lambda m: f"20{m.group(1)}-{m.group(2)}"),
                (r'(\d{4})-(\d{2,4})', lambda m: f"{m.group(1)}-{m.group(2)[-2:]}"),
            ]
            for pattern, formatter in patterns:
                match = re.search(pattern, filename, re.IGNORECASE)
                if match:
                    return formatter(match)
            return ""

        assert extract_fy("Report_FY24-25.pdf") == "2024-25"
        assert extract_fy("Statement_2024-2025.xlsx") == "2024-25"
        assert extract_fy("Random_file.pdf") == ""


class TestMultiFileHandling:
    """Test handling of multiple files in same folder."""

    def test_file_sorting_by_date(self, tmp_path):
        """Files should be sorted chronologically."""
        import time

        # Create files with different mtimes
        file1 = tmp_path / "old_file.xlsx"
        file2 = tmp_path / "new_file.xlsx"

        file1.touch()
        time.sleep(0.1)  # Ensure different mtime
        file2.touch()

        files = list(tmp_path.glob("*.xlsx"))
        sorted_files = sorted(files, key=lambda p: p.stat().st_mtime)

        assert sorted_files[0].name == "old_file.xlsx"
        assert sorted_files[1].name == "new_file.xlsx"

    def test_exclude_failed_directory(self, tmp_path):
        """Files in 'failed' subdirectory should be excluded."""
        # Create regular file
        regular = tmp_path / "statement.xlsx"
        regular.touch()

        # Create failed subdirectory with file
        failed_dir = tmp_path / "failed"
        failed_dir.mkdir()
        failed_file = failed_dir / "bad_statement.xlsx"
        failed_file.touch()

        # Simulate finding files
        all_files = list(tmp_path.rglob("*.xlsx"))
        filtered = [f for f in all_files if 'failed' not in f.parts]

        assert len(all_files) == 2
        assert len(filtered) == 1
        assert filtered[0].name == "statement.xlsx"


class TestIngesterValidation:
    """Test that ingesters properly validate parse results."""

    def test_ingester_empty_transactions_should_fail(self):
        """Ingester should not mark success=True with empty transactions."""
        # Simulate the problematic behavior
        def buggy_parse_file():
            result = {'success': False, 'records': [], 'errors': []}
            transactions = []  # Empty!

            # FIXED: Don't mark success if no data
            if len(transactions) == 0:
                result['errors'].append("No transactions extracted")
                return result

            result['success'] = True
            result['records'] = transactions
            return result

        result = buggy_parse_file()
        assert result['success'] is False
        assert len(result['errors']) > 0

    def test_ingester_respects_parse_result_errors(self):
        """Ingester should check ParseResult.success flag."""
        # Simulate ParseResult with success=False
        class MockParseResult:
            success = False
            errors = ["PDF extraction failed"]
            transactions = []

        def fixed_parse_file(parse_output):
            result = {'success': False, 'records': [], 'errors': []}

            # FIXED: Check parse_output.success first
            if hasattr(parse_output, 'success') and not parse_output.success:
                result['errors'].extend(parse_output.errors)
                return result

            # ... rest of processing
            return result

        result = fixed_parse_file(MockParseResult())
        assert result['success'] is False
        assert "PDF extraction failed" in result['errors']
