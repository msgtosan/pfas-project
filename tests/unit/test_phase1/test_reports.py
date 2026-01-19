"""Tests for MF Report Generator."""

import pytest
from pathlib import Path
from decimal import Decimal
from datetime import date
import tempfile

from pfas.core.database import DatabaseManager
from pfas.reports.mf_ingestion_report import (
    MFIngestionReportGenerator,
    ReportConfig,
    generate_mf_reports
)


@pytest.fixture
def test_db():
    """Create in-memory test database with sample data."""
    db = DatabaseManager()
    DatabaseManager.reset_instance()
    db = DatabaseManager()
    conn = db.init(":memory:", "test_password")

    # Create test user
    conn.execute(
        "INSERT INTO users (name, pan_encrypted, pan_salt) VALUES (?, ?, ?)",
        ("TestUser", b"encrypted", b"salt")
    )

    # Create AMC
    conn.execute("INSERT INTO mf_amcs (name) VALUES (?)", ("Test AMC",))

    # Create scheme
    conn.execute(
        """
        INSERT INTO mf_schemes (amc_id, name, isin, asset_class, user_id)
        VALUES (1, 'Test Equity Fund', 'INF123456789', 'EQUITY', 1)
        """
    )

    # Create folio
    conn.execute(
        """
        INSERT INTO mf_folios (user_id, scheme_id, folio_number)
        VALUES (1, 1, '12345')
        """
    )

    # Create transactions
    conn.execute(
        """
        INSERT INTO mf_transactions
        (folio_id, transaction_type, date, units, nav, amount, user_id, source_file)
        VALUES
        (1, 'PURCHASE', '2025-01-15', '100.0', '50.0', '5000.0', 1, 'cas.xlsx'),
        (1, 'PURCHASE', '2025-06-15', '50.0', '55.0', '2750.0', 1, 'cas.xlsx'),
        (1, 'REDEMPTION', '2025-12-15', '30.0', '60.0', '1800.0', 1, 'cas.xlsx')
        """
    )

    # Create holdings
    conn.execute(
        """
        INSERT INTO mf_holdings
        (user_id, scheme_name, folio_number, units, nav, nav_date, current_value, cost_value, rta, amc_name)
        VALUES (1, 'Test Equity Fund', '12345', '120.0', '60.0', '2026-01-15', '7200.0', '5950.0', 'CAMS', 'Test AMC')
        """
    )

    # Create ingestion log
    conn.execute(
        """
        INSERT INTO ingestion_log
        (user_id, source_file, file_hash, asset_type, rta_source, status, records_processed, records_skipped)
        VALUES (1, 'cas.xlsx', 'hash123', 'Mutual-Fund', 'CAMS', 'COMPLETED', 3, 0)
        """
    )

    conn.commit()

    yield conn

    db.close()
    DatabaseManager.reset_instance()


@pytest.fixture
def temp_output_dir():
    """Create temporary output directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestReportConfig:
    """Test suite for ReportConfig."""

    def test_filename_generation(self, temp_output_dir):
        """Test standardized filename generation."""
        config = ReportConfig(
            user_name="Sanjay",
            output_dir=temp_output_dir,
            asset_type="Mutual-Fund"
        )

        filename = config.get_filename("Transactions")
        today = date.today().isoformat()

        assert filename.startswith("Sanjay_Mutual-Fund_Transactions_")
        assert today in filename
        assert filename.endswith(".xlsx")

    def test_path_generation(self, temp_output_dir):
        """Test full path generation."""
        config = ReportConfig(
            user_name="Sanjay",
            output_dir=temp_output_dir
        )

        path = config.get_path("Holdings")

        assert path.parent == temp_output_dir
        assert "Holdings" in path.name


class TestMFIngestionReportGenerator:
    """Test suite for MFIngestionReportGenerator."""

    def test_generator_init(self, test_db, temp_output_dir):
        """Test generator initialization."""
        config = ReportConfig(user_name="TestUser", output_dir=temp_output_dir)
        generator = MFIngestionReportGenerator(test_db, 1, config)

        assert generator.conn == test_db
        assert generator.user_id == 1

    def test_generate_transactions_report(self, test_db, temp_output_dir):
        """Test transactions report generation."""
        config = ReportConfig(user_name="TestUser", output_dir=temp_output_dir)
        generator = MFIngestionReportGenerator(test_db, 1, config)

        path = generator.generate_transactions_report()

        assert path.exists()
        assert "Transactions" in path.name

        # Verify it's a valid Excel file
        import pandas as pd
        df = pd.read_excel(path, sheet_name='Transactions')
        assert len(df) == 3  # 3 transactions

    def test_generate_holdings_report(self, test_db, temp_output_dir):
        """Test holdings report generation."""
        config = ReportConfig(user_name="TestUser", output_dir=temp_output_dir)
        generator = MFIngestionReportGenerator(test_db, 1, config)

        path = generator.generate_holdings_report()

        assert path.exists()
        assert "Holdings" in path.name

        import pandas as pd
        df = pd.read_excel(path, sheet_name='Holdings')
        assert len(df) == 1

    def test_generate_ingestion_report(self, test_db, temp_output_dir):
        """Test ingestion history report generation."""
        config = ReportConfig(user_name="TestUser", output_dir=temp_output_dir)
        generator = MFIngestionReportGenerator(test_db, 1, config)

        path = generator.generate_ingestion_report()

        assert path.exists()
        assert "Ingestion" in path.name

        import pandas as pd
        df = pd.read_excel(path, sheet_name='Ingestion History')
        assert len(df) == 1

    def test_generate_all_reports(self, test_db, temp_output_dir):
        """Test generating all report types."""
        config = ReportConfig(user_name="TestUser", output_dir=temp_output_dir)
        generator = MFIngestionReportGenerator(test_db, 1, config)

        reports = generator.generate_all_reports()

        assert 'transactions' in reports
        assert 'holdings' in reports
        assert 'ingestion' in reports

        for report_type, path in reports.items():
            assert path.exists()

    def test_report_naming_convention(self, test_db, temp_output_dir):
        """Test report files follow naming convention."""
        config = ReportConfig(user_name="Sanjay", output_dir=temp_output_dir)
        generator = MFIngestionReportGenerator(test_db, 1, config)

        path = generator.generate_transactions_report()
        today = date.today().isoformat()

        # Pattern: {user}_{asset}_{report_type}_{date}.xlsx
        name = path.name
        assert name.startswith("Sanjay_")
        assert "Mutual-Fund_" in name
        assert "Transactions_" in name
        assert today in name
        assert name.endswith(".xlsx")


def test_convenience_function(test_db, temp_output_dir):
    """Test generate_mf_reports convenience function."""
    reports = generate_mf_reports(
        test_db, 1, "TestUser", temp_output_dir,
        report_types=['transactions', 'holdings']
    )

    assert len(reports) == 2
    assert 'transactions' in reports
    assert 'holdings' in reports
