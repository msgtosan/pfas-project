"""Tests for Reconciliation Engine."""

import pytest
from pathlib import Path
from decimal import Decimal
from datetime import date
import tempfile

from pfas.core.database import DatabaseManager
from pfas.audit.reconciler import Reconciler, ReconciliationResult, Mismatch
from pfas.audit.mf_audit_parser import HoldingRecord, AuditData


@pytest.fixture
def test_db():
    """Create in-memory test database."""
    db = DatabaseManager()
    DatabaseManager.reset_instance()
    db = DatabaseManager()
    conn = db.init(":memory:", "test_password")

    # Create test user
    conn.execute(
        "INSERT INTO users (name, pan_encrypted, pan_salt) VALUES (?, ?, ?)",
        ("TestUser", b"encrypted", b"salt")
    )
    conn.commit()

    yield conn

    db.close()
    DatabaseManager.reset_instance()


@pytest.fixture
def test_db_with_holdings(test_db):
    """Database with sample holdings data."""
    # Insert test holdings
    test_db.execute(
        """
        INSERT INTO mf_holdings
        (user_id, scheme_name, folio_number, units, nav, nav_date, current_value, cost_value, rta, amc_name)
        VALUES
        (1, 'Test Equity Fund', '12345', '100.5', '50.0', '2026-01-15', '5025.0', '4500.0', 'CAMS', 'Test AMC'),
        (1, 'Test Debt Fund', '12346', '200.0', '25.0', '2026-01-15', '5000.0', '4800.0', 'CAMS', 'Test AMC')
        """
    )
    test_db.commit()

    return test_db


class TestReconciler:
    """Test suite for Reconciler."""

    def test_reconciler_init(self, test_db):
        """Test reconciler initialization."""
        reconciler = Reconciler(test_db, 1)

        assert reconciler.conn == test_db
        assert reconciler.user_id == 1
        assert reconciler.mismatch_threshold == Decimal("100")

    def test_reconciler_custom_threshold(self, test_db):
        """Test reconciler with custom threshold."""
        reconciler = Reconciler(test_db, 1, mismatch_threshold=Decimal("500"))

        assert reconciler.mismatch_threshold == Decimal("500")

    def test_reconcile_matching_holdings(self, test_db_with_holdings):
        """Test reconciliation with matching data."""
        reconciler = Reconciler(test_db_with_holdings, 1)

        # Create audit data matching DB
        audit_data = AuditData(
            source_file="test.xlsx",
            holdings=[
                HoldingRecord(
                    scheme_name="Test Equity Fund",
                    folio_number="12345",
                    units=Decimal("100.5"),
                    nav=Decimal("50.0"),
                    nav_date=date(2026, 1, 15),
                    current_value=Decimal("5025.0"),
                    cost_value=Decimal("4500.0"),
                    rta="CAMS"
                )
            ]
        )

        result = reconciler.reconcile_holdings(audit_data)

        assert result.records_compared == 1
        assert result.records_matched == 1
        assert result.mismatch_count == 0

    def test_reconcile_detects_unit_mismatch(self, test_db_with_holdings):
        """Test reconciliation detects unit mismatch."""
        reconciler = Reconciler(test_db_with_holdings, 1)

        # Create audit data with different units
        audit_data = AuditData(
            source_file="test.xlsx",
            holdings=[
                HoldingRecord(
                    scheme_name="Test Equity Fund",
                    folio_number="12345",
                    units=Decimal("105.5"),  # Different from DB (100.5)
                    nav=Decimal("50.0"),
                    nav_date=date(2026, 1, 15),
                    current_value=Decimal("5275.0"),
                    cost_value=Decimal("4500.0"),
                    rta="CAMS"
                )
            ]
        )

        result = reconciler.reconcile_holdings(audit_data)

        assert result.mismatch_count > 0

        # Find unit mismatch
        unit_mismatches = [m for m in result.mismatches if m.field_name == 'units']
        assert len(unit_mismatches) == 1
        assert unit_mismatches[0].difference == pytest.approx(5.0, rel=0.01)

    def test_reconcile_detects_value_mismatch(self, test_db_with_holdings):
        """Test reconciliation detects value mismatch above threshold."""
        reconciler = Reconciler(test_db_with_holdings, 1, mismatch_threshold=Decimal("50"))

        audit_data = AuditData(
            source_file="test.xlsx",
            holdings=[
                HoldingRecord(
                    scheme_name="Test Equity Fund",
                    folio_number="12345",
                    units=Decimal("100.5"),
                    nav=Decimal("50.0"),
                    nav_date=date(2026, 1, 15),
                    current_value=Decimal("5100.0"),  # +75 from DB (5025)
                    cost_value=Decimal("4500.0"),
                    rta="CAMS"
                )
            ]
        )

        result = reconciler.reconcile_holdings(audit_data)

        value_mismatches = [m for m in result.mismatches if m.field_name == 'current_value']
        assert len(value_mismatches) == 1

    def test_reconcile_ignores_small_differences(self, test_db_with_holdings):
        """Test small differences below threshold are ignored."""
        reconciler = Reconciler(test_db_with_holdings, 1, mismatch_threshold=Decimal("100"))

        audit_data = AuditData(
            source_file="test.xlsx",
            holdings=[
                HoldingRecord(
                    scheme_name="Test Equity Fund",
                    folio_number="12345",
                    units=Decimal("100.5"),
                    nav=Decimal("50.0"),
                    nav_date=date(2026, 1, 15),
                    current_value=Decimal("5075.0"),  # +50 from DB (below threshold)
                    cost_value=Decimal("4500.0"),
                    rta="CAMS"
                )
            ]
        )

        result = reconciler.reconcile_holdings(audit_data)

        value_mismatches = [m for m in result.mismatches if m.field_name == 'current_value']
        assert len(value_mismatches) == 0

    def test_reconcile_missing_in_db(self, test_db_with_holdings):
        """Test detection of records in file but not in DB."""
        reconciler = Reconciler(test_db_with_holdings, 1)

        audit_data = AuditData(
            source_file="test.xlsx",
            holdings=[
                HoldingRecord(
                    scheme_name="New Fund Not In DB",
                    folio_number="99999",
                    units=Decimal("50.0"),
                    nav=Decimal("100.0"),
                    nav_date=date(2026, 1, 15),
                    current_value=Decimal("5000.0"),
                    rta="CAMS"
                )
            ]
        )

        result = reconciler.reconcile_holdings(audit_data)

        assert len(result.missing_in_db) == 1
        assert "99999" in result.missing_in_db[0]

    def test_log_mismatches_to_db(self, test_db_with_holdings):
        """Test mismatches are logged to reconciliation_audit table."""
        reconciler = Reconciler(test_db_with_holdings, 1)

        audit_data = AuditData(
            source_file="test.xlsx",
            holdings=[
                HoldingRecord(
                    scheme_name="Test Equity Fund",
                    folio_number="12345",
                    units=Decimal("200.0"),  # Large difference
                    nav=Decimal("50.0"),
                    nav_date=date(2026, 1, 15),
                    current_value=Decimal("10000.0"),
                    rta="CAMS"
                )
            ]
        )

        result = reconciler.reconcile_holdings(audit_data)

        # Check database
        cursor = test_db_with_holdings.execute(
            "SELECT COUNT(*) FROM reconciliation_audit WHERE user_id = 1"
        )
        count = cursor.fetchone()[0]

        assert count > 0

    def test_get_unresolved_mismatches(self, test_db_with_holdings):
        """Test retrieving unresolved mismatches."""
        # Insert some mismatches
        test_db_with_holdings.execute(
            """
            INSERT INTO reconciliation_audit
            (user_id, asset_type, audit_date, source_file, scheme_name, folio_number,
             field_name, db_value, file_value, severity, resolved)
            VALUES
            (1, 'Mutual-Fund', '2026-01-15', 'test.xlsx', 'Fund A', '123', 'units', '100', '110', 'WARNING', 0),
            (1, 'Mutual-Fund', '2026-01-15', 'test.xlsx', 'Fund B', '124', 'value', '5000', '5500', 'ERROR', 0),
            (1, 'Mutual-Fund', '2026-01-14', 'old.xlsx', 'Fund C', '125', 'units', '50', '55', 'WARNING', 1)
            """
        )
        test_db_with_holdings.commit()

        reconciler = Reconciler(test_db_with_holdings, 1)
        unresolved = reconciler.get_unresolved_mismatches()

        assert len(unresolved) == 2
        # ERROR should come first
        assert unresolved[0]['severity'] == 'ERROR'

    def test_resolve_mismatch(self, test_db_with_holdings):
        """Test resolving a mismatch."""
        # Insert mismatch
        test_db_with_holdings.execute(
            """
            INSERT INTO reconciliation_audit
            (user_id, asset_type, audit_date, source_file, scheme_name, folio_number,
             field_name, db_value, file_value, severity, resolved)
            VALUES (1, 'Mutual-Fund', '2026-01-15', 'test.xlsx', 'Fund A', '123', 'units', '100', '110', 'WARNING', 0)
            """
        )
        test_db_with_holdings.commit()

        reconciler = Reconciler(test_db_with_holdings, 1)
        reconciler.resolve_mismatch(1, "Verified - file was outdated")

        # Check resolution
        cursor = test_db_with_holdings.execute(
            "SELECT resolved, resolution_notes FROM reconciliation_audit WHERE id = 1"
        )
        row = cursor.fetchone()

        assert row['resolved'] == 1
        assert "outdated" in row['resolution_notes']


class TestMismatch:
    """Test suite for Mismatch dataclass."""

    def test_mismatch_to_dict(self):
        """Test conversion to dictionary."""
        mismatch = Mismatch(
            scheme_name="Test Fund",
            folio_number="12345",
            field_name="units",
            db_value="100",
            file_value="110",
            difference=10.0,
            severity="WARNING"
        )

        d = mismatch.to_dict()

        assert d['scheme_name'] == "Test Fund"
        assert d['difference'] == 10.0


class TestReconciliationResult:
    """Test suite for ReconciliationResult."""

    def test_mismatch_count(self):
        """Test mismatch count property."""
        result = ReconciliationResult()
        result.mismatches = [
            Mismatch("A", "1", "units", "100", "110"),
            Mismatch("B", "2", "value", "1000", "1100"),
        ]

        assert result.mismatch_count == 2

    def test_match_rate_calculation(self):
        """Test match rate calculation."""
        result = ReconciliationResult()
        result.records_compared = 10
        result.records_matched = 8

        assert result.match_rate == 80.0

    def test_match_rate_zero_records(self):
        """Test match rate with zero records."""
        result = ReconciliationResult()
        result.records_compared = 0

        assert result.match_rate == 0.0
