"""
Unit tests for Financial Statement Services.

Tests:
- CashFlowStatementService
- BalanceSheetService
- PortfolioValuationService
- LiabilitiesService
"""

import pytest
import sqlite3
import sqlcipher3
from datetime import date
from decimal import Decimal

from pfas.core.database import DatabaseManager
from pfas.core.models import (
    ActivityType,
    FlowDirection,
    AssetCategory,
    LiabilityType,
    CashFlow,
    BalanceSheetSnapshot,
    CashFlowStatement,
    get_financial_year,
    get_fy_dates,
)
from pfas.services import (
    CashFlowStatementService,
    BalanceSheetService,
    PortfolioValuationService,
    LiabilitiesService,
)


@pytest.fixture
def db_connection():
    """Create test database with schema."""
    DatabaseManager.reset_instance()
    db = DatabaseManager()
    conn = db.init(":memory:", "test_password")

    # Create a test user
    conn.execute("""
        INSERT INTO users (id, pan_encrypted, pan_salt, name, email)
        VALUES (1, X'00', X'00', 'Test User', 'test@example.com')
    """)
    conn.commit()

    yield conn

    db.close()
    DatabaseManager.reset_instance()


class TestCoreModels:
    """Tests for core model functions."""

    def test_get_financial_year_april(self):
        """April date should be start of new FY."""
        assert get_financial_year(date(2024, 4, 15)) == "2024-25"

    def test_get_financial_year_march(self):
        """March date should be end of previous FY."""
        assert get_financial_year(date(2025, 3, 31)) == "2024-25"

    def test_get_financial_year_january(self):
        """January should be in previous FY."""
        assert get_financial_year(date(2025, 1, 15)) == "2024-25"

    def test_get_fy_dates(self):
        """Get start and end dates for FY."""
        start, end = get_fy_dates("2024-25")
        assert start == date(2024, 4, 1)
        assert end == date(2025, 3, 31)

    def test_cash_flow_signed_amount_inflow(self):
        """Inflow should be positive."""
        flow = CashFlow(
            flow_date=date.today(),
            activity_type=ActivityType.OPERATING,
            flow_direction=FlowDirection.INFLOW,
            amount=Decimal("1000"),
            category="SALARY",
        )
        assert flow.signed_amount == Decimal("1000")

    def test_cash_flow_signed_amount_outflow(self):
        """Outflow should be negative."""
        flow = CashFlow(
            flow_date=date.today(),
            activity_type=ActivityType.OPERATING,
            flow_direction=FlowDirection.OUTFLOW,
            amount=Decimal("1000"),
            category="TAX_PAID",
        )
        assert flow.signed_amount == Decimal("-1000")

    def test_balance_sheet_net_worth(self):
        """Net worth = assets - liabilities."""
        snapshot = BalanceSheetSnapshot(
            snapshot_date=date.today(),
            bank_savings=Decimal("100000"),
            mutual_funds_equity=Decimal("500000"),
            home_loans=Decimal("200000"),
        )
        assert snapshot.total_assets == Decimal("600000")
        assert snapshot.total_liabilities == Decimal("200000")
        assert snapshot.net_worth == Decimal("400000")

    def test_cash_flow_statement_properties(self):
        """Test cash flow statement computed properties."""
        statement = CashFlowStatement(
            period_start=date(2024, 4, 1),
            period_end=date(2025, 3, 31),
            financial_year="2024-25",
            salary_received=Decimal("1200000"),
            dividends_received=Decimal("50000"),
            taxes_paid=Decimal("150000"),
            mf_purchases=Decimal("100000"),
            mf_redemptions=Decimal("50000"),
            loan_proceeds=Decimal("500000"),
            loan_repayments=Decimal("60000"),
        )

        assert statement.net_operating == Decimal("1100000")  # 1200000 + 50000 - 150000
        assert statement.net_investing == Decimal("-50000")  # 50000 - 100000
        assert statement.net_financing == Decimal("440000")  # 500000 - 60000
        assert statement.net_change_in_cash == Decimal("1490000")


class TestCashFlowStatementService:
    """Tests for CashFlowStatementService."""

    def test_service_initialization(self, db_connection):
        """Service should initialize with connection."""
        service = CashFlowStatementService(db_connection)
        assert service.conn is not None

    def test_get_cash_flow_statement_empty(self, db_connection):
        """Should return empty statement when no data."""
        service = CashFlowStatementService(db_connection)
        statement = service.get_cash_flow_statement(user_id=1, financial_year="2024-25")

        assert statement.financial_year == "2024-25"
        assert statement.period_start == date(2024, 4, 1)
        assert statement.period_end == date(2025, 3, 31)
        assert statement.net_operating == Decimal("0")
        assert statement.net_investing == Decimal("0")
        assert statement.net_financing == Decimal("0")

    def test_save_cash_flow_statement(self, db_connection):
        """Should save statement to database."""
        service = CashFlowStatementService(db_connection)

        statement = CashFlowStatement(
            period_start=date(2024, 4, 1),
            period_end=date(2025, 3, 31),
            financial_year="2024-25",
            salary_received=Decimal("1000000"),
        )

        record_id = service.save_cash_flow_statement(user_id=1, statement=statement)
        assert record_id > 0

        # Verify saved
        cursor = db_connection.execute(
            "SELECT net_operating FROM cash_flow_statements WHERE id = ?",
            (record_id,)
        )
        row = cursor.fetchone()
        assert float(row[0]) == 1000000.0

    def test_classify_salary_transaction(self, db_connection):
        """Should classify salary as operating inflow."""
        service = CashFlowStatementService(db_connection)

        result = service._classify_transaction("QUALCOMM SALARY CREDIT", is_credit=True)

        assert result is not None
        assert result["activity_type"] == ActivityType.OPERATING
        assert result["flow_direction"] == FlowDirection.INFLOW
        assert "SALARY" in result["category"]


class TestBalanceSheetService:
    """Tests for BalanceSheetService."""

    def test_service_initialization(self, db_connection):
        """Service should initialize with connection."""
        service = BalanceSheetService(db_connection)
        assert service.conn is not None

    def test_get_balance_sheet_empty(self, db_connection):
        """Should return empty balance sheet when no data."""
        service = BalanceSheetService(db_connection)
        snapshot = service.get_balance_sheet(user_id=1, as_of=date.today())

        assert snapshot.snapshot_date == date.today()
        assert snapshot.total_assets == Decimal("0")
        assert snapshot.total_liabilities == Decimal("0")
        assert snapshot.net_worth == Decimal("0")

    def test_save_balance_sheet(self, db_connection):
        """Should save snapshot to database."""
        service = BalanceSheetService(db_connection)

        snapshot = BalanceSheetSnapshot(
            snapshot_date=date.today(),
            bank_savings=Decimal("100000"),
            mutual_funds_equity=Decimal("500000"),
        )

        record_id = service.save_balance_sheet(user_id=1, snapshot=snapshot)
        assert record_id > 0

        # Verify saved
        cursor = db_connection.execute(
            "SELECT total_assets, net_worth FROM balance_sheet_snapshots WHERE id = ?",
            (record_id,)
        )
        row = cursor.fetchone()
        assert float(row[0]) == 600000.0
        assert float(row[1]) == 600000.0


class TestPortfolioValuationService:
    """Tests for PortfolioValuationService."""

    def test_service_initialization(self, db_connection):
        """Service should initialize with connection."""
        service = PortfolioValuationService(db_connection)
        assert service.conn is not None

    def test_get_portfolio_summary_empty(self, db_connection):
        """Should return empty summary when no holdings."""
        service = PortfolioValuationService(db_connection)
        summary = service.get_portfolio_summary(user_id=1)

        assert summary.total_invested == Decimal("0")
        assert summary.total_current_value == Decimal("0")
        assert summary.total_unrealized_gain == Decimal("0")
        assert len(summary.holdings) == 0

    def test_xirr_no_transactions(self, db_connection):
        """XIRR should indicate no transactions."""
        service = PortfolioValuationService(db_connection)
        result = service.calculate_xirr(user_id=1)

        assert result.xirr_percent is None
        assert "No transactions" in result.error


class TestLiabilitiesService:
    """Tests for LiabilitiesService."""

    def test_service_initialization(self, db_connection):
        """Service should initialize with connection."""
        service = LiabilitiesService(db_connection)
        assert service.conn is not None

    def test_add_liability(self, db_connection):
        """Should add new liability."""
        service = LiabilitiesService(db_connection)

        loan_id = service.add_liability(
            user_id=1,
            liability_type=LiabilityType.HOME_LOAN,
            lender_name="HDFC Bank",
            principal_amount=Decimal("5000000"),
            interest_rate=Decimal("8.5"),
            start_date=date(2024, 1, 1),
            tenure_months=240,
        )

        assert loan_id > 0

        # Verify
        liability = service.get_liability(loan_id)
        assert liability is not None
        assert liability.lender_name == "HDFC Bank"
        assert liability.principal_amount == Decimal("5000000")
        assert liability.outstanding_amount == Decimal("5000000")
        assert liability.emi_amount is not None
        assert liability.emi_amount > Decimal("0")

    def test_calculate_emi(self, db_connection):
        """EMI calculation should be accurate."""
        service = LiabilitiesService(db_connection)

        # Home loan: 50L, 8.5%, 20 years
        emi = service._calculate_emi(
            principal=Decimal("5000000"),
            annual_rate=Decimal("8.5"),
            tenure_months=240
        )

        # Expected EMI around 43,391
        assert emi > Decimal("43000")
        assert emi < Decimal("44000")

    def test_record_emi_payment(self, db_connection):
        """Should record EMI and update outstanding."""
        service = LiabilitiesService(db_connection)

        # Add loan
        loan_id = service.add_liability(
            user_id=1,
            liability_type=LiabilityType.HOME_LOAN,
            lender_name="HDFC Bank",
            principal_amount=Decimal("5000000"),
            interest_rate=Decimal("8.5"),
            start_date=date(2024, 1, 1),
            tenure_months=240,
        )

        original = service.get_liability(loan_id)
        original_outstanding = original.outstanding_amount

        # Record EMI
        txn_id = service.record_emi_payment(
            liability_id=loan_id,
            payment_date=date(2024, 2, 1),
            amount=original.emi_amount,
            user_id=1
        )

        assert txn_id > 0

        # Check outstanding reduced
        updated = service.get_liability(loan_id)
        assert updated.outstanding_amount < original_outstanding

    def test_record_prepayment(self, db_connection):
        """Prepayment should reduce principal directly."""
        service = LiabilitiesService(db_connection)

        loan_id = service.add_liability(
            user_id=1,
            liability_type=LiabilityType.HOME_LOAN,
            lender_name="HDFC Bank",
            principal_amount=Decimal("5000000"),
            interest_rate=Decimal("8.5"),
            start_date=date(2024, 1, 1),
            tenure_months=240,
        )

        # Prepay 10L
        service.record_prepayment(
            liability_id=loan_id,
            payment_date=date(2024, 6, 1),
            amount=Decimal("1000000"),
            user_id=1
        )

        updated = service.get_liability(loan_id)
        assert updated.outstanding_amount == Decimal("4000000")

    def test_get_loan_summary(self, db_connection):
        """Should aggregate all loans."""
        service = LiabilitiesService(db_connection)

        # Add multiple loans
        service.add_liability(
            user_id=1,
            liability_type=LiabilityType.HOME_LOAN,
            lender_name="HDFC",
            principal_amount=Decimal("5000000"),
            interest_rate=Decimal("8.5"),
            start_date=date(2024, 1, 1),
            tenure_months=240,
        )

        service.add_liability(
            user_id=1,
            liability_type=LiabilityType.CAR_LOAN,
            lender_name="SBI",
            principal_amount=Decimal("800000"),
            interest_rate=Decimal("9.0"),
            start_date=date(2024, 1, 1),
            tenure_months=60,
        )

        summary = service.get_loan_summary(user_id=1)

        assert summary.loan_count == 2
        assert summary.total_principal == Decimal("5800000")
        assert summary.total_outstanding == Decimal("5800000")
        assert summary.monthly_emi_total > Decimal("0")

    def test_amortization_schedule(self, db_connection):
        """Should generate amortization schedule."""
        service = LiabilitiesService(db_connection)

        loan_id = service.add_liability(
            user_id=1,
            liability_type=LiabilityType.PERSONAL_LOAN,
            lender_name="ICICI",
            principal_amount=Decimal("100000"),
            interest_rate=Decimal("12.0"),
            start_date=date(2024, 1, 1),
            tenure_months=12,
        )

        schedule = service.generate_amortization_schedule(loan_id)

        assert len(schedule) == 12
        assert schedule[0].month == 1
        assert schedule[0].opening_balance == Decimal("100000")
        assert schedule[-1].closing_balance <= Decimal("1")  # Should be ~0 at end

        # EMI = Principal + Interest
        for entry in schedule:
            assert entry.emi_amount == entry.principal_component + entry.interest_component


class TestDatabaseSchema:
    """Tests for database schema additions."""

    def test_cash_flows_table_exists(self, db_connection):
        """cash_flows table should exist."""
        cursor = db_connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='cash_flows'"
        )
        assert cursor.fetchone() is not None

    def test_liabilities_table_exists(self, db_connection):
        """liabilities table should exist."""
        cursor = db_connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='liabilities'"
        )
        assert cursor.fetchone() is not None

    def test_balance_sheet_snapshots_table_exists(self, db_connection):
        """balance_sheet_snapshots table should exist."""
        cursor = db_connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='balance_sheet_snapshots'"
        )
        assert cursor.fetchone() is not None

    def test_asset_holdings_snapshot_table_exists(self, db_connection):
        """asset_holdings_snapshot table should exist."""
        cursor = db_connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='asset_holdings_snapshot'"
        )
        assert cursor.fetchone() is not None

    def test_liability_transactions_table_exists(self, db_connection):
        """liability_transactions table should exist."""
        cursor = db_connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='liability_transactions'"
        )
        assert cursor.fetchone() is not None

    def test_cash_flow_statements_table_exists(self, db_connection):
        """cash_flow_statements table should exist."""
        cursor = db_connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='cash_flow_statements'"
        )
        assert cursor.fetchone() is not None

    def test_user_id_not_null_on_new_tables(self, db_connection):
        """New tables should have user_id NOT NULL."""
        # Try inserting without user_id - should fail
        with pytest.raises(sqlcipher3.dbapi2.IntegrityError):
            db_connection.execute("""
                INSERT INTO liabilities (liability_type, lender_name, principal_amount, start_date)
                VALUES ('HOME_LOAN', 'Test', 100000, '2024-01-01')
            """)
