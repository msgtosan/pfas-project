"""Unit tests for ledger_integration module."""

import pytest
from datetime import date
from decimal import Decimal
from unittest.mock import Mock, MagicMock, patch
import sqlite3

from pfas.parsers.ledger_integration import (
    AccountCode,
    LedgerRecordResult,
    get_account_id_by_code,
    generate_file_hash,
    record_mf_purchase,
    record_mf_redemption,
    record_mf_switch,
    record_mf_dividend,
    record_bank_credit,
    record_bank_debit,
    record_stock_buy,
    record_stock_sell,
    record_salary,
    record_epf_contribution,
    record_epf_interest,
    record_ppf_deposit,
    record_ppf_interest,
    record_ppf_withdrawal,
    _map_bank_category_to_account,
)
from pfas.core.transaction_service import TransactionResult, TransactionRecord, TransactionSource
from pfas.core.journal import JournalEntry


class TestAccountCode:
    """Tests for AccountCode enum."""

    def test_bank_accounts(self):
        """Verify bank account codes."""
        assert AccountCode.BANK_SAVINGS.value == "1101"
        assert AccountCode.BANK_CURRENT.value == "1102"
        assert AccountCode.BANK_FD.value == "1103"

    def test_investment_accounts(self):
        """Verify investment account codes."""
        assert AccountCode.MF_EQUITY.value == "1201"
        assert AccountCode.MF_DEBT.value == "1202"
        assert AccountCode.INDIAN_STOCKS.value == "1203"

    def test_retirement_accounts(self):
        """Verify retirement account codes."""
        assert AccountCode.EPF_EMPLOYEE.value == "1301"
        assert AccountCode.EPF_EMPLOYER.value == "1302"
        assert AccountCode.PPF.value == "1303"

    def test_capital_gains_accounts(self):
        """Verify capital gains account codes."""
        assert AccountCode.STCG_EQUITY.value == "4301"
        assert AccountCode.LTCG_EQUITY.value == "4302"
        assert AccountCode.CG_DEBT.value == "4303"


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_generate_file_hash(self):
        """Test file hash generation."""
        hash1 = generate_file_hash("/path/to/file.xlsx")
        hash2 = generate_file_hash("/path/to/file.xlsx")
        hash3 = generate_file_hash("/different/path.xlsx")

        # Same path should give same hash
        assert hash1 == hash2
        # Different paths should give different hashes
        assert hash1 != hash3
        # Hash should be 8 characters
        assert len(hash1) == 8

    def test_map_bank_category_credit_salary(self):
        """Test bank category mapping for salary credit."""
        result = _map_bank_category_to_account("SALARY", is_credit=True)
        assert result == AccountCode.SALARY_INCOME.value

    def test_map_bank_category_credit_interest(self):
        """Test bank category mapping for interest credit."""
        result = _map_bank_category_to_account("INTEREST", is_credit=True)
        assert result == AccountCode.BANK_INTEREST.value

    def test_map_bank_category_debit_investment(self):
        """Test bank category mapping for investment debit."""
        result = _map_bank_category_to_account("MF INVESTMENT", is_credit=False)
        assert result == AccountCode.MF_EQUITY.value

    def test_map_bank_category_debit_tax(self):
        """Test bank category mapping for tax debit."""
        result = _map_bank_category_to_account("ADVANCE TAX", is_credit=False)
        assert result == AccountCode.ADVANCE_TAX.value


class TestMutualFundRecording:
    """Tests for MF transaction recording."""

    @pytest.fixture
    def mock_conn(self):
        """Create mock database connection."""
        conn = MagicMock()
        # Mock get_account_by_code to return account objects
        return conn

    @pytest.fixture
    def mock_txn_service(self):
        """Create mock TransactionService."""
        service = MagicMock()
        service.record.return_value = TransactionRecord(
            result=TransactionResult.SUCCESS,
            journal_id=1,
            idempotency_key="test_key"
        )
        return service

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_mf_purchase_success(self, mock_get_account, mock_conn, mock_txn_service):
        """Test successful MF purchase recording."""
        # Setup mock account lookup
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_mf_purchase(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            folio_number="12345",
            scheme_name="Test Scheme",
            txn_date=date(2024, 3, 15),
            amount=Decimal("10000"),
            units=Decimal("100"),
            is_equity=True,
            source_file="/path/to/file.xlsx",
            row_idx=0,
        )

        assert result.success is True
        assert result.is_duplicate is False
        assert result.journal_id == 1

        # Verify TransactionService.record was called
        mock_txn_service.record.assert_called_once()
        call_args = mock_txn_service.record.call_args

        # Verify entries were created
        entries = call_args.kwargs['entries']
        assert len(entries) == 2  # Debit MF, Credit Bank

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_mf_purchase_duplicate(self, mock_get_account, mock_conn, mock_txn_service):
        """Test MF purchase with duplicate detection."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        # Setup duplicate response
        mock_txn_service.record.return_value = TransactionRecord(
            result=TransactionResult.DUPLICATE,
            idempotency_key="test_key",
            error_message="Transaction already processed"
        )

        result = record_mf_purchase(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            folio_number="12345",
            scheme_name="Test Scheme",
            txn_date=date(2024, 3, 15),
            amount=Decimal("10000"),
            units=Decimal("100"),
            is_equity=True,
            source_file="/path/to/file.xlsx",
            row_idx=0,
        )

        assert result.success is False
        assert result.is_duplicate is True

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_mf_redemption_with_gain(self, mock_get_account, mock_conn, mock_txn_service):
        """Test MF redemption with capital gain."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_mf_redemption(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            folio_number="12345",
            scheme_name="Test Scheme",
            txn_date=date(2024, 3, 15),
            proceeds=Decimal("15000"),
            cost_basis=Decimal("10000"),
            units=Decimal("100"),
            is_equity=True,
            is_long_term=True,
            source_file="/path/to/file.xlsx",
            row_idx=0,
            stt=Decimal("75"),
        )

        assert result.success is True

        # Verify entries include capital gain
        call_args = mock_txn_service.record.call_args
        entries = call_args.kwargs['entries']
        # Should have: Bank (debit), STT (debit), MF (credit), Capital Gain (credit)
        assert len(entries) == 4

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_mf_redemption_with_loss(self, mock_get_account, mock_conn, mock_txn_service):
        """Test MF redemption with capital loss."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_mf_redemption(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            folio_number="12345",
            scheme_name="Test Scheme",
            txn_date=date(2024, 3, 15),
            proceeds=Decimal("8000"),
            cost_basis=Decimal("10000"),
            units=Decimal("100"),
            is_equity=True,
            is_long_term=False,
            source_file="/path/to/file.xlsx",
            row_idx=0,
        )

        assert result.success is True

        # Verify entries include capital loss (debit)
        call_args = mock_txn_service.record.call_args
        entries = call_args.kwargs['entries']
        # Should have: Bank (debit), MF (credit), Capital Loss (debit)
        assert len(entries) == 3


class TestBankRecording:
    """Tests for bank transaction recording."""

    @pytest.fixture
    def mock_conn(self):
        return MagicMock()

    @pytest.fixture
    def mock_txn_service(self):
        service = MagicMock()
        service.record.return_value = TransactionRecord(
            result=TransactionResult.SUCCESS,
            journal_id=1,
            idempotency_key="test_key"
        )
        return service

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_bank_credit(self, mock_get_account, mock_conn, mock_txn_service):
        """Test bank credit recording."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_bank_credit(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            account_number="1234567890",
            txn_date=date(2024, 3, 15),
            amount=Decimal("50000"),
            description="Salary Credit",
            category="SALARY",
            ref_no="NEFT123",
            source_file="/path/to/file.xlsx",
            row_idx=0,
        )

        assert result.success is True

        call_args = mock_txn_service.record.call_args
        entries = call_args.kwargs['entries']
        assert len(entries) == 2  # Debit Bank, Credit Income

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_bank_debit(self, mock_get_account, mock_conn, mock_txn_service):
        """Test bank debit recording."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_bank_debit(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            account_number="1234567890",
            txn_date=date(2024, 3, 15),
            amount=Decimal("10000"),
            description="MF Investment",
            category="INVESTMENT",
            ref_no="IMPS456",
            source_file="/path/to/file.xlsx",
            row_idx=0,
        )

        assert result.success is True


class TestStockRecording:
    """Tests for stock transaction recording."""

    @pytest.fixture
    def mock_conn(self):
        return MagicMock()

    @pytest.fixture
    def mock_txn_service(self):
        service = MagicMock()
        service.record.return_value = TransactionRecord(
            result=TransactionResult.SUCCESS,
            journal_id=1,
            idempotency_key="test_key"
        )
        return service

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_stock_buy(self, mock_get_account, mock_conn, mock_txn_service):
        """Test stock buy recording."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_stock_buy(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            symbol="RELIANCE",
            txn_date=date(2024, 3, 15),
            quantity=10,
            price=Decimal("2500"),
            amount=Decimal("25000"),
            brokerage=Decimal("50"),
            source_file="/path/to/file.xlsx",
            row_idx=0,
        )

        assert result.success is True

        call_args = mock_txn_service.record.call_args
        entries = call_args.kwargs['entries']
        assert len(entries) == 2  # Debit Stock, Credit Bank

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_stock_sell_with_gain(self, mock_get_account, mock_conn, mock_txn_service):
        """Test stock sell with gain."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_stock_sell(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            symbol="RELIANCE",
            txn_date=date(2024, 3, 15),
            quantity=10,
            price=Decimal("3000"),
            proceeds=Decimal("30000"),
            cost_basis=Decimal("25000"),
            brokerage=Decimal("60"),
            stt=Decimal("30"),
            is_long_term=True,
            source_file="/path/to/file.xlsx",
            row_idx=0,
        )

        assert result.success is True


class TestRetirementRecording:
    """Tests for EPF and PPF recording."""

    @pytest.fixture
    def mock_conn(self):
        return MagicMock()

    @pytest.fixture
    def mock_txn_service(self):
        service = MagicMock()
        service.record.return_value = TransactionRecord(
            result=TransactionResult.SUCCESS,
            journal_id=1,
            idempotency_key="test_key"
        )
        return service

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_epf_contribution(self, mock_get_account, mock_conn, mock_txn_service):
        """Test EPF contribution recording."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_epf_contribution(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            uan="100123456789",
            wage_month="Mar-2024",
            employee_contribution=Decimal("1800"),
            employer_contribution=Decimal("1800"),
            txn_date=date(2024, 3, 15),
            source_file="/path/to/file.pdf",
            row_idx=0,
        )

        assert result.success is True

        call_args = mock_txn_service.record.call_args
        entries = call_args.kwargs['entries']
        # Should have: EPF EE (debit), EPF ER (debit), Bank (credit)
        assert len(entries) == 3

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_epf_interest(self, mock_get_account, mock_conn, mock_txn_service):
        """Test EPF interest recording."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_epf_interest(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            uan="100123456789",
            financial_year="2023-2024",
            employee_interest=Decimal("5000"),
            employer_interest=Decimal("3000"),
            txn_date=date(2024, 3, 31),
            source_file="/path/to/file.pdf",
        )

        assert result.success is True

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_ppf_deposit(self, mock_get_account, mock_conn, mock_txn_service):
        """Test PPF deposit recording."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_ppf_deposit(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            account_number="PPF123456",
            txn_date=date(2024, 3, 5),
            amount=Decimal("50000"),
            source_file="/path/to/file.xlsx",
            row_idx=0,
        )

        assert result.success is True

        call_args = mock_txn_service.record.call_args
        entries = call_args.kwargs['entries']
        assert len(entries) == 2  # Debit PPF, Credit Bank

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_ppf_interest(self, mock_get_account, mock_conn, mock_txn_service):
        """Test PPF interest recording."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_ppf_interest(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            account_number="PPF123456",
            txn_date=date(2024, 3, 31),
            amount=Decimal("8500"),
            financial_year="2023-24",
            source_file="/path/to/file.xlsx",
            row_idx=0,
        )

        assert result.success is True

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_ppf_withdrawal(self, mock_get_account, mock_conn, mock_txn_service):
        """Test PPF withdrawal recording."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_ppf_withdrawal(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            account_number="PPF123456",
            txn_date=date(2024, 3, 15),
            amount=Decimal("100000"),
            source_file="/path/to/file.xlsx",
            row_idx=0,
        )

        assert result.success is True

        call_args = mock_txn_service.record.call_args
        entries = call_args.kwargs['entries']
        assert len(entries) == 2  # Debit Bank, Credit PPF


class TestSalaryRecording:
    """Tests for salary recording."""

    @pytest.fixture
    def mock_conn(self):
        return MagicMock()

    @pytest.fixture
    def mock_txn_service(self):
        service = MagicMock()
        service.record.return_value = TransactionRecord(
            result=TransactionResult.SUCCESS,
            journal_id=1,
            idempotency_key="test_key"
        )
        return service

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_record_salary(self, mock_get_account, mock_conn, mock_txn_service):
        """Test salary recording."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_salary(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            employer="Acme Corp",
            pay_period="March 2024",
            gross_salary=Decimal("100000"),
            net_salary=Decimal("75000"),
            tds_deducted=Decimal("15000"),
            epf_employee=Decimal("10000"),
            txn_date=date(2024, 3, 31),
            source_file="/path/to/payslip.pdf",
            row_idx=0,
        )

        assert result.success is True

        call_args = mock_txn_service.record.call_args
        entries = call_args.kwargs['entries']
        # Should have: Bank (debit), TDS (debit), EPF (debit), Salary (credit)
        assert len(entries) == 4


class TestIdempotencyKeys:
    """Tests for idempotency key generation."""

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_mf_idempotency_key_format(self, mock_get_account):
        """Verify MF idempotency key format."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        mock_conn = MagicMock()
        mock_txn_service = MagicMock()
        mock_txn_service.record.return_value = TransactionRecord(
            result=TransactionResult.SUCCESS,
            journal_id=1,
            idempotency_key="test_key"
        )

        record_mf_purchase(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            folio_number="12345",
            scheme_name="Test Scheme",
            txn_date=date(2024, 3, 15),
            amount=Decimal("10000"),
            units=Decimal("100"),
            is_equity=True,
            source_file="/path/to/file.xlsx",
            row_idx=5,
        )

        call_args = mock_txn_service.record.call_args
        idempotency_key = call_args.kwargs['idempotency_key']

        # Key should start with "mf:" and contain file hash, row_idx, folio, date, amount, units
        assert idempotency_key.startswith("mf:")
        assert "12345" in idempotency_key
        assert "2024-03-15" in idempotency_key

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_bank_idempotency_key_hashes_account(self, mock_get_account):
        """Verify bank idempotency key hashes account number."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        mock_conn = MagicMock()
        mock_txn_service = MagicMock()
        mock_txn_service.record.return_value = TransactionRecord(
            result=TransactionResult.SUCCESS,
            journal_id=1,
            idempotency_key="test_key"
        )

        record_bank_credit(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            account_number="1234567890",
            txn_date=date(2024, 3, 15),
            amount=Decimal("50000"),
            description="Salary",
            category="SALARY",
            ref_no="NEFT123",
            source_file="/path/to/file.xlsx",
            row_idx=0,
        )

        call_args = mock_txn_service.record.call_args
        idempotency_key = call_args.kwargs['idempotency_key']

        # Key should start with "bank:" but NOT contain plain account number
        assert idempotency_key.startswith("bank:")
        assert "1234567890" not in idempotency_key  # Account should be hashed
