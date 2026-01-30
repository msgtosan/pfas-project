"""Unit tests for deep accounting logic in ledger_integration.py."""

import pytest
from datetime import date
from decimal import Decimal
from unittest.mock import Mock, MagicMock, patch
import sqlite3

from pfas.core.exceptions import AccountingBalanceError, InsufficientSharesError, ForexRateNotFoundError
from pfas.core.transaction_service import TransactionResult, TransactionRecord, TransactionSource
from pfas.parsers.ledger_integration import (
    AccountCode,
    validate_salary_components,
    record_salary_multi_leg,
    record_employer_pf_contribution,
    record_mf_purchase_with_cost_basis,
    record_mf_redemption_with_cost_basis,
    record_stock_buy_with_cost_basis,
    record_stock_sell_with_cost_basis,
    record_rsu_vest,
    record_rsu_sale,
    record_espp_purchase,
    record_foreign_dividend,
    get_sbi_tt_rate,
)
from pfas.services.cost_basis_tracker import (
    CostBasisTracker,
    CostMethod,
    Lot,
    CostBasisResult,
    HoldingSummary,
)


class TestSalaryValidation:
    """Tests for salary component validation."""

    def test_valid_salary_components(self):
        """Test that valid salary components pass validation."""
        result = validate_salary_components(
            gross_salary=Decimal("100000"),
            net_salary=Decimal("75000"),
            tds_deducted=Decimal("15000"),
            epf_employee=Decimal("8000"),
            professional_tax=Decimal("200"),
            other_deductions=Decimal("1800"),
        )
        assert result is True

    def test_invalid_salary_components_raises_error(self):
        """Test that invalid salary components raise AccountingBalanceError."""
        with pytest.raises(AccountingBalanceError) as exc_info:
            validate_salary_components(
                gross_salary=Decimal("100000"),
                net_salary=Decimal("70000"),  # Wrong - should be 75000
                tds_deducted=Decimal("15000"),
                epf_employee=Decimal("8000"),
                professional_tax=Decimal("200"),
                other_deductions=Decimal("1800"),
            )

        assert "do not sum to Gross Salary" in str(exc_info.value)
        assert exc_info.value.expected == "100000"
        assert exc_info.value.actual == "95000"
        assert exc_info.value.difference == "5000"

    def test_salary_within_tolerance(self):
        """Test that small rounding differences are allowed."""
        # Within ₹1 tolerance
        result = validate_salary_components(
            gross_salary=Decimal("100000"),
            net_salary=Decimal("75000.50"),
            tds_deducted=Decimal("15000"),
            epf_employee=Decimal("8000"),
            professional_tax=Decimal("200"),
            other_deductions=Decimal("1799"),
            tolerance=Decimal("1.00"),
        )
        assert result is True

    def test_salary_exceeds_tolerance(self):
        """Test that differences exceeding tolerance raise error."""
        with pytest.raises(AccountingBalanceError):
            validate_salary_components(
                gross_salary=Decimal("100000"),
                net_salary=Decimal("75000"),
                tds_deducted=Decimal("15000"),
                epf_employee=Decimal("8000"),
                professional_tax=Decimal("200"),
                other_deductions=Decimal("1795"),  # ₹5 difference
                tolerance=Decimal("1.00"),
            )


class TestSalaryMultiLeg:
    """Tests for multi-legged salary journal entries."""

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
    def test_salary_multi_leg_creates_correct_entries(
        self, mock_get_account, mock_conn, mock_txn_service
    ):
        """Test that salary creates correct multi-leg journal entry."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_salary_multi_leg(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            employer="Acme Corp",
            pay_period="March 2024",
            gross_salary=Decimal("100000"),
            net_salary=Decimal("75000"),
            tds_deducted=Decimal("15000"),
            epf_employee=Decimal("8000"),
            txn_date=date(2024, 3, 31),
            source_file="/path/to/payslip.pdf",
            row_idx=0,
            professional_tax=Decimal("200"),
            other_deductions=Decimal("1800"),
            validate=True,
        )

        assert result.success is True

        # Verify entries were created
        call_args = mock_txn_service.record.call_args
        entries = call_args.kwargs['entries']

        # Should have: Bank, TDS, EPF, Professional Tax, Other Deductions, Salary Income
        assert len(entries) == 6

        # Verify total debits = total credits = gross salary
        total_debits = sum(e.debit for e in entries if e.debit)
        total_credits = sum(e.credit for e in entries if e.credit)
        assert total_debits == total_credits == Decimal("100000")

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_salary_multi_leg_validation_failure_prevents_entry(
        self, mock_get_account, mock_conn, mock_txn_service
    ):
        """Test that validation failure prevents journal entry creation."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        with pytest.raises(AccountingBalanceError):
            record_salary_multi_leg(
                txn_service=mock_txn_service,
                conn=mock_conn,
                user_id=1,
                employer="Acme Corp",
                pay_period="March 2024",
                gross_salary=Decimal("100000"),
                net_salary=Decimal("70000"),  # Incorrect
                tds_deducted=Decimal("15000"),
                epf_employee=Decimal("8000"),
                txn_date=date(2024, 3, 31),
                source_file="/path/to/payslip.pdf",
                row_idx=0,
                validate=True,
            )

        # Service should not have been called
        mock_txn_service.record.assert_not_called()

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    def test_employer_pf_contribution_entry(
        self, mock_get_account, mock_conn, mock_txn_service
    ):
        """Test employer PF contribution creates correct entry."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        result = record_employer_pf_contribution(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            employer="Acme Corp",
            pay_period="March 2024",
            employer_contribution=Decimal("8000"),
            txn_date=date(2024, 3, 31),
            source_file="/path/to/payslip.pdf",
            row_idx=0,
        )

        assert result.success is True

        call_args = mock_txn_service.record.call_args
        entries = call_args.kwargs['entries']

        # Should have 2 entries: EPF Employer Asset (Dr) and Employer PF Income (Cr)
        assert len(entries) == 2
        assert entries[0].debit == Decimal("8000")
        assert entries[1].credit == Decimal("8000")


class TestCostBasisTracker:
    """Tests for cost basis tracking service."""

    @pytest.fixture
    def mock_conn(self):
        """Create mock connection with row factory."""
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = []
        return conn

    def test_fifo_cost_calculation(self, mock_conn):
        """Test FIFO cost basis calculation."""
        tracker = CostBasisTracker(mock_conn, cost_method=CostMethod.FIFO)

        # Mock lots data
        lots = [
            Lot(
                lot_id=1,
                acquisition_date=date(2024, 1, 15),
                units_acquired=Decimal("100"),
                units_remaining=Decimal("100"),
                cost_per_unit=Decimal("100"),
                total_cost=Decimal("10000"),
            ),
            Lot(
                lot_id=2,
                acquisition_date=date(2024, 3, 15),
                units_acquired=Decimal("50"),
                units_remaining=Decimal("50"),
                cost_per_unit=Decimal("120"),
                total_cost=Decimal("6000"),
            ),
        ]

        tracker._lots_cache["1:STOCK:RELIANCE"] = lots

        # Sell 120 units - should use FIFO (100 from lot 1, 20 from lot 2)
        result = tracker.calculate_cost_basis(
            user_id=1,
            asset_type="STOCK",
            symbol="RELIANCE",
            units_to_sell=Decimal("120"),
            sell_date=date(2024, 6, 15),
            sale_proceeds=Decimal("15000"),
        )

        # Cost should be: 100 × ₹100 + 20 × ₹120 = ₹12,400
        assert result.total_cost_basis == Decimal("12400")
        assert result.units_sold == Decimal("120")
        assert len(result.matched_lots) == 2

    def test_insufficient_shares_raises_error(self, mock_conn):
        """Test that selling more than available raises error."""
        tracker = CostBasisTracker(mock_conn, cost_method=CostMethod.FIFO)

        lots = [
            Lot(
                lot_id=1,
                acquisition_date=date(2024, 1, 15),
                units_acquired=Decimal("50"),
                units_remaining=Decimal("50"),
                cost_per_unit=Decimal("100"),
                total_cost=Decimal("5000"),
            ),
        ]

        tracker._lots_cache["1:STOCK:RELIANCE"] = lots

        with pytest.raises(InsufficientSharesError) as exc_info:
            tracker.calculate_cost_basis(
                user_id=1,
                asset_type="STOCK",
                symbol="RELIANCE",
                units_to_sell=Decimal("100"),  # Only 50 available
                sell_date=date(2024, 6, 15),
            )

        assert exc_info.value.symbol == "RELIANCE"
        assert exc_info.value.requested == "100"
        assert exc_info.value.available == "50"

    def test_ltcg_determination_equity(self, mock_conn):
        """Test LTCG determination for equity (>365 days)."""
        tracker = CostBasisTracker(mock_conn, cost_method=CostMethod.FIFO)

        lots = [
            Lot(
                lot_id=1,
                acquisition_date=date(2023, 1, 1),
                units_acquired=Decimal("100"),
                units_remaining=Decimal("100"),
                cost_per_unit=Decimal("100"),
                total_cost=Decimal("10000"),
            ),
        ]

        tracker._lots_cache["1:STOCK:RELIANCE"] = lots

        result = tracker.calculate_cost_basis(
            user_id=1,
            asset_type="STOCK",
            symbol="RELIANCE",
            units_to_sell=Decimal("50"),
            sell_date=date(2024, 6, 15),  # >365 days
        )

        assert result.is_long_term is True
        assert result.holding_period_days > 365

    def test_stcg_determination(self, mock_conn):
        """Test STCG determination (<365 days)."""
        tracker = CostBasisTracker(mock_conn, cost_method=CostMethod.FIFO)

        lots = [
            Lot(
                lot_id=1,
                acquisition_date=date(2024, 3, 1),
                units_acquired=Decimal("100"),
                units_remaining=Decimal("100"),
                cost_per_unit=Decimal("100"),
                total_cost=Decimal("10000"),
            ),
        ]

        tracker._lots_cache["1:STOCK:RELIANCE"] = lots

        result = tracker.calculate_cost_basis(
            user_id=1,
            asset_type="STOCK",
            symbol="RELIANCE",
            units_to_sell=Decimal("50"),
            sell_date=date(2024, 6, 15),  # <365 days
        )

        assert result.is_long_term is False
        assert result.holding_period_days < 365

    def test_average_cost_calculation(self, mock_conn):
        """Test average cost basis calculation."""
        tracker = CostBasisTracker(mock_conn, cost_method=CostMethod.AVERAGE)

        lots = [
            Lot(
                lot_id=1,
                acquisition_date=date(2024, 1, 15),
                units_acquired=Decimal("100"),
                units_remaining=Decimal("100"),
                cost_per_unit=Decimal("100"),
                total_cost=Decimal("10000"),
            ),
            Lot(
                lot_id=2,
                acquisition_date=date(2024, 3, 15),
                units_acquired=Decimal("100"),
                units_remaining=Decimal("100"),
                cost_per_unit=Decimal("120"),
                total_cost=Decimal("12000"),
            ),
        ]

        tracker._lots_cache["1:MF_EQUITY:INF123"] = lots

        result = tracker.calculate_cost_basis(
            user_id=1,
            asset_type="MF_EQUITY",
            symbol="INF123",
            units_to_sell=Decimal("50"),
            sell_date=date(2024, 6, 15),
        )

        # Average cost = (10000 + 12000) / 200 = ₹110/unit
        # Cost for 50 units = 50 × 110 = ₹5500
        assert result.cost_per_unit == Decimal("110.0000")
        assert result.total_cost_basis == Decimal("5500.0000")


class TestRSUAccounting:
    """Tests for RSU vesting and sale accounting."""

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
    @patch('pfas.parsers.ledger_integration.get_sbi_tt_rate')
    @patch('pfas.services.cost_basis_tracker.CostBasisTracker')
    def test_rsu_vest_creates_correct_entries(
        self, mock_tracker_class, mock_get_rate, mock_get_account, mock_conn, mock_txn_service
    ):
        """Test RSU vest creates correct journal entry with INR conversion."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account
        mock_get_rate.return_value = Decimal("83.50")  # TT rate

        mock_tracker = MagicMock()
        mock_tracker_class.return_value = mock_tracker

        result = record_rsu_vest(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            grant_number="RSU-2024-001",
            symbol="AAPL",
            vest_date=date(2024, 3, 15),
            shares_vested=Decimal("100"),
            fmv_usd=Decimal("175.50"),
            shares_withheld_for_tax=Decimal("35"),
            source_file="/path/to/vest.pdf",
            row_idx=0,
        )

        assert result.success is True

        # Verify entries
        call_args = mock_txn_service.record.call_args
        entries = call_args.kwargs['entries']

        # Should have 2 entries: US Stock Asset (Dr) and Foreign Salary Income (Cr)
        assert len(entries) == 2

        # Perquisite = 100 shares × $175.50 × 83.50 = ₹14,65,425
        expected_perquisite = Decimal("100") * Decimal("175.50") * Decimal("83.50")
        assert entries[0].debit == expected_perquisite.quantize(Decimal("0.01"))
        assert entries[1].credit == expected_perquisite.quantize(Decimal("0.01"))

        # Verify cost basis lot was created for net shares (65)
        mock_tracker.record_purchase.assert_called_once()
        call_args = mock_tracker.record_purchase.call_args
        assert call_args.kwargs['units'] == Decimal("65")  # net shares

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    @patch('pfas.parsers.ledger_integration.get_sbi_tt_rate')
    def test_rsu_vest_with_provided_tt_rate(
        self, mock_get_rate, mock_get_account, mock_conn, mock_txn_service
    ):
        """Test RSU vest uses provided TT rate instead of lookup."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        with patch('pfas.services.cost_basis_tracker.CostBasisTracker'):
            result = record_rsu_vest(
                txn_service=mock_txn_service,
                conn=mock_conn,
                user_id=1,
                grant_number="RSU-2024-001",
                symbol="AAPL",
                vest_date=date(2024, 3, 15),
                shares_vested=Decimal("100"),
                fmv_usd=Decimal("175.50"),
                shares_withheld_for_tax=Decimal("35"),
                source_file="/path/to/vest.pdf",
                row_idx=0,
                tt_rate=Decimal("84.00"),  # Provided rate
            )

        # Rate lookup should not have been called
        mock_get_rate.assert_not_called()


class TestESPPAccounting:
    """Tests for ESPP purchase accounting."""

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
    @patch('pfas.parsers.ledger_integration.get_sbi_tt_rate')
    @patch('pfas.services.cost_basis_tracker.CostBasisTracker')
    def test_espp_purchase_with_discount_perquisite(
        self, mock_tracker_class, mock_get_rate, mock_get_account, mock_conn, mock_txn_service
    ):
        """Test ESPP purchase records discount as perquisite."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account
        mock_get_rate.return_value = Decimal("83.50")

        mock_tracker = MagicMock()
        mock_tracker_class.return_value = mock_tracker

        result = record_espp_purchase(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            symbol="AAPL",
            purchase_date=date(2024, 3, 15),
            shares_purchased=Decimal("50"),
            purchase_price_usd=Decimal("150.00"),  # 15% discount
            market_price_usd=Decimal("176.47"),
            source_file="/path/to/espp.pdf",
            row_idx=0,
        )

        assert result.success is True

        call_args = mock_txn_service.record.call_args
        entries = call_args.kwargs['entries']

        # Should have 3 entries:
        # 1. ESPP Asset (Dr) at FMV
        # 2. Bank (Cr) at purchase price
        # 3. ESPP Perquisite Income (Cr) for discount
        assert len(entries) == 3

        # Verify amounts
        tt_rate = Decimal("83.50")
        market_value_inr = (Decimal("50") * Decimal("176.47") * tt_rate).quantize(Decimal("0.01"))
        purchase_value_inr = (Decimal("50") * Decimal("150.00") * tt_rate).quantize(Decimal("0.01"))
        perquisite_inr = market_value_inr - purchase_value_inr

        assert entries[0].debit == market_value_inr  # Asset at FMV
        assert entries[1].credit == purchase_value_inr  # Bank payment
        assert entries[2].credit == perquisite_inr  # Perquisite income


class TestForeignDividendAccounting:
    """Tests for foreign dividend with DTAA credit."""

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
    @patch('pfas.parsers.ledger_integration.get_sbi_tt_rate')
    def test_foreign_dividend_with_withholding(
        self, mock_get_rate, mock_get_account, mock_conn, mock_txn_service
    ):
        """Test foreign dividend records gross income and DTAA credit."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account
        mock_get_rate.return_value = Decimal("83.50")

        result = record_foreign_dividend(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            symbol="AAPL",
            dividend_date=date(2024, 3, 15),
            gross_dividend_usd=Decimal("100.00"),
            withholding_tax_usd=Decimal("25.00"),  # 25% US withholding
            source_file="/path/to/dividend.pdf",
            row_idx=0,
        )

        assert result.success is True

        call_args = mock_txn_service.record.call_args
        entries = call_args.kwargs['entries']

        # Should have 3 entries:
        # 1. Bank (Dr) - net dividend
        # 2. Foreign Tax Credit (Dr) - withholding
        # 3. Foreign Dividend Income (Cr) - gross
        assert len(entries) == 3

        tt_rate = Decimal("83.50")
        gross_inr = (Decimal("100.00") * tt_rate).quantize(Decimal("0.01"))
        withholding_inr = (Decimal("25.00") * tt_rate).quantize(Decimal("0.01"))
        net_inr = gross_inr - withholding_inr

        # Total debits should equal total credits
        total_debits = sum(e.debit for e in entries if e.debit)
        total_credits = sum(e.credit for e in entries if e.credit)
        assert total_debits == total_credits == gross_inr


class TestForexRateLookup:
    """Tests for SBI TT rate lookup."""

    def test_forex_rate_not_found_raises_error(self):
        """Test that missing forex rate raises appropriate error."""
        mock_conn = MagicMock()

        with patch('pfas.services.currency.rate_provider.SBITTRateProvider') as mock_provider_class:
            mock_provider = MagicMock()
            mock_provider.get_rate.side_effect = ValueError("Rate not found")
            mock_provider_class.return_value = mock_provider

            with pytest.raises(ForexRateNotFoundError) as exc_info:
                get_sbi_tt_rate(mock_conn, date(2024, 3, 15))

            assert exc_info.value.rate_date == "2024-03-15"
            assert exc_info.value.from_currency == "USD"


class TestInventoryAccountingIntegration:
    """Integration tests for inventory accounting with ledger."""

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
    @patch('pfas.parsers.ledger_integration.record_mf_purchase')
    @patch('pfas.services.cost_basis_tracker.CostBasisTracker')
    def test_mf_purchase_creates_lot(
        self, mock_tracker_class, mock_record_purchase, mock_get_account, mock_conn, mock_txn_service
    ):
        """Test MF purchase creates cost basis lot."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        from pfas.parsers.ledger_integration import LedgerRecordResult
        mock_record_purchase.return_value = LedgerRecordResult(
            success=True, is_duplicate=False, journal_id=1
        )

        mock_tracker = MagicMock()
        mock_tracker_class.return_value = mock_tracker

        result = record_mf_purchase_with_cost_basis(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            folio_number="12345",
            scheme_name="Test Scheme",
            isin="INF123456789",
            txn_date=date(2024, 3, 15),
            amount=Decimal("10000"),
            units=Decimal("100"),
            is_equity=True,
            source_file="/path/to/cas.pdf",
            row_idx=0,
        )

        assert result.success is True

        # Verify cost basis lot was created
        mock_tracker.record_purchase.assert_called_once()
        call_args = mock_tracker.record_purchase.call_args
        assert call_args.kwargs['asset_type'] == "MF_EQUITY"
        assert call_args.kwargs['symbol'] == "INF123456789"
        assert call_args.kwargs['units'] == Decimal("100")
        assert call_args.kwargs['total_cost'] == Decimal("10000")

    @patch('pfas.parsers.ledger_integration.get_account_by_code')
    @patch('pfas.parsers.ledger_integration.record_mf_redemption')
    @patch('pfas.services.cost_basis_tracker.CostBasisTracker')
    def test_mf_redemption_uses_fifo_and_depletes_lots(
        self, mock_tracker_class, mock_record_redemption, mock_get_account, mock_conn, mock_txn_service
    ):
        """Test MF redemption uses FIFO cost and depletes lots."""
        mock_account = MagicMock()
        mock_account.id = 101
        mock_get_account.return_value = mock_account

        from pfas.parsers.ledger_integration import LedgerRecordResult
        mock_record_redemption.return_value = LedgerRecordResult(
            success=True, is_duplicate=False, journal_id=1
        )

        mock_tracker = MagicMock()
        mock_tracker.calculate_cost_basis.return_value = CostBasisResult(
            units_sold=Decimal("50"),
            total_cost_basis=Decimal("5000"),
            cost_per_unit=Decimal("100"),
            is_long_term=True,
            holding_period_days=400,
            realized_gain=Decimal("1000"),
        )
        mock_tracker_class.return_value = mock_tracker

        result, gain = record_mf_redemption_with_cost_basis(
            txn_service=mock_txn_service,
            conn=mock_conn,
            user_id=1,
            folio_number="12345",
            scheme_name="Test Scheme",
            isin="INF123456789",
            txn_date=date(2024, 6, 15),
            proceeds=Decimal("6000"),
            units=Decimal("50"),
            is_equity=True,
            source_file="/path/to/cas.pdf",
            row_idx=0,
        )

        assert result.success is True
        assert gain == Decimal("1000")

        # Verify FIFO cost was calculated
        mock_tracker.calculate_cost_basis.assert_called_once()

        # Verify lots were depleted
        mock_tracker.deplete_lots.assert_called_once()
