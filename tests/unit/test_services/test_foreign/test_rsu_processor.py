"""Tests for RSU Processor."""

import pytest
from datetime import date
from decimal import Decimal

from pfas.services.foreign.rsu_processor import RSUProcessor, RSULot, RSUSaleResult, RSUAnnualSummary
from pfas.parsers.foreign.models import RSUVest, RSUSale


class TestRSUProcessor:
    """Tests for RSUProcessor class."""

    def test_processor_initialization(self, db_connection):
        """Test processor can be initialized."""
        processor = RSUProcessor(db_connection)
        assert processor.conn is not None
        assert processor.rate_provider is not None

    def test_ltcg_threshold_constant(self, db_connection):
        """Test LTCG threshold is 730 days (24 months)."""
        processor = RSUProcessor(db_connection)
        assert processor.LTCG_THRESHOLD_DAYS == 730


class TestRSULot:
    """Tests for RSULot dataclass."""

    def test_lot_creation(self):
        """Test creating RSU lot."""
        lot = RSULot(
            vest_id=1,
            vest_date=date(2024, 6, 15),
            shares_available=Decimal("100"),
            cost_basis_usd=Decimal("150.00"),
            cost_basis_inr=Decimal("12525.00"),
            tt_rate=Decimal("83.50"),
            grant_number="RSU-001"
        )

        assert lot.vest_id == 1
        assert lot.vest_date == date(2024, 6, 15)
        assert lot.shares_available == Decimal("100")
        assert lot.cost_basis_usd == Decimal("150.00")
        assert lot.cost_basis_inr == Decimal("12525.00")


class TestRSUSaleResult:
    """Tests for RSUSaleResult dataclass."""

    def test_sale_result_creation(self):
        """Test creating sale result."""
        result = RSUSaleResult(
            sale_date=date(2024, 12, 15),
            shares_sold=Decimal("50"),
            sell_price_usd=Decimal("175.00"),
            sell_value_usd=Decimal("8750.00"),
            sell_value_inr=Decimal("729687.50"),
            cost_basis_usd=Decimal("7500.00"),
            cost_basis_inr=Decimal("626250.00"),
            gain_usd=Decimal("1250.00"),
            gain_inr=Decimal("103437.50"),
            is_ltcg=False,
            holding_period_days=183,
            fees_usd=Decimal("10.00"),
            fees_inr=Decimal("833.50")
        )

        assert result.sale_date == date(2024, 12, 15)
        assert result.shares_sold == Decimal("50")
        assert result.is_ltcg is False

    def test_net_gain_property(self):
        """Test net gain calculation."""
        result = RSUSaleResult(
            sale_date=date(2024, 12, 15),
            shares_sold=Decimal("50"),
            sell_price_usd=Decimal("175.00"),
            sell_value_usd=Decimal("8750.00"),
            sell_value_inr=Decimal("729687.50"),
            cost_basis_usd=Decimal("7500.00"),
            cost_basis_inr=Decimal("626250.00"),
            gain_usd=Decimal("1250.00"),
            gain_inr=Decimal("103437.50"),
            is_ltcg=False,
            holding_period_days=183,
            fees_usd=Decimal("10.00"),
            fees_inr=Decimal("833.50")
        )

        assert result.net_gain_usd == Decimal("1240.00")
        assert result.net_gain_inr == Decimal("102604.00")


class TestRSUAnnualSummary:
    """Tests for RSUAnnualSummary dataclass."""

    def test_summary_creation(self):
        """Test creating annual summary."""
        summary = RSUAnnualSummary(financial_year="2024-25")

        assert summary.financial_year == "2024-25"
        assert summary.total_perquisite_usd == Decimal("0")
        assert summary.total_perquisite_inr == Decimal("0")
        assert summary.vest_count == 0
        assert summary.total_shares_vested == Decimal("0")
        assert summary.ltcg_usd == Decimal("0")
        assert summary.ltcg_inr == Decimal("0")
        assert summary.stcg_usd == Decimal("0")
        assert summary.stcg_inr == Decimal("0")
        assert summary.sale_count == 0
        assert summary.total_shares_sold == Decimal("0")


class TestRSUVestModel:
    """Tests for RSUVest model."""

    def test_vest_creation(self):
        """Test creating RSU vest."""
        vest = RSUVest(
            grant_number="RSU-001",
            vest_date=date(2024, 6, 15),
            shares_vested=Decimal("100"),
            fmv_usd=Decimal("150.00"),
            shares_withheld_for_tax=Decimal("35"),
        )

        assert vest.grant_number == "RSU-001"
        assert vest.vest_date == date(2024, 6, 15)
        assert vest.shares_vested == Decimal("100")
        assert vest.fmv_usd == Decimal("150.00")
        assert vest.shares_withheld_for_tax == Decimal("35")
        # net_shares should be calculated in __post_init__
        assert vest.net_shares == Decimal("65")

    def test_vest_cost_basis_property(self):
        """Test cost basis per share property."""
        vest = RSUVest(
            grant_number="RSU-001",
            vest_date=date(2024, 6, 15),
            shares_vested=Decimal("100"),
            fmv_usd=Decimal("150.00")
        )

        assert vest.cost_basis_per_share_usd == Decimal("150.00")

    def test_vest_cost_basis_inr_without_rate(self):
        """Test cost basis INR returns None without TT rate."""
        vest = RSUVest(
            grant_number="RSU-001",
            vest_date=date(2024, 6, 15),
            shares_vested=Decimal("100"),
            fmv_usd=Decimal("150.00")
        )

        assert vest.cost_basis_per_share_inr is None

    def test_vest_cost_basis_inr_with_rate(self):
        """Test cost basis INR with TT rate."""
        vest = RSUVest(
            grant_number="RSU-001",
            vest_date=date(2024, 6, 15),
            shares_vested=Decimal("100"),
            fmv_usd=Decimal("150.00"),
            tt_rate=Decimal("83.50")
        )

        assert vest.cost_basis_per_share_inr == Decimal("12525.00")

    def test_vest_calculate_perquisite(self):
        """Test perquisite calculation."""
        vest = RSUVest(
            grant_number="RSU-001",
            vest_date=date(2024, 6, 15),
            shares_vested=Decimal("100"),
            fmv_usd=Decimal("150.00")
        )

        perquisite = vest.calculate_perquisite(Decimal("83.50"))

        # Perquisite = shares × FMV × TT rate
        expected = Decimal("100") * Decimal("150.00") * Decimal("83.50")
        assert perquisite == expected
        assert vest.tt_rate == Decimal("83.50")
        assert vest.perquisite_inr == expected


class TestRSUSaleModel:
    """Tests for RSUSale model."""

    def test_sale_creation(self):
        """Test creating RSU sale."""
        sale = RSUSale(
            sell_date=date(2024, 12, 15),
            shares_sold=Decimal("50"),
            sell_price_usd=Decimal("175.00"),
            sell_value_usd=Decimal("8750.00"),
            vest_date=date(2024, 6, 15),
            cost_basis_per_share_usd=Decimal("150.00"),
            cost_basis_usd=Decimal("7500.00")
        )

        assert sale.sell_date == date(2024, 12, 15)
        assert sale.shares_sold == Decimal("50")
        assert sale.sell_price_usd == Decimal("175.00")

    def test_sale_ltcg_threshold(self):
        """Test LTCG threshold constant."""
        assert RSUSale.LTCG_THRESHOLD_DAYS == 730

    def test_sale_calculate_gain_stcg(self):
        """Test gain calculation for short term."""
        sale = RSUSale(
            sell_date=date(2024, 12, 15),
            shares_sold=Decimal("50"),
            sell_price_usd=Decimal("175.00"),
            sell_value_usd=Decimal("8750.00"),
            vest_date=date(2024, 6, 15),  # Less than 2 years ago
            cost_basis_per_share_usd=Decimal("150.00"),
            cost_basis_usd=Decimal("7500.00")
        )

        sale.calculate_gain(Decimal("83.50"))

        assert sale.is_ltcg is False
        assert sale.holding_period_days == 183  # ~6 months
        assert sale.gain_usd == Decimal("1250.00")

    def test_sale_calculate_gain_ltcg(self):
        """Test gain calculation for long term."""
        sale = RSUSale(
            sell_date=date(2026, 12, 15),
            shares_sold=Decimal("50"),
            sell_price_usd=Decimal("200.00"),
            sell_value_usd=Decimal("10000.00"),
            vest_date=date(2024, 6, 15),  # More than 2 years ago
            cost_basis_per_share_usd=Decimal("150.00"),
            cost_basis_usd=Decimal("7500.00")
        )

        sale.calculate_gain(Decimal("84.00"))

        assert sale.is_ltcg is True
        assert sale.holding_period_days > 730


class TestGetAnnualSummary:
    """Tests for annual summary generation."""

    def test_summary_empty(self, db_connection, sample_user):
        """Test summary with no data."""
        processor = RSUProcessor(db_connection)

        summary = processor.get_annual_summary(sample_user["id"], "2024-25")

        assert summary.financial_year == "2024-25"
        assert summary.vest_count == 0
        assert summary.sale_count == 0
        assert summary.total_perquisite_inr == Decimal("0")
