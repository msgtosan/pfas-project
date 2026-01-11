"""Tests for ESPP Processor."""

import pytest
from datetime import date
from decimal import Decimal

from pfas.services.foreign.espp_processor import (
    ESPPProcessor,
    ESPPLot,
    ESPPSaleResult,
    ESPPAnnualSummary,
    TCSTracking
)
from pfas.parsers.foreign.models import ESPPPurchase, ESPPSale


class TestESPPProcessor:
    """Tests for ESPPProcessor class."""

    def test_processor_initialization(self, db_connection):
        """Test processor can be initialized."""
        processor = ESPPProcessor(db_connection)
        assert processor.conn is not None
        assert processor.rate_provider is not None

    def test_ltcg_threshold_constant(self, db_connection):
        """Test LTCG threshold is 730 days."""
        processor = ESPPProcessor(db_connection)
        assert processor.LTCG_THRESHOLD_DAYS == 730

    def test_tcs_threshold_constant(self, db_connection):
        """Test TCS threshold is 7 Lakh."""
        processor = ESPPProcessor(db_connection)
        assert processor.TCS_THRESHOLD == Decimal("700000")

    def test_tcs_rate_constant(self, db_connection):
        """Test TCS rate is 20%."""
        processor = ESPPProcessor(db_connection)
        assert processor.TCS_RATE == Decimal("0.20")


class TestTCSTracking:
    """Tests for TCS tracking."""

    def test_tcs_tracking_creation(self):
        """Test TCS tracking initialization."""
        tracking = TCSTracking(financial_year="2024-25")

        assert tracking.financial_year == "2024-25"
        assert tracking.cumulative_lrs == Decimal("0")
        assert tracking.tcs_collected == Decimal("0")
        assert tracking.remaining_exemption == Decimal("700000")

    def test_tcs_below_threshold(self):
        """Test no TCS when below threshold."""
        tracking = TCSTracking(financial_year="2024-25")

        tcs = tracking.add_remittance(Decimal("500000"))

        assert tcs == Decimal("0")
        assert tracking.cumulative_lrs == Decimal("500000")
        assert tracking.remaining_exemption == Decimal("200000")

    def test_tcs_above_threshold(self):
        """Test TCS calculation above threshold."""
        tracking = TCSTracking(financial_year="2024-25")

        # First remittance of 800K
        tcs = tracking.add_remittance(Decimal("800000"))

        # TCS on 100K (800K - 700K threshold) = 20K
        assert tcs == Decimal("20000.00")
        assert tracking.cumulative_lrs == Decimal("800000")
        assert tracking.remaining_exemption == Decimal("0")

    def test_tcs_multiple_remittances(self):
        """Test TCS across multiple remittances."""
        tracking = TCSTracking(financial_year="2024-25")

        # First remittance within threshold
        tcs1 = tracking.add_remittance(Decimal("600000"))
        assert tcs1 == Decimal("0")
        assert tracking.remaining_exemption == Decimal("100000")

        # Second remittance crosses threshold
        tcs2 = tracking.add_remittance(Decimal("200000"))
        # TCS on 100K (excess over 7L) = 20K
        assert tcs2 == Decimal("20000.00")

        # Third remittance entirely above threshold
        tcs3 = tracking.add_remittance(Decimal("100000"))
        # TCS on full 100K = 20K
        assert tcs3 == Decimal("20000.00")

        assert tracking.tcs_collected == Decimal("40000.00")


class TestESPPLot:
    """Tests for ESPPLot dataclass."""

    def test_lot_creation(self):
        """Test creating ESPP lot."""
        lot = ESPPLot(
            purchase_id=1,
            purchase_date=date(2024, 6, 30),
            shares_available=Decimal("50"),
            cost_basis_usd=Decimal("127.50"),  # 15% discount from 150
            cost_basis_inr=Decimal("10646.25"),
            market_price_usd=Decimal("150.00"),
            tt_rate=Decimal("83.50")
        )

        assert lot.purchase_id == 1
        assert lot.purchase_date == date(2024, 6, 30)
        assert lot.shares_available == Decimal("50")
        assert lot.cost_basis_usd == Decimal("127.50")


class TestESPPSaleResult:
    """Tests for ESPPSaleResult dataclass."""

    def test_sale_result_creation(self):
        """Test creating sale result."""
        result = ESPPSaleResult(
            sale_date=date(2024, 12, 31),
            shares_sold=Decimal("25"),
            sell_price_usd=Decimal("175.00"),
            sell_value_usd=Decimal("4375.00"),
            sell_value_inr=Decimal("365312.50"),
            cost_basis_usd=Decimal("3187.50"),
            cost_basis_inr=Decimal("266156.25"),
            gain_usd=Decimal("1187.50"),
            gain_inr=Decimal("99156.25"),
            is_ltcg=False,
            holding_period_days=184
        )

        assert result.sale_date == date(2024, 12, 31)
        assert result.shares_sold == Decimal("25")

    def test_net_gain_property(self):
        """Test net gain properties."""
        result = ESPPSaleResult(
            sale_date=date(2024, 12, 31),
            shares_sold=Decimal("25"),
            sell_price_usd=Decimal("175.00"),
            sell_value_usd=Decimal("4375.00"),
            sell_value_inr=Decimal("365312.50"),
            cost_basis_usd=Decimal("3187.50"),
            cost_basis_inr=Decimal("266156.25"),
            gain_usd=Decimal("1187.50"),
            gain_inr=Decimal("99156.25"),
            is_ltcg=False,
            holding_period_days=184,
            fees_usd=Decimal("5.00"),
            fees_inr=Decimal("417.50")
        )

        assert result.net_gain_usd == Decimal("1182.50")
        assert result.net_gain_inr == Decimal("98738.75")


class TestESPPAnnualSummary:
    """Tests for ESPPAnnualSummary dataclass."""

    def test_summary_creation(self):
        """Test creating annual summary."""
        summary = ESPPAnnualSummary(financial_year="2024-25")

        assert summary.financial_year == "2024-25"
        assert summary.total_perquisite_usd == Decimal("0")
        assert summary.total_perquisite_inr == Decimal("0")
        assert summary.purchase_count == 0
        assert summary.total_shares_purchased == Decimal("0")
        assert summary.total_lrs_inr == Decimal("0")
        assert summary.total_tcs_collected == Decimal("0")


class TestESPPPurchaseModel:
    """Tests for ESPPPurchase model."""

    def test_purchase_creation(self):
        """Test creating ESPP purchase."""
        purchase = ESPPPurchase(
            purchase_date=date(2024, 6, 30),
            shares_purchased=Decimal("50"),
            purchase_price_usd=Decimal("127.50"),  # 15% discount
            market_price_usd=Decimal("150.00")
        )

        assert purchase.purchase_date == date(2024, 6, 30)
        assert purchase.shares_purchased == Decimal("50")
        assert purchase.purchase_price_usd == Decimal("127.50")
        assert purchase.market_price_usd == Decimal("150.00")

    def test_purchase_tcs_constants(self):
        """Test TCS constants on ESPPPurchase."""
        assert ESPPPurchase.TCS_THRESHOLD == Decimal("700000")
        assert ESPPPurchase.TCS_RATE == Decimal("0.20")

    def test_purchase_calculate_perquisite(self):
        """Test perquisite calculation."""
        purchase = ESPPPurchase(
            purchase_date=date(2024, 6, 30),
            shares_purchased=Decimal("50"),
            purchase_price_usd=Decimal("127.50"),
            market_price_usd=Decimal("150.00")
        )

        purchase.calculate_perquisite(Decimal("83.50"))

        # Perquisite per share = 150 - 127.50 = 22.50
        assert purchase.perquisite_per_share_usd == Decimal("22.50")

        # Total perquisite = 22.50 × 50 = 1125
        assert purchase.total_perquisite_usd == Decimal("1125.00")

        # Perquisite INR = 1125 × 83.50 = 93937.50
        assert purchase.perquisite_inr == Decimal("93937.50")

        # Discount % = 22.50 / 150 × 100 = 15%
        assert purchase.discount_percentage == Decimal("15.00")

    def test_purchase_lrs_calculation(self):
        """Test LRS amount calculation."""
        purchase = ESPPPurchase(
            purchase_date=date(2024, 6, 30),
            shares_purchased=Decimal("100"),
            purchase_price_usd=Decimal("127.50"),
            market_price_usd=Decimal("150.00")
        )

        purchase.calculate_perquisite(Decimal("83.50"))

        # LRS = shares × purchase price × TT rate
        expected_lrs = Decimal("100") * Decimal("127.50") * Decimal("83.50")
        assert purchase.lrs_amount_inr == expected_lrs

    def test_purchase_tcs_below_threshold(self):
        """Test no TCS when LRS below threshold."""
        purchase = ESPPPurchase(
            purchase_date=date(2024, 6, 30),
            shares_purchased=Decimal("50"),  # ~50 × 127.50 × 83.50 = ~532K INR
            purchase_price_usd=Decimal("127.50"),
            market_price_usd=Decimal("150.00")
        )

        purchase.calculate_perquisite(Decimal("83.50"))

        assert purchase.tcs_collected == Decimal("0")

    def test_purchase_tcs_above_threshold(self):
        """Test TCS when LRS above threshold."""
        purchase = ESPPPurchase(
            purchase_date=date(2024, 6, 30),
            shares_purchased=Decimal("100"),  # ~100 × 127.50 × 83.50 = ~1.06M INR
            purchase_price_usd=Decimal("127.50"),
            market_price_usd=Decimal("150.00")
        )

        purchase.calculate_perquisite(Decimal("83.50"))

        # LRS = 100 × 127.50 × 83.50 = 1,064,625
        # Taxable = 1,064,625 - 700,000 = 364,625
        # TCS = 364,625 × 0.20 = 72,925
        expected_tcs = (purchase.lrs_amount_inr - Decimal("700000")) * Decimal("0.20")
        assert purchase.tcs_collected == expected_tcs


class TestESPPSaleModel:
    """Tests for ESPPSale model."""

    def test_sale_creation(self):
        """Test creating ESPP sale."""
        sale = ESPPSale(
            sell_date=date(2024, 12, 31),
            shares_sold=Decimal("25"),
            sell_price_usd=Decimal("175.00"),
            sell_value_usd=Decimal("4375.00"),
            purchase_date=date(2024, 6, 30),
            cost_basis_per_share_usd=Decimal("127.50"),
            cost_basis_usd=Decimal("3187.50")
        )

        assert sale.sell_date == date(2024, 12, 31)
        assert sale.shares_sold == Decimal("25")

    def test_sale_ltcg_threshold(self):
        """Test LTCG threshold constant."""
        assert ESPPSale.LTCG_THRESHOLD_DAYS == 730

    def test_sale_calculate_gain_stcg(self):
        """Test STCG calculation."""
        sale = ESPPSale(
            sell_date=date(2024, 12, 31),
            shares_sold=Decimal("25"),
            sell_price_usd=Decimal("175.00"),
            sell_value_usd=Decimal("4375.00"),
            purchase_date=date(2024, 6, 30),  # Less than 2 years
            cost_basis_per_share_usd=Decimal("127.50"),
            cost_basis_usd=Decimal("3187.50")
        )

        sale.calculate_gain(Decimal("83.50"))

        assert sale.is_ltcg is False
        assert sale.holding_period_days == 184
        assert sale.gain_usd == Decimal("1187.50")

    def test_sale_calculate_gain_ltcg(self):
        """Test LTCG calculation."""
        sale = ESPPSale(
            sell_date=date(2026, 12, 31),
            shares_sold=Decimal("25"),
            sell_price_usd=Decimal("200.00"),
            sell_value_usd=Decimal("5000.00"),
            purchase_date=date(2024, 6, 30),  # More than 2 years
            cost_basis_per_share_usd=Decimal("127.50"),
            cost_basis_usd=Decimal("3187.50")
        )

        sale.calculate_gain(Decimal("84.00"))

        assert sale.is_ltcg is True
        assert sale.holding_period_days > 730


class TestGetAnnualSummary:
    """Tests for annual summary."""

    def test_summary_empty(self, db_connection, sample_user):
        """Test summary with no data."""
        processor = ESPPProcessor(db_connection)

        summary = processor.get_annual_summary(sample_user["id"], "2024-25")

        assert summary.financial_year == "2024-25"
        assert summary.purchase_count == 0
        assert summary.sale_count == 0
