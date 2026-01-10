"""Tests for mutual fund capital gains calculator."""

import pytest
from datetime import date
from decimal import Decimal

from pfas.parsers.mf.capital_gains import CapitalGainsCalculator, CapitalGainsSummary
from pfas.parsers.mf.models import MFScheme, MFTransaction, AssetClass, TransactionType


class TestCapitalGainsCalculator:
    """Tests for capital gains calculations."""

    def test_equity_stcg_calculation(self, db_connection):
        """Test STCG calculation for equity <12 months."""
        calc = CapitalGainsCalculator(db_connection)

        scheme = MFScheme(
            name="Test Equity Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.EQUITY
        )

        txn = MFTransaction(
            folio_number="12345",
            scheme=scheme,
            transaction_type=TransactionType.REDEMPTION,
            date=date(2024, 7, 15),
            units=Decimal("100"),
            nav=Decimal("150"),
            amount=Decimal("15000"),  # Sale value
            purchase_date=date(2024, 3, 1),  # <12 months
            purchase_units=Decimal("100"),
            purchase_nav=Decimal("120"),  # Cost = ₹12,000
        )

        stcg, ltcg = calc.calculate_for_transaction(txn)

        assert stcg == Decimal("3000")  # 15000 - 12000
        assert ltcg == Decimal("0")

    def test_equity_ltcg_calculation(self, db_connection):
        """Test LTCG calculation for equity >12 months."""
        calc = CapitalGainsCalculator(db_connection)

        scheme = MFScheme(
            name="Test Equity Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.EQUITY
        )

        txn = MFTransaction(
            folio_number="12345",
            scheme=scheme,
            transaction_type=TransactionType.REDEMPTION,
            date=date(2024, 7, 15),
            units=Decimal("100"),
            nav=Decimal("150"),
            amount=Decimal("15000"),  # Sale value
            purchase_date=date(2023, 1, 1),  # >12 months
            purchase_units=Decimal("100"),
            purchase_nav=Decimal("120"),  # Cost = ₹12,000
        )

        stcg, ltcg = calc.calculate_for_transaction(txn)

        assert stcg == Decimal("0")
        assert ltcg == Decimal("3000")  # 15000 - 12000

    def test_grandfathering_pre_31jan2018(self, db_connection):
        """Test grandfathering for pre-31-Jan-2018 purchases."""
        calc = CapitalGainsCalculator(db_connection)

        scheme = MFScheme(
            name="Test Equity Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.EQUITY
        )

        txn = MFTransaction(
            folio_number="12345",
            scheme=scheme,
            transaction_type=TransactionType.REDEMPTION,
            date=date(2024, 7, 15),
            units=Decimal("100"),
            nav=Decimal("200"),
            amount=Decimal("20000"),  # Sale value
            purchase_date=date(2017, 6, 1),  # Before 31-Jan-2018
            purchase_units=Decimal("100"),
            purchase_nav=Decimal("100"),  # Actual cost = ₹10,000
            grandfathered_nav=Decimal("150"),  # FMV on 31-Jan-2018
            grandfathered_value=Decimal("15000"),  # FMV > actual cost
        )

        stcg, ltcg = calc.calculate_for_transaction(txn)

        # Should use FMV (₹15,000) as cost since it's higher than actual (₹10,000)
        # LTCG = 20000 - 15000 = 5000
        assert ltcg == Decimal("5000")

    def test_grandfathering_fmv_capped_at_sale_price(self, db_connection):
        """Test that FMV is capped at sale price to avoid artificial loss."""
        calc = CapitalGainsCalculator(db_connection)

        scheme = MFScheme(
            name="Test Equity Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.EQUITY
        )

        txn = MFTransaction(
            folio_number="12345",
            scheme=scheme,
            transaction_type=TransactionType.REDEMPTION,
            date=date(2024, 7, 15),
            units=Decimal("100"),
            nav=Decimal("100"),
            amount=Decimal("10000"),  # Sale value
            purchase_date=date(2017, 6, 1),  # Before 31-Jan-2018
            purchase_units=Decimal("100"),
            purchase_nav=Decimal("80"),  # Actual cost = ₹8,000
            grandfathered_nav=Decimal("150"),  # FMV on 31-Jan-2018
            grandfathered_value=Decimal("15000"),  # FMV = ₹15,000 (> sale price)
        )

        stcg, ltcg = calc.calculate_for_transaction(txn)

        # FMV (₹15,000) should be capped at sale price (₹10,000)
        # LTCG = 10000 - 10000 = 0
        assert ltcg == Decimal("0")

    def test_stt_deducted_from_gain(self, db_connection):
        """Test that STT is deducted from capital gain."""
        calc = CapitalGainsCalculator(db_connection)

        scheme = MFScheme(
            name="Test Equity Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.EQUITY
        )

        txn = MFTransaction(
            folio_number="12345",
            scheme=scheme,
            transaction_type=TransactionType.REDEMPTION,
            date=date(2024, 7, 15),
            units=Decimal("100"),
            nav=Decimal("150"),
            amount=Decimal("15000"),  # Sale value
            stt=Decimal("100"),  # STT paid
            purchase_date=date(2023, 1, 1),  # >12 months
            purchase_units=Decimal("100"),
            purchase_nav=Decimal("120"),  # Cost = ₹12,000
        )

        stcg, ltcg = calc.calculate_for_transaction(txn)

        # LTCG = 15000 - 12000 - 100 (STT) = 2900
        assert ltcg == Decimal("2900")

    def test_purchase_transaction_no_gain(self, db_connection):
        """Test that purchase transactions have no capital gain."""
        calc = CapitalGainsCalculator(db_connection)

        scheme = MFScheme(
            name="Test Equity Fund",
            amc_name="Test AMC",
            asset_class=AssetClass.EQUITY
        )

        txn = MFTransaction(
            folio_number="12345",
            scheme=scheme,
            transaction_type=TransactionType.PURCHASE,
            date=date(2024, 7, 15),
            units=Decimal("100"),
            nav=Decimal("150"),
            amount=Decimal("15000"),
        )

        stcg, ltcg = calc.calculate_for_transaction(txn)

        assert stcg == Decimal("0")
        assert ltcg == Decimal("0")


class TestCapitalGainsSummary:
    """Tests for CapitalGainsSummary dataclass."""

    def test_summary_creation(self):
        """Test creating a capital gains summary."""
        summary = CapitalGainsSummary(
            financial_year="2024-25",
            asset_class=AssetClass.EQUITY,
            stcg_amount=Decimal("10000"),
            ltcg_amount=Decimal("200000"),
            ltcg_exemption=Decimal("125000"),
            taxable_stcg=Decimal("10000"),
            taxable_ltcg=Decimal("75000"),
            stcg_tax_rate=Decimal("20"),
            ltcg_tax_rate=Decimal("12.5")
        )

        assert summary.financial_year == "2024-25"
        assert summary.asset_class == AssetClass.EQUITY
        assert summary.stcg_amount == Decimal("10000")
        assert summary.ltcg_amount == Decimal("200000")
        assert summary.ltcg_exemption == Decimal("125000")
        assert summary.taxable_ltcg == Decimal("75000")

    def test_equity_ltcg_exemption_applied(self):
        """Test that ₹1.25L exemption is applied to equity LTCG."""
        summary = CapitalGainsSummary(
            financial_year="2024-25",
            asset_class=AssetClass.EQUITY,
            ltcg_amount=Decimal("200000"),
            ltcg_exemption=Decimal("125000"),
            taxable_ltcg=Decimal("75000"),
        )

        # Taxable = 200000 - 125000 = 75000
        assert summary.taxable_ltcg == Decimal("75000")

    def test_equity_ltcg_below_exemption(self):
        """Test LTCG below exemption limit."""
        summary = CapitalGainsSummary(
            financial_year="2024-25",
            asset_class=AssetClass.EQUITY,
            ltcg_amount=Decimal("100000"),
            ltcg_exemption=Decimal("100000"),  # Full exemption
            taxable_ltcg=Decimal("0"),
        )

        # LTCG < ₹1.25L, fully exempt
        assert summary.taxable_ltcg == Decimal("0")


class TestTaxRates:
    """Tests for tax rate constants."""

    def test_equity_stcg_rate(self):
        """Test equity STCG rate is 20%."""
        calc = CapitalGainsCalculator(None)
        assert calc.EQUITY_STCG_RATE == Decimal("20")

    def test_equity_ltcg_rate(self):
        """Test equity LTCG rate is 12.5%."""
        calc = CapitalGainsCalculator(None)
        assert calc.EQUITY_LTCG_RATE == Decimal("12.5")

    def test_equity_ltcg_exemption(self):
        """Test equity LTCG exemption is ₹1.25 lakh."""
        calc = CapitalGainsCalculator(None)
        assert calc.EQUITY_LTCG_EXEMPTION == Decimal("125000")

    def test_debt_taxed_at_slab(self):
        """Test debt funds taxed at slab rate (rate = 0)."""
        calc = CapitalGainsCalculator(None)
        assert calc.DEBT_STCG_RATE == Decimal("0")
        assert calc.DEBT_LTCG_RATE == Decimal("0")
