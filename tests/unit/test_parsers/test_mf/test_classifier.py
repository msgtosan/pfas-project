"""Tests for mutual fund classifier."""

import pytest

from pfas.parsers.mf.classifier import (
    classify_scheme, get_holding_period_threshold, is_elss_scheme
)
from pfas.parsers.mf.models import AssetClass


class TestEquityClassification:
    """Tests for equity fund classification."""

    def test_bluechip_equity(self):
        """Test bluechip fund classification."""
        assert classify_scheme("SBI Bluechip Fund Direct Growth") == AssetClass.EQUITY

    def test_large_cap_equity(self):
        """Test large cap fund classification."""
        assert classify_scheme("HDFC Top 100 Fund") == AssetClass.EQUITY
        assert classify_scheme("Mirae Asset Large Cap Fund") == AssetClass.EQUITY

    def test_mid_cap_equity(self):
        """Test mid cap fund classification."""
        assert classify_scheme("Kotak Emerging Equity Fund") == AssetClass.EQUITY

    def test_small_cap_equity(self):
        """Test small cap fund classification."""
        assert classify_scheme("SBI Small Cap Fund") == AssetClass.EQUITY

    def test_multi_cap_equity(self):
        """Test multi cap fund classification."""
        assert classify_scheme("HDFC Multi Cap Fund") == AssetClass.EQUITY

    def test_flexi_cap_equity(self):
        """Test flexi cap fund classification."""
        assert classify_scheme("Parag Parikh Flexi Cap Fund") == AssetClass.EQUITY

    def test_index_equity(self):
        """Test index fund classification."""
        assert classify_scheme("UTI Nifty Index Fund") == AssetClass.EQUITY
        assert classify_scheme("HDFC Sensex Fund") == AssetClass.EQUITY

    def test_sectoral_equity(self):
        """Test sectoral fund classification."""
        assert classify_scheme("SBI Consumption Opportunities Fund") == AssetClass.EQUITY
        assert classify_scheme("ICICI Pharma Fund") == AssetClass.EQUITY
        assert classify_scheme("Axis Banking Fund") == AssetClass.EQUITY

    def test_elss_equity(self):
        """Test ELSS classification."""
        assert classify_scheme("SBI Tax Saver Fund") == AssetClass.EQUITY
        assert classify_scheme("HDFC ELSS Fund") == AssetClass.EQUITY


class TestDebtClassification:
    """Tests for debt fund classification."""

    def test_liquid_debt(self):
        """Test liquid fund classification."""
        assert classify_scheme("SBI Liquid Fund") == AssetClass.DEBT

    def test_corporate_bond_debt(self):
        """Test corporate bond fund classification."""
        assert classify_scheme("Kotak Corporate Bond Fund") == AssetClass.DEBT
        assert classify_scheme("HDFC Corporate Bond Fund Direct Growth") == AssetClass.DEBT

    def test_short_duration_debt(self):
        """Test short duration fund classification."""
        assert classify_scheme("HDFC Short Term Debt Fund") == AssetClass.DEBT
        assert classify_scheme("ICICI Ultra Short Term Fund") == AssetClass.DEBT

    def test_gilt_debt(self):
        """Test gilt fund classification."""
        assert classify_scheme("SBI Gilt Fund") == AssetClass.DEBT

    def test_dynamic_bond_debt(self):
        """Test dynamic bond fund classification."""
        assert classify_scheme("ICICI Dynamic Bond Fund") == AssetClass.DEBT

    def test_money_market_debt(self):
        """Test money market fund classification."""
        assert classify_scheme("Aditya Birla Money Market Fund") == AssetClass.DEBT


class TestHybridClassification:
    """Tests for hybrid fund classification."""

    def test_balanced_hybrid(self):
        """Test balanced fund classification."""
        assert classify_scheme("HDFC Balanced Advantage Fund") == AssetClass.HYBRID

    def test_aggressive_hybrid(self):
        """Test aggressive hybrid fund classification."""
        assert classify_scheme("ICICI Aggressive Hybrid Fund") == AssetClass.HYBRID

    def test_conservative_hybrid(self):
        """Test conservative hybrid fund classification."""
        assert classify_scheme("SBI Conservative Hybrid Fund") == AssetClass.HYBRID

    def test_equity_savings_hybrid(self):
        """Test equity savings fund classification."""
        assert classify_scheme("HDFC Equity Savings Fund") == AssetClass.HYBRID

    def test_arbitrage_hybrid(self):
        """Test arbitrage fund classification."""
        assert classify_scheme("Kotak Arbitrage Fund") == AssetClass.HYBRID


class TestOtherClassification:
    """Tests for unclassified funds."""

    def test_unknown_scheme(self):
        """Test unknown scheme classification."""
        assert classify_scheme("Unknown Fund XYZ") == AssetClass.OTHER

    def test_empty_scheme(self):
        """Test empty scheme name."""
        assert classify_scheme("") == AssetClass.OTHER
        assert classify_scheme(None) == AssetClass.OTHER


class TestHoldingPeriodThreshold:
    """Tests for holding period threshold."""

    def test_equity_threshold(self):
        """Test equity threshold is 365 days."""
        assert get_holding_period_threshold(AssetClass.EQUITY) == 365

    def test_debt_threshold(self):
        """Test debt threshold is 730 days."""
        assert get_holding_period_threshold(AssetClass.DEBT) == 730

    def test_hybrid_threshold(self):
        """Test hybrid treated as debt (730 days)."""
        assert get_holding_period_threshold(AssetClass.HYBRID) == 730

    def test_other_threshold(self):
        """Test other defaults to equity (365 days)."""
        assert get_holding_period_threshold(AssetClass.OTHER) == 365


class TestELSSDetection:
    """Tests for ELSS scheme detection."""

    def test_elss_keyword(self):
        """Test ELSS keyword detection."""
        assert is_elss_scheme("SBI ELSS Fund") is True
        assert is_elss_scheme("HDFC ELSS Direct Growth") is True

    def test_tax_saver_keyword(self):
        """Test tax saver keyword detection."""
        assert is_elss_scheme("Axis Tax Saver Fund") is True
        assert is_elss_scheme("Mirae Tax Saving Fund") is True

    def test_not_elss(self):
        """Test non-ELSS fund."""
        assert is_elss_scheme("SBI Bluechip Fund") is False
        assert is_elss_scheme("HDFC Corporate Bond Fund") is False

    def test_case_insensitive(self):
        """Test case insensitive detection."""
        assert is_elss_scheme("sbi elss fund") is True
        assert is_elss_scheme("HDFC TAX SAVER") is True
