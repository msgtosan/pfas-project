"""
Unit tests for MF Parser Phase 1 gap fixes.

Tests:
- TransactionType enum expansion and classification
- Exception hierarchy
- FIFO unit tracking
- CAS PDF parser components
"""

import pytest
from datetime import date
from decimal import Decimal
from pathlib import Path


class TestTransactionTypeClassification:
    """Test TransactionType.from_description() classification."""

    def test_purchase_sip(self):
        from pfas.parsers.mf.models import TransactionType

        result = TransactionType.from_description(
            "Purchase-SIP - Instalment 1/83 - via Internet",
            Decimal("57.428")
        )
        assert result == TransactionType.PURCHASE_SIP

    def test_purchase(self):
        from pfas.parsers.mf.models import TransactionType

        result = TransactionType.from_description(
            "Purchase - via Internet",
            Decimal("100")
        )
        assert result == TransactionType.PURCHASE

    def test_redemption(self):
        from pfas.parsers.mf.models import TransactionType

        result = TransactionType.from_description(
            "Redemption",
            Decimal("-50")
        )
        assert result == TransactionType.REDEMPTION

    def test_switch_in(self):
        from pfas.parsers.mf.models import TransactionType

        result = TransactionType.from_description(
            "Switch In - from ABC Fund",
            Decimal("100")
        )
        assert result == TransactionType.SWITCH_IN

    def test_switch_out(self):
        from pfas.parsers.mf.models import TransactionType

        result = TransactionType.from_description(
            "Switch Out - to XYZ Fund",
            Decimal("-100")
        )
        assert result == TransactionType.SWITCH_OUT

    def test_dividend_reinvest(self):
        from pfas.parsers.mf.models import TransactionType

        result = TransactionType.from_description(
            "Dividend Reinvested",
            Decimal("5")
        )
        assert result == TransactionType.DIVIDEND_REINVEST

    def test_stt_tax(self):
        from pfas.parsers.mf.models import TransactionType

        result = TransactionType.from_description(
            "STT Paid",
            Decimal("0")
        )
        assert result == TransactionType.STT_TAX

    def test_stamp_duty(self):
        from pfas.parsers.mf.models import TransactionType

        result = TransactionType.from_description(
            "Stamp Duty",
            Decimal("0")
        )
        assert result == TransactionType.STAMP_DUTY_TAX


class TestExceptionHierarchy:
    """Test MF exception hierarchy."""

    def test_base_exception(self):
        from pfas.parsers.mf.exceptions import MFParserError

        exc = MFParserError("Test error", {"key": "value"})
        assert "Test error" in str(exc)
        assert exc.details == {"key": "value"}

    def test_cas_parse_error(self):
        from pfas.parsers.mf.exceptions import CASParseError, MFParserError

        exc = CASParseError("Parse failed")
        assert isinstance(exc, MFParserError)

    def test_incorrect_password_error(self):
        from pfas.parsers.mf.exceptions import IncorrectPasswordError, CASParseError

        exc = IncorrectPasswordError("/path/to/file.pdf")
        assert isinstance(exc, CASParseError)
        assert "password" in str(exc).lower()
        assert exc.file_path == "/path/to/file.pdf"

    def test_balance_mismatch_error(self):
        from pfas.parsers.mf.exceptions import BalanceMismatchError, IntegrityError

        exc = BalanceMismatchError(
            scheme_name="Test Fund",
            folio="12345",
            stated_balance="100.00",
            calculated_balance="99.50"
        )
        assert isinstance(exc, IntegrityError)
        assert "mismatch" in str(exc).lower()

    def test_fifo_mismatch_error(self):
        from pfas.parsers.mf.exceptions import FIFOMismatchError, GainsCalculationError

        exc = FIFOMismatchError(
            scheme_name="Test Fund",
            folio="12345",
            redemption_units="100",
            available_units="50"
        )
        assert isinstance(exc, GainsCalculationError)


class TestFIFOUnitTracker:
    """Test FIFO unit tracking for capital gains."""

    def test_add_purchase(self):
        from pfas.parsers.mf.fifo_tracker import FIFOUnitTracker
        from pfas.parsers.mf.models import AssetClass

        tracker = FIFOUnitTracker(
            scheme_name="Test Fund",
            folio="12345",
            asset_class=AssetClass.EQUITY
        )

        tracker.add_purchase(
            purchase_date=date(2020, 1, 1),
            units=Decimal("100"),
            nav=Decimal("50"),
            amount=Decimal("5000")
        )

        assert tracker.available_units == Decimal("100")
        assert tracker.total_purchased == Decimal("100")

    def test_simple_redemption(self):
        from pfas.parsers.mf.fifo_tracker import FIFOUnitTracker
        from pfas.parsers.mf.models import AssetClass

        tracker = FIFOUnitTracker(
            scheme_name="Test Fund",
            folio="12345",
            asset_class=AssetClass.EQUITY
        )

        # Add purchase
        tracker.add_purchase(
            purchase_date=date(2020, 1, 1),
            units=Decimal("100"),
            nav=Decimal("50"),
            amount=Decimal("5000")
        )

        # Redeem 50 units after 2 years
        gains = tracker.process_redemption(
            sale_date=date(2022, 6, 1),
            units=Decimal("50"),
            nav=Decimal("70"),
            amount=Decimal("3500")
        )

        assert len(gains) == 1
        assert gains[0].is_long_term is True  # >365 days
        assert gains[0].sale_units == Decimal("50")
        assert tracker.available_units == Decimal("50")

    def test_fifo_order(self):
        from pfas.parsers.mf.fifo_tracker import FIFOUnitTracker
        from pfas.parsers.mf.models import AssetClass

        tracker = FIFOUnitTracker(
            scheme_name="Test Fund",
            folio="12345",
            asset_class=AssetClass.EQUITY
        )

        # Add two purchases at different NAVs
        tracker.add_purchase(
            purchase_date=date(2020, 1, 1),
            units=Decimal("100"),
            nav=Decimal("50"),
            amount=Decimal("5000")
        )
        tracker.add_purchase(
            purchase_date=date(2021, 1, 1),
            units=Decimal("100"),
            nav=Decimal("60"),
            amount=Decimal("6000")
        )

        # Redeem 100 units - should match first purchase
        gains = tracker.process_redemption(
            sale_date=date(2022, 6, 1),
            units=Decimal("100"),
            nav=Decimal("70"),
            amount=Decimal("7000")
        )

        assert len(gains) == 1
        assert gains[0].purchase_date == date(2020, 1, 1)
        assert gains[0].purchase_nav == Decimal("50")

    def test_partial_lot_matching(self):
        from pfas.parsers.mf.fifo_tracker import FIFOUnitTracker
        from pfas.parsers.mf.models import AssetClass

        tracker = FIFOUnitTracker(
            scheme_name="Test Fund",
            folio="12345",
            asset_class=AssetClass.EQUITY
        )

        tracker.add_purchase(
            purchase_date=date(2020, 1, 1),
            units=Decimal("50"),
            nav=Decimal("50"),
            amount=Decimal("2500")
        )
        tracker.add_purchase(
            purchase_date=date(2021, 1, 1),
            units=Decimal("50"),
            nav=Decimal("60"),
            amount=Decimal("3000")
        )

        # Redeem 75 units - spans both lots
        gains = tracker.process_redemption(
            sale_date=date(2022, 6, 1),
            units=Decimal("75"),
            nav=Decimal("70"),
            amount=Decimal("5250")
        )

        assert len(gains) == 2
        assert gains[0].sale_units == Decimal("50")  # Full first lot
        assert gains[1].sale_units == Decimal("25")  # Partial second lot

    def test_stcg_classification(self):
        from pfas.parsers.mf.fifo_tracker import FIFOUnitTracker
        from pfas.parsers.mf.models import AssetClass

        tracker = FIFOUnitTracker(
            scheme_name="Test Fund",
            folio="12345",
            asset_class=AssetClass.EQUITY
        )

        tracker.add_purchase(
            purchase_date=date(2022, 1, 1),
            units=Decimal("100"),
            nav=Decimal("50"),
            amount=Decimal("5000")
        )

        # Redeem within 1 year - STCG
        gains = tracker.process_redemption(
            sale_date=date(2022, 6, 1),
            units=Decimal("100"),
            nav=Decimal("55"),
            amount=Decimal("5500")
        )

        assert gains[0].is_long_term is False
        assert gains[0].gain_type == "STCG"


class TestPortfolioFIFOTracker:
    """Test portfolio-wide FIFO tracking."""

    def test_multiple_schemes(self):
        from pfas.parsers.mf.fifo_tracker import PortfolioFIFOTracker
        from pfas.parsers.mf.models import AssetClass, TransactionType

        portfolio = PortfolioFIFOTracker()

        # Process transactions for two schemes
        portfolio.process_transaction(
            folio="12345",
            scheme_name="Fund A",
            asset_class=AssetClass.EQUITY,
            txn_type=TransactionType.PURCHASE,
            txn_date=date(2020, 1, 1),
            units=Decimal("100"),
            nav=Decimal("50"),
            amount=Decimal("5000")
        )

        portfolio.process_transaction(
            folio="12345",
            scheme_name="Fund B",
            asset_class=AssetClass.DEBT,
            txn_type=TransactionType.PURCHASE,
            txn_date=date(2020, 1, 1),
            units=Decimal("200"),
            nav=Decimal("25"),
            amount=Decimal("5000")
        )

        summary = portfolio.get_summary()
        assert summary["schemes_tracked"] == 2


class TestCASModels:
    """Test CAS-specific data models."""

    def test_cas_file_type_enum(self):
        from pfas.parsers.mf.models import CASFileType

        assert CASFileType.DETAILED.value == "DETAILED"
        assert CASFileType.SUMMARY.value == "SUMMARY"

    def test_cas_source_enum(self):
        from pfas.parsers.mf.models import CASSource

        assert CASSource.CAMS.value == "CAMS"
        assert CASSource.KFINTECH.value == "KFINTECH"
        assert CASSource.NSDL.value == "NSDL"

    def test_statement_period(self):
        from pfas.parsers.mf.models import StatementPeriod

        period = StatementPeriod(
            from_date=date(2024, 1, 1),
            to_date=date(2024, 12, 31)
        )
        assert "01-Jan-2024" in str(period)
        assert "31-Dec-2024" in str(period)

    def test_cas_scheme_balance_mismatch(self):
        from pfas.parsers.mf.models import CASScheme

        scheme = CASScheme(scheme="Test Fund")
        scheme.open = Decimal("100")
        scheme.close = Decimal("150")
        scheme.close_calculated = Decimal("148")

        assert scheme.has_mismatch is True
        assert scheme.balance_mismatch == Decimal("2")

    def test_cas_data_totals(self):
        from pfas.parsers.mf.models import (
            CASData, CASFolio, CASScheme, CASTransaction,
            StatementPeriod, InvestorInfo, SchemeValuation
        )

        cas = CASData(
            statement_period=StatementPeriod(date(2024, 1, 1), date(2024, 12, 31)),
            investor_info=InvestorInfo(name="Test User")
        )

        folio = CASFolio(folio="12345", amc="Test AMC")
        scheme = CASScheme(scheme="Test Fund")
        scheme.transactions = [
            CASTransaction(date=date(2024, 1, 15), description="Purchase")
        ]
        scheme.valuation = SchemeValuation(
            date=date(2024, 12, 31),
            nav=Decimal("100"),
            value=Decimal("10000")
        )
        folio.schemes.append(scheme)
        cas.folios.append(folio)

        assert cas.total_schemes == 1
        assert cas.total_transactions == 1
        assert cas.total_value == Decimal("10000")


class TestCASPDFParserHelpers:
    """Test CAS PDF parser helper methods."""

    def test_is_transaction_line(self):
        from pfas.parsers.mf.cas_pdf_parser import CASPDFParser

        parser = CASPDFParser()

        assert parser._is_transaction_line("02-Mar-2015 Purchase") is True
        assert parser._is_transaction_line("Opening Balance: 0.000") is False
        assert parser._is_transaction_line("Some random text") is False

    def test_is_scheme_line(self):
        from pfas.parsers.mf.cas_pdf_parser import CASPDFParser

        parser = CASPDFParser()

        # Should match scheme lines
        assert parser._is_scheme_line(
            "B92Z-Aditya Birla Sun Life Large Cap Fund -Growth-Direct Plan"
        ) is True
        assert parser._is_scheme_line(
            "ISIN: INF209K01YY7(Advisor: DIRECT)"
        ) is True

        # Should NOT match AMC names in portfolio summary
        assert parser._is_scheme_line("HDFC Mutual Fund") is False
        assert parser._is_scheme_line("HDFC Mutual Fund 10,000.00 15,000.00") is False

        # Should NOT match transaction lines
        assert parser._is_scheme_line("02-Mar-2015 Purchase-SIP") is False

    def test_parse_decimal(self):
        from pfas.parsers.mf.cas_pdf_parser import CASPDFParser

        parser = CASPDFParser()

        assert parser._parse_decimal("10,000.00") == Decimal("10000.00")
        assert parser._parse_decimal("57.428") == Decimal("57.428")
        assert parser._parse_decimal("") == Decimal("0")

    def test_parse_date(self):
        from pfas.parsers.mf.cas_pdf_parser import CASPDFParser

        parser = CASPDFParser()

        assert parser._parse_date("02-Mar-2015") == date(2015, 3, 2)
        assert parser._parse_date("31-Dec-2024") == date(2024, 12, 31)
        assert parser._parse_date("invalid") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
