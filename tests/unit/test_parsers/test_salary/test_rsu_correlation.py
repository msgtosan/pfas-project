"""Tests for RSU Tax Credit Correlation."""

import pytest
from datetime import date
from decimal import Decimal

from pfas.parsers.salary.rsu_correlation import RSUTaxCreditCorrelator
from pfas.parsers.salary.models import RSUTaxCredit, CorrelationStatus


class TestRSUTaxCreditCorrelator:
    """Tests for RSUTaxCreditCorrelator class."""

    def test_correlator_initialization(self, db_connection):
        """Test correlator can be initialized."""
        correlator = RSUTaxCreditCorrelator(db_connection)
        assert correlator.conn is not None

    def test_tax_rate_constants(self, db_connection):
        """Test tax rate constants are reasonable."""
        correlator = RSUTaxCreditCorrelator(db_connection)

        # Expected tax rate range: 25-45%
        assert correlator.MIN_TAX_RATE == Decimal("0.25")
        assert correlator.MAX_TAX_RATE == Decimal("0.45")


class TestRSUTaxCreditModel:
    """Tests for RSUTaxCredit model."""

    def test_credit_creation(self):
        """Test RSU tax credit can be created."""
        credit = RSUTaxCredit(
            salary_record_id=1,
            credit_amount=Decimal("1957774.65"),
            credit_date=date(2024, 6, 30),
        )

        assert credit.salary_record_id == 1
        assert credit.credit_amount == Decimal("1957774.65")
        assert credit.credit_date == date(2024, 6, 30)

    def test_credit_default_status(self):
        """Test credit starts with PENDING status."""
        credit = RSUTaxCredit(
            salary_record_id=1,
            credit_amount=Decimal("1000000"),
            credit_date=date(2024, 6, 30),
        )

        assert credit.correlation_status == CorrelationStatus.PENDING

    def test_credit_no_vest_link(self):
        """Test credit vest_id is None by default."""
        credit = RSUTaxCredit(
            salary_record_id=1,
            credit_amount=Decimal("1000000"),
            credit_date=date(2024, 6, 30),
        )

        assert credit.vest_id is None


class TestCorrelationStatus:
    """Tests for CorrelationStatus enum."""

    def test_pending_status(self):
        """Test PENDING status exists."""
        assert CorrelationStatus.PENDING.value == "PENDING"

    def test_matched_status(self):
        """Test MATCHED status exists."""
        assert CorrelationStatus.MATCHED.value == "MATCHED"

    def test_unmatched_status(self):
        """Test UNMATCHED status exists."""
        assert CorrelationStatus.UNMATCHED.value == "UNMATCHED"


class TestCorrelationLogic:
    """Tests for correlation logic."""

    def test_expected_perquisite_range(self, db_connection):
        """Test expected perquisite range calculation from credit amount."""
        correlator = RSUTaxCreditCorrelator(db_connection)

        # If credit is 1M, perquisite should be between 2.2M and 4M
        credit_amount = Decimal("1000000")

        min_perquisite = credit_amount / correlator.MAX_TAX_RATE  # 1M / 0.45 = 2.22M
        max_perquisite = credit_amount / correlator.MIN_TAX_RATE  # 1M / 0.25 = 4M

        assert min_perquisite > credit_amount  # Perquisite > tax
        assert max_perquisite > min_perquisite

    def test_tax_credit_is_percentage_of_perquisite(self, db_connection):
        """Test that tax credit is reasonable percentage of perquisite."""
        # Example: RSU perquisite of 5M, tax credit of ~1.5M (30%)
        perquisite = Decimal("5000000")
        tax_credit = Decimal("1500000")

        tax_rate = tax_credit / perquisite
        assert Decimal("0.25") <= tax_rate <= Decimal("0.45")


class TestGetUnmatchedCredits:
    """Tests for getting unmatched credits."""

    def test_get_unmatched_empty(self, db_connection, sample_user):
        """Test getting unmatched credits when none exist."""
        correlator = RSUTaxCreditCorrelator(db_connection)

        unmatched = correlator.get_unmatched_credits(sample_user["id"])

        assert len(unmatched) == 0


class TestAnnualRSUSummary:
    """Tests for annual RSU credit summary."""

    def test_summary_no_credits(self, db_connection, sample_user):
        """Test summary when no credits exist."""
        correlator = RSUTaxCreditCorrelator(db_connection)

        summary = correlator.get_annual_rsu_summary(sample_user["id"], "2024-25")

        assert summary['financial_year'] == "2024-25"
        assert summary['credit_count'] == 0
        assert summary['total_credits'] == Decimal("0")
        assert summary['matched'] == 0
        assert summary['pending'] == 0
        assert summary['unmatched'] == 0
