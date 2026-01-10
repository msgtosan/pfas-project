"""Tests for PPF statement parser."""

import pytest
from datetime import date
from decimal import Decimal
from pathlib import Path

from pfas.parsers.ppf.ppf import PPFParser, PPFAccount, PPFTransaction


class TestPPFParser:
    """Tests for PPF parser."""

    def test_parser_initialization(self, db_connection):
        """Test parser can be initialized."""
        parser = PPFParser(db_connection)
        assert parser.conn is not None

    def test_parse_nonexistent_file(self, db_connection):
        """Test parsing nonexistent file returns error."""
        parser = PPFParser(db_connection)
        result = parser.parse(Path("/nonexistent/file.xlsx"))

        assert result.success is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0].lower()

    def test_get_financial_year(self, db_connection):
        """Test financial year calculation."""
        parser = PPFParser(db_connection)

        # Date in Apr-Mar (FY starts in Apr)
        fy1 = parser._get_financial_year(date(2024, 5, 15))
        assert fy1 == "2024-25"

        # Date in Jan-Mar (previous FY)
        fy2 = parser._get_financial_year(date(2024, 2, 15))
        assert fy2 == "2023-24"

    def test_to_decimal_valid(self, db_connection):
        """Test Decimal conversion."""
        parser = PPFParser(db_connection)

        assert parser._to_decimal(50000) == Decimal("50000")
        assert parser._to_decimal("75000.50") == Decimal("75000.50")
        assert parser._to_decimal("1,00,000") == Decimal("100000")

    def test_to_decimal_invalid(self, db_connection):
        """Test Decimal conversion for invalid values."""
        parser = PPFParser(db_connection)

        assert parser._to_decimal(None) == Decimal("0")
        assert parser._to_decimal("") == Decimal("0")

    def test_calculate_80c_eligible_below_limit(self, db_connection):
        """Test 80C calculation below ₹1.5L."""
        parser = PPFParser(db_connection)

        transactions = [
            PPFTransaction(
                date=date(2024, 4, 10),
                transaction_type="DEPOSIT",
                amount=Decimal("50000"),
                balance=Decimal("50000"),
                financial_year="2024-25"
            ),
            PPFTransaction(
                date=date(2024, 7, 10),
                transaction_type="DEPOSIT",
                amount=Decimal("40000"),
                balance=Decimal("90000"),
                financial_year="2024-25"
            ),
        ]

        eligible = parser.calculate_80c_eligible(transactions, "2024-25")

        # Total deposits = 90000 (below limit)
        assert eligible == Decimal("90000")

    def test_calculate_80c_eligible_above_limit(self, db_connection):
        """Test 80C calculation above ₹1.5L limit."""
        parser = PPFParser(db_connection)

        transactions = [
            PPFTransaction(
                date=date(2024, 4, 10),
                transaction_type="DEPOSIT",
                amount=Decimal("100000"),
                balance=Decimal("100000"),
                financial_year="2024-25"
            ),
            PPFTransaction(
                date=date(2024, 7, 10),
                transaction_type="DEPOSIT",
                amount=Decimal("80000"),
                balance=Decimal("180000"),
                financial_year="2024-25"
            ),
        ]

        eligible = parser.calculate_80c_eligible(transactions, "2024-25")

        # Total deposits = 180000, but capped at 150000
        assert eligible == Decimal("150000")

    def test_get_or_create_account(self, db_connection):
        """Test creating PPF account."""
        parser = PPFParser(db_connection)

        account = PPFAccount(
            account_number="PPF123456",
            bank_name="SBI",
            branch="Test Branch",
            opening_date=date(2020, 4, 1)
        )
        account.calculate_maturity_date()

        account_id = parser._get_or_create_account(account, user_id=1)
        assert account_id > 0

        # Verify maturity date (15 years from opening)
        assert account.maturity_date == date(2035, 4, 1)


class TestPPFModels:
    """Tests for PPF data models."""

    def test_ppf_account_creation(self):
        """Test creating PPF account."""
        account = PPFAccount(
            account_number="PPF123456",
            bank_name="SBI",
            branch="Test Branch",
            opening_date=date(2020, 4, 1)
        )

        assert account.account_number == "PPF123456"
        assert account.bank_name == "SBI"

    def test_ppf_account_maturity_calculation(self):
        """Test PPF maturity date calculation."""
        account = PPFAccount(
            account_number="PPF123456",
            bank_name="SBI",
            opening_date=date(2020, 4, 1)
        )

        account.calculate_maturity_date()

        # Maturity after 15 years
        assert account.maturity_date == date(2035, 4, 1)

    def test_ppf_transaction_creation(self):
        """Test creating PPF transaction."""
        txn = PPFTransaction(
            date=date(2024, 4, 10),
            transaction_type="DEPOSIT",
            amount=Decimal("50000"),
            balance=Decimal("250000"),
            financial_year="2024-25"
        )

        assert txn.transaction_type == "DEPOSIT"
        assert txn.amount == Decimal("50000")
