"""Tests for EPF passbook parser."""

import pytest
from datetime import date
from decimal import Decimal
from pathlib import Path

from pfas.parsers.epf.epf import EPFParser, EPFAccount, EPFTransaction, EPFInterest


class TestEPFParser:
    """Tests for EPF parser."""

    def test_parser_initialization(self, db_connection):
        """Test parser can be initialized."""
        parser = EPFParser(db_connection)
        assert parser.conn is not None

    def test_parse_nonexistent_file(self, db_connection):
        """Test parsing nonexistent file returns error."""
        parser = EPFParser(db_connection)
        result = parser.parse(Path("/nonexistent/file.pdf"))

        assert result.success is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0].lower()

    def test_to_decimal_valid(self, db_connection):
        """Test Decimal conversion with commas."""
        parser = EPFParser(db_connection)

        assert parser._to_decimal("1,23,456.78") == Decimal("123456.78")
        assert parser._to_decimal("50000") == Decimal("50000")

    def test_to_decimal_invalid(self, db_connection):
        """Test Decimal conversion for invalid values."""
        parser = EPFParser(db_connection)

        assert parser._to_decimal("") == Decimal("0")
        assert parser._to_decimal(None) == Decimal("0")

    def test_calculate_80c_eligible(self, db_connection):
        """Test 80C eligible amount calculation."""
        parser = EPFParser(db_connection)

        transactions = [
            EPFTransaction(
                wage_month="Apr-2024",
                transaction_date=date(2024, 4, 10),
                transaction_type="CR",
                employee_contribution=Decimal("12000"),
                vpf_contribution=Decimal("5000")
            ),
            EPFTransaction(
                wage_month="May-2024",
                transaction_date=date(2024, 5, 10),
                transaction_type="CR",
                employee_contribution=Decimal("12000"),
                vpf_contribution=Decimal("0")
            ),
        ]

        eligible = parser.calculate_80c_eligible(transactions)

        # Total = 12000 + 5000 + 12000 = 29000
        assert eligible == Decimal("29000")

    def test_get_or_create_account_new(self, db_connection, sample_user):
        """Test creating a new EPF account."""
        parser = EPFParser(db_connection)

        account = EPFAccount(
            uan="100123456789",
            establishment_id="TEST001",
            establishment_name="Test Company",
            member_id="TEST001/001",
            member_name="Test User"
        )

        account_id = parser._get_or_create_account(account, user_id=sample_user["id"])
        assert account_id > 0

        # Verify it was created
        cursor = db_connection.execute(
            "SELECT uan FROM epf_accounts WHERE id = ?", (account_id,)
        )
        row = cursor.fetchone()
        assert row["uan"] == "100123456789"

    def test_get_or_create_account_existing(self, db_connection, sample_user):
        """Test getting existing EPF account."""
        parser = EPFParser(db_connection)

        account = EPFAccount(
            uan="100123456789",
            establishment_id="TEST001",
            establishment_name="Test Company",
            member_id="TEST001/001",
            member_name="Test User"
        )

        # Create first time
        account_id1 = parser._get_or_create_account(account, user_id=sample_user["id"])

        # Get same account again
        account_id2 = parser._get_or_create_account(account, user_id=sample_user["id"])

        # Should return same ID
        assert account_id1 == account_id2


class TestEPFModels:
    """Tests for EPF data models."""

    def test_epf_account_creation(self):
        """Test creating EPF account."""
        account = EPFAccount(
            uan="100123456789",
            establishment_id="TEST001",
            establishment_name="Test Company",
            member_id="TEST001/001",
            member_name="Test User"
        )

        assert account.uan == "100123456789"
        assert account.establishment_id == "TEST001"

    def test_epf_transaction_creation(self):
        """Test creating EPF transaction."""
        txn = EPFTransaction(
            wage_month="Apr-2024",
            transaction_date=date(2024, 4, 10),
            transaction_type="CR",
            employee_contribution=Decimal("12000"),
            employer_contribution=Decimal("6600"),
            pension_contribution=Decimal("1250")
        )

        assert txn.wage_month == "Apr-2024"
        assert txn.employee_contribution == Decimal("12000")

    def test_epf_interest_creation(self):
        """Test creating EPF interest record."""
        interest = EPFInterest(
            financial_year="2023-24",
            employee_interest=Decimal("50000"),
            employer_interest=Decimal("25000"),
            tds_deducted=Decimal("5000")
        )

        assert interest.financial_year == "2023-24"
        assert interest.employee_interest == Decimal("50000")
