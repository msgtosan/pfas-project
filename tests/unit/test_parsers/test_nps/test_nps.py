"""Tests for NPS statement parser."""

import pytest
from datetime import date
from decimal import Decimal
from pathlib import Path

from pfas.parsers.nps.nps import NPSParser, NPSAccount, NPSTransaction


class TestNPSParser:
    """Tests for NPS parser."""

    def test_parser_initialization(self, db_connection):
        """Test parser can be initialized."""
        parser = NPSParser(db_connection)
        assert parser.conn is not None

    def test_parse_nonexistent_file(self, db_connection):
        """Test parsing nonexistent file returns error."""
        parser = NPSParser(db_connection)
        result = parser.parse(Path("/nonexistent/file.csv"))

        assert result.success is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0].lower()

    def test_get_financial_year(self, db_connection):
        """Test financial year calculation."""
        parser = NPSParser(db_connection)

        # Date in Apr-Mar (FY starts in Apr)
        fy1 = parser._get_financial_year(date(2024, 5, 15))
        assert fy1 == "2024-25"

        # Date in Jan-Mar (previous FY)
        fy2 = parser._get_financial_year(date(2024, 2, 15))
        assert fy2 == "2023-24"

    def test_to_decimal_valid(self, db_connection):
        """Test Decimal conversion."""
        parser = NPSParser(db_connection)

        assert parser._to_decimal(25000) == Decimal("25000")
        assert parser._to_decimal("30000.50") == Decimal("30000.50")
        assert parser._to_decimal("1,25,000") == Decimal("125000")

    def test_to_decimal_invalid(self, db_connection):
        """Test Decimal conversion for invalid values."""
        parser = NPSParser(db_connection)

        assert parser._to_decimal(None) == Decimal("0")
        assert parser._to_decimal("") == Decimal("0")

    def test_calculate_deductions_below_limits(self, db_connection):
        """Test NPS deductions calculation below limits."""
        parser = NPSParser(db_connection)

        transactions = [
            NPSTransaction(
                pran="110012345678",
                date=date(2024, 4, 15),
                transaction_type="CONTRIBUTION",
                tier="I",
                contribution_type="EMPLOYEE",
                amount=Decimal("30000"),
                financial_year="2024-25"
            ),
            NPSTransaction(
                pran="110012345678",
                date=date(2024, 4, 15),
                transaction_type="CONTRIBUTION",
                tier="I",
                contribution_type="EMPLOYER",
                amount=Decimal("50000"),
                financial_year="2024-25"
            ),
        ]

        deductions = parser.calculate_deductions(
            transactions,
            basic_salary=Decimal("600000"),
            fy="2024-25"
        )

        # 80CCD(1) = Employee contribution
        assert deductions['80CCD_1'] == Decimal("30000")

        # 80CCD(1B) = min(EE contribution, 50000)
        assert deductions['80CCD_1B'] == Decimal("30000")

        # 80CCD(2) = min(ER contribution, 10% of Basic)
        # 10% of 600000 = 60000
        assert deductions['80CCD_2'] == Decimal("50000")
        assert deductions['80CCD_2_limit'] == Decimal("60000")

    def test_calculate_deductions_above_limits(self, db_connection):
        """Test NPS deductions at limits."""
        parser = NPSParser(db_connection)

        transactions = [
            NPSTransaction(
                pran="110012345678",
                date=date(2024, 4, 15),
                transaction_type="CONTRIBUTION",
                tier="I",
                contribution_type="EMPLOYEE",
                amount=Decimal("80000"),
                financial_year="2024-25"
            ),
            NPSTransaction(
                pran="110012345678",
                date=date(2024, 4, 15),
                transaction_type="CONTRIBUTION",
                tier="I",
                contribution_type="EMPLOYER",
                amount=Decimal("100000"),
                financial_year="2024-25"
            ),
        ]

        deductions = parser.calculate_deductions(
            transactions,
            basic_salary=Decimal("800000"),
            fy="2024-25"
        )

        # 80CCD(1B) capped at ₹50,000
        assert deductions['80CCD_1B'] == Decimal("50000")

        # 80CCD(2) capped at 10% of Basic (80000)
        assert deductions['80CCD_2'] == Decimal("80000")

    def test_get_or_create_account(self, db_connection):
        """Test creating NPS account."""
        parser = NPSParser(db_connection)

        account = NPSAccount(
            pran="110012345678",
            nodal_office="NSDL",
            scheme_preference="Aggressive"
        )

        account_id = parser._get_or_create_account(account, user_id=1)
        assert account_id > 0

        # Verify it was created
        cursor = db_connection.execute(
            "SELECT pran FROM nps_accounts WHERE id = ?", (account_id,)
        )
        row = cursor.fetchone()
        assert row["pran"] == "110012345678"


class TestNPSModels:
    """Tests for NPS data models."""

    def test_nps_account_creation(self):
        """Test creating NPS account."""
        account = NPSAccount(
            pran="110012345678",
            nodal_office="NSDL"
        )

        assert account.pran == "110012345678"
        assert account.nodal_office == "NSDL"

    def test_nps_transaction_creation(self):
        """Test creating NPS transaction."""
        txn = NPSTransaction(
            pran="110012345678",
            date=date(2024, 4, 15),
            transaction_type="CONTRIBUTION",
            tier="I",
            contribution_type="EMPLOYEE",
            amount=Decimal("25000"),
            units=Decimal("1100.50"),
            nav=Decimal("22.70")
        )

        assert txn.pran == "110012345678"
        assert txn.tier == "I"
        assert txn.contribution_type == "EMPLOYEE"
        assert txn.amount == Decimal("25000")
