"""
Unit tests for bank parser models.

Tests BankTransaction, BankAccount, ParseResult dataclasses.
"""

import pytest
from datetime import date
from decimal import Decimal

from pfas.parsers.bank.models import (
    BankTransaction,
    BankAccount,
    ParseResult,
    TransactionCategory,
)


class TestBankTransaction:
    """Tests for BankTransaction dataclass."""

    def test_transaction_creation(self):
        """Test creating a basic bank transaction."""
        txn = BankTransaction(
            date=date(2024, 4, 5),
            description="NEFT-QUALCOMM INDIA PVT LTD",
            debit=Decimal("0"),
            credit=Decimal("630338.38"),
            balance=Decimal("1864906.27"),
        )

        assert txn.date == date(2024, 4, 5)
        assert txn.description == "NEFT-QUALCOMM INDIA PVT LTD"
        assert txn.debit == Decimal("0")
        assert txn.credit == Decimal("630338.38")
        assert txn.balance == Decimal("1864906.27")
        assert txn.value_date is None
        assert txn.reference_number is None

    def test_transaction_with_numeric_amounts(self):
        """Test that numeric amounts are converted to Decimal."""
        txn = BankTransaction(
            date=date(2024, 4, 5),
            description="TEST",
            debit=100.50,
            credit=200.75,
            balance=1000.25,
        )

        assert isinstance(txn.debit, Decimal)
        assert isinstance(txn.credit, Decimal)
        assert isinstance(txn.balance, Decimal)
        assert txn.debit == Decimal("100.50")
        assert txn.credit == Decimal("200.75")
        assert txn.balance == Decimal("1000.25")

    def test_auto_categorize_interest(self):
        """Test auto-categorization of interest transactions."""
        test_cases = [
            "INT PD 01-04-24 TO 30-06-24",
            "INTEREST CREDIT",
            "INT.PD FOR QUARTER",
            "INT PAID",
            "INT.CREDIT 01-APR-24",
            "INT CR",
        ]

        for desc in test_cases:
            txn = BankTransaction(
                date=date(2024, 6, 30),
                description=desc,
                debit=Decimal("0"),
                credit=Decimal("12345.00"),
                balance=Decimal("1000000.00"),
            )
            assert txn.is_interest is True, f"Failed for: {desc}"
            assert txn.category == TransactionCategory.INTEREST

    def test_auto_categorize_salary(self):
        """Test auto-categorization of salary transactions."""
        test_cases = [
            "NEFT-QUALCOMM INDIA PVT LTD",
            "NEFT-QUALCOMM INDIA PRIVATE LIMITED",
            "SAL-QUALCOMM",
            "SALARY CREDIT",
        ]

        for desc in test_cases:
            txn = BankTransaction(
                date=date(2024, 4, 5),
                description=desc,
                debit=Decimal("0"),
                credit=Decimal("630338.38"),
                balance=Decimal("1000000.00"),
            )
            assert txn.category == TransactionCategory.SALARY, f"Failed for: {desc}"

    def test_auto_categorize_upi(self):
        """Test auto-categorization of UPI transactions."""
        test_cases = [
            "UPI-PAYTM-123@ybl",
            "UPI/123456789/SWIGGY",
        ]

        for desc in test_cases:
            txn = BankTransaction(
                date=date(2024, 4, 10),
                description=desc,
                debit=Decimal("5000.00"),
                credit=Decimal("0"),
                balance=Decimal("995000.00"),
            )
            assert txn.category == TransactionCategory.UPI, f"Failed for: {desc}"

    def test_auto_categorize_imps(self):
        """Test auto-categorization of IMPS transactions."""
        txn = BankTransaction(
            date=date(2024, 4, 10),
            description="IMPS-PHONEPE-9876543210",
            debit=Decimal("2000.00"),
            credit=Decimal("0"),
            balance=Decimal("998000.00"),
        )
        assert txn.category == TransactionCategory.IMPS

    def test_auto_categorize_neft(self):
        """Test auto-categorization of NEFT transactions."""
        txn = BankTransaction(
            date=date(2024, 4, 5),
            description="NEFT-SOME COMPANY-N123456",
            debit=Decimal("0"),
            credit=Decimal("100000.00"),
            balance=Decimal("1100000.00"),
        )
        assert txn.category == TransactionCategory.NEFT

    def test_auto_categorize_rtgs(self):
        """Test auto-categorization of RTGS transactions."""
        txn = BankTransaction(
            date=date(2024, 7, 15),
            description="RTGS-ICICI BANK-REF789",
            debit=Decimal("500000.00"),
            credit=Decimal("0"),
            balance=Decimal("1500000.00"),
        )
        assert txn.category == TransactionCategory.RTGS

    def test_auto_categorize_atm(self):
        """Test auto-categorization of ATM transactions."""
        test_cases = [
            "ATM WD REF 789456123",
            "ATM CASH-REF 123",
            "ATM WDL REF NO 789456123",
        ]

        for desc in test_cases:
            txn = BankTransaction(
                date=date(2024, 4, 15),
                description=desc,
                debit=Decimal("25000.00"),
                credit=Decimal("0"),
                balance=Decimal("975000.00"),
            )
            assert txn.category == TransactionCategory.ATM, f"Failed for: {desc}"

    def test_auto_categorize_card(self):
        """Test auto-categorization of card transactions."""
        test_cases = [
            "DEBIT CARD POS-WALMART",
            "CARD PURCHASE-SWIGGY",
        ]

        for desc in test_cases:
            txn = BankTransaction(
                date=date(2024, 4, 20),
                description=desc,
                debit=Decimal("8999.00"),
                credit=Decimal("0"),
                balance=Decimal("991001.00"),
            )
            assert txn.category == TransactionCategory.CARD, f"Failed for: {desc}"

    def test_auto_categorize_other(self):
        """Test default categorization for unknown transactions."""
        txn = BankTransaction(
            date=date(2024, 4, 1),
            description="SOME UNKNOWN TRANSACTION",
            debit=Decimal("100.00"),
            credit=Decimal("0"),
            balance=Decimal("999900.00"),
        )
        assert txn.category == TransactionCategory.OTHER

    def test_properties(self):
        """Test transaction properties."""
        txn = BankTransaction(
            date=date(2024, 4, 5),
            description="TEST",
            debit=Decimal("100"),
            credit=Decimal("500"),
            balance=Decimal("10000"),
        )

        assert txn.amount == Decimal("400")  # credit - debit
        assert txn.is_credit is True
        assert txn.is_debit is True  # Both > 0

        txn2 = BankTransaction(
            date=date(2024, 4, 5),
            description="TEST",
            debit=Decimal("0"),
            credit=Decimal("500"),
            balance=Decimal("10000"),
        )

        assert txn2.amount == Decimal("500")
        assert txn2.is_credit is True
        assert txn2.is_debit is False


class TestBankAccount:
    """Tests for BankAccount dataclass."""

    def test_account_creation(self):
        """Test creating a bank account."""
        account = BankAccount(
            account_number="003101008527",
            bank_name="ICICI Bank",
            ifsc_code="ICIC0000031",
            branch="Mumbai Main Branch",
        )

        assert account.account_number == "003101008527"
        assert account.bank_name == "ICICI Bank"
        assert account.ifsc_code == "ICIC0000031"
        assert account.branch == "Mumbai Main Branch"
        assert account.account_type == "SAVINGS"

    def test_masked_number(self):
        """Test masked account number property."""
        account = BankAccount(
            account_number="003101008527",
            bank_name="ICICI Bank",
        )

        assert account.masked_number == "****8527"

    def test_masked_number_short(self):
        """Test masked number for short account numbers."""
        account = BankAccount(
            account_number="123",
            bank_name="Test Bank",
        )

        assert account.masked_number == "***"

    def test_last4(self):
        """Test last4 property."""
        account = BankAccount(
            account_number="12345678",
            bank_name="Test Bank",
        )

        assert account.last4 == "5678"


class TestParseResult:
    """Tests for ParseResult dataclass."""

    def test_successful_parse_result(self):
        """Test creating a successful parse result."""
        transactions = [
            BankTransaction(
                date=date(2024, 4, 5),
                description="NEFT-SALARY",
                debit=Decimal("0"),
                credit=Decimal("100000.00"),
                balance=Decimal("1100000.00"),
            )
        ]

        account = BankAccount(
            account_number="12345678",
            bank_name="Test Bank",
        )

        result = ParseResult(
            success=True,
            account=account,
            transactions=transactions,
        )

        assert result.success is True
        assert result.account == account
        assert len(result.transactions) == 1
        assert result.errors == []
        assert result.warnings == []

    def test_add_error_and_warning(self):
        """Test adding errors and warnings."""
        result = ParseResult(success=False)

        result.add_error("Error 1")
        result.add_error("Error 2")
        result.add_warning("Warning 1")

        assert len(result.errors) == 2
        assert len(result.warnings) == 1

    def test_properties(self):
        """Test ParseResult properties."""
        transactions = [
            BankTransaction(
                date=date(2024, 4, 5),
                description="DEBIT",
                debit=Decimal("100"),
                credit=Decimal("0"),
                balance=Decimal("900"),
            ),
            BankTransaction(
                date=date(2024, 4, 10),
                description="CREDIT",
                debit=Decimal("0"),
                credit=Decimal("500"),
                balance=Decimal("1400"),
            ),
            BankTransaction(
                date=date(2024, 4, 15),
                description="INT PAID",
                debit=Decimal("0"),
                credit=Decimal("50"),
                balance=Decimal("1450"),
                is_interest=True,
            ),
        ]

        result = ParseResult(success=True, transactions=transactions)

        assert result.transaction_count == 3
        assert result.total_debits == Decimal("100")
        assert result.total_credits == Decimal("550")
        assert result.interest_total == Decimal("50")
