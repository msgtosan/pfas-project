"""
Unit tests for bank parser utilities.

Tests consolidation, validation, and balance verification.
"""

import pytest
from decimal import Decimal
from datetime import date

from pfas.parsers.bank.utils import (
    consolidate_transactions,
    validate_transactions,
    calculate_balance_verification,
)
from pfas.parsers.bank.models import BankTransaction, ParseResult


class TestConsolidateTransactions:
    """Tests for consolidate_transactions function."""

    def test_consolidate_single_result(self):
        """Test consolidating a single parse result."""
        transactions = [
            BankTransaction(
                date=date(2024, 4, 5),
                description="SALARY",
                debit=Decimal("0"),
                credit=Decimal("100000.00"),
                balance=Decimal("1100000.00"),
            ),
            BankTransaction(
                date=date(2024, 4, 10),
                description="UPI-AMAZON",
                debit=Decimal("5000.00"),
                credit=Decimal("0"),
                balance=Decimal("1095000.00"),
            ),
        ]

        result = ParseResult(success=True, transactions=transactions)
        consolidated = consolidate_transactions([result])

        assert len(consolidated) == 2
        assert consolidated[0].date == date(2024, 4, 5)
        assert consolidated[1].date == date(2024, 4, 10)

    def test_consolidate_multiple_results(self):
        """Test consolidating multiple parse results."""
        result1 = ParseResult(
            success=True,
            transactions=[
                BankTransaction(
                    date=date(2024, 4, 5),
                    description="TXN1",
                    debit=Decimal("0"),
                    credit=Decimal("1000"),
                    balance=Decimal("11000"),
                ),
            ],
        )

        result2 = ParseResult(
            success=True,
            transactions=[
                BankTransaction(
                    date=date(2024, 4, 10),
                    description="TXN2",
                    debit=Decimal("500"),
                    credit=Decimal("0"),
                    balance=Decimal("10500"),
                ),
            ],
        )

        consolidated = consolidate_transactions([result1, result2])

        assert len(consolidated) == 2
        # Should be sorted by date
        assert consolidated[0].date == date(2024, 4, 5)
        assert consolidated[1].date == date(2024, 4, 10)

    def test_consolidate_removes_duplicates(self):
        """Test that duplicate transactions are removed."""
        txn = BankTransaction(
            date=date(2024, 4, 5),
            description="SALARY",
            debit=Decimal("0"),
            credit=Decimal("100000"),
            balance=Decimal("1100000"),
        )

        result1 = ParseResult(success=True, transactions=[txn])
        result2 = ParseResult(success=True, transactions=[txn])

        consolidated = consolidate_transactions([result1, result2])

        # Should only have one transaction (duplicate removed)
        assert len(consolidated) == 1

    def test_consolidate_sorts_by_date(self):
        """Test that transactions are sorted by date."""
        result = ParseResult(
            success=True,
            transactions=[
                BankTransaction(
                    date=date(2024, 4, 15),
                    description="TXN3",
                    debit=Decimal("100"),
                    credit=Decimal("0"),
                    balance=Decimal("1000"),
                ),
                BankTransaction(
                    date=date(2024, 4, 5),
                    description="TXN1",
                    debit=Decimal("0"),
                    credit=Decimal("500"),
                    balance=Decimal("1500"),
                ),
                BankTransaction(
                    date=date(2024, 4, 10),
                    description="TXN2",
                    debit=Decimal("200"),
                    credit=Decimal("0"),
                    balance=Decimal("1300"),
                ),
            ],
        )

        consolidated = consolidate_transactions([result])

        # Should be sorted oldest first
        assert consolidated[0].date == date(2024, 4, 5)
        assert consolidated[1].date == date(2024, 4, 10)
        assert consolidated[2].date == date(2024, 4, 15)

    def test_consolidate_ignores_failed_results(self):
        """Test that failed parse results are ignored."""
        result1 = ParseResult(success=False, errors=["Parse failed"])
        result2 = ParseResult(
            success=True,
            transactions=[
                BankTransaction(
                    date=date(2024, 4, 5),
                    description="TXN1",
                    debit=Decimal("0"),
                    credit=Decimal("1000"),
                    balance=Decimal("11000"),
                ),
            ],
        )

        consolidated = consolidate_transactions([result1, result2])

        assert len(consolidated) == 1

    def test_consolidate_empty_results(self):
        """Test consolidating empty results."""
        consolidated = consolidate_transactions([])
        assert len(consolidated) == 0

    def test_consolidate_with_none_transactions(self):
        """Test consolidating results with None transactions."""
        result = ParseResult(success=True, transactions=None)
        consolidated = consolidate_transactions([result])

        assert len(consolidated) == 0


class TestValidateTransactions:
    """Tests for validate_transactions function."""

    def test_validate_valid_transactions(self):
        """Test validating correct transactions."""
        transactions = [
            BankTransaction(
                date=date(2024, 4, 5),
                description="SALARY",
                debit=Decimal("0"),
                credit=Decimal("100000"),
                balance=Decimal("1100000"),
            ),
        ]

        warnings = validate_transactions(transactions)
        assert len(warnings) == 0

    def test_validate_missing_date(self):
        """Test warning for missing date."""
        transactions = [
            BankTransaction(
                date=None,
                description="TXN1",
                debit=Decimal("0"),
                credit=Decimal("1000"),
                balance=Decimal("11000"),
            ),
        ]

        warnings = validate_transactions(transactions)
        assert len(warnings) == 1
        assert "Missing date" in warnings[0]

    def test_validate_empty_description(self):
        """Test warning for empty description."""
        transactions = [
            BankTransaction(
                date=date(2024, 4, 5),
                description="",
                debit=Decimal("0"),
                credit=Decimal("1000"),
                balance=Decimal("11000"),
            ),
            BankTransaction(
                date=date(2024, 4, 6),
                description="   ",
                debit=Decimal("100"),
                credit=Decimal("0"),
                balance=Decimal("10900"),
            ),
        ]

        warnings = validate_transactions(transactions)
        assert len(warnings) == 2
        assert all("Empty description" in w for w in warnings)

    def test_validate_both_debit_and_credit(self):
        """Test warning for transaction with both debit and credit."""
        transactions = [
            BankTransaction(
                date=date(2024, 4, 5),
                description="INVALID TXN",
                debit=Decimal("1000"),
                credit=Decimal("500"),
                balance=Decimal("11000"),
            ),
        ]

        warnings = validate_transactions(transactions)
        assert len(warnings) == 1
        assert "both debit and credit" in warnings[0].lower()

    def test_validate_zero_amount(self):
        """Test warning for zero amount transaction."""
        transactions = [
            BankTransaction(
                date=date(2024, 4, 5),
                description="ZERO TXN",
                debit=Decimal("0"),
                credit=Decimal("0"),
                balance=Decimal("11000"),
            ),
        ]

        warnings = validate_transactions(transactions)
        assert len(warnings) == 1
        assert "Zero amount" in warnings[0]

    def test_validate_multiple_issues(self):
        """Test validating transactions with multiple issues."""
        transactions = [
            BankTransaction(
                date=None,
                description="",
                debit=Decimal("0"),
                credit=Decimal("0"),
                balance=Decimal("11000"),
            ),
        ]

        warnings = validate_transactions(transactions)
        # Should have warnings for: missing date, empty description, zero amount
        assert len(warnings) == 3


class TestCalculateBalanceVerification:
    """Tests for calculate_balance_verification function."""

    def test_verify_correct_balances(self):
        """Test verification of correct balance progression."""
        transactions = [
            BankTransaction(
                date=date(2024, 4, 1),
                description="Opening",
                debit=Decimal("0"),
                credit=Decimal("0"),
                balance=Decimal("100000"),
            ),
            BankTransaction(
                date=date(2024, 4, 5),
                description="Credit",
                debit=Decimal("0"),
                credit=Decimal("50000"),
                balance=Decimal("150000"),
            ),
            BankTransaction(
                date=date(2024, 4, 10),
                description="Debit",
                debit=Decimal("20000"),
                credit=Decimal("0"),
                balance=Decimal("130000"),
            ),
        ]

        result = calculate_balance_verification(transactions)

        assert result["verified"] is True
        assert len(result["errors"]) == 0
        assert result["final_balance"] == Decimal("130000")

    def test_verify_balance_mismatch(self):
        """Test detection of balance mismatch."""
        transactions = [
            BankTransaction(
                date=date(2024, 4, 1),
                description="Opening",
                debit=Decimal("0"),
                credit=Decimal("0"),
                balance=Decimal("100000"),
            ),
            BankTransaction(
                date=date(2024, 4, 5),
                description="Credit",
                debit=Decimal("0"),
                credit=Decimal("50000"),
                balance=Decimal("140000"),  # Wrong! Should be 150000
            ),
        ]

        result = calculate_balance_verification(transactions)

        assert result["verified"] is False
        assert len(result["errors"]) == 1
        assert "Balance mismatch" in result["errors"][0]

    def test_verify_empty_transactions(self):
        """Test verification with empty transaction list."""
        result = calculate_balance_verification([])

        assert result["verified"] is True
        assert len(result["errors"]) == 0
        assert result["final_balance"] is None

    def test_verify_transactions_without_balance(self):
        """Test transactions without balance field."""
        transactions = [
            BankTransaction(
                date=date(2024, 4, 5),
                description="Credit",
                debit=Decimal("0"),
                credit=Decimal("50000"),
                balance=None,
            ),
        ]

        result = calculate_balance_verification(transactions)

        # Should verify successfully (no balance to check)
        assert result["verified"] is True
        assert len(result["errors"]) == 0

    def test_verify_allows_small_rounding_errors(self):
        """Test that small rounding differences are allowed."""
        transactions = [
            BankTransaction(
                date=date(2024, 4, 1),
                description="Opening",
                debit=Decimal("0"),
                credit=Decimal("0"),
                balance=Decimal("100000.00"),
            ),
            BankTransaction(
                date=date(2024, 4, 5),
                description="Credit",
                debit=Decimal("0"),
                credit=Decimal("50000.33"),
                balance=Decimal("150000.33"),  # Exact
            ),
        ]

        result = calculate_balance_verification(transactions)
        assert result["verified"] is True

        # Now with tiny rounding error
        transactions[1].balance = Decimal("150000.34")  # Off by 0.01

        result = calculate_balance_verification(transactions)
        assert result["verified"] is True  # Should still pass

    def test_verify_rejects_large_errors(self):
        """Test that large balance errors are detected."""
        transactions = [
            BankTransaction(
                date=date(2024, 4, 1),
                description="Opening",
                debit=Decimal("0"),
                credit=Decimal("0"),
                balance=Decimal("100000"),
            ),
            BankTransaction(
                date=date(2024, 4, 5),
                description="Credit",
                debit=Decimal("0"),
                credit=Decimal("50000"),
                balance=Decimal("150010"),  # Off by 10
            ),
        ]

        result = calculate_balance_verification(transactions)

        assert result["verified"] is False
        assert len(result["errors"]) == 1
