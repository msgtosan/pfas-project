"""
Utility functions for bank parsers.

Provides consolidation and helper functions.
"""

from typing import List
from pfas.parsers.bank.models import BankTransaction, ParseResult


def consolidate_transactions(results: List[ParseResult]) -> List[BankTransaction]:
    """
    Consolidate transactions from multiple parse results.

    - Merges transactions from all results
    - Sorts by date (oldest first)
    - Removes duplicates based on (date, description, debit, credit)

    Args:
        results: List of ParseResult objects

    Returns:
        Sorted list of unique transactions
    """
    all_transactions = []

    # Collect all transactions
    for result in results:
        if result.success and result.transactions:
            all_transactions.extend(result.transactions)

    # Remove duplicates
    seen = set()
    unique_transactions = []

    for txn in all_transactions:
        # Create a unique key
        key = (
            txn.date,
            txn.description,
            txn.debit,
            txn.credit
        )

        if key not in seen:
            seen.add(key)
            unique_transactions.append(txn)

    # Sort by date
    unique_transactions.sort(key=lambda t: t.date)

    return unique_transactions


def validate_transactions(transactions: List[BankTransaction]) -> List[str]:
    """
    Validate transactions for common issues.

    Args:
        transactions: List of transactions to validate

    Returns:
        List of warning messages
    """
    warnings = []

    for i, txn in enumerate(transactions):
        # Check for missing dates
        if not txn.date:
            warnings.append(f"Transaction {i+1}: Missing date")

        # Check for empty descriptions
        if not txn.description or txn.description.strip() == "":
            warnings.append(f"Transaction {i+1}: Empty description")

        # Check for both debit and credit
        if txn.debit > 0 and txn.credit > 0:
            warnings.append(
                f"Transaction {i+1}: Has both debit and credit ({txn.date})"
            )

        # Check for zero amount
        if txn.debit == 0 and txn.credit == 0:
            warnings.append(
                f"Transaction {i+1}: Zero amount transaction ({txn.date})"
            )

    return warnings


def calculate_balance_verification(transactions: List[BankTransaction]) -> dict:
    """
    Verify balance progression in transactions.

    Args:
        transactions: List of transactions (should be sorted by date)

    Returns:
        Dictionary with verification results
    """
    if not transactions:
        return {
            "verified": True,
            "errors": [],
            "final_balance": None
        }

    errors = []
    running_balance = None

    for i, txn in enumerate(transactions):
        if txn.balance is None:
            continue

        if running_balance is None:
            running_balance = txn.balance
            continue

        # Calculate expected balance
        expected_balance = running_balance + txn.credit - txn.debit

        # Allow small rounding differences
        diff = abs(expected_balance - txn.balance)
        if diff > 0.01:  # More than 1 paisa difference
            errors.append(
                f"Balance mismatch at transaction {i+1} ({txn.date}): "
                f"Expected {expected_balance}, Got {txn.balance}"
            )

        running_balance = txn.balance

    return {
        "verified": len(errors) == 0,
        "errors": errors,
        "final_balance": running_balance
    }
