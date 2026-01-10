"""
Unit tests for journal module.

Tests double-entry journal engine and balance validation.
"""

import pytest
from decimal import Decimal
from datetime import date

from pfas.core.journal import JournalEngine, JournalEntry, Journal
from pfas.core.accounts import setup_chart_of_accounts, get_account_by_code
from pfas.core.exceptions import UnbalancedJournalError, AccountNotFoundError


@pytest.fixture
def journal_engine(db_with_accounts):
    """Provide a JournalEngine with populated accounts."""
    return JournalEngine(db_with_accounts)


@pytest.fixture
def bank_account(db_with_accounts):
    """Get the Bank - Savings account."""
    return get_account_by_code(db_with_accounts, "1101")


@pytest.fixture
def salary_account(db_with_accounts):
    """Get the Basic Salary account."""
    return get_account_by_code(db_with_accounts, "4101")


class TestJournalBalanceValidation:
    """Tests for journal balance validation."""

    def test_journal_balance_validation(self, journal_engine, bank_account, salary_account):
        """Test rejection of unbalanced journal entries (TC-CORE-003)."""
        # Unbalanced entry should raise error
        unbalanced_entries = [
            JournalEntry(account_id=bank_account.id, debit=Decimal("1000")),
            JournalEntry(account_id=salary_account.id, credit=Decimal("999")),  # Intentionally wrong
        ]

        with pytest.raises(UnbalancedJournalError):
            journal_engine.create_journal(
                txn_date=date.today(),
                description="Test unbalanced",
                entries=unbalanced_entries,
            )

        # Balanced entry should succeed
        balanced_entries = [
            JournalEntry(account_id=bank_account.id, debit=Decimal("1000")),
            JournalEntry(account_id=salary_account.id, credit=Decimal("1000")),
        ]

        journal_id = journal_engine.create_journal(
            txn_date=date.today(),
            description="Test balanced",
            entries=balanced_entries,
        )
        assert journal_id > 0

    def test_balance_with_tolerance(self, journal_engine, bank_account, salary_account):
        """Test balance validation with small tolerance."""
        # Within tolerance (0.01) should pass
        entries = [
            JournalEntry(account_id=bank_account.id, debit=Decimal("1000.005")),
            JournalEntry(account_id=salary_account.id, credit=Decimal("1000")),
        ]

        journal_id = journal_engine.create_journal(
            txn_date=date.today(),
            description="Test tolerance",
            entries=entries,
        )
        assert journal_id > 0

    def test_balance_with_exchange_rate(self, journal_engine, db_with_accounts):
        """Test balance validation with multi-currency."""
        bank = get_account_by_code(db_with_accounts, "1101")
        usd_account = get_account_by_code(db_with_accounts, "1401")  # US Stocks RSU

        # $100 at rate 83.50 = ₹8350
        entries = [
            JournalEntry(
                account_id=usd_account.id,
                debit=Decimal("100"),
                currency="USD",
                exchange_rate=Decimal("83.50"),
            ),
            JournalEntry(
                account_id=bank.id,
                credit=Decimal("8350"),
                currency="INR",
            ),
        ]

        journal_id = journal_engine.create_journal(
            txn_date=date.today(),
            description="USD purchase",
            entries=entries,
        )
        assert journal_id > 0


class TestJournalCreation:
    """Tests for journal creation."""

    def test_create_simple_journal(self, journal_engine, bank_account, salary_account):
        """Test creating a simple journal entry."""
        entries = [
            JournalEntry(account_id=bank_account.id, debit=Decimal("50000")),
            JournalEntry(account_id=salary_account.id, credit=Decimal("50000")),
        ]

        journal_id = journal_engine.create_journal(
            txn_date=date(2024, 6, 15),
            description="Salary for June 2024",
            entries=entries,
            reference_type="SALARY",
        )

        assert journal_id > 0

    def test_create_multi_line_journal(self, journal_engine, db_with_accounts):
        """Test creating a journal with multiple line items."""
        bank = get_account_by_code(db_with_accounts, "1101")
        basic = get_account_by_code(db_with_accounts, "4101")
        hra = get_account_by_code(db_with_accounts, "4102")
        tds = get_account_by_code(db_with_accounts, "1601")

        entries = [
            JournalEntry(account_id=bank.id, debit=Decimal("45000"), narration="Net salary"),
            JournalEntry(account_id=tds.id, debit=Decimal("5000"), narration="TDS deducted"),
            JournalEntry(account_id=basic.id, credit=Decimal("30000"), narration="Basic"),
            JournalEntry(account_id=hra.id, credit=Decimal("20000"), narration="HRA"),
        ]

        journal_id = journal_engine.create_journal(
            txn_date=date.today(),
            description="Salary breakdown",
            entries=entries,
        )

        assert journal_id > 0

    def test_create_journal_validates_accounts(self, journal_engine):
        """Test that non-existent accounts are rejected."""
        entries = [
            JournalEntry(account_id=99999, debit=Decimal("1000")),
            JournalEntry(account_id=99998, credit=Decimal("1000")),
        ]

        with pytest.raises(AccountNotFoundError):
            journal_engine.create_journal(
                txn_date=date.today(),
                description="Invalid accounts",
                entries=entries,
            )


class TestJournalRetrieval:
    """Tests for journal retrieval."""

    def test_get_journal(self, journal_engine, bank_account, salary_account):
        """Test retrieving a journal with entries."""
        entries = [
            JournalEntry(account_id=bank_account.id, debit=Decimal("1000")),
            JournalEntry(account_id=salary_account.id, credit=Decimal("1000")),
        ]

        journal_id = journal_engine.create_journal(
            txn_date=date(2024, 6, 15),
            description="Test journal",
            entries=entries,
        )

        journal = journal_engine.get_journal(journal_id)

        assert journal is not None
        assert isinstance(journal, Journal)
        assert journal.id == journal_id
        assert journal.date == date(2024, 6, 15)
        assert journal.description == "Test journal"
        assert len(journal.entries) == 2

    def test_get_journal_not_found(self, journal_engine):
        """Test retrieving non-existent journal returns None."""
        journal = journal_engine.get_journal(99999)
        assert journal is None


class TestJournalReversal:
    """Tests for journal reversal."""

    def test_reverse_journal(self, journal_engine, bank_account, salary_account):
        """Test reversing a journal entry."""
        # Create original journal
        entries = [
            JournalEntry(account_id=bank_account.id, debit=Decimal("1000")),
            JournalEntry(account_id=salary_account.id, credit=Decimal("1000")),
        ]

        original_id = journal_engine.create_journal(
            txn_date=date(2024, 6, 15),
            description="Original",
            entries=entries,
        )

        # Reverse it
        reversal_id = journal_engine.reverse_journal(original_id)

        assert reversal_id > original_id

        # Check original is marked as reversed
        original = journal_engine.get_journal(original_id)
        assert original.is_reversed is True

        # Check reversal has swapped entries
        reversal = journal_engine.get_journal(reversal_id)
        assert reversal.reference_type == "REVERSAL"

        # The reversal should have credits where original had debits
        for entry in reversal.entries:
            if entry.account_id == bank_account.id:
                assert entry.credit == Decimal("1000")
                assert entry.debit == Decimal("0")


class TestAccountBalance:
    """Tests for account balance calculation."""

    def test_get_account_balance(self, journal_engine, bank_account, salary_account):
        """Test calculating account balance."""
        # Create a journal entry
        entries = [
            JournalEntry(account_id=bank_account.id, debit=Decimal("5000")),
            JournalEntry(account_id=salary_account.id, credit=Decimal("5000")),
        ]

        journal_engine.create_journal(
            txn_date=date.today(),
            description="Salary",
            entries=entries,
        )

        # Bank (asset) balance should be positive (debit)
        bank_balance = journal_engine.get_account_balance(bank_account.id)
        assert bank_balance == Decimal("5000")

        # Salary (income) balance should be positive (credit)
        salary_balance = journal_engine.get_account_balance(salary_account.id)
        assert salary_balance == Decimal("5000")

    def test_balance_excludes_reversed(self, journal_engine, bank_account, salary_account):
        """Test that reversed journals are excluded from balance."""
        # Create and reverse a journal
        entries = [
            JournalEntry(account_id=bank_account.id, debit=Decimal("1000")),
            JournalEntry(account_id=salary_account.id, credit=Decimal("1000")),
        ]

        journal_id = journal_engine.create_journal(
            txn_date=date.today(),
            description="To be reversed",
            entries=entries,
        )

        journal_engine.reverse_journal(journal_id)

        # Net balance should be zero
        balance = journal_engine.get_account_balance(bank_account.id)
        assert balance == Decimal("0")


class TestAccountLedger:
    """Tests for account ledger."""

    def test_get_account_ledger(self, journal_engine, bank_account, salary_account):
        """Test getting account ledger with running balance."""
        # Create multiple journals
        for i in range(3):
            entries = [
                JournalEntry(account_id=bank_account.id, debit=Decimal("1000")),
                JournalEntry(account_id=salary_account.id, credit=Decimal("1000")),
            ]
            journal_engine.create_journal(
                txn_date=date(2024, 6, i + 1),
                description=f"Entry {i + 1}",
                entries=entries,
            )

        ledger = journal_engine.get_account_ledger(bank_account.id)

        assert len(ledger) == 3

        # Check running balance
        assert ledger[0]["balance"] == Decimal("1000")
        assert ledger[1]["balance"] == Decimal("2000")
        assert ledger[2]["balance"] == Decimal("3000")


class TestJournalEntryDataclass:
    """Tests for JournalEntry dataclass."""

    def test_default_values(self):
        """Test JournalEntry default values."""
        entry = JournalEntry(account_id=1)

        assert entry.debit == Decimal("0")
        assert entry.credit == Decimal("0")
        assert entry.currency == "INR"
        assert entry.exchange_rate == Decimal("1")
        assert entry.narration == ""

    def test_numeric_conversion(self):
        """Test that numeric types are converted to Decimal."""
        entry = JournalEntry(
            account_id=1,
            debit=1000,
            credit=0,
            exchange_rate=83.5,
        )

        assert isinstance(entry.debit, Decimal)
        assert isinstance(entry.credit, Decimal)
        assert isinstance(entry.exchange_rate, Decimal)
