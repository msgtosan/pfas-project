"""
Unit tests for accounts module.

Tests Chart of Accounts setup and management.
"""

import pytest
from pfas.core.accounts import (
    setup_chart_of_accounts,
    get_account_by_code,
    get_account_by_id,
    get_accounts_by_type,
    get_child_accounts,
    get_account_hierarchy,
    CHART_OF_ACCOUNTS,
    Account,
)
from pfas.core.exceptions import AccountNotFoundError


class TestChartOfAccountsSetup:
    """Tests for chart of accounts setup."""

    def test_chart_of_accounts_setup(self, db_connection):
        """Test 18 asset class account creation (TC-CORE-002)."""
        count = setup_chart_of_accounts(db_connection)

        # Should have created all accounts
        assert count == len(CHART_OF_ACCOUNTS)

        # Verify key accounts exist
        assert get_account_by_code(db_connection, "1101") is not None  # Bank Savings
        assert get_account_by_code(db_connection, "1201") is not None  # MF Equity
        assert get_account_by_code(db_connection, "1401") is not None  # US Stocks RSU
        assert get_account_by_code(db_connection, "4101") is not None  # Basic Salary

    def test_hierarchy_setup(self, db_with_accounts):
        """Test account hierarchy is properly established."""
        # MF Equity should have Investments as parent
        mf_equity = get_account_by_code(db_with_accounts, "1201")
        assert mf_equity is not None
        assert mf_equity.parent_id is not None

        # Get parent
        parent = get_account_by_id(db_with_accounts, mf_equity.parent_id)
        assert parent is not None
        assert parent.code == "1200"  # Investments

    def test_idempotent_setup(self, db_connection):
        """Test that setup can be run multiple times without errors."""
        count1 = setup_chart_of_accounts(db_connection)
        count2 = setup_chart_of_accounts(db_connection)

        # Should have same count both times (INSERT OR IGNORE)
        assert count1 == count2

    def test_root_accounts_have_no_parent(self, db_with_accounts):
        """Test that root accounts have no parent."""
        root_codes = ["1000", "2000", "3000", "4000", "5000"]

        for code in root_codes:
            account = get_account_by_code(db_with_accounts, code)
            assert account is not None
            assert account.parent_id is None

    def test_account_types(self, db_with_accounts):
        """Test that accounts have correct types."""
        test_cases = [
            ("1000", "ASSET"),
            ("2000", "LIABILITY"),
            ("3000", "EQUITY"),
            ("4000", "INCOME"),
            ("5000", "EXPENSE"),
        ]

        for code, expected_type in test_cases:
            account = get_account_by_code(db_with_accounts, code)
            assert account.account_type == expected_type

    def test_foreign_asset_currency(self, db_with_accounts):
        """Test that foreign asset accounts have USD currency."""
        usd_codes = ["1400", "1401", "1402", "1403", "1404"]

        for code in usd_codes:
            account = get_account_by_code(db_with_accounts, code)
            assert account.currency == "USD"


class TestAccountLookup:
    """Tests for account lookup functions."""

    def test_get_account_by_code(self, db_with_accounts):
        """Test getting account by code."""
        account = get_account_by_code(db_with_accounts, "1101")

        assert account is not None
        assert isinstance(account, Account)
        assert account.code == "1101"
        assert account.name == "Bank - Savings"
        assert account.account_type == "ASSET"

    def test_get_account_by_code_not_found(self, db_with_accounts):
        """Test getting non-existent account returns None."""
        account = get_account_by_code(db_with_accounts, "9999")
        assert account is None

    def test_get_account_by_id(self, db_with_accounts):
        """Test getting account by ID."""
        # First get an account by code to know its ID
        by_code = get_account_by_code(db_with_accounts, "1101")
        by_id = get_account_by_id(db_with_accounts, by_code.id)

        assert by_id is not None
        assert by_id.code == by_code.code

    def test_get_accounts_by_type(self, db_with_accounts):
        """Test getting all accounts of a type."""
        assets = get_accounts_by_type(db_with_accounts, "ASSET")

        assert len(assets) > 0
        for account in assets:
            assert account.account_type == "ASSET"

    def test_get_child_accounts(self, db_with_accounts):
        """Test getting child accounts."""
        children = get_child_accounts(db_with_accounts, "1200")  # Investments

        assert len(children) > 0

        # All children should have 1200's ID as parent
        investments = get_account_by_code(db_with_accounts, "1200")
        for child in children:
            assert child.parent_id == investments.id

    def test_get_child_accounts_not_found(self, db_with_accounts):
        """Test getting children of non-existent account raises error."""
        with pytest.raises(AccountNotFoundError):
            get_child_accounts(db_with_accounts, "9999")


class TestAccountHierarchy:
    """Tests for account hierarchy functions."""

    def test_get_account_hierarchy_full(self, db_with_accounts):
        """Test getting full account hierarchy."""
        hierarchy = get_account_hierarchy(db_with_accounts)

        assert "roots" in hierarchy
        assert len(hierarchy["roots"]) > 0

        # Should have root accounts
        root_codes = {node["code"] for node in hierarchy["roots"]}
        assert "1000" in root_codes
        assert "4000" in root_codes

    def test_get_account_hierarchy_subtree(self, db_with_accounts):
        """Test getting hierarchy subtree."""
        hierarchy = get_account_hierarchy(db_with_accounts, "1200")

        assert hierarchy["code"] == "1200"
        assert hierarchy["name"] == "Investments"
        assert "children" in hierarchy
        assert len(hierarchy["children"]) > 0

    def test_get_account_hierarchy_not_found(self, db_with_accounts):
        """Test getting hierarchy of non-existent account raises error."""
        with pytest.raises(AccountNotFoundError):
            get_account_hierarchy(db_with_accounts, "9999")


class TestAccountDataclass:
    """Tests for Account dataclass."""

    def test_account_from_row(self, db_with_accounts):
        """Test creating Account from database row."""
        cursor = db_with_accounts.execute(
            "SELECT * FROM accounts WHERE code = ?", ("1101",)
        )
        row = cursor.fetchone()

        account = Account.from_row(row)

        assert account.code == "1101"
        assert account.name == "Bank - Savings"
        assert account.is_active is True
