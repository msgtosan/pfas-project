"""EPF Parser Integration Test - Refactored with PathResolver

Features:
- PathResolver for all paths (NO hardcoding)
- Multi-user support via parameterization
- Golden master comparison
- Graceful skip if no files
- In-memory DB for speed
"""

import pytest
from decimal import Decimal
from pfas.parsers.epf.epf import EPFParser


class TestEPFParser:
    """EPF Parser integration tests with real data."""

    def test_epf_parse_basic(self, epf_file, test_db):
        """Test basic EPF PDF parsing."""
        print(f"\n{'='*70}")
        print(f"EPF Parser - Basic Parse Test")
        print(f"File: {epf_file.name}")
        print(f"{'='*70}")

        # Parse
        parser = EPFParser(test_db)
        result = parser.parse(epf_file)

        # Basic assertions
        assert result.success, f"Parse failed: {result.errors}"
        assert result.account is not None, "No account info extracted"
        assert len(result.transactions) > 0, "No transactions extracted"

        print(f"\n✓ Parsed successfully")
        print(f"  Account: {result.account.uan}")
        print(f"  Transactions: {len(result.transactions)}")

    def test_epf_account_details(self, epf_file, test_db):
        """Test EPF account information extraction."""
        parser = EPFParser(test_db)
        result = parser.parse(epf_file)

        assert result.success
        account = result.account

        # Verify account fields
        assert account.uan is not None, "UAN not extracted"
        assert account.member_id is not None, "Member ID not extracted"
        assert account.member_name is not None, "Member name not extracted"

        print(f"\n✓ Account Details:")
        print(f"  UAN: {account.uan}")
        print(f"  Member: {account.member_name}")

    def test_epf_transactions_structure(self, epf_file, test_db):
        """Test EPF transaction structure and fields."""
        parser = EPFParser(test_db)
        result = parser.parse(epf_file)

        assert len(result.transactions) > 0

        # Check first transaction
        txn = result.transactions[0]
        assert txn.wage_month is not None
        assert txn.transaction_date is not None
        assert txn.employee_contribution is not None
        assert txn.employer_contribution is not None

        print(f"\n✓ Transaction Structure Valid")
        print(f"  Sample: {txn.wage_month}")

    def test_epf_save_to_db(self, epf_file, clean_db, test_user_id):
        """Test EPF data persistence to database."""
        parser = EPFParser(clean_db)
        result = parser.parse(epf_file)

        count = parser.save_to_db(result, user_id=test_user_id)
        assert count > 0, "No records saved"

        # Verify
        cursor = clean_db.execute("SELECT COUNT(*) as cnt FROM epf_transactions")
        db_count = cursor.fetchone()['cnt']
        assert db_count == count

        print(f"\n✓ Saved: {count} transactions")

    def test_epf_80c_calculation(self, epf_file, test_db):
        """Test 80C deduction calculation."""
        parser = EPFParser(test_db)
        result = parser.parse(epf_file)

        eligible_80c = parser.calculate_80c_eligible(result.transactions)
        assert eligible_80c >= 0
        assert eligible_80c <= Decimal('9999999')

        print(f"\n✓ 80C Eligible: ₹{eligible_80c:,.2f}")


# Multi-User Parameterized Tests
@pytest.mark.parametrize("path_resolver", ["Sanjay"], indirect=True)
class TestEPFMultiUser:
    """Multi-user EPF parser tests."""

    def test_epf_multi_user_parse(self, path_resolver, epf_file, test_db):
        """Test EPF parsing for multiple users."""
        print(f"\nTesting for user: {path_resolver.user_name}")

        parser = EPFParser(test_db)
        result = parser.parse(epf_file)

        assert result.success
        print(f"✓ User {path_resolver.user_name}: {len(result.transactions)} transactions")


# Golden Master Tests
class TestEPFGoldenMaster:
    """EPF parser golden master tests."""

    def test_epf_totals_golden(self, epf_file, test_db):
        """Test EPF totals against golden master."""
        parser = EPFParser(test_db)
        result = parser.parse(epf_file)

        totals = {
            'total_transactions': len(result.transactions),
            'total_ee': str(sum(t.employee_contribution for t in result.transactions)),
            'total_er': str(sum(t.employer_contribution for t in result.transactions)),
        }

        from conftest import assert_golden_match
        assert_golden_match(
            totals,
            f'epf_totals_{epf_file.stem}',
            format='json',
            save_if_missing=True
        )
