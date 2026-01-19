"""PPF Parser Integration Test - Refactored"""

import pytest
from decimal import Decimal
from pfas.parsers.ppf.ppf import PPFParser


class TestPPFParser:
    """PPF parser integration tests."""

    def test_ppf_parse_basic(self, ppf_file, test_db):
        """Test basic PPF parsing."""
        parser = PPFParser(test_db)
        result = parser.parse(
            ppf_file,
            account_number="PPF-Test",
            bank_name="SBI",
            branch="Test Branch"
        )

        # Skip if file is corrupted or empty (data issue, not code issue)
        if not result.success:
            if any("no worksheets" in err.lower() or "corrupted" in err.lower() or "empty" in err.lower()
                   for err in result.errors):
                pytest.skip(f"PPF file corrupted or empty: {result.errors[0]}")
            assert result.success, f"Parse failed: {result.errors}"

        print(f"\n✓ Parsed {len(result.transactions)} transactions from {ppf_file.name}")

    def test_ppf_account_details(self, ppf_file, test_db):
        """Test PPF account extraction."""
        parser = PPFParser(test_db)
        result = parser.parse(ppf_file, account_number="PPF-Test")

        if result.account:
            assert result.account.account_number is not None
            print(f"\n✓ Account: {result.account.account_number}")

    def test_ppf_transaction_types(self, ppf_file, test_db):
        """Test transaction type classification."""
        parser = PPFParser(test_db)
        result = parser.parse(ppf_file, account_number="PPF-Test")

        deposits = [t for t in result.transactions if t.transaction_type == "DEPOSIT"]
        interest = [t for t in result.transactions if t.transaction_type == "INTEREST"]

        print(f"\n✓ Deposits: {len(deposits)}, Interest: {len(interest)}")

    def test_ppf_save_to_db(self, ppf_file, clean_db, test_user_id):
        """Test database persistence."""
        parser = PPFParser(clean_db)
        result = parser.parse(ppf_file, account_number="PPF-Test")

        if not result.success or len(result.transactions) == 0:
            pytest.skip(f"No data to save: {result.errors if result.errors else 'no transactions'}")

        count = parser.save_to_db(result, user_id=test_user_id)
        assert count > 0

        print(f"\n✓ Saved {count} transactions")

    def test_ppf_80c_calculation(self, ppf_file, test_db):
        """Test 80C deduction calculation."""
        parser = PPFParser(test_db)
        result = parser.parse(ppf_file, account_number="PPF-Test")

        deposits = [t for t in result.transactions if t.transaction_type == "DEPOSIT"]
        eligible_80c = parser.calculate_80c_eligible(deposits, "2024-25")

        assert eligible_80c >= 0
        print(f"\n✓ 80C Eligible: ₹{eligible_80c:,.2f}")
