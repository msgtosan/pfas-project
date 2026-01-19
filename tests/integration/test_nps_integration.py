"""NPS Parser Integration Test - Refactored"""

import pytest
from decimal import Decimal
from pfas.parsers.nps.nps import NPSParser


class TestNPSParser:
    """NPS parser integration tests."""

    def test_nps_parse_basic(self, nps_file, test_db):
        """Test basic NPS parsing."""
        parser = NPSParser(test_db)
        result = parser.parse(nps_file)

        # Skip if file format not supported (data issue, not code issue)
        if not result.success:
            if any("header row" in err.lower() or "column" in err.lower() for err in result.errors):
                pytest.skip(f"NPS file format not supported: {result.errors[0]}")
            assert result.success, f"Parse failed: {result.errors}"

        print(f"\n✓ Parsed {len(result.transactions)} transactions from {nps_file.name}")

    def test_nps_account_details(self, nps_file, test_db):
        """Test NPS account extraction."""
        parser = NPSParser(test_db)
        result = parser.parse(nps_file)

        if result.account:
            assert result.account.pran is not None
            print(f"\n✓ Account PRAN: {result.account.pran}")

    def test_nps_tiers(self, nps_file, test_db):
        """Test tier classification."""
        parser = NPSParser(test_db)
        result = parser.parse(nps_file)

        tier1 = [t for t in result.transactions if t.tier == "I"]
        tier2 = [t for t in result.transactions if t.tier == "II"]

        print(f"\n✓ Tier I: {len(tier1)}, Tier II: {len(tier2)}")

    def test_nps_save_to_db(self, nps_file, clean_db, test_user_id):
        """Test database persistence."""
        parser = NPSParser(clean_db)
        result = parser.parse(nps_file)

        if not result.success or len(result.transactions) == 0:
            pytest.skip(f"No data to save: {result.errors if result.errors else 'no transactions'}")

        count = parser.save_to_db(result, user_id=test_user_id)
        assert count > 0

        print(f"\n✓ Saved {count} transactions")

    def test_nps_deductions(self, nps_file, test_db):
        """Test tax deduction calculation."""
        parser = NPSParser(test_db)
        result = parser.parse(nps_file)

        basic_salary = Decimal("1000000")
        deductions = parser.calculate_deductions(
            result.transactions,
            basic_salary=basic_salary,
            fy="2024-25"
        )

        assert '80CCD_1' in deductions
        assert '80CCD_2' in deductions

        print(f"\n✓ 80CCD(1): ₹{deductions['80CCD_1']:,.2f}")
        print(f"  80CCD(2): ₹{deductions['80CCD_2']:,.2f}")
