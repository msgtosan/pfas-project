"""MF CAMS Parser Integration Test - Refactored"""

import pytest
from decimal import Decimal
from pfas.parsers.mf.cams import CAMSParser


class TestCAMSParser:
    """CAMS parser integration tests."""

    def test_cams_parse_basic(self, cams_file, test_db):
        """Test basic CAMS parsing."""
        parser = CAMSParser(test_db)
        result = parser.parse(cams_file)

        assert result.success, f"Parse failed: {result.errors}"
        assert len(result.transactions) > 0

        print(f"\n✓ Parsed {len(result.transactions)} transactions")

    def test_cams_capital_gains(self, cams_file, test_db):
        """Test capital gains extraction."""
        parser = CAMSParser(test_db)
        result = parser.parse(cams_file)

        # Check for capital gains data
        redemptions = [t for t in result.transactions if t.transaction_type.value == "REDEMPTION"]

        if redemptions:
            total_stcg = sum(t.short_term_gain or Decimal("0") for t in redemptions)
            total_ltcg = sum(t.long_term_gain or Decimal("0") for t in redemptions)

            print(f"\n✓ Capital Gains:")
            print(f"  STCG: ₹{total_stcg:,.2f}")
            print(f"  LTCG: ₹{total_ltcg:,.2f}")

    def test_cams_save_to_db(self, cams_file, clean_db, test_user_id):
        """Test database persistence."""
        parser = CAMSParser(clean_db)
        result = parser.parse(cams_file)

        count = parser.save_to_db(result, user_id=test_user_id)
        assert count > 0

        cursor = clean_db.execute("SELECT COUNT(*) as cnt FROM mf_transactions")
        assert cursor.fetchone()['cnt'] == count

        print(f"\n✓ Saved {count} transactions")
