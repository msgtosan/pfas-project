#!/usr/bin/env python3
"""Integration test for MF CAMS parser with real data."""

import sys
from pathlib import Path
from datetime import date
from decimal import Decimal

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Try to import required modules
try:
    import sqlite3
    from pfas.parsers.mf.cams import CAMSParser
    from pfas.parsers.mf.capital_gains import CapitalGainsCalculator
    from pfas.core.database import DatabaseManager
    print("✅ All imports successful\n")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

def test_mf_cams_parser():
    """Run integration test with real CAMS data."""

    # File path
    cams_file = Path.home() / "projects/pfas-project/Data/Users/Sanjay/Mutual-Fund/CAMS/Sanjay_CAMS_CG_FY2024-25_v1.xlsx"

    print(f"📁 Testing with file: {cams_file}")
    print(f"   File size: {cams_file.stat().st_size / 1024:.1f} KB")
    print(f"   File exists: {cams_file.exists()}\n")

    if not cams_file.exists():
        print(f"❌ File not found: {cams_file}")
        return False

    # Create in-memory database
    print("📊 Initializing in-memory database...")
    try:
        db_manager = DatabaseManager()
        conn = db_manager.init(":memory:", "test_password")
        print("✅ Database initialized\n")
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return False

    # Parse CAMS file
    print("📖 Parsing CAMS file...")
    try:
        parser = CAMSParser(conn)
        result = parser.parse(cams_file)

        print(f"   Success: {result.success}")
        print(f"   Transactions parsed: {len(result.transactions)}")
        print(f"   Errors: {len(result.errors)}")
        print(f"   Warnings: {len(result.warnings)}\n")

        if result.errors:
            print("   Errors:")
            for err in result.errors:
                print(f"      - {err}")
            print()

        if result.warnings:
            print("   Warnings:")
            for warn in result.warnings[:5]:  # Show first 5
                print(f"      - {warn}")
            if len(result.warnings) > 5:
                print(f"      ... and {len(result.warnings) - 5} more")
            print()

    except Exception as e:
        print(f"❌ Parsing failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Display parsed data
    if result.transactions:
        print("📋 Sample Transactions (first 3):")
        for i, txn in enumerate(result.transactions[:3]):
            print(f"\n   Transaction {i+1}:")
            print(f"      Folio: {txn.folio_number}")
            print(f"      Scheme: {txn.scheme.name[:50]}...")
            print(f"      AMC: {txn.scheme.amc_name}")
            print(f"      Asset Class: {txn.scheme.asset_class.value}")
            print(f"      Type: {txn.transaction_type.value}")
            print(f"      Date: {txn.date}")
            print(f"      Units: {txn.units}")
            print(f"      NAV: ₹{txn.nav}")
            print(f"      Amount: ₹{txn.amount}")

            if txn.short_term_gain or txn.long_term_gain:
                print(f"      Short Term Gain: ₹{txn.short_term_gain}")
                print(f"      Long Term Gain: ₹{txn.long_term_gain}")

        print(f"\n   ... and {len(result.transactions) - 3} more transactions\n")

    # Calculate capital gains summary
    print("💰 Capital Gains Summary:")
    if result.transactions:
        # Group by asset class
        equity_stcg = Decimal("0")
        equity_ltcg = Decimal("0")
        debt_stcg = Decimal("0")
        debt_ltcg = Decimal("0")

        for txn in result.transactions:
            if txn.transaction_type.value == "REDEMPTION":
                if txn.scheme.asset_class.value == "EQUITY":
                    if txn.short_term_gain:
                        equity_stcg += txn.short_term_gain
                    if txn.long_term_gain:
                        equity_ltcg += txn.long_term_gain
                elif txn.scheme.asset_class.value == "DEBT":
                    if txn.short_term_gain:
                        debt_stcg += txn.short_term_gain
                    if txn.long_term_gain:
                        debt_ltcg += txn.long_term_gain

        print(f"\n   Equity Funds:")
        print(f"      STCG: ₹{equity_stcg:,.2f}")
        print(f"      LTCG: ₹{equity_ltcg:,.2f}")

        print(f"\n   Debt Funds:")
        print(f"      STCG: ₹{debt_stcg:,.2f}")
        print(f"      LTCG: ₹{debt_ltcg:,.2f}")

        print(f"\n   Total:")
        total_stcg = equity_stcg + debt_stcg
        total_ltcg = equity_ltcg + debt_ltcg
        print(f"      STCG: ₹{total_stcg:,.2f}")
        print(f"      LTCG: ₹{total_ltcg:,.2f}")

    # Scheme statistics
    print("\n📊 Scheme Statistics:")
    schemes = {}
    for txn in result.transactions:
        scheme_key = txn.scheme.name
        if scheme_key not in schemes:
            schemes[scheme_key] = {
                "amc": txn.scheme.amc_name,
                "asset_class": txn.scheme.asset_class.value,
                "count": 0
            }
        schemes[scheme_key]["count"] += 1

    print(f"\n   Total unique schemes: {len(schemes)}")
    print("\n   Schemes (first 5):")
    for i, (name, info) in enumerate(list(schemes.items())[:5]):
        print(f"\n      {i+1}. {info['amc']}")
        print(f"         Scheme: {name[:50]}...")
        print(f"         Asset Class: {info['asset_class']}")
        print(f"         Transactions: {info['count']}")

    if len(schemes) > 5:
        print(f"\n      ... and {len(schemes) - 5} more schemes")

    # Database save test
    print("\n\n💾 Testing database persistence...")
    try:
        count = parser.save_to_db(result, user_id=1)
        print(f"✅ Saved {count} transactions to database")

        # Verify data in database
        cursor = conn.execute("SELECT COUNT(*) as cnt FROM mf_transactions")
        row = cursor.fetchone()
        print(f"✅ Verified: {row['cnt']} transactions in database")

        cursor = conn.execute("SELECT COUNT(*) as cnt FROM mf_schemes")
        row = cursor.fetchone()
        print(f"✅ Verified: {row['cnt']} schemes in database")

        cursor = conn.execute("SELECT COUNT(*) as cnt FROM mf_amcs")
        row = cursor.fetchone()
        print(f"✅ Verified: {row['cnt']} AMCs in database")

    except Exception as e:
        print(f"❌ Database save failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n" + "="*60)
    print("✅ INTEGRATION TEST PASSED")
    print("="*60)

    return True

if __name__ == "__main__":
    success = test_mf_cams_parser()
    sys.exit(0 if success else 1)
