#!/usr/bin/env python3
"""Integration test for NPS parser with real data."""

import sys
from pathlib import Path
from datetime import date
from decimal import Decimal

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_nps_parser():
    """Run integration test with real NPS statement data."""

    print("="*70)
    print("NPS (NATIONAL PENSION SYSTEM) INTEGRATION TEST")
    print("="*70 + "\n")

    # File path
    nps_file = Path.home() / "projects/pfas-project/Data/Users/Sanjay/NPS/110091211424_NPS.csv"

    print(f"📁 Test File: {nps_file.name}")
    print(f"   Full Path: {nps_file}")
    print(f"   File exists: {nps_file.exists()}")

    if nps_file.exists():
        print(f"   Size: {nps_file.stat().st_size / 1024:.1f} KB")
    print()

    if not nps_file.exists():
        print(f"❌ File not found: {nps_file}")
        return False

    # Import parsers
    print("📦 Importing modules...")
    try:
        import sqlite3
        from pfas.parsers.nps.nps import NPSParser
        from pfas.core.database import DatabaseManager
        print("✅ Imports successful\n")
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("   Install dependencies: pip install pandas sqlcipher3")
        return False

    # Create in-memory database
    print("📊 Initializing database...")
    try:
        db_manager = DatabaseManager()
        conn = db_manager.init(":memory:", "test_password")
        print("✅ Database initialized\n")
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return False

    # Parse NPS file
    print("📖 Parsing NPS Statement CSV...")
    try:
        parser = NPSParser(conn)
        result = parser.parse(nps_file)

        print(f"   Success: {result.success}")
        print(f"   Transactions parsed: {len(result.transactions)}")
        print(f"   Errors: {len(result.errors)}")
        print(f"   Warnings: {len(result.warnings)}\n")

        if result.errors:
            print("   Errors:")
            for err in result.errors[:3]:
                print(f"      - {err}")
            if len(result.errors) > 3:
                print(f"      ... and {len(result.errors) - 3} more")
            print()

    except Exception as e:
        print(f"❌ Parsing failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Display account info
    if result.account:
        print("👤 Account Information:")
        print(f"   PRAN: {result.account.pran}")
        print(f"   Nodal Office: {result.account.nodal_office}")
        print(f"   Scheme Preference: {result.account.scheme_preference}\n")

    # Display transactions
    if result.transactions:
        print("📋 NPS Transactions:")
        print(f"   Total transactions: {len(result.transactions)}\n")

        # Categorize by tier
        tier1 = [t for t in result.transactions if t.tier == "I"]
        tier2 = [t for t in result.transactions if t.tier == "II"]

        print(f"   By Tier:")
        print(f"      Tier I: {len(tier1)} transactions")
        print(f"      Tier II: {len(tier2)} transactions\n")

        # By contribution type
        ee_contrib = [t for t in result.transactions if t.contribution_type == "EMPLOYEE"]
        er_contrib = [t for t in result.transactions if t.contribution_type == "EMPLOYER"]

        print(f"   By Contributor:")
        print(f"      Employee (EE): {len(ee_contrib)} transactions")
        print(f"      Employer (ER): {len(er_contrib)} transactions\n")

        # Sample transactions
        print(f"   Sample Transactions (first 5):")
        for i, txn in enumerate(result.transactions[:5], 1):
            print(f"\n      Transaction {i}:")
            print(f"         Date: {txn.date}")
            print(f"         Type: {txn.transaction_type}")
            print(f"         Tier: {txn.tier}")
            print(f"         Contributor: {txn.contribution_type}")
            print(f"         Amount: ₹{txn.amount:,.2f}")
            print(f"         Units: {txn.units}")
            print(f"         NAV: ₹{txn.nav}")
            print(f"         Scheme: {txn.scheme[:40]}...")
            print(f"         FY: {txn.financial_year}")

        if len(result.transactions) > 5:
            print(f"\n      ... and {len(result.transactions) - 5} more transactions")

        # Calculate summary
        print("\n\n💰 NPS Summary:")

        # Total contributions
        total_ee_tier1 = sum(
            t.amount for t in result.transactions
            if t.tier == "I" and t.contribution_type == "EMPLOYEE"
            and t.transaction_type in ["Contribution", "CONTRIBUTION", "Purchase"]
        )

        total_er_tier1 = sum(
            t.amount for t in result.transactions
            if t.tier == "I" and t.contribution_type == "EMPLOYER"
            and t.transaction_type in ["Contribution", "CONTRIBUTION", "Purchase"]
        )

        print(f"\n   Contributions (All FY):")
        print(f"      Employee (Tier I): ₹{total_ee_tier1:,.2f}")
        print(f"      Employer (Tier I): ₹{total_er_tier1:,.2f}")

        # Calculate deductions
        print(f"\n   Tax Deductions (FY2024-25):")

        # Estimate basic salary for demo
        basic_salary = Decimal("1000000")  # Assume ₹10L basic

        deductions = parser.calculate_deductions(
            result.transactions,
            basic_salary=basic_salary,
            fy="2024-25"
        )

        print(f"      (Assuming Basic Salary: ₹{basic_salary:,.0f})")
        print(f"      80CCD(1) - EE Tier I: ₹{deductions['80CCD_1']:,.2f}")
        print(f"      80CCD(1B) - Additional ₹50K: ₹{deductions['80CCD_1B']:,.2f}")
        print(f"      80CCD(2) - ER Contribution: ₹{deductions['80CCD_2']:,.2f}")
        print(f"      80CCD(2) Limit (10% Basic): ₹{deductions['80CCD_2_limit']:,.2f}")

        # Combined 80C
        total_80c = min(deductions['80CCD_1'] + deductions['80CCD_1B'], Decimal("150000"))
        print(f"\n      Combined 80C (1+1B, capped): ₹{total_80c:,.2f}")
        print(f"      Total 80CCD (1+1B+2): ₹{deductions['80CCD_1'] + deductions['80CCD_1B'] + deductions['80CCD_2']:,.2f}")

        # NAV tracking
        avg_nav = sum(t.nav for t in result.transactions if t.nav) / len([t for t in result.transactions if t.nav]) if result.transactions else Decimal("0")
        print(f"\n   NAV Tracking:")
        print(f"      Average NAV: ₹{avg_nav:.2f}")
        if result.transactions:
            min_nav = min(t.nav for t in result.transactions if t.nav)
            max_nav = max(t.nav for t in result.transactions if t.nav)
            print(f"      Min NAV: ₹{min_nav:.2f}")
            print(f"      Max NAV: ₹{max_nav:.2f}")

    # Database save test
    print("\n\n💾 Testing database persistence...")
    try:
        count = parser.save_to_db(result, user_id=1)
        print(f"✅ Saved {count} transactions to database")

        # Verify data in database
        cursor = conn.execute("SELECT COUNT(*) as cnt FROM nps_transactions")
        row = cursor.fetchone()
        print(f"✅ Verified: {row['cnt']} transactions in database")

        cursor = conn.execute("SELECT COUNT(*) as cnt FROM nps_accounts")
        row = cursor.fetchone()
        print(f"✅ Verified: {row['cnt']} NPS accounts in database")

    except Exception as e:
        print(f"❌ Database save failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n" + "="*70)
    print("✅ NPS PARSER INTEGRATION TEST PASSED")
    print("="*70)

    return True

if __name__ == "__main__":
    success = test_nps_parser()
    sys.exit(0 if success else 1)
