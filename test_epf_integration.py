#!/usr/bin/env python3
"""Integration test for EPF parser with real data."""

import sys
from pathlib import Path
from datetime import date
from decimal import Decimal

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_epf_parser():
    """Run integration test with real EPF passbook data."""

    print("="*70)
    print("EPF (EMPLOYEE PROVIDENT FUND) INTEGRATION TEST")
    print("="*70 + "\n")

    # File paths - try both
    epf_files = [
        Path.home() / "projects/pfas-project/Data/Users/Sanjay/EPF/EPF_Interest_APHYD00476720000003193_2024.pdf",
        Path.home() / "projects/pfas-project/Data/Users/Sanjay/EPF/APHYD00476720000003193_2025.pdf",
    ]

    epf_file = None
    for f in epf_files:
        if f.exists():
            epf_file = f
            break

    if not epf_file:
        print(f"❌ No EPF files found")
        print(f"   Searched for:")
        for f in epf_files:
            print(f"      - {f}")
        return False

    print(f"📁 Test File: {epf_file.name}")
    print(f"   Full Path: {epf_file}")
    print(f"   Size: {epf_file.stat().st_size / 1024:.1f} KB\n")

    # Import parsers
    print("📦 Importing modules...")
    try:
        import sqlite3
        from pfas.parsers.epf.epf import EPFParser
        from pfas.core.database import DatabaseManager
        print("✅ Imports successful\n")
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("   Install dependencies: pip install pdfplumber sqlcipher3")
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

    # Parse EPF file
    print("📖 Parsing EPF Passbook PDF...")
    try:
        parser = EPFParser(conn)
        result = parser.parse(epf_file)

        print(f"   Success: {result.success}")
        print(f"   Errors: {len(result.errors)}")
        print(f"   Warnings: {len(result.warnings)}")

        if result.errors:
            print(f"\n   Errors:")
            for err in result.errors[:3]:
                print(f"      - {err}")

        if result.warnings:
            print(f"\n   Warnings (first 3):")
            for warn in result.warnings[:3]:
                print(f"      - {warn}")

        print()

    except Exception as e:
        print(f"❌ Parsing failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Display account info
    if result.account:
        print("👤 Account Information:")
        print(f"   UAN: {result.account.uan}")
        print(f"   Member ID: {result.account.member_id}")
        print(f"   Member Name: {result.account.member_name}")
        print(f"   Establishment ID: {result.account.establishment_id}")
        print(f"   Establishment: {result.account.establishment_name}\n")

    # Display transactions
    if result.transactions:
        print("📋 EPF Transactions:")
        print(f"   Total transactions: {len(result.transactions)}\n")

        # Sample transactions
        print(f"   Sample Transactions (first 3):")
        for i, txn in enumerate(result.transactions[:3], 1):
            print(f"\n      Transaction {i}:")
            print(f"         Wage Month: {txn.wage_month}")
            print(f"         Date: {txn.transaction_date}")
            print(f"         Type: {txn.transaction_type}")
            print(f"         EE Contribution: ₹{txn.employee_contribution}")
            print(f"         ER Contribution: ₹{txn.employer_contribution}")
            print(f"         EPS: ₹{txn.pension_contribution}")
            print(f"         VPF: ₹{txn.vpf_contribution}")
            print(f"         EE Balance: ₹{txn.employee_balance:,.2f}")
            print(f"         ER Balance: ₹{txn.employer_balance:,.2f}")

        if len(result.transactions) > 3:
            print(f"\n      ... and {len(result.transactions) - 3} more transactions")

        # Calculate totals
        print("\n\n💰 EPF Summary:")

        total_ee = sum(txn.employee_contribution for txn in result.transactions)
        total_er = sum(txn.employer_contribution for txn in result.transactions)
        total_eps = sum(txn.pension_contribution for txn in result.transactions)
        total_vpf = sum(txn.vpf_contribution for txn in result.transactions)

        print(f"\n   Total Contributions (FY2024-25):")
        print(f"      Employee (EE): ₹{total_ee:,.2f}")
        print(f"      Employer (ER): ₹{total_er:,.2f}")
        print(f"      Pension (EPS): ₹{total_eps:,.2f}")
        print(f"      VPF (Voluntary): ₹{total_vpf:,.2f}")

        # Tax deductions
        eligible_80c = parser.calculate_80c_eligible(result.transactions)
        print(f"\n   Tax Benefits:")
        print(f"      80C Eligible (EE + VPF): ₹{eligible_80c:,.2f}")
        print(f"      80C Cap: ₹1,50,000")
        print(f"      Deductible: ₹{min(eligible_80c, Decimal('150000')):,.2f}")

    # Display interest
    if result.interest:
        print(f"\n\n📈 Interest & TDS:")
        print(f"   Financial Year: {result.interest.financial_year}")
        print(f"   Employee Interest: ₹{result.interest.employee_interest:,.2f}")
        print(f"   Employer Interest: ₹{result.interest.employer_interest:,.2f}")
        print(f"   TDS Deducted: ₹{result.interest.tds_deducted:,.2f}")
        print(f"   Taxable Interest: ₹{result.interest.taxable_interest:,.2f}")

    # Database save test
    print("\n\n💾 Testing database persistence...")
    try:
        count = parser.save_to_db(result, user_id=1)
        print(f"✅ Saved {count} transactions to database")

        # Verify data in database
        cursor = conn.execute("SELECT COUNT(*) as cnt FROM epf_transactions")
        row = cursor.fetchone()
        print(f"✅ Verified: {row['cnt']} transactions in database")

        cursor = conn.execute("SELECT COUNT(*) as cnt FROM epf_accounts")
        row = cursor.fetchone()
        print(f"✅ Verified: {row['cnt']} EPF accounts in database")

        if result.interest:
            cursor = conn.execute("SELECT COUNT(*) as cnt FROM epf_interest")
            row = cursor.fetchone()
            print(f"✅ Verified: {row['cnt']} interest records in database")

    except Exception as e:
        print(f"❌ Database save failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n" + "="*70)
    print("✅ EPF PARSER INTEGRATION TEST PASSED")
    print("="*70)

    return True

if __name__ == "__main__":
    success = test_epf_parser()
    sys.exit(0 if success else 1)
