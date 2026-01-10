#!/usr/bin/env python3
"""Integration test for PPF parser with real data."""

import sys
from pathlib import Path
from datetime import date
from decimal import Decimal

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_ppf_parser():
    """Run integration test with real PPF statement data."""

    print("="*70)
    print("PPF (PUBLIC PROVIDENT FUND) INTEGRATION TEST")
    print("="*70 + "\n")

    # File path
    ppf_file = Path.home() / "projects/pfas-project/Data/Users/Sanjay/PPF/FY24-25-PPF-Sanjay.xlsx"

    print(f"📁 Test File: {ppf_file.name}")
    print(f"   Full Path: {ppf_file}")
    print(f"   File exists: {ppf_file.exists()}")

    if ppf_file.exists():
        print(f"   Size: {ppf_file.stat().st_size / 1024:.1f} KB")
    print()

    if not ppf_file.exists():
        print(f"❌ File not found: {ppf_file}")
        return False

    # Import parsers
    print("📦 Importing modules...")
    try:
        import sqlite3
        from pfas.parsers.ppf.ppf import PPFParser
        from pfas.core.database import DatabaseManager
        print("✅ Imports successful\n")
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("   Install dependencies: pip install pandas openpyxl sqlcipher3")
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

    # Parse PPF file
    print("📖 Parsing PPF Statement...")
    try:
        parser = PPFParser(conn)
        result = parser.parse(
            ppf_file,
            account_number="PPF-Sanjay",
            bank_name="SBI",
            branch="Hyderabad"
        )

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
        print("📋 Account Information:")
        print(f"   Account Number: {result.account.account_number}")
        print(f"   Bank: {result.account.bank_name}")
        print(f"   Branch: {result.account.branch}")
        print(f"   Opening Date: {result.account.opening_date}")
        print(f"   Maturity Date: {result.account.maturity_date}")

        if result.account.maturity_date:
            years_left = (result.account.maturity_date - date.today()).days / 365.25
            print(f"   Years until maturity: {years_left:.1f}\n")
        else:
            print()

    # Display transactions
    if result.transactions:
        print("📋 PPF Transactions:")
        print(f"   Total transactions: {len(result.transactions)}\n")

        # Categorize transactions
        deposits = [t for t in result.transactions if t.transaction_type == "DEPOSIT"]
        interest = [t for t in result.transactions if t.transaction_type == "INTEREST"]
        withdrawals = [t for t in result.transactions if t.transaction_type == "WITHDRAWAL"]

        print(f"   By Type:")
        print(f"      Deposits: {len(deposits)}")
        print(f"      Interest: {len(interest)}")
        print(f"      Withdrawals: {len(withdrawals)}\n")

        # Sample transactions
        print(f"   Sample Transactions (first 5):")
        for i, txn in enumerate(result.transactions[:5], 1):
            print(f"\n      Transaction {i}:")
            print(f"         Date: {txn.date}")
            print(f"         Type: {txn.transaction_type}")
            print(f"         Amount: ₹{txn.amount:,.2f}")
            print(f"         Balance: ₹{txn.balance:,.2f}")
            if txn.interest_rate:
                print(f"         Rate: {txn.interest_rate}%")
            print(f"         FY: {txn.financial_year}")

        if len(result.transactions) > 5:
            print(f"\n      ... and {len(result.transactions) - 5} more transactions")

        # Calculate summary
        print("\n\n💰 PPF Summary:")

        total_deposits = sum(d.amount for d in deposits)
        total_interest = sum(i.amount for i in interest)
        total_withdrawals = sum(w.amount for w in withdrawals)

        print(f"\n   Total Deposits: ₹{total_deposits:,.2f}")
        print(f"   Total Interest: ₹{total_interest:,.2f}")
        print(f"   Total Withdrawals: ₹{total_withdrawals:,.2f}")

        if result.transactions:
            final_balance = result.transactions[-1].balance
            print(f"   Current Balance: ₹{final_balance:,.2f}")

        # Tax benefits
        print(f"\n   Tax Benefits:")
        eligible_80c = parser.calculate_80c_eligible(deposits, "2024-25")
        print(f"      80C Eligible (FY2024-25): ₹{eligible_80c:,.2f}")
        print(f"      80C Cap: ₹1,50,000")
        print(f"      Deductible: ₹{min(eligible_80c, Decimal('150000')):,.2f}")
        print(f"      Interest: Tax-free")

    # Database save test
    print("\n\n💾 Testing database persistence...")
    try:
        count = parser.save_to_db(result, user_id=1)
        print(f"✅ Saved {count} transactions to database")

        # Verify data in database
        cursor = conn.execute("SELECT COUNT(*) as cnt FROM ppf_transactions")
        row = cursor.fetchone()
        print(f"✅ Verified: {row['cnt']} transactions in database")

        cursor = conn.execute("SELECT COUNT(*) as cnt FROM ppf_accounts")
        row = cursor.fetchone()
        print(f"✅ Verified: {row['cnt']} PPF accounts in database")

    except Exception as e:
        print(f"❌ Database save failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n" + "="*70)
    print("✅ PPF PARSER INTEGRATION TEST PASSED")
    print("="*70)

    return True

if __name__ == "__main__":
    success = test_ppf_parser()
    sys.exit(0 if success else 1)
