"""
Manual test for parsing real ICICI bank statements.

This test uses actual data from Data/Users/Sanjay/Bank/ICICI/
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pfas.core.database import DatabaseManager
from pfas.core.accounts import setup_chart_of_accounts
from pfas.parsers.bank.icici import ICICIParser
from pfas.parsers.bank.sbi import SBIParser

# Test credentials
TEST_DB_PASSWORD = "test_password_123"
TEST_MASTER_KEY = b"test_master_key_32_bytes_long!!"


def test_icici_parser():
    """Test parsing actual ICICI Excel statements."""
    print("\n" + "=" * 80)
    print("Testing ICICI Bank Statement Parser with Real Data")
    print("=" * 80)

    # Initialize database
    DatabaseManager.reset_instance()
    db_manager = DatabaseManager()
    conn = db_manager.init(":memory:", TEST_DB_PASSWORD)
    setup_chart_of_accounts(conn)

    # Create parser
    parser = ICICIParser(conn, TEST_MASTER_KEY)

    # Test files
    data_dir = Path("Data/Users/Sanjay/Bank/ICICI")

    if not data_dir.exists():
        print(f"\n❌ ERROR: Data directory not found: {data_dir}")
        print("   Please ensure the Data symlink is set up correctly.")
        return

    files = list(data_dir.glob("*.xls"))

    if not files:
        print(f"\n❌ ERROR: No .xls files found in {data_dir}")
        return

    print(f"\nFound {len(files)} ICICI statement files:")
    for f in files:
        print(f"  - {f.name}")

    # Test the first file
    test_file = files[0]
    print(f"\n📄 Parsing: {test_file.name}")
    print("-" * 80)

    try:
        # Note: ICICI parser expects PDF format, but we have Excel files
        # This might fail - we may need to use a different parser or modify the approach
        result = parser.parse(str(test_file))

        if result.success:
            print(f"✅ SUCCESS: Parsed {len(result.transactions)} transactions")

            if result.account:
                print(f"\n📋 Account Information:")
                print(f"   Bank: {result.account.bank_name}")
                print(f"   Account Number: {result.account.masked_number}")
                if result.account.ifsc_code:
                    print(f"   IFSC Code: {result.account.ifsc_code}")
                if result.account.branch:
                    print(f"   Branch: {result.account.branch}")

            if result.statement_period_start and result.statement_period_end:
                print(f"\n📅 Statement Period:")
                print(f"   From: {result.statement_period_start}")
                print(f"   To: {result.statement_period_end}")

            if result.transactions:
                print(f"\n💰 Transaction Summary:")
                print(f"   Total Transactions: {result.transaction_count}")
                print(f"   Total Debits: ₹{result.total_debits:,.2f}")
                print(f"   Total Credits: ₹{result.total_credits:,.2f}")
                print(f"   Interest Earned: ₹{result.interest_total:,.2f}")

                # Show first 5 transactions
                print(f"\n📊 First 5 Transactions:")
                for i, txn in enumerate(result.transactions[:5], 1):
                    print(f"\n   {i}. Date: {txn.date}")
                    print(f"      Description: {txn.description}")
                    print(f"      Debit: ₹{txn.debit:,.2f}")
                    print(f"      Credit: ₹{txn.credit:,.2f}")
                    print(f"      Balance: ₹{txn.balance:,.2f}" if txn.balance else "      Balance: N/A")
                    print(f"      Category: {txn.category.value}")
                    if txn.is_interest:
                        print(f"      🌟 INTEREST TRANSACTION")

                # Show interest transactions
                interest_txns = [t for t in result.transactions if t.is_interest]
                if interest_txns:
                    print(f"\n💸 Interest Transactions ({len(interest_txns)}):")
                    for i, txn in enumerate(interest_txns, 1):
                        print(f"   {i}. {txn.date}: ₹{txn.credit:,.2f} - {txn.description}")

            if result.warnings:
                print(f"\n⚠️  Warnings:")
                for warning in result.warnings:
                    print(f"   - {warning}")

        else:
            print(f"❌ FAILED to parse file")
            if result.errors:
                print(f"\n🔴 Errors:")
                for error in result.errors:
                    print(f"   - {error}")

    except Exception as e:
        print(f"❌ EXCEPTION: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

    finally:
        db_manager.close()
        DatabaseManager.reset_instance()

    print("\n" + "=" * 80)


def test_with_sbi_parser():
    """Try parsing ICICI Excel files with SBI parser (since both use Excel)."""
    print("\n" + "=" * 80)
    print("Testing with SBI Parser (Excel format)")
    print("=" * 80)

    # Initialize database
    DatabaseManager.reset_instance()
    db_manager = DatabaseManager()
    conn = db_manager.init(":memory:", TEST_DB_PASSWORD)
    setup_chart_of_accounts(conn)

    # Create SBI parser
    parser = SBIParser(conn, TEST_MASTER_KEY)

    # Test files
    data_dir = Path("Data/Users/Sanjay/Bank/ICICI")
    files = list(data_dir.glob("*.xls"))

    if not files:
        print(f"❌ ERROR: No .xls files found")
        return

    test_file = files[0]
    print(f"\n📄 Parsing with SBI parser: {test_file.name}")
    print("-" * 80)

    try:
        result = parser.parse(str(test_file))

        if result.success:
            print(f"✅ SUCCESS: Parsed {len(result.transactions)} transactions")

            if result.transactions:
                print(f"\n💰 Transaction Summary:")
                print(f"   Total Transactions: {result.transaction_count}")
                print(f"   Total Debits: ₹{result.total_debits:,.2f}")
                print(f"   Total Credits: ₹{result.total_credits:,.2f}")
                print(f"   Interest Earned: ₹{result.interest_total:,.2f}")

                # Show first 5 transactions
                print(f"\n📊 First 5 Transactions:")
                for i, txn in enumerate(result.transactions[:5], 1):
                    print(f"\n   {i}. Date: {txn.date}")
                    print(f"      Description: {txn.description}")
                    print(f"      Debit: ₹{txn.debit:,.2f}")
                    print(f"      Credit: ₹{txn.credit:,.2f}")
                    print(f"      Balance: ₹{txn.balance:,.2f}" if txn.balance else "      Balance: N/A")
                    print(f"      Category: {txn.category.value}")
        else:
            print(f"❌ FAILED to parse file")
            if result.errors:
                print(f"\n🔴 Errors:")
                for error in result.errors:
                    print(f"   - {error}")

    except Exception as e:
        print(f"❌ EXCEPTION: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

    finally:
        db_manager.close()
        DatabaseManager.reset_instance()

    print("\n" + "=" * 80)


if __name__ == "__main__":
    print("\n🚀 PFAS Bank Parser - Real Data Test")

    # Try with ICICI parser first
    test_icici_parser()

    # Try with SBI parser (since files are Excel)
    test_with_sbi_parser()

    print("\n✨ Test complete!")
