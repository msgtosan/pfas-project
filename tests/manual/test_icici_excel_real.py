"""
Manual test for parsing real ICICI Excel bank statements.

This test uses actual data from Data/Users/Sanjay/Bank/ICICI/
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pfas.core.database import DatabaseManager
from pfas.core.accounts import setup_chart_of_accounts
from pfas.parsers.bank.icici_excel import ICICIExcelParser
from pfas.parsers.bank.interest import InterestCalculator

# Test credentials
TEST_DB_PASSWORD = "test_password_123"
TEST_MASTER_KEY = b"test_master_key_32_bytes_long!!"


def test_icici_excel_parser():
    """Test parsing actual ICICI Excel statements."""
    print("\n" + "=" * 80)
    print("PFAS - ICICI Bank Excel Statement Parser Test")
    print("=" * 80)

    # Initialize database
    DatabaseManager.reset_instance()
    db_manager = DatabaseManager()
    conn = db_manager.init(":memory:", TEST_DB_PASSWORD)
    setup_chart_of_accounts(conn)

    # Create parser
    parser = ICICIExcelParser(conn, TEST_MASTER_KEY)

    # Test files
    data_dir = Path("Data/Users/Sanjay/Bank/ICICI")

    if not data_dir.exists():
        print(f"\n❌ ERROR: Data directory not found: {data_dir}")
        print("   Please ensure the Data symlink is set up correctly.")
        return

    files = sorted(data_dir.glob("*.xls"))

    if not files:
        print(f"\n❌ ERROR: No .xls files found in {data_dir}")
        return

    print(f"\nFound {len(files)} ICICI Excel statement files:")
    for i, f in enumerate(files, 1):
        print(f"  {i}. {f.name}")

    # Parse all files
    all_results = []

    for file_path in files:
        print(f"\n{'=' * 80}")
        print(f"📄 Parsing: {file_path.name}")
        print("-" * 80)

        try:
            result = parser.parse(str(file_path))
            all_results.append(result)

            if result.success:
                print(f"✅ SUCCESS: Parsed {len(result.transactions)} transactions")

                if result.account:
                    print(f"\n📋 Account Information:")
                    print(f"   Bank: {result.account.bank_name}")
                    print(f"   Account Number: {result.account.masked_number}")
                    print(f"   Account Type: {result.account.account_type}")

                if result.statement_period_start and result.statement_period_end:
                    print(f"\n📅 Statement Period:")
                    print(f"   From: {result.statement_period_start}")
                    print(f"   To: {result.statement_period_end}")

                if result.transactions:
                    print(f"\n💰 Transaction Summary:")
                    print(f"   Total Transactions: {result.transaction_count}")
                    print(f"   Total Debits: ₹{result.total_debits:,.2f}")
                    print(f"   Total Credits: ₹{result.total_credits:,.2f}")
                    print(f"   Net Movement: ₹{(result.total_credits - result.total_debits):,.2f}")
                    print(f"   Interest Earned: ₹{result.interest_total:,.2f}")

                    # Transaction category breakdown
                    from collections import Counter
                    categories = Counter(t.category.value for t in result.transactions)
                    print(f"\n📊 Transaction Categories:")
                    for category, count in categories.most_common():
                        print(f"   {category}: {count}")

                    # Show first 10 transactions
                    print(f"\n📝 First 10 Transactions:")
                    for i, txn in enumerate(result.transactions[:10], 1):
                        sign = "+" if txn.credit > 0 else "-"
                        amount = txn.credit if txn.credit > 0 else txn.debit
                        interest_flag = " 🌟" if txn.is_interest else ""
                        print(f"\n   {i}. {txn.date} | {sign}₹{amount:,.2f} | {txn.category.value}{interest_flag}")
                        print(f"      {txn.description[:70]}...")
                        if txn.balance:
                            print(f"      Balance: ₹{txn.balance:,.2f}")

                    # Show interest transactions
                    interest_txns = [t for t in result.transactions if t.is_interest]
                    if interest_txns:
                        print(f"\n💸 Interest Transactions ({len(interest_txns)}):")
                        total_interest = sum(t.credit for t in interest_txns)
                        for i, txn in enumerate(interest_txns, 1):
                            print(f"   {i}. {txn.date}: ₹{txn.credit:,.2f} - {txn.description}")
                        print(f"\n   Total Interest: ₹{total_interest:,.2f}")

                        # Calculate 80TTA deduction
                        print(f"\n💡 Section 80TTA Analysis:")
                        print(f"   Total Interest: ₹{total_interest:,.2f}")
                        eligible_80tta = min(total_interest, 10000)
                        print(f"   Eligible u/s 80TTA (limit ₹10,000): ₹{eligible_80tta:,.2f}")
                        if total_interest > 10000:
                            print(f"   Excess (taxable): ₹{(total_interest - 10000):,.2f}")

                    # Show salary transactions
                    salary_txns = [t for t in result.transactions if t.category.value == "SALARY"]
                    if salary_txns:
                        print(f"\n💼 Salary Credits ({len(salary_txns)}):")
                        total_salary = sum(t.credit for t in salary_txns)
                        for i, txn in enumerate(salary_txns[:5], 1):  # Show first 5
                            print(f"   {i}. {txn.date}: ₹{txn.credit:,.2f}")
                        print(f"   Total Salary Credits: ₹{total_salary:,.2f}")

                if result.warnings:
                    print(f"\n⚠️  Warnings ({len(result.warnings)}):")
                    for warning in result.warnings[:5]:
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

    # Summary across all files
    print(f"\n{'=' * 80}")
    print("📊 OVERALL SUMMARY")
    print("=" * 80)

    successful = [r for r in all_results if r.success]
    failed = [r for r in all_results if not r.success]

    print(f"\nFiles Processed: {len(all_results)}")
    print(f"  ✅ Successful: {len(successful)}")
    print(f"  ❌ Failed: {len(failed)}")

    if successful:
        total_txns = sum(r.transaction_count for r in successful)
        total_debits = sum(r.total_debits for r in successful)
        total_credits = sum(r.total_credits for r in successful)
        total_interest = sum(r.interest_total for r in successful)

        print(f"\nCombined Statistics:")
        print(f"  Total Transactions: {total_txns:,}")
        print(f"  Total Debits: ₹{total_debits:,.2f}")
        print(f"  Total Credits: ₹{total_credits:,.2f}")
        print(f"  Net Movement: ₹{(total_credits - total_debits):,.2f}")
        print(f"  Total Interest: ₹{total_interest:,.2f}")

        # 80TTA calculation
        eligible_80tta = min(total_interest, 10000)
        print(f"\n💡 Combined 80TTA Analysis:")
        print(f"  Total Interest Earned: ₹{total_interest:,.2f}")
        print(f"  Eligible u/s 80TTA: ₹{eligible_80tta:,.2f}")
        if total_interest > 10000:
            print(f"  Taxable Interest: ₹{(total_interest - 10000):,.2f}")

    db_manager.close()
    DatabaseManager.reset_instance()

    print("\n" + "=" * 80)


if __name__ == "__main__":
    print("\n🚀 PFAS - Real ICICI Bank Data Test")
    test_icici_excel_parser()
    print("\n✨ Test complete!")
