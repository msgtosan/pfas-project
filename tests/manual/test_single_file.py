"""
Test parser on SanjaySB_FY24-25.xls file.
"""

import sys
from pathlib import Path
from collections import Counter

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pfas.core.database import DatabaseManager
from pfas.core.accounts import setup_chart_of_accounts
from pfas.parsers.bank.icici_excel import ICICIExcelParser

# Test credentials
TEST_DB_PASSWORD = "test_password_123"
TEST_MASTER_KEY = b"test_master_key_32_bytes_long!!"


def main():
    """Test parsing SanjaySB_FY24-25.xls."""
    print("\n" + "=" * 100)
    print("PFAS BANK PARSER - DETAILED TEST")
    print("File: SanjaySB_FY24-25.xls")
    print("=" * 100)

    # Initialize database
    DatabaseManager.reset_instance()
    db_manager = DatabaseManager()
    conn = db_manager.init(":memory:", TEST_DB_PASSWORD)
    setup_chart_of_accounts(conn)

    # Create parser
    parser = ICICIExcelParser(conn, TEST_MASTER_KEY)

    # Test file
    file_path = "Data/Users/Sanjay/Bank/ICICI/SanjaySB_FY24-25.xls"

    if not Path(file_path).exists():
        print(f"\n❌ ERROR: File not found: {file_path}")
        return

    print(f"\n📄 Parsing file...")
    print("-" * 100)

    try:
        result = parser.parse(file_path)

        if not result.success:
            print(f"❌ FAILED to parse file")
            if result.errors:
                print(f"\n🔴 Errors:")
                for error in result.errors:
                    print(f"   - {error}")
            return

        print(f"✅ SUCCESS: Parsed {len(result.transactions)} transactions")

        # Account Information
        print(f"\n{'─' * 100}")
        print("📋 ACCOUNT INFORMATION")
        print("─" * 100)
        if result.account:
            print(f"Bank Name:          {result.account.bank_name}")
            print(f"Account Number:     {result.account.account_number}")
            print(f"Masked Number:      {result.account.masked_number}")
            print(f"Account Type:       {result.account.account_type}")

        # Statement Period
        print(f"\n{'─' * 100}")
        print("📅 STATEMENT PERIOD")
        print("─" * 100)
        if result.statement_period_start and result.statement_period_end:
            print(f"From:               {result.statement_period_start}")
            print(f"To:                 {result.statement_period_end}")
            days = (result.statement_period_end - result.statement_period_start).days
            print(f"Duration:           {days} days")

        # Financial Summary
        print(f"\n{'─' * 100}")
        print("💰 FINANCIAL SUMMARY")
        print("─" * 100)
        print(f"Total Transactions: {result.transaction_count:,}")
        print(f"Total Debits:       ₹{result.total_debits:,.2f}")
        print(f"Total Credits:      ₹{result.total_credits:,.2f}")
        print(f"Net Movement:       ₹{(result.total_credits - result.total_debits):,.2f}")
        print(f"Interest Earned:    ₹{result.interest_total:,.2f}")

        # Opening and Closing Balance
        if result.transactions:
            first_txn = result.transactions[0]
            last_txn = result.transactions[-1]
            if first_txn.balance and last_txn.balance:
                opening = first_txn.balance - first_txn.credit + first_txn.debit
                print(f"\nOpening Balance:    ₹{opening:,.2f} (approx)")
                print(f"Closing Balance:    ₹{last_txn.balance:,.2f}")

        # Category Breakdown
        print(f"\n{'─' * 100}")
        print("📊 TRANSACTION CATEGORIES")
        print("─" * 100)
        categories = Counter(t.category.value for t in result.transactions)

        print(f"{'Category':<20} {'Count':>8} {'Percentage':>12} {'Amount (Debit)':>18} {'Amount (Credit)':>18}")
        print("-" * 100)

        for category, count in categories.most_common():
            cat_txns = [t for t in result.transactions if t.category.value == category]
            total_debit = sum(t.debit for t in cat_txns)
            total_credit = sum(t.credit for t in cat_txns)
            percentage = (count / result.transaction_count) * 100

            print(f"{category:<20} {count:>8,} {percentage:>11.1f}% {total_debit:>17,.2f} {total_credit:>17,.2f}")

        # Interest Transactions Detail
        interest_txns = [t for t in result.transactions if t.is_interest]
        if interest_txns:
            print(f"\n{'─' * 100}")
            print(f"💸 INTEREST TRANSACTIONS ({len(interest_txns)})")
            print("─" * 100)
            print(f"{'Date':<15} {'Amount':>15} {'Description':<70}")
            print("-" * 100)

            total_interest = 0
            for txn in interest_txns:
                total_interest += txn.credit
                desc = txn.description[:67] + "..." if len(txn.description) > 70 else txn.description
                print(f"{str(txn.date):<15} ₹{txn.credit:>13,.2f} {desc:<70}")

            print("-" * 100)
            print(f"{'TOTAL':<15} ₹{total_interest:>13,.2f}")

            # 80TTA Calculation
            print(f"\n💡 Section 80TTA Analysis:")
            print(f"   Total Interest Earned:        ₹{total_interest:,.2f}")
            eligible = min(total_interest, 10000)
            print(f"   Eligible u/s 80TTA (max 10K): ₹{eligible:,.2f}")
            if total_interest > 10000:
                print(f"   Excess (Taxable):             ₹{(total_interest - 10000):,.2f} ⚠️")

        # Salary Transactions Detail
        salary_txns = [t for t in result.transactions if t.category.value == "SALARY"]
        if salary_txns:
            print(f"\n{'─' * 100}")
            print(f"💼 SALARY TRANSACTIONS ({len(salary_txns)})")
            print("─" * 100)
            print(f"{'Date':<15} {'Amount':>15} {'Description':<70}")
            print("-" * 100)

            total_salary = 0
            for txn in salary_txns[:10]:  # Show first 10
                total_salary += txn.credit
                desc = txn.description[:67] + "..." if len(txn.description) > 70 else txn.description
                print(f"{str(txn.date):<15} ₹{txn.credit:>13,.2f} {desc:<70}")

            if len(salary_txns) > 10:
                print(f"... and {len(salary_txns) - 10} more salary transactions")

            total_all_salary = sum(t.credit for t in salary_txns)
            print("-" * 100)
            print(f"{'TOTAL':<15} ₹{total_all_salary:>13,.2f}")

        # Sample Transactions by Category
        print(f"\n{'─' * 100}")
        print("📝 SAMPLE TRANSACTIONS (First 20)")
        print("─" * 100)
        print(f"{'#':<5} {'Date':<12} {'Category':<15} {'Debit':>15} {'Credit':>15} {'Balance':>15}")
        print("-" * 100)

        for i, txn in enumerate(result.transactions[:20], 1):
            interest_flag = " ⭐" if txn.is_interest else ""
            category = txn.category.value + interest_flag
            print(f"{i:<5} {str(txn.date):<12} {category:<15} "
                  f"₹{txn.debit:>13,.2f} ₹{txn.credit:>13,.2f} ₹{txn.balance:>13,.2f}")

        # UPI Analysis
        upi_txns = [t for t in result.transactions if t.category.value == "UPI"]
        if upi_txns:
            print(f"\n{'─' * 100}")
            print(f"📱 UPI TRANSACTION ANALYSIS ({len(upi_txns)} transactions)")
            print("─" * 100)

            upi_debits = sum(t.debit for t in upi_txns)
            upi_credits = sum(t.credit for t in upi_txns)

            print(f"Total UPI Debits:   ₹{upi_debits:,.2f}")
            print(f"Total UPI Credits:  ₹{upi_credits:,.2f}")
            print(f"Net UPI Outflow:    ₹{(upi_debits - upi_credits):,.2f}")

            # Show sample UPI transactions
            print(f"\nSample UPI Transactions (First 10):")
            print(f"{'Date':<15} {'Type':<8} {'Amount':>15} {'Description':<55}")
            print("-" * 100)

            for txn in upi_txns[:10]:
                txn_type = "Debit" if txn.debit > 0 else "Credit"
                amount = txn.debit if txn.debit > 0 else txn.credit
                desc = txn.description[:52] + "..." if len(txn.description) > 55 else txn.description
                print(f"{str(txn.date):<15} {txn_type:<8} ₹{amount:>13,.2f} {desc:<55}")

        # Monthly Breakdown
        print(f"\n{'─' * 100}")
        print("📆 MONTHLY TRANSACTION SUMMARY")
        print("─" * 100)

        from collections import defaultdict
        monthly = defaultdict(lambda: {"count": 0, "debit": 0, "credit": 0})

        for txn in result.transactions:
            month_key = txn.date.strftime("%Y-%m")
            monthly[month_key]["count"] += 1
            monthly[month_key]["debit"] += txn.debit
            monthly[month_key]["credit"] += txn.credit

        print(f"{'Month':<10} {'Transactions':>15} {'Debits':>18} {'Credits':>18} {'Net':>18}")
        print("-" * 100)

        for month in sorted(monthly.keys()):
            data = monthly[month]
            net = data["credit"] - data["debit"]
            print(f"{month:<10} {data['count']:>15,} ₹{data['debit']:>16,.2f} "
                  f"₹{data['credit']:>16,.2f} ₹{net:>16,.2f}")

        # Warnings
        if result.warnings:
            print(f"\n{'─' * 100}")
            print(f"⚠️  WARNINGS ({len(result.warnings)})")
            print("─" * 100)
            for warning in result.warnings:
                print(f"   - {warning}")

    except Exception as e:
        print(f"❌ EXCEPTION: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

    finally:
        db_manager.close()
        DatabaseManager.reset_instance()

    print(f"\n{'=' * 100}")
    print("✨ Test Complete!")
    print("=" * 100 + "\n")


if __name__ == "__main__":
    main()
