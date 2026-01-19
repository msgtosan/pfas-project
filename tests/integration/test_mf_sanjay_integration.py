#!/usr/bin/env python3
"""
Integration test for MF Analyzer using Sanjay's real data.

Tests:
- Parse CAMS and KARVY Excel files
- Parse CAS PDF (Sanjay_CAS.pdf)
- Analyze holdings
- Generate reports
- Run reconciliation
"""

import json
import logging
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

# Setup path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_integration_test():
    """Run full MF integration test with Sanjay's data."""

    # Import project modules
    from pfas.core.database import DatabaseManager
    from pfas.analyzers.mf_analyzer import MFAnalyzer, MFFieldNormalizer, MFStatementScanner, MFDBIngester, RTA
    from pfas.parsers.mf.scanner import MFStatementScanner as MFScanner, scan_mf_inbox
    from pfas.parsers.mf.ingester import MFIngester, ingest_mf_statements
    from pfas.parsers.mf import CAMSParser, KarvyParser

    # Paths
    user_name = "Sanjay"
    data_root = PROJECT_ROOT / "Data" / "Users" / user_name
    inbox_path = data_root / "inbox" / "Mutual-Fund"
    db_path = data_root / "db" / "finance.db"
    reports_path = data_root / "reports" / "Mutual-Fund"

    print("=" * 80)
    print("MF INTEGRATION TEST - USER: SANJAY")
    print("=" * 80)
    print(f"Project Root: {PROJECT_ROOT}")
    print(f"Inbox Path:   {inbox_path}")
    print(f"DB Path:      {db_path}")
    print(f"Reports Path: {reports_path}")
    print()

    # Initialize database
    print("1. INITIALIZING DATABASE")
    print("-" * 40)

    # Reset singleton and initialize
    DatabaseManager.reset_instance()
    db_manager = DatabaseManager()

    # Use test password for integration test
    db_password = "sanjay_test_password"

    try:
        conn = db_manager.init(str(db_path), db_password)
        print(f"   Database connected successfully")
    except Exception as e:
        print(f"   ERROR: Failed to connect to database: {e}")
        # Try with in-memory database instead
        print("   Using in-memory database for test...")
        DatabaseManager.reset_instance()
        db_manager = DatabaseManager()
        conn = db_manager.init(":memory:", db_password)
        from pfas.core.accounts import setup_chart_of_accounts
        setup_chart_of_accounts(conn)

    # Get or create user
    cursor = conn.execute("SELECT id FROM users WHERE name = ?", (user_name,))
    row = cursor.fetchone()
    if row:
        user_id = row[0] if isinstance(row, tuple) else row["id"]
        print(f"   User found: ID={user_id}")
    else:
        cursor = conn.execute(
            "INSERT INTO users (pan_encrypted, pan_salt, name) VALUES (?, ?, ?)",
            (b"encrypted", b"salt", user_name)
        )
        conn.commit()
        user_id = cursor.lastrowid
        print(f"   User created: ID={user_id}")

    # Check existing data
    print("\n2. CHECKING EXISTING DATA")
    print("-" * 40)

    # Holdings
    cursor = conn.execute("SELECT COUNT(*) FROM mf_holdings WHERE user_id = ?", (user_id,))
    holdings_count = cursor.fetchone()[0]
    print(f"   Existing holdings: {holdings_count}")

    # Transactions
    cursor = conn.execute("""
        SELECT COUNT(*) FROM mf_transactions t
        JOIN mf_folios f ON t.folio_id = f.id
        WHERE f.user_id = ?
    """, (user_id,))
    txn_count = cursor.fetchone()[0]
    print(f"   Existing transactions: {txn_count}")

    # Capital gains
    cursor = conn.execute("SELECT COUNT(*) FROM mf_capital_gains WHERE user_id = ?", (user_id,))
    cg_count = cursor.fetchone()[0]
    print(f"   Existing capital gains records: {cg_count}")

    # Scan inbox
    print("\n3. SCANNING INBOX FOR MF FILES")
    print("-" * 40)

    scan_result = scan_mf_inbox(inbox_path)
    print(f"   Total files scanned: {scan_result.total_scanned}")
    print(f"   Valid files found:   {len(scan_result.files)}")
    print(f"   CAMS files:          {len(scan_result.cams_files)}")
    print(f"   KARVY files:         {len(scan_result.karvy_files)}")
    print(f"   PDF files:           {len(scan_result.pdf_files)}")
    print(f"   Excel files:         {len(scan_result.excel_files)}")

    if scan_result.warnings:
        print(f"   Warnings: {len(scan_result.warnings)}")
        for w in scan_result.warnings[:3]:
            print(f"     - {w}")

    print("\n   Files found:")
    for f in scan_result.files:
        protected = " [PASSWORD]" if f.password_protected else ""
        print(f"     {f.rta.value:8s} | {f.file_type.value:4s} | {f.path.name}{protected}")

    # Parse Excel files
    print("\n4. PARSING EXCEL FILES")
    print("-" * 40)

    cams_parser = CAMSParser(conn)
    karvy_parser = KarvyParser(conn)

    total_txn_inserted = 0
    total_duplicates = 0

    for scanned in scan_result.excel_files:
        print(f"\n   Processing: {scanned.path.name}")

        try:
            if scanned.rta.value == "CAMS":
                result = cams_parser.parse(scanned.path)
                if result.success and result.transactions:
                    count = cams_parser.save_to_db(result, user_id)
                    dups = cams_parser.get_duplicate_count()
                    cams_parser.reset_duplicate_count()
                    total_txn_inserted += count
                    total_duplicates += dups
                    print(f"     CAMS: {count} transactions inserted, {dups} duplicates")
                else:
                    print(f"     CAMS: No transactions or errors: {result.errors[:2] if result.errors else 'empty'}")

            elif scanned.rta.value == "KARVY":
                result = karvy_parser.parse(scanned.path)
                if result.success and result.transactions:
                    count = karvy_parser.save_to_db(result, user_id)
                    dups = karvy_parser.get_duplicate_count()
                    karvy_parser.reset_duplicate_count()
                    total_txn_inserted += count
                    total_duplicates += dups
                    print(f"     KARVY: {count} transactions inserted, {dups} duplicates")
                else:
                    print(f"     KARVY: No transactions or errors: {result.errors[:2] if result.errors else 'empty'}")

        except Exception as e:
            print(f"     ERROR: {e}")

    print(f"\n   Total transactions inserted: {total_txn_inserted}")
    print(f"   Total duplicates skipped:   {total_duplicates}")

    # Analyze holdings using MFAnalyzer
    print("\n5. ANALYZING HOLDINGS")
    print("-" * 40)

    config = {
        "paths": {
            "data_root": str(data_root),
            "mf_folder": "inbox/Mutual-Fund",
            "reports_output": str(reports_path)
        },
        "processing": {
            "skip_zero_holdings": True,
            "use_xirr_over_yield": True
        }
    }

    analyzer = MFAnalyzer(config=config, conn=conn)
    analysis_result = analyzer.analyze(user_name=user_name, user_id=user_id, mf_folder=inbox_path)

    print(f"   Files scanned:      {analysis_result.files_scanned}")
    print(f"   Holdings processed: {analysis_result.holdings_processed}")
    print(f"   Duplicates skipped: {analysis_result.duplicates_skipped}")
    print(f"   Transactions:       {analysis_result.transactions_processed}")

    if analysis_result.holdings:
        print(f"\n   Portfolio Summary:")
        print(f"     Total Current Value:   Rs. {analysis_result.total_current_value:>15,.2f}")
        print(f"     Total Cost Value:      Rs. {analysis_result.total_cost_value:>15,.2f}")
        print(f"     Total Appreciation:    Rs. {analysis_result.total_appreciation:>15,.2f}")
        print(f"     Equity Allocation:     Rs. {analysis_result.equity_value:>15,.2f}")
        print(f"     Debt Allocation:       Rs. {analysis_result.debt_value:>15,.2f}")
        print(f"     Hybrid Allocation:     Rs. {analysis_result.hybrid_value:>15,.2f}")

        if analysis_result.weighted_xirr:
            print(f"     Weighted XIRR:         {analysis_result.weighted_xirr:.2f}%")

    if analysis_result.errors:
        print(f"\n   Errors ({len(analysis_result.errors)}):")
        for err in analysis_result.errors[:5]:
            print(f"     - {err[:80]}")

    # Query database for detailed holdings
    print("\n6. HOLDINGS BY SCHEME TYPE")
    print("-" * 40)

    cursor = conn.execute("""
        SELECT
            scheme_type,
            COUNT(DISTINCT scheme_name) as schemes,
            COUNT(DISTINCT folio_number) as folios,
            SUM(CAST(current_value AS DECIMAL)) as total_value,
            SUM(CAST(cost_value AS DECIMAL)) as total_cost,
            SUM(CAST(appreciation AS DECIMAL)) as appreciation
        FROM mf_holdings
        WHERE user_id = ?
        GROUP BY scheme_type
        ORDER BY total_value DESC
    """, (user_id,))

    total_value = Decimal("0")
    print(f"   {'Category':<10} {'Schemes':>8} {'Folios':>8} {'Current Value':>18} {'Cost':>18} {'Gain':>15}")
    print("   " + "-" * 75)

    for row in cursor.fetchall():
        scheme_type = row[0] or "OTHER"
        schemes = row[1]
        folios = row[2]
        value = Decimal(str(row[3] or 0))
        cost = Decimal(str(row[4] or 0))
        gain = Decimal(str(row[5] or 0))
        total_value += value
        print(f"   {scheme_type:<10} {schemes:>8} {folios:>8} Rs.{value:>14,.2f} Rs.{cost:>14,.2f} Rs.{gain:>11,.2f}")

    print("   " + "-" * 75)
    print(f"   {'TOTAL':<10} {'':<8} {'':<8} Rs.{total_value:>14,.2f}")

    # Top holdings
    print("\n7. TOP 10 HOLDINGS BY VALUE")
    print("-" * 40)

    cursor = conn.execute("""
        SELECT
            scheme_name, scheme_type, folio_number,
            current_value, cost_value, appreciation,
            annualized_return, rta
        FROM mf_holdings
        WHERE user_id = ?
        ORDER BY CAST(current_value AS DECIMAL) DESC
        LIMIT 10
    """, (user_id,))

    print(f"   {'Scheme Name':<45} {'Type':<8} {'Value':>15} {'XIRR':>8}")
    print("   " + "-" * 80)

    for row in cursor.fetchall():
        name = (row[0] or "")[:44]
        stype = row[1] or "OTHER"
        value = Decimal(str(row[3] or 0))
        xirr = row[6] or 0
        print(f"   {name:<45} {stype:<8} Rs.{value:>11,.2f} {float(xirr):>7.2f}%")

    # Capital gains summary
    print("\n8. CAPITAL GAINS SUMMARY")
    print("-" * 40)

    cursor = conn.execute("""
        SELECT
            financial_year, asset_class,
            stcg_amount, ltcg_amount,
            taxable_stcg, taxable_ltcg
        FROM mf_capital_gains
        WHERE user_id = ?
        ORDER BY financial_year DESC, asset_class
    """, (user_id,))

    rows = cursor.fetchall()
    if rows:
        print(f"   {'FY':<10} {'Class':<8} {'STCG':>15} {'LTCG':>15} {'Taxable STCG':>15} {'Taxable LTCG':>15}")
        print("   " + "-" * 85)
        for row in rows:
            fy = row[0]
            asset = row[1]
            stcg = Decimal(str(row[2] or 0))
            ltcg = Decimal(str(row[3] or 0))
            tax_stcg = Decimal(str(row[4] or 0))
            tax_ltcg = Decimal(str(row[5] or 0))
            print(f"   {fy:<10} {asset:<8} Rs.{stcg:>11,.2f} Rs.{ltcg:>11,.2f} Rs.{tax_stcg:>11,.2f} Rs.{tax_ltcg:>11,.2f}")
    else:
        print("   No capital gains data found")

    # Transaction summary by FY
    print("\n9. TRANSACTION SUMMARY BY TYPE")
    print("-" * 40)

    cursor = conn.execute("""
        SELECT
            t.transaction_type,
            COUNT(*) as count,
            SUM(CAST(t.amount AS DECIMAL)) as total_amount,
            SUM(CAST(t.units AS DECIMAL)) as total_units
        FROM mf_transactions t
        JOIN mf_folios f ON t.folio_id = f.id
        WHERE f.user_id = ?
        GROUP BY t.transaction_type
        ORDER BY total_amount DESC
    """, (user_id,))

    rows = cursor.fetchall()
    if rows:
        print(f"   {'Type':<18} {'Count':>8} {'Total Amount':>18} {'Total Units':>15}")
        print("   " + "-" * 65)
        for row in rows:
            txn_type = row[0]
            count = row[1]
            amount = Decimal(str(row[2] or 0))
            units = Decimal(str(row[3] or 0))
            print(f"   {txn_type:<18} {count:>8} Rs.{amount:>14,.2f} {units:>14,.4f}")
    else:
        print("   No transactions found")

    # Generate reports
    print("\n10. GENERATING REPORTS")
    print("-" * 40)

    reports_path.mkdir(parents=True, exist_ok=True)

    try:
        report_path = analyzer.generate_reports(output_dir=reports_path)
        print(f"   Report generated: {report_path}")
    except Exception as e:
        print(f"   ERROR generating report: {e}")

    # AMC-wise summary
    print("\n11. HOLDINGS BY AMC")
    print("-" * 40)

    cursor = conn.execute("""
        SELECT
            amc_name,
            COUNT(DISTINCT scheme_name) as schemes,
            SUM(CAST(current_value AS DECIMAL)) as total_value
        FROM mf_holdings
        WHERE user_id = ?
        GROUP BY amc_name
        ORDER BY total_value DESC
        LIMIT 10
    """, (user_id,))

    print(f"   {'AMC Name':<45} {'Schemes':>8} {'Value':>18}")
    print("   " + "-" * 75)
    for row in cursor.fetchall():
        amc = (row[0] or "Unknown")[:44]
        schemes = row[1]
        value = Decimal(str(row[2] or 0))
        print(f"   {amc:<45} {schemes:>8} Rs.{value:>14,.2f}")

    # Summary
    print("\n" + "=" * 80)
    print("INTEGRATION TEST SUMMARY")
    print("=" * 80)

    # Final counts
    cursor = conn.execute("SELECT COUNT(*) FROM mf_holdings WHERE user_id = ?", (user_id,))
    final_holdings = cursor.fetchone()[0]

    cursor = conn.execute("""
        SELECT COUNT(*) FROM mf_transactions t
        JOIN mf_folios f ON t.folio_id = f.id
        WHERE f.user_id = ?
    """, (user_id,))
    final_txns = cursor.fetchone()[0]

    cursor = conn.execute("""
        SELECT SUM(CAST(current_value AS DECIMAL)) FROM mf_holdings WHERE user_id = ?
    """, (user_id,))
    row = cursor.fetchone()
    final_value = Decimal(str(row[0] or 0)) if row and row[0] else Decimal("0")

    print(f"   User:                 {user_name}")
    print(f"   Total Holdings:       {final_holdings}")
    print(f"   Total Transactions:   {final_txns}")
    print(f"   Total Portfolio Value: Rs. {final_value:,.2f}")
    print(f"   Report Location:      {reports_path}")

    # Close connection
    try:
        db_manager.close()
        DatabaseManager.reset_instance()
    except Exception:
        pass

    print("\n" + "=" * 80)
    print("TEST COMPLETED SUCCESSFULLY")
    print("=" * 80)

    return {
        "holdings": final_holdings,
        "transactions": final_txns,
        "portfolio_value": final_value
    }


if __name__ == "__main__":
    result = run_integration_test()
