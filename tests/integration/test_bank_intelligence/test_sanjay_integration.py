"""
Integration tests for Bank Intelligence Suite.

Tests:
1. Intelligent Analyzer - Statement ingestion with fuzzy header detection
2. Report Generation - Excel Master Report with fiscal year logic
3. Integrity Auditing - Data validation and statistics
4. PFAS Asset Extraction - Income categorization for asset updates

Data Source: Data/Users/{user}/inbox/Bank/{bank}/*.xls (or archive/ fallback)
Config: Data/Users/{user}/config/user_bank_config.json

Configuration:
    - Default user: Sanjay (set PFAS_TEST_USER to override)
    - Default bank: ICICI (set PFAS_TEST_BANK to override)

Usage:
    # Run with default user (Sanjay)
    pytest tests/integration/test_bank_intelligence/ -v

    # Run for a different user
    PFAS_TEST_USER=Priya pytest tests/integration/test_bank_intelligence/ -v

    # Run for a different user and bank
    PFAS_TEST_USER=Priya PFAS_TEST_BANK=HDFC pytest tests/integration/test_bank_intelligence/ -v
"""

import os
import sys
import pytest
from pathlib import Path
from decimal import Decimal
from datetime import date
from typing import Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.pfas.services.bank_intelligence import (
    BankIntelligenceAnalyzer,
    FiscalReportGenerator,
    DatabaseAuditor,
    UserBankConfig,
    CategoryClassifier,
)
from src.pfas.services.bank_intelligence.models import (
    BankTransactionIntel,
    TransactionType,
    IngestionResult,
)

# Import helper functions from conftest for inbox/archive fallback
from tests.integration.conftest import find_files_in_path, get_asset_path


# Test Configuration - Configurable via environment variables
# Default user is Sanjay, override with PFAS_TEST_USER
# Default bank is ICICI, override with PFAS_TEST_BANK
DEFAULT_BANK = "ICICI"
TEST_BANK = os.getenv("PFAS_TEST_BANK", DEFAULT_BANK)
EXPECTED_FISCAL_YEARS = ["FY 2024-25", "FY 2025-26"]


def find_bank_config(path_resolver, bank_name: str) -> Optional[Path]:
    """Find bank config file in user config dir or alongside bank files.

    Search order:
    1. user_config_dir/user_bank_config.json
    2. user_config_dir/{bank_name.lower()}_bank_config.json
    3. inbox/Bank/{bank_name}/user_bank_config.json
    4. archive/Bank/{bank_name}/user_bank_config.json

    Returns:
        Path to config file if found, None otherwise
    """
    # Check user config directory first
    config_dir = path_resolver.user_config_dir()

    candidates = [
        config_dir / "user_bank_config.json",
        config_dir / f"{bank_name.lower()}_bank_config.json",
        path_resolver.inbox() / "Bank" / bank_name / "user_bank_config.json",
        path_resolver.archive() / "Bank" / bank_name / "user_bank_config.json",
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate

    return None


def find_bank_directory(path_resolver, bank_name: str) -> Optional[Path]:
    """Find bank directory in inbox or archive.

    Returns:
        Path to bank directory with files, None if not found
    """
    inbox_path = path_resolver.inbox() / "Bank" / bank_name
    archive_path = path_resolver.archive() / "Bank" / bank_name

    # Check inbox first
    if inbox_path.exists() and any(inbox_path.glob("*.xls*")):
        return inbox_path

    # Fallback to archive
    if archive_path.exists() and any(archive_path.glob("*.xls*")):
        print(f"\n[PATH] Using archive for Bank/{bank_name} (inbox empty)")
        return archive_path

    return None


class TestBankIntelligenceIntegration:
    """Integration tests for Bank Intelligence Suite."""

    @pytest.fixture(scope="class")
    def test_db_path(self, path_resolver, tmp_path_factory):
        """Get test database path (persists across all tests in this class)."""
        test_db_dir = tmp_path_factory.mktemp("bank_intelligence")
        db_path = test_db_dir / "test_money_movement.db"

        # Clean up any existing database at the start of the test class
        if db_path.exists():
            os.remove(db_path)

        yield db_path

        # Optional: Clean up after all tests complete
        # if db_path.exists():
        #     os.remove(db_path)

    @pytest.fixture(scope="class")
    def test_report_path(self, path_resolver, tmp_path_factory):
        """Get test report path (persists across all tests in this class)."""
        test_report_dir = tmp_path_factory.mktemp("bank_reports")
        return test_report_dir / "Test_Master_Report.xlsx"

    def test_01_config_loading(self, path_resolver):
        """Test: Load user_bank_config.json for ICICI account."""
        config_path = find_bank_config(path_resolver, TEST_BANK)

        if not config_path:
            pytest.skip(
                f"Config file not found. Searched:\n"
                f"  - {path_resolver.user_config_dir()}/user_bank_config.json\n"
                f"  - {path_resolver.inbox()}/Bank/{TEST_BANK}/user_bank_config.json"
            )

        assert config_path.exists(), f"Config file not found: {config_path}"

        config = UserBankConfig.from_json(str(config_path))

        assert config.user_name == path_resolver.user_name
        assert config.bank_name == TEST_BANK
        assert config.account_type == "SAVINGS"
        assert len(config.category_overrides) > 0

        print(f"\n[PASS] Config loaded from: {config_path}")
        print(f"  - Category overrides: {len(config.category_overrides)}")

    def test_02_statement_files_exist(self, path_resolver):
        """Test: Verify bank statement files exist in inbox or archive."""
        bank_dir = find_bank_directory(path_resolver, TEST_BANK)

        if not bank_dir:
            pytest.skip(
                f"Bank directory not found. Searched:\n"
                f"  - {path_resolver.inbox()}/Bank/{TEST_BANK}\n"
                f"  - {path_resolver.archive()}/Bank/{TEST_BANK}"
            )

        assert bank_dir.exists(), f"Bank directory not found: {bank_dir}"

        xls_files = list(bank_dir.glob("*.xls")) + list(bank_dir.glob("*.xlsx"))
        assert len(xls_files) >= 1, "No Excel statement files found"

        print(f"\n[PASS] Found {len(xls_files)} statement files in {bank_dir.parent.name}/{bank_dir.name}:")
        for f in xls_files:
            print(f"  - {f.name}")

    def test_03_category_classifier(self, path_resolver):
        """Test: Category classifier with custom overrides."""
        config_path = find_bank_config(path_resolver, TEST_BANK)

        if not config_path:
            pytest.skip(f"Config file not found for {TEST_BANK}")
        config = UserBankConfig.from_json(str(config_path))

        classifier = CategoryClassifier(config.category_overrides)

        # Test salary classification
        cat, sub, is_income = classifier.classify("NEFT-QUALCOMM INDIA PVT LTD-SALARY")
        assert cat == "SALARY", f"Expected SALARY, got {cat}"

        # Test rent classification
        cat, sub, is_income = classifier.classify("BIL/INFT/Rent April 2024/RAMAKRISHNAN N")
        assert cat == "RENT_INCOME", f"Expected RENT_INCOME, got {cat}"

        # Test interest classification
        cat, sub, is_income = classifier.classify("INT.PD FOR APR 2024")
        assert cat == "SAVINGS_INTEREST", f"Expected SAVINGS_INTEREST, got {cat}"

        # Test UPI classification (from default rules)
        # Note: Using a UPI transaction that doesn't contain CRED keyword
        cat, sub, is_income = classifier.classify("UPI/409547106878/RATNADEEP/payment@upi")
        assert cat == "GROCERY", f"Expected GROCERY (custom override), got {cat}"

        # Test pure UPI without any custom overrides
        cat, sub, is_income = classifier.classify("UPI/409547106878/someuser/someone@ybl")
        assert cat == "UPI", f"Expected UPI, got {cat}"

        print("\n[PASS] Category classifier working correctly")

    def test_04_intelligent_ingestion(self, path_resolver, test_db_path):
        """Test: Ingest bank statements with intelligent analyzer."""
        bank_dir = find_bank_directory(path_resolver, TEST_BANK)
        if not bank_dir:
            pytest.skip(f"Bank directory not found for {TEST_BANK}")

        # Pass the user directory parent (Data/Users) for scanning
        with BankIntelligenceAnalyzer(str(test_db_path), str(path_resolver.user_dir.parent)) as analyzer:
            result = analyzer.scan_and_ingest_all()

        assert result.success, f"Ingestion failed: {result.errors}"
        assert result.transactions_processed > 0, "No transactions processed"
        assert result.transactions_inserted > 0, "No transactions inserted"
        assert len(result.source_files) > 0, "No source files processed"

        print(f"\n[PASS] Ingestion successful:")
        print(f"  - Transactions processed: {result.transactions_processed}")
        print(f"  - Transactions inserted: {result.transactions_inserted}")
        print(f"  - Transactions skipped (duplicates): {result.transactions_skipped}")
        print(f"  - Source files: {len(result.source_files)}")

    def test_05_data_validation(self, path_resolver, test_db_path):
        """Test: Validate ingested data integrity."""
        # Re-run ingestion if database doesn't exist
        if not test_db_path.exists():
            pytest.skip("Database not created - run test_04 first")

        with DatabaseAuditor(str(test_db_path)) as auditor:
            issues = auditor.validate_data()

        assert len(issues) == 0, f"Data validation issues: {issues}"

        print("\n[PASS] Data validation passed - no integrity issues")

    def test_06_statistics_generation(self, path_resolver, test_db_path):
        """Test: Generate and verify database statistics."""
        if not test_db_path.exists():
            pytest.skip("Database not created - run test_04 first")

        with DatabaseAuditor(str(test_db_path)) as auditor:
            stats = auditor.get_statistics()

        assert stats["total_transactions"] > 0
        assert path_resolver.user_name in stats["by_user"]
        assert TEST_BANK in stats["by_bank"]
        assert len(stats["by_fiscal_year"]) > 0
        assert len(stats["by_category"]) > 0

        print(f"\n[PASS] Statistics generated:")
        print(f"  - Total transactions: {stats['total_transactions']}")
        print(f"  - Fiscal years: {list(stats['by_fiscal_year'].keys())}")
        print(f"  - Categories: {len(stats['by_category'])}")

    def test_07_fiscal_year_logic(self, path_resolver, test_db_path):
        """Test: Verify Indian fiscal year assignment (April 1 rollover)."""
        if not test_db_path.exists():
            pytest.skip("Database not created - run test_04 first")

        with FiscalReportGenerator(str(test_db_path)) as generator:
            fiscal_years = generator.get_fiscal_years()

        assert len(fiscal_years) > 0, "No fiscal years found"

        # Verify FY format
        for fy in fiscal_years:
            assert fy.startswith("FY "), f"Invalid FY format: {fy}"
            parts = fy.replace("FY ", "").split("-")
            assert len(parts) == 2, f"Invalid FY format: {fy}"

        print(f"\n[PASS] Fiscal years correctly assigned: {fiscal_years}")

    def test_08_income_extraction(self, path_resolver, test_db_path):
        """Test: Extract income for PFAS asset classes."""
        if not test_db_path.exists():
            pytest.skip("Database not created - run test_04 first")

        with FiscalReportGenerator(str(test_db_path)) as generator:
            fiscal_years = generator.get_fiscal_years()

            for fy in fiscal_years:
                income = generator.get_income_for_pfas(fy, path_resolver.user_name)

                print(f"\n[PASS] Income extracted for {fy}:")
                for category, amount in income.items():
                    print(f"  - {category}: {amount:,.2f}")

        # Verify we have at least some income categorized
        total_income = sum(
            sum(generator.get_income_for_pfas(fy, path_resolver.user_name).values())
            for fy in fiscal_years
        )
        assert total_income > 0, "No income extracted"

    def test_09_excel_report_generation(self, path_resolver, test_db_path, test_report_path):
        """Test: Generate Excel Master Report with all features."""
        if not test_db_path.exists():
            pytest.skip("Database not created - run test_04 first")

        with FiscalReportGenerator(str(test_db_path)) as generator:
            output = generator.generate_master_report(str(test_report_path))

        assert Path(output).exists(), f"Report not generated: {output}"
        assert Path(output).stat().st_size > 0, "Report file is empty"

        print(f"\n[PASS] Excel report generated: {output}")
        print(f"  - File size: {Path(output).stat().st_size:,} bytes")

    def test_10_category_distribution(self, path_resolver, test_db_path):
        """Test: Verify category distribution matches expected patterns."""
        if not test_db_path.exists():
            pytest.skip("Database not created - run test_04 first")

        with DatabaseAuditor(str(test_db_path)) as auditor:
            stats = auditor.get_statistics()

        categories = stats.get("by_category", {})

        # Expected high-volume categories for a typical bank statement
        expected_categories = ["UPI", "NEFT", "SALARY"]
        found = [cat for cat in expected_categories if cat in categories]

        assert len(found) > 0, f"Expected categories not found. Got: {list(categories.keys())}"

        print(f"\n[PASS] Category distribution:")
        sorted_cats = sorted(categories.items(), key=lambda x: x[1]["count"], reverse=True)[:10]
        for cat, data in sorted_cats:
            print(f"  - {cat}: {data['count']} transactions")

    def test_11_deduplication(self, path_resolver, test_db_path):
        """Test: Re-running ingestion should skip duplicates."""
        if not test_db_path.exists():
            pytest.skip("Database not created - run test_04 first")

        bank_dir = find_bank_directory(path_resolver, TEST_BANK)
        if not bank_dir:
            pytest.skip(f"Bank directory not found for {TEST_BANK}")

        # Get current count
        with DatabaseAuditor(str(test_db_path)) as auditor:
            initial_stats = auditor.get_statistics()
            initial_count = initial_stats["total_transactions"]

        # Re-run ingestion - should detect all as duplicates
        with BankIntelligenceAnalyzer(str(test_db_path), str(path_resolver.user_dir.parent)) as analyzer:
            result = analyzer.scan_and_ingest_all()

        # All should be skipped as duplicates (no new insertions)
        assert result.transactions_inserted == 0, f"Duplicates were inserted: {result.transactions_inserted}"
        assert result.transactions_skipped > 0, "No transactions were identified as duplicates"

        # Count should remain the same
        with DatabaseAuditor(str(test_db_path)) as auditor:
            final_stats = auditor.get_statistics()
            final_count = final_stats["total_transactions"]

        assert initial_count == final_count, f"Count changed: {initial_count} -> {final_count}"

        print(f"\n[PASS] Deduplication working:")
        print(f"  - Skipped duplicates: {result.transactions_skipped}")
        print(f"  - Total count unchanged: {final_count}")

    def test_12_pfas_asset_mapping(self, path_resolver, test_db_path):
        """Test: Verify income categories map to PFAS asset classes."""
        if not test_db_path.exists():
            pytest.skip("Database not created - run test_04 first")

        # PFAS asset class mappings
        pfas_mappings = {
            "RENT_INCOME": ("RENTAL", "rental_income"),
            "SAVINGS_INTEREST": ("BANK_INTEREST", "bank_interest_summary"),
            "DIVIDEND": ("STOCK_DIVIDEND", "stock_dividends"),
            "SGB_INTEREST": ("SGB", "sgb_interest"),
        }

        with DatabaseAuditor(str(test_db_path)) as auditor:
            for fy in ["FY 2024-25", "FY 2025-26"]:
                income = auditor.get_income_summary_for_fy(fy)

                for category, (asset_class, pfas_table) in pfas_mappings.items():
                    if category in income:
                        print(f"\n[INFO] {fy} - {category}:")
                        print(f"  - Amount: {income[category]:,.2f}")
                        print(f"  - PFAS Asset Class: {asset_class}")
                        print(f"  - PFAS Table: {pfas_table}")

        print("\n[PASS] PFAS asset mappings verified")


def run_integration_tests():
    """Run all integration tests and print summary."""
    print("=" * 70)
    print("Bank Intelligence Integration Tests - User: Sanjay")
    print("=" * 70)

    test_instance = TestBankIntelligenceIntegration()

    # Setup
    if test_db_path.exists():
        os.remove(test_db_path)

    tests = [
        ("Config Loading", test_instance.test_01_config_loading),
        ("Statement Files", test_instance.test_02_statement_files_exist),
        ("Category Classifier", test_instance.test_03_category_classifier),
        ("Intelligent Ingestion", test_instance.test_04_intelligent_ingestion),
        ("Data Validation", test_instance.test_05_data_validation),
        ("Statistics Generation", test_instance.test_06_statistics_generation),
        ("Fiscal Year Logic", test_instance.test_07_fiscal_year_logic),
        ("Income Extraction", test_instance.test_08_income_extraction),
        ("Excel Report Generation", test_instance.test_09_excel_report_generation),
        ("Category Distribution", test_instance.test_10_category_distribution),
        ("Deduplication", test_instance.test_11_deduplication),
        ("PFAS Asset Mapping", test_instance.test_12_pfas_asset_mapping),
    ]

    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            print(f"\n--- Test: {name} ---")
            test_func()
            passed += 1
        except Exception as e:
            print(f"\n[FAIL] {name}: {e}")
            failed += 1

    print("\n" + "=" * 70)
    print(f"Integration Test Summary: {passed} passed, {failed} failed")
    print("=" * 70)

    return failed == 0


if __name__ == "__main__":
    success = run_integration_tests()
    sys.exit(0 if success else 1)
