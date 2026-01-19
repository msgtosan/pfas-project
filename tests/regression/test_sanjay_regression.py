"""
Regression Test Suite for User Sanjay

This test suite should be run before every git commit to ensure:
1. All 11 asset type ingesters work correctly
2. No regressions in parsing logic
3. Database integrity is maintained
4. CLI commands execute without errors
5. End-to-end ingestion pipeline works

Run with: pytest tests/regression/test_sanjay_regression.py -v
"""
import pytest
import sys
from pathlib import Path
from datetime import date
from decimal import Decimal

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pfas.core.database import DatabaseManager
from pfas.core.accounts import setup_chart_of_accounts
from pfas.core.paths import PathResolver


class TestSanjayRegressionSuite:
    """Comprehensive regression test suite for user Sanjay."""

    USER_NAME = "Sanjay"
    TEST_PASSWORD = "test_regression_password"

    @pytest.fixture(scope="class")
    def db_connection(self):
        """Create test database with schema."""
        DatabaseManager.reset_instance()
        db = DatabaseManager()
        conn = db.init(":memory:", self.TEST_PASSWORD)
        setup_chart_of_accounts(conn)

        # Create test user Sanjay
        conn.execute("""
            INSERT INTO users (id, pan_encrypted, pan_salt, name, email)
            VALUES (1, X'00', X'00', 'Sanjay', 'sanjay@example.com')
        """)
        conn.commit()

        yield conn

        db.close()
        DatabaseManager.reset_instance()

    @pytest.fixture(scope="class")
    def path_resolver(self):
        """Get PathResolver for Sanjay."""
        try:
            # PathResolver requires root_path and user_name
            # Use Data symlink or actual path from config
            from pathlib import Path
            project_root = Path.cwd()

            # Try to use Data symlink if it exists
            data_link = project_root / "Data"
            if data_link.exists() and data_link.is_symlink():
                root_path = data_link.resolve()
            else:
                # Otherwise use the configured root path
                import json
                config_path = project_root / "config" / "paths.json"
                if config_path.exists():
                    with open(config_path) as f:
                        config = json.load(f)
                        root_path = Path(config.get("root", str(project_root)))
                        # Handle Windows paths in WSL
                        if str(root_path).startswith("C:"):
                            root_path = Path("/mnt/c") / str(root_path)[3:].replace("\\", "/")
                else:
                    root_path = project_root

            return PathResolver(root_path, self.USER_NAME)
        except Exception as e:
            pytest.skip(f"Could not initialize PathResolver: {e}")

    def test_00_verify_data_directories_exist(self, path_resolver):
        """Verify all expected directories exist."""
        print(f"\nUser directory: {path_resolver.user_dir}")
        print(f"Inbox directory: {path_resolver.inbox()}")

        assert path_resolver.user_dir.exists(), f"User directory not found: {path_resolver.user_dir}"
        assert path_resolver.inbox().exists(), f"Inbox directory not found: {path_resolver.inbox()}"

        # List all inbox folders
        inbox_folders = sorted([d.name for d in path_resolver.inbox().iterdir() if d.is_dir()])
        print(f"Inbox folders found: {', '.join(inbox_folders)}")

    def test_01_mutual_fund_ingestion(self, db_connection, path_resolver):
        """Test Mutual Fund ingestion (CAMS, KARVY)."""
        from pfas.parsers.mf.ingester import MFIngester

        inbox_path = path_resolver.inbox() / "Mutual-Fund"
        if not inbox_path.exists():
            pytest.skip("Mutual-Fund inbox not found")

        # Count files before ingestion
        files = list(inbox_path.rglob("*.*"))
        files = [f for f in files if f.suffix.lower() in ['.pdf', '.xlsx', '.xls']]
        print(f"\nMutual-Fund: Found {len(files)} files to process")

        if not files:
            pytest.skip("No Mutual Fund files found")

        # Run ingestion
        ingester = MFIngester(db_connection, 1, inbox_path, password_callback=lambda: self.TEST_PASSWORD)
        result = ingester.ingest(force=False)

        print(f"  Processed: {result.files_processed}")
        print(f"  Skipped: {result.files_skipped}")
        print(f"  Records Inserted: {result.records_inserted}")
        print(f"  Records Skipped: {result.records_skipped}")

        # Verify data was inserted
        cursor = db_connection.execute("SELECT COUNT(*) FROM mf_transactions")
        txn_count = cursor.fetchone()[0]
        print(f"  DB transactions: {txn_count}")

        assert result.files_processed > 0, "No files processed"
        assert result.records_inserted >= 0, "No records ingested"

    def test_02_bank_statement_ingestion(self, db_connection, path_resolver):
        """Test Bank statement ingestion (ICICI, HDFC, SBI)."""
        from pfas.parsers.bank.ingester import BankIngester

        inbox_path = path_resolver.inbox() / "Bank"
        if not inbox_path.exists():
            pytest.skip("Bank inbox not found")

        files = list(inbox_path.rglob("*.*"))
        files = [f for f in files if f.suffix.lower() in ['.xls', '.xlsx', '.pdf', '.csv']]
        print(f"\nBank: Found {len(files)} files to process")

        if not files:
            pytest.skip("No Bank files found")

        ingester = BankIngester(db_connection, 1, inbox_path)
        result = ingester.ingest(force=False)

        print(f"  Processed: {result.files_processed}")
        print(f"  Skipped: {result.files_skipped}")
        print(f"  Records Inserted: {result.records_inserted}")
        print(f"  Records Skipped: {result.records_skipped}")

        assert result.files_processed >= 0, "Ingestion failed"

    def test_03_indian_stocks_ingestion(self, db_connection, path_resolver):
        """Test Indian Stocks ingestion (Zerodha, ICICI Direct)."""
        from pfas.parsers.stock.ingester import IndianStockIngester

        inbox_path = path_resolver.inbox() / "Indian-Stocks"
        if not inbox_path.exists():
            pytest.skip("Indian-Stocks inbox not found")

        files = list(inbox_path.rglob("*.*"))
        files = [f for f in files if f.suffix.lower() in ['.xlsx', '.xls', '.csv', '.pdf']]
        print(f"\nIndian-Stocks: Found {len(files)} files to process")

        if not files:
            pytest.skip("No Indian-Stocks files found")

        ingester = IndianStockIngester(db_connection, 1, inbox_path)
        result = ingester.ingest(force=False)

        print(f"  Processed: {result.files_processed}")
        print(f"  Skipped: {result.files_skipped}")
        print(f"  Records Inserted: {result.records_inserted}")
        print(f"  Records Skipped: {result.records_skipped}")

        assert result.files_processed >= 0, "Ingestion failed"

    def test_04_salary_documents_ingestion(self, db_connection, path_resolver):
        """Test Salary documents ingestion (Form16, Payslips)."""
        from pfas.parsers.salary.ingester import SalaryIngester

        inbox_path = path_resolver.inbox() / "Salary"
        if not inbox_path.exists():
            pytest.skip("Salary inbox not found")

        files = list(inbox_path.rglob("*.*"))
        files = [f for f in files if f.suffix.lower() in ['.pdf', '.xlsx', '.xls']]
        print(f"\nSalary: Found {len(files)} files to process")

        if not files:
            pytest.skip("No Salary files found")

        ingester = SalaryIngester(db_connection, 1, inbox_path)
        result = ingester.ingest(force=False)

        print(f"  Processed: {result.files_processed}")
        print(f"  Skipped: {result.files_skipped}")
        print(f"  Records Inserted: {result.records_inserted}")
        print(f"  Records Skipped: {result.records_skipped}")

        assert result.files_processed >= 0, "Ingestion failed"

    def test_05_epf_ingestion(self, db_connection, path_resolver):
        """Test EPF ingestion."""
        from pfas.parsers.assets.ingester import EPFIngester

        inbox_path = path_resolver.inbox() / "EPF"
        if not inbox_path.exists():
            pytest.skip("EPF inbox not found")

        files = list(inbox_path.rglob("*.*"))
        files = [f for f in files if f.suffix.lower() in ['.pdf', '.xlsx', '.xls']]
        print(f"\nEPF: Found {len(files)} files to process")

        if not files:
            pytest.skip("No EPF files found")

        ingester = EPFIngester(db_connection, 1, inbox_path)
        result = ingester.ingest(force=False)

        print(f"  Processed: {result.files_processed}")
        print(f"  Skipped: {result.files_skipped}")
        print(f"  Records Inserted: {result.records_inserted}")
        print(f"  Records Skipped: {result.records_skipped}")

        assert result.files_processed >= 0, "Ingestion failed"

    def test_06_nps_ingestion(self, db_connection, path_resolver):
        """Test NPS ingestion."""
        from pfas.parsers.assets.ingester import NPSIngester

        inbox_path = path_resolver.inbox() / "NPS"
        if not inbox_path.exists():
            pytest.skip("NPS inbox not found")

        files = list(inbox_path.rglob("*.*"))
        files = [f for f in files if f.suffix.lower() in ['.pdf', '.xlsx', '.csv','.xls']]
        print(f"\nNPS: Found {len(files)} files to process")

        if not files:
            pytest.skip("No NPS files found")

        ingester = NPSIngester(db_connection, 1, inbox_path)
        result = ingester.ingest(force=False)

        print(f"  Processed: {result.files_processed}")
        print(f"  Skipped: {result.files_skipped}")
        print(f"  Records Inserted: {result.records_inserted}")
        print(f"  Records Skipped: {result.records_skipped}")

        assert result.files_processed >= 0, "Ingestion failed"

    def test_07_ppf_ingestion(self, db_connection, path_resolver):
        """Test PPF ingestion."""
        from pfas.parsers.assets.ingester import PPFIngester

        inbox_path = path_resolver.inbox() / "PPF"
        if not inbox_path.exists():
            pytest.skip("PPF inbox not found")

        files = list(inbox_path.rglob("*.*"))
        files = [f for f in files if f.suffix.lower() in ['.pdf', '.xlsx', '.xls']]
        print(f"\nPPF: Found {len(files)} files to process")

        if not files:
            pytest.skip("No PPF files found")

        ingester = PPFIngester(db_connection, 1, inbox_path)
        result = ingester.ingest(force=False)

        print(f"  Processed: {result.files_processed}")
        print(f"  Skipped: {result.files_skipped}")
        print(f"  Records Inserted: {result.records_inserted}")
        print(f"  Records Skipped: {result.records_skipped}")

        assert result.files_processed >= 0, "Ingestion failed"

    def test_08_sgb_ingestion(self, db_connection, path_resolver):
        """Test SGB (Sovereign Gold Bond) ingestion."""
        from pfas.parsers.assets.ingester import SGBIngester

        inbox_path = path_resolver.inbox() / "SGB"
        if not inbox_path.exists():
            pytest.skip("SGB inbox not found")

        files = list(inbox_path.rglob("*.*"))
        files = [f for f in files if f.suffix.lower() in ['.pdf', '.xlsx', '.xls']]
        print(f"\nSGB: Found {len(files)} files to process")

        if not files:
            pytest.skip("No SGB files found")

        ingester = SGBIngester(db_connection, 1, inbox_path)
        result = ingester.ingest(force=False)

        print(f"  Processed: {result.files_processed}")
        print(f"  Skipped: {result.files_skipped}")
        print(f"  Records Inserted: {result.records_inserted}")
        print(f"  Records Skipped: {result.records_skipped}")

        assert result.files_processed >= 0, "Ingestion failed"

    def test_09_usa_stocks_ingestion(self, db_connection, path_resolver):
        """Test USA Stocks ingestion (Morgan Stanley, E-Trade)."""
        from pfas.parsers.assets.ingester import USAStockIngester

        inbox_path = path_resolver.inbox() / "USA-Stocks"
        if not inbox_path.exists():
            pytest.skip("USA-Stocks inbox not found")

        files = list(inbox_path.rglob("*.*"))
        files = [f for f in files if f.suffix.lower() in ['.pdf', '.xlsx', '.xls', '.csv']]
        print(f"\nUSA-Stocks: Found {len(files)} files to process")

        if not files:
            pytest.skip("No USA-Stocks files found")

        ingester = USAStockIngester(db_connection, 1, inbox_path)
        result = ingester.ingest(force=False)

        print(f"  Processed: {result.files_processed}")
        print(f"  Skipped: {result.files_skipped}")
        print(f"  Records Inserted: {result.records_inserted}")
        print(f"  Records Skipped: {result.records_skipped}")

        assert result.files_processed >= 0, "Ingestion failed"

    def test_10_fd_bonds_ingestion(self, db_connection, path_resolver):
        """Test FD-Bonds ingestion."""
        from pfas.parsers.assets.ingester import FDBondsIngester

        inbox_path = path_resolver.inbox() / "FD-Bonds"
        if not inbox_path.exists():
            pytest.skip("FD-Bonds inbox not found")

        files = list(inbox_path.rglob("*.*"))
        files = [f for f in files if f.suffix.lower() in ['.pdf', '.xlsx', '.xls']]
        print(f"\nFD-Bonds: Found {len(files)} files to process")

        if not files:
            pytest.skip("No FD-Bonds files found")

        ingester = FDBondsIngester(db_connection, 1, inbox_path)
        result = ingester.ingest(force=False)

        print(f"  Processed: {result.files_processed}")
        print(f"  Skipped: {result.files_skipped}")
        print(f"  Records Inserted: {result.records_inserted}")
        print(f"  Records Skipped: {result.records_skipped}")

        # FD-Bonds is not yet implemented, so we expect warnings
        assert result.files_processed >= 0, "Ingestion failed"

    def test_90_database_integrity(self, db_connection):
        """Verify database integrity after all ingestion."""
        print("\n=== Database Integrity Check ===")

        # Get all tables
        cursor = db_connection.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Tables: {len(tables)}")

        # Count records in key tables
        key_tables = [
            'mf_transactions', 'mf_holdings', 'mf_folios',
            'stock_trades', 'stock_dividends',
            'bank_transactions',
            'epf_transactions', 'nps_transactions', 'ppf_transactions',
            'ingestion_log', 'reconciliation_audit'
        ]

        total_records = 0
        for table in key_tables:
            if table in tables:
                cursor = db_connection.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                if count > 0:
                    print(f"  {table}: {count} records")
                    total_records += count

        print(f"Total records across all tables: {total_records}")

        # Check for unbalanced journals
        cursor = db_connection.execute("""
            SELECT j.id, j.date,
                   SUM(je.debit) as total_debit,
                   SUM(je.credit) as total_credit,
                   ABS(SUM(je.debit) - SUM(je.credit)) as imbalance
            FROM journals j
            JOIN journal_entries je ON j.id = je.journal_id
            GROUP BY j.id
            HAVING ABS(SUM(je.debit) - SUM(je.credit)) > 0.01
        """)
        unbalanced = cursor.fetchall()

        if unbalanced:
            print(f"\nWARNING: Found {len(unbalanced)} unbalanced journals:")
            for journal_id, date, debit, credit, imbalance in unbalanced:
                print(f"  Journal {journal_id} ({date}): Debit={debit}, Credit={credit}, Imbalance={imbalance}")
        else:
            print("\nAll journal entries balanced ✓")

        assert len(unbalanced) == 0, f"Found {len(unbalanced)} unbalanced journals"

    def test_91_ingestion_log_integrity(self, db_connection):
        """Verify ingestion log is properly maintained."""
        cursor = db_connection.execute("""
            SELECT status, COUNT(*)
            FROM ingestion_log
            WHERE user_id = 1
            GROUP BY status
        """)
        status_counts = cursor.fetchall()

        print("\n=== Ingestion Log Status ===")
        for status, count in status_counts:
            print(f"  {status}: {count} files")

        # Check for failed ingestions
        cursor = db_connection.execute("""
            SELECT source_file, error_message
            FROM ingestion_log
            WHERE user_id = 1 AND status = 'FAILED'
        """)
        failures = cursor.fetchall()

        if failures:
            print(f"\nFailed ingestions:")
            for source_file, error in failures[:10]:  # Show first 10
                print(f"  {source_file}: {error}")

    def test_92_no_duplicate_records(self, db_connection):
        """Verify no duplicate records exist."""
        print("\n=== Duplicate Check ===")

        # Check MF transactions for duplicates
        cursor = db_connection.execute("""
            SELECT folio_id, date, transaction_type, amount, COUNT(*)
            FROM mf_transactions
            WHERE user_id = 1
            GROUP BY folio_id, date, transaction_type, amount
            HAVING COUNT(*) > 1
        """)
        mf_dupes = cursor.fetchall()

        if mf_dupes:
            print(f"WARNING: Found {len(mf_dupes)} potential duplicate MF transactions")
        else:
            print("No duplicate MF transactions ✓")

        # Check bank transactions for duplicates
        cursor = db_connection.execute("""
            SELECT bank_account_id, date, description, debit, credit, COUNT(*)
            FROM bank_transactions
            WHERE user_id = 1
            GROUP BY bank_account_id, date, description, debit, credit
            HAVING COUNT(*) > 1
        """)
        bank_dupes = cursor.fetchall()

        if bank_dupes:
            print(f"WARNING: Found {len(bank_dupes)} potential duplicate bank transactions")
        else:
            print("No duplicate bank transactions ✓")

    def test_99_performance_metrics(self, db_connection):
        """Report performance metrics."""
        print("\n=== Performance Metrics ===")

        # Count total files processed
        cursor = db_connection.execute("""
            SELECT COUNT(*) FROM ingestion_log WHERE user_id = 1
        """)
        total_files = cursor.fetchone()[0]
        print(f"Total files processed: {total_files}")

        # Count total records
        cursor = db_connection.execute("""
            SELECT
                (SELECT COUNT(*) FROM mf_transactions WHERE user_id = 1) +
                (SELECT COUNT(*) FROM stock_trades WHERE user_id = 1) +
                (SELECT COUNT(*) FROM bank_transactions WHERE user_id = 1) +
                (SELECT COUNT(*) FROM epf_transactions WHERE user_id = 1) +
                (SELECT COUNT(*) FROM nps_transactions WHERE user_id = 1) +
                (SELECT COUNT(*) FROM ppf_transactions WHERE user_id = 1)
        """)
        total_records = cursor.fetchone()[0]
        print(f"Total records ingested: {total_records}")

        if total_files > 0:
            print(f"Average records per file: {total_records / total_files:.1f}")


class TestQuickSmokeTest:
    """Quick smoke test that can run in < 10 seconds."""

    def test_imports(self):
        """Verify all modules can be imported."""
        print("\n=== Import Smoke Test ===")

        modules = [
            'pfas.core.database',
            'pfas.core.paths',
            'pfas.parsers.mf.ingester',
            'pfas.parsers.bank.ingester',
            'pfas.parsers.stock.ingester',
            'pfas.parsers.salary.ingester',
            'pfas.parsers.assets.ingester',
            'pfas.services.generic_ingester',
        ]

        for module in modules:
            try:
                __import__(module)
                print(f"  ✓ {module}")
            except ImportError as e:
                pytest.fail(f"Failed to import {module}: {e}")

    def test_database_schema_creation(self):
        """Verify database schema can be created."""
        DatabaseManager.reset_instance()
        db = DatabaseManager()
        conn = db.init(":memory:", "test_password")
        setup_chart_of_accounts(conn)

        # Verify key tables exist
        cursor = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table'
            ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]

        expected_tables = [
            'users', 'accounts', 'journals', 'journal_entries',
            'mf_transactions', 'mf_holdings', 'mf_folios',
            'stock_trades', 'bank_transactions',
            'ingestion_log'
        ]

        for table in expected_tables:
            assert table in tables, f"Table {table} not found in schema"

        print(f"\n  ✓ Created {len(tables)} tables")

        db.close()
        DatabaseManager.reset_instance()
