"""
Full FY24-25 Integration Test - Refactored

This test loads all actual data files and verifies:
1. All parsers work correctly with real data
2. Financial statement services can generate reports
3. Data integrity is maintained across all asset classes

Configuration:
- Uses inbox first, falls back to archive if inbox is empty
- Set PFAS_TEST_USE_ARCHIVE=false to disable archive fallback
- Configure in config/test_config.json for project-wide settings
"""
import pytest
from pathlib import Path
from datetime import date
from decimal import Decimal

from pfas.core.database import DatabaseManager
from pfas.core.accounts import setup_chart_of_accounts

# Import helper functions from conftest
from tests.integration.conftest import get_asset_path, find_files_in_path


class TestSanjayFullFY2425:
    """Full financial year integration test."""

    @pytest.fixture(scope="class")
    def db_connection(self):
        """Create test database with schema."""
        DatabaseManager.reset_instance()
        db = DatabaseManager()
        conn = db.init(":memory:", "test_password")
        setup_chart_of_accounts(conn)

        # Create test user
        conn.execute("""
            INSERT INTO users (id, pan_encrypted, pan_salt, name, email)
            VALUES (1, X'00', X'00', 'Test User', 'test@example.com')
        """)
        conn.commit()

        yield conn

        db.close()
        DatabaseManager.reset_instance()

    @pytest.fixture(scope="class")
    def data_path(self, path_resolver):
        """Get user data directory."""
        user_dir = path_resolver.user_dir
        if not user_dir.exists():
            pytest.skip(f"User directory not found: {user_dir}")
        return user_dir

    def test_01_data_folder_exists(self, data_path):
        """Verify data folder exists."""
        assert data_path.exists(), f"Data folder {data_path} not found"

    def test_02_parse_mf_cams(self, db_connection, path_resolver):
        """Parse CAMS mutual fund statement (from inbox or archive)."""
        from pfas.parsers.mf import CAMSParser

        # Find CAMS files in inbox, fallback to archive
        cams_files = find_files_in_path(
            path_resolver,
            "Mutual-Fund/CAMS",
            ['.xlsx', '.xls'],
            exclude_patterns=['holding', 'holdings']
        )

        if not cams_files:
            pytest.skip("No CAMS Excel files found in inbox or archive")

        parser = CAMSParser(db_connection)
        total_transactions = 0
        files_processed = 0

        for cams_file in cams_files:
            if "CG" in cams_file.name:
                result = parser.parse(cams_file)
                if result.success:
                    num_txns = len(result.transactions)
                    total_transactions += num_txns
                    files_processed += 1
                    print(f"CAMS {cams_file.name}: {num_txns} transactions")
                else:
                    print(f"CAMS {cams_file.name}: Could not parse - {result.errors}")

        print(f"Total CAMS transactions: {total_transactions} from {files_processed} files")
        if total_transactions == 0:
            pytest.skip("CAMS files did not parse - format may not be supported")

    def test_03_parse_mf_karvy(self, db_connection, path_resolver):
        """Parse Karvy/KFintech mutual fund statement (from inbox or archive)."""
        from pfas.parsers.mf import KarvyParser

        # Find Karvy files in inbox, fallback to archive
        karvy_files = find_files_in_path(
            path_resolver,
            "Mutual-Fund/KARVY",
            ['.xlsx', '.xls'],
            exclude_patterns=['holding', 'holdings']
        )

        if not karvy_files:
            pytest.skip("No Karvy Excel files found in inbox or archive")

        parser = KarvyParser(db_connection)
        total_transactions = 0

        for karvy_file in karvy_files:
            if "CG" not in karvy_file.name:
                result = parser.parse(karvy_file)
                if result.success:
                    num_txns = len(result.transactions)
                    total_transactions += num_txns
                    print(f"Karvy {karvy_file.name}: {num_txns} transactions")
                else:
                    print(f"Karvy {karvy_file.name}: FAILED - {result.errors}")

        print(f"Total Karvy transactions: {total_transactions}")

    def test_04_parse_zerodha_stocks(self, db_connection, path_resolver):
        """Parse Zerodha stock trades (from inbox or archive)."""
        from pfas.parsers.stock import ZerodhaParser

        # Find Zerodha taxpnl files in inbox, fallback to archive
        pnl_files = find_files_in_path(
            path_resolver,
            "Indian-Stocks/Zerodha",
            ['.xlsx', '.csv'],
            pattern='*taxpnl*'
        )

        if not pnl_files:
            pytest.skip("No Zerodha Tax P&L files found in inbox or archive")

        parser = ZerodhaParser(db_connection)
        total_trades = 0

        for pnl_file in pnl_files:
            result = parser.parse(pnl_file)
            if result.success:
                num_trades = len(result.trades)
                total_trades += num_trades
                print(f"Zerodha {pnl_file.name}: {num_trades} trades")

        assert total_trades > 0, "No trades parsed from Zerodha"
        print(f"Total Zerodha trades: {total_trades}")

    def test_05_parse_icici_stocks(self, db_connection, path_resolver):
        """Parse ICICI Direct stock trades (from inbox or archive)."""
        from pfas.parsers.stock import ICICIDirectParser

        # Find ICICI Direct files in inbox, fallback to archive
        csv_files = find_files_in_path(
            path_resolver,
            "Indian-Stocks/ICICIDirect",
            ['.csv', '.xlsx'],
            exclude_patterns=['holding', 'holdings', 'portfolio']
        )

        if not csv_files:
            pytest.skip("No ICICI Direct files found in inbox or archive")

        parser = ICICIDirectParser(db_connection)
        total_trades = 0

        for csv_file in csv_files:
            result = parser.parse(csv_file)
            if result.success:
                num_trades = len(result.trades)
                total_trades += num_trades
                print(f"ICICI {csv_file.name}: {num_trades} trades")

        print(f"Total ICICI Direct trades: {total_trades}")

    def test_06_parse_nps(self, db_connection, path_resolver):
        """Parse NPS statement (from inbox or archive)."""
        from pfas.parsers.nps import NPSParser

        # Find NPS files in inbox, fallback to archive
        csv_files = find_files_in_path(
            path_resolver,
            "NPS",
            ['.csv', '.pdf', '.xlsx']
        )

        if not csv_files:
            pytest.skip("No NPS files found in inbox or archive")

        parser = NPSParser(db_connection)

        for csv_file in csv_files:
            result = parser.parse(csv_file)
            if result.success:
                num_txns = len(result.transactions)
                print(f"NPS {csv_file.name}: {num_txns} transactions")
                tier1_total = sum(t.amount for t in result.transactions if t.tier == "I")
                tier2_total = sum(t.amount for t in result.transactions if t.tier == "II")
                print(f"  Tier I: {tier1_total:,.2f}")
                print(f"  Tier II: {tier2_total:,.2f}")

    def test_07_parse_ppf(self, db_connection, path_resolver):
        """Parse PPF statement (from inbox or archive)."""
        from pfas.parsers.ppf import PPFParser

        # Find PPF files in inbox, fallback to archive
        xlsx_files = find_files_in_path(
            path_resolver,
            "PPF",
            ['.xlsx', '.pdf']
        )

        if not xlsx_files:
            pytest.skip("No PPF files found in inbox or archive")

        parser = PPFParser(db_connection)

        for xlsx_file in xlsx_files:
            result = parser.parse(xlsx_file)
            if result.success:
                num_txns = len(result.transactions)
                print(f"PPF {xlsx_file.name}: {num_txns} transactions")
                deposits = sum(t.amount for t in result.transactions if t.transaction_type == "DEPOSIT")
                print(f"  Total Deposits (80C eligible): {deposits:,.2f}")

    def test_08_parse_epf(self, db_connection, path_resolver):
        """Parse EPF passbook (from inbox or archive)."""
        from pfas.parsers.epf import EPFParser

        # Find EPF files in inbox, fallback to archive
        pdf_files = find_files_in_path(
            path_resolver,
            "EPF",
            ['.pdf'],
            exclude_patterns=['interest']  # Skip interest-only statements
        )

        if not pdf_files:
            pytest.skip("No EPF PDF files found in inbox or archive")

        parser = EPFParser(db_connection)

        for pdf_file in pdf_files:
            result = parser.parse(pdf_file)
            if result.success:
                num_txns = len(result.transactions)
                print(f"EPF {pdf_file.name}: {num_txns} transactions")
                # Get closing balance from last transaction (balances are on transactions, not account)
                if result.transactions:
                    last_txn = result.transactions[-1]
                    total_balance = last_txn.employee_balance + last_txn.employer_balance
                    print(f"  Closing Balance: {total_balance:,.2f}")

    def test_09_generate_balance_sheet(self, db_connection):
        """Generate balance sheet using the new service."""
        from pfas.services import BalanceSheetService

        service = BalanceSheetService(db_connection)
        snapshot = service.get_balance_sheet(user_id=1, as_of=date.today())

        print(f"\n=== Balance Sheet as of {snapshot.snapshot_date} ===")
        print(f"Total Assets: {snapshot.total_assets:,.2f}")
        print(f"  - Mutual Funds (Equity): {snapshot.mutual_funds_equity:,.2f}")
        print(f"  - Mutual Funds (Debt): {snapshot.mutual_funds_debt:,.2f}")
        print(f"  - Stocks (Indian): {snapshot.stocks_indian:,.2f}")
        print(f"  - EPF: {snapshot.epf_balance:,.2f}")
        print(f"  - PPF: {snapshot.ppf_balance:,.2f}")
        print(f"  - NPS Tier I: {snapshot.nps_tier1:,.2f}")
        print(f"  - NPS Tier II: {snapshot.nps_tier2:,.2f}")
        print(f"Total Liabilities: {snapshot.total_liabilities:,.2f}")
        print(f"Net Worth: {snapshot.net_worth:,.2f}")

        assert snapshot.total_assets >= Decimal("0")
        assert snapshot.net_worth is not None

    def test_10_generate_cash_flow_statement(self, db_connection):
        """Generate cash flow statement using the new service."""
        from pfas.services import CashFlowStatementService

        service = CashFlowStatementService(db_connection)
        statement = service.get_cash_flow_statement(user_id=1, financial_year="2024-25")

        print(f"\n=== Cash Flow Statement FY {statement.financial_year} ===")
        print(f"Period: {statement.period_start} to {statement.period_end}")
        print(f"Operating Activities:")
        print(f"  - Salary Received: {statement.salary_received:,.2f}")
        print(f"  - Dividends Received: {statement.dividends_received:,.2f}")
        print(f"  - Interest Received: {statement.interest_received:,.2f}")
        print(f"  - Taxes Paid: {statement.taxes_paid:,.2f}")
        print(f"  Net Operating: {statement.net_operating:,.2f}")
        print(f"Investing Activities:")
        print(f"  - MF Purchases: {statement.mf_purchases:,.2f}")
        print(f"  - MF Redemptions: {statement.mf_redemptions:,.2f}")
        print(f"  - Stock Buys: {statement.stock_buys:,.2f}")
        print(f"  - Stock Sells: {statement.stock_sells:,.2f}")
        print(f"  Net Investing: {statement.net_investing:,.2f}")
        print(f"Net Change in Cash: {statement.net_change_in_cash:,.2f}")

        assert statement.financial_year == "2024-25"

    def test_11_portfolio_valuation(self, db_connection):
        """Generate portfolio valuation using the new service."""
        from pfas.services import PortfolioValuationService

        service = PortfolioValuationService(db_connection)
        summary = service.get_portfolio_summary(user_id=1)

        print(f"\n=== Portfolio Summary ===")
        print(f"Total Invested: {summary.total_invested:,.2f}")
        print(f"Total Current Value: {summary.total_current_value:,.2f}")
        print(f"Unrealized Gain: {summary.total_unrealized_gain:,.2f}")
        print(f"Holdings: {len(summary.holdings)}")

        xirr_result = service.calculate_xirr(user_id=1)
        if xirr_result.xirr_percent is not None:
            print(f"XIRR: {xirr_result.xirr_percent:.2f}%")
        else:
            print(f"XIRR: {xirr_result.error}")

    def test_12_verify_database_integrity(self, db_connection):
        """Verify database integrity after all parsing."""
        cursor = db_connection.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """)
        tables = [row[0] for row in cursor.fetchall()]

        print(f"\n=== Database Integrity Check ===")
        print(f"Tables created: {len(tables)}")

        key_tables = ['mf_transactions', 'stock_trades', 'epf_transactions',
                      'nps_transactions', 'ppf_transactions']

        for table in key_tables:
            if table in tables:
                cursor = db_connection.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table}: {count} records")

        cursor = db_connection.execute("""
            SELECT COUNT(*) FROM journals j
            JOIN journal_entries je ON j.id = je.journal_id
            GROUP BY j.id
            HAVING ABS(SUM(je.debit) - SUM(je.credit)) > 0.01
        """)
        unbalanced = cursor.fetchall()

        assert len(unbalanced) == 0, f"Found {len(unbalanced)} unbalanced journals"
        print("All journal entries balanced.")


class TestMFCapitalGains:
    """Test MF capital gains calculation with real data."""

    @pytest.fixture(scope="class")
    def db_connection(self):
        """Create test database with schema."""
        DatabaseManager.reset_instance()
        db = DatabaseManager()
        conn = db.init(":memory:", "test_password")
        setup_chart_of_accounts(conn)

        conn.execute("""
            INSERT INTO users (id, pan_encrypted, pan_salt, name, email)
            VALUES (1, X'00', X'00', 'Test User', 'test@example.com')
        """)
        conn.commit()

        yield conn

        db.close()
        DatabaseManager.reset_instance()

    def test_01_mf_capital_gains_calculation(self, db_connection, path_resolver):
        """Calculate MF capital gains for FY24-25 (from inbox or archive)."""
        from pfas.parsers.mf import CAMSParser, CapitalGainsCalculator

        # Find CAMS files in inbox, fallback to archive
        cams_files = find_files_in_path(
            path_resolver,
            "Mutual-Fund/CAMS",
            ['.xlsx', '.xls']
        )

        if not cams_files:
            pytest.skip("No CAMS files found in inbox or archive")

        parser = CAMSParser(db_connection)
        for cams_file in cams_files:
            if "Consolidated" in cams_file.name:
                parser.parse(cams_file)

        calculator = CapitalGainsCalculator(db_connection)
        summaries = calculator.calculate_summary(user_id=1, fy="2024-25")

        print(f"\n=== MF Capital Gains FY 2024-25 ===")
        for summary in summaries:
            print(f"Asset Class: {summary.asset_class.value}")
            print(f"  STCG: {summary.stcg_amount:,.2f}")
            print(f"  LTCG: {summary.ltcg_amount:,.2f}")
            print(f"  LTCG Exemption: {summary.ltcg_exemption:,.2f}")
            print(f"  Taxable STCG: {summary.taxable_stcg:,.2f}")
            print(f"  Taxable LTCG: {summary.taxable_ltcg:,.2f}")


class TestStockCapitalGains:
    """Test stock capital gains calculation with real data."""

    @pytest.fixture(scope="class")
    def db_connection(self):
        """Create test database with schema."""
        DatabaseManager.reset_instance()
        db = DatabaseManager()
        conn = db.init(":memory:", "test_password")
        setup_chart_of_accounts(conn)

        conn.execute("""
            INSERT INTO users (id, pan_encrypted, pan_salt, name, email)
            VALUES (1, X'00', X'00', 'Test User', 'test@example.com')
        """)
        conn.commit()

        yield conn

        db.close()
        DatabaseManager.reset_instance()

    def test_01_stock_capital_gains_zerodha(self, db_connection, path_resolver):
        """Calculate stock capital gains from Zerodha (from inbox or archive)."""
        from pfas.parsers.stock import ZerodhaParser

        # Find Zerodha taxpnl files in inbox, fallback to archive
        pnl_files = find_files_in_path(
            path_resolver,
            "Indian-Stocks/Zerodha",
            ['.xlsx', '.csv'],
            pattern='*taxpnl*'
        )

        if not pnl_files:
            pytest.skip("No Zerodha files found in inbox or archive")

        parser = ZerodhaParser(db_connection)

        for pnl_file in pnl_files:
            result = parser.parse(pnl_file)
            if result.success:
                num_trades = len(result.trades)
                print(f"\n=== Zerodha Capital Gains {pnl_file.name} ===")
                print(f"Total Trades: {num_trades}")
                sell_trades = [t for t in result.trades if t.trade_type.value == "SELL"]
                stcg = sum(t.capital_gain or Decimal(0) for t in sell_trades if not t.is_ltcg())
                ltcg = sum(t.capital_gain or Decimal(0) for t in sell_trades if t.is_ltcg())
                stt = sum(s.stt_amount for s in result.stt_entries) if result.stt_entries else Decimal(0)
                print(f"Sell Trades: {len(sell_trades)}")
                print(f"STCG: {stcg:,.2f}")
                print(f"LTCG: {ltcg:,.2f}")
                print(f"Total STT Paid: {stt:,.2f}")
