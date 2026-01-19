# PFAS Test Refactoring - Complete Guide

## Executive Summary

This document provides the complete refactored test suite for the PFAS project, addressing:
- ✅ PathResolver-based paths (NO hardcoded paths)
- ✅ Multi-user support via parameterization
- ✅ Multi-asset support via parameterization
- ✅ Golden master comparison for outputs
- ✅ Graceful skips with helpful messages
- ✅ In-memory DB for speed and isolation
- ✅ CI/CD support via environment variables

## 1. Complete Refactored conftest.py

### tests/integration/conftest.py

```python
"""
Integration Test Fixtures - Scalable, Multi-User, Multi-Asset

Features:
- PathResolver for ALL paths (NO hardcoding)
- Multi-user support (PFAS_TEST_USER env var)
- Multi-asset parameterization
- Golden master comparison helpers
- Graceful skips with helpful messages
- In-memory DB for isolation
"""

import os
import json
import pytest
from pathlib import Path
from datetime import date
from decimal import Decimal
from typing import Generator, Dict, Any, List

from pfas.core.paths import PathResolver
from pfas.core.database import DatabaseManager
from pfas.core.accounts import setup_chart_of_accounts

# =============================================================================
# Environment Configuration
# =============================================================================

# Default test user (fallback for local development)
DEFAULT_TEST_USER = "Sanjay"

# Override via environment variable for CI or multi-user testing
TEST_USER = os.getenv("PFAS_TEST_USER", DEFAULT_TEST_USER)

# PFAS root path - critical for CI/CD
PFAS_TEST_ROOT = os.getenv("PFAS_ROOT", str(Path.cwd()))

# Supported asset types
TEST_ASSETS = [
    "Mutual-Fund",
    "Bank",
    "Indian-Stocks",
    "Salary",
    "EPF",
    "NPS",
    "PPF",
    "SGB",
    "USA-Stocks",
    "FD-Bonds"
]

# Test password for encrypted database
TEST_DB_PASSWORD = "test_password_integration"

# Golden master directory
GOLDEN_MASTER_DIR = Path(__file__).parent / "golden_masters"


# =============================================================================
# Core Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def test_root() -> Path:
    """Return PFAS test root directory."""
    root = Path(PFAS_TEST_ROOT)
    print(f"\n[FIXTURE] Test Root: {root}")
    return root


@pytest.fixture(scope="session", params=[TEST_USER])
def path_resolver(request, test_root) -> PathResolver:
    """
    Session-scoped PathResolver fixture with multi-user support.

    Usage:
        def test_something(path_resolver):
            inbox = path_resolver.inbox() / "Mutual-Fund"

    Multi-user usage:
        @pytest.mark.parametrize("path_resolver", ["Sanjay", "Priya"], indirect=True)
        def test_multi_user(path_resolver):
            assert path_resolver.user_name in ["Sanjay", "Priya"]
    """
    user = request.param
    resolver = PathResolver(root_path=test_root, user_name=user)

    print(f"\n[FIXTURE] PathResolver for user: {user}")
    print(f"   Root: {resolver.root}")
    print(f"   User dir: {resolver.user_dir}")
    print(f"   Inbox: {resolver.inbox()}")
    print(f"   Archive: {resolver.archive()}")
    print(f"   Reports: {resolver.reports()}")

    # Verify user directory exists
    if not resolver.user_dir.exists():
        pytest.skip(
            f"User directory not found: {resolver.user_dir}\n"
            f"Create it or set PFAS_TEST_USER to an existing user"
        )

    return resolver


@pytest.fixture(scope="session")
def test_db() -> Generator:
    """
    Session-scoped in-memory database for fast, isolated tests.

    Usage:
        def test_parser(test_db):
            cursor = test_db.execute("SELECT * FROM users")
    """
    print("\n[FIXTURE] Creating in-memory test database...")

    DatabaseManager.reset_instance()
    db_manager = DatabaseManager()
    conn = db_manager.init(":memory:", TEST_DB_PASSWORD)

    # Set up chart of accounts and all tables
    setup_chart_of_accounts(conn)

    # Create test user
    conn.execute("""
        INSERT INTO users (id, pan_encrypted, pan_salt, name, email)
        VALUES (1, X'00', X'00', 'Test User', 'test@example.com')
    """)
    conn.commit()

    print("   ✓ Database initialized with schema and test user")

    yield conn

    print("\n[FIXTURE] Closing test database...")
    db_manager.close()
    DatabaseManager.reset_instance()


@pytest.fixture
def clean_db(test_db):
    """
    Function-scoped fixture that resets database state between tests.

    Usage:
        def test_ingestion(clean_db):
            # Database is clean here
            clean_db.execute("INSERT INTO...")
    """
    # Clear all transaction tables
    tables_to_clear = [
        'journal_entries',
        'journals',
        'bank_transactions',
        'mf_transactions',
        'mf_holdings',
        'stock_trades',
        'epf_transactions',
        'nps_transactions',
        'ppf_transactions',
        'salary_records',
        'ingestion_log'
    ]

    for table in tables_to_clear:
        try:
            test_db.execute(f"DELETE FROM {table}")
        except:
            pass  # Table might not exist

    test_db.commit()

    yield test_db


# =============================================================================
# Asset-Specific Fixtures with Graceful Skips
# =============================================================================

def _find_latest_file(inbox_path: Path, extensions: List[str], asset_type: str, user_name: str) -> Path:
    """
    Find latest file in inbox with graceful skip message.

    Args:
        inbox_path: Path to inbox directory
        extensions: List of file extensions to search for (e.g., ['.pdf', '.xlsx'])
        asset_type: Asset type name for skip message
        user_name: User name for skip message

    Returns:
        Path to latest file

    Raises:
        pytest.skip: If no files found
    """
    if not inbox_path.exists():
        pytest.skip(
            f"Inbox directory does not exist: {inbox_path}\n"
            f"User: {user_name}, Asset: {asset_type}\n"
            f"Create directory and add test files to run this test."
        )

    files = []
    for ext in extensions:
        files.extend(inbox_path.rglob(f"*{ext}"))

    # Exclude files in failed/ subdirectory
    files = [f for f in files if 'failed' not in f.parts]

    if not files:
        pytest.skip(
            f"No {asset_type} files found in: {inbox_path}\n"
            f"Expected extensions: {', '.join(extensions)}\n"
            f"User: {user_name}\n"
            f"Add test files to run this test."
        )

    # Return most recent file
    latest = max(files, key=lambda p: p.stat().st_mtime)
    print(f"\n[FIXTURE] Selected {asset_type} file: {latest.name}")
    return latest


@pytest.fixture
def mutual_fund_file(path_resolver) -> Path:
    """Latest Mutual Fund file from inbox."""
    inbox = path_resolver.inbox() / "Mutual-Fund"
    return _find_latest_file(inbox, ['.pdf', '.xlsx', '.xls'], "Mutual-Fund", path_resolver.user_name)


@pytest.fixture
def epf_file(path_resolver) -> Path:
    """Latest EPF PDF from inbox."""
    inbox = path_resolver.inbox() / "EPF"
    return _find_latest_file(inbox, ['.pdf'], "EPF", path_resolver.user_name)


@pytest.fixture
def nps_file(path_resolver) -> Path:
    """Latest NPS file from inbox."""
    inbox = path_resolver.inbox() / "NPS"
    return _find_latest_file(inbox, ['.pdf', '.csv', '.xlsx'], "NPS", path_resolver.user_name)


@pytest.fixture
def ppf_file(path_resolver) -> Path:
    """Latest PPF file from inbox."""
    inbox = path_resolver.inbox() / "PPF"
    return _find_latest_file(inbox, ['.pdf', '.xlsx'], "PPF", path_resolver.user_name)


@pytest.fixture
def bank_file(path_resolver) -> Path:
    """Latest Bank statement from inbox."""
    inbox = path_resolver.inbox() / "Bank"
    return _find_latest_file(inbox, ['.pdf', '.xlsx', '.xls', '.csv'], "Bank", path_resolver.user_name)


@pytest.fixture
def stock_file(path_resolver) -> Path:
    """Latest Indian Stock file from inbox."""
    inbox = path_resolver.inbox() / "Indian-Stocks"
    return _find_latest_file(inbox, ['.pdf', '.xlsx', '.csv'], "Indian-Stocks", path_resolver.user_name)


@pytest.fixture
def salary_file(path_resolver) -> Path:
    """Latest Salary file (Form16/Payslip) from inbox."""
    inbox = path_resolver.inbox() / "Salary"
    return _find_latest_file(inbox, ['.pdf', '.xlsx'], "Salary", path_resolver.user_name)


# =============================================================================
# Parameterized Fixtures for Multi-Asset Testing
# =============================================================================

@pytest.fixture(params=TEST_ASSETS)
def asset_type(request) -> str:
    """
    Parameterized asset type fixture.

    Usage:
        def test_all_assets(asset_type, path_resolver):
            inbox = path_resolver.inbox() / asset_type
            # Test runs for all asset types
    """
    return request.param


@pytest.fixture
def asset_inbox(path_resolver, asset_type) -> Path:
    """
    Parameterized asset inbox directory.

    Usage:
        def test_inbox_structure(asset_inbox):
            assert asset_inbox.exists()
    """
    inbox = path_resolver.inbox() / asset_type

    if not inbox.exists():
        pytest.skip(f"Inbox not found for {asset_type}: {inbox}")

    return inbox


# =============================================================================
# Golden Master Comparison Helpers
# =============================================================================

def save_golden(data: Any, test_name: str, format: str = 'json') -> None:
    """
    Save golden master data for comparison.

    Args:
        data: Data to save (dict, DataFrame, etc.)
        test_name: Unique test name (e.g., 'epf_parser_basic')
        format: Format ('json', 'csv', 'excel')

    Usage:
        save_golden({'total': 1234.56}, 'test_epf_totals')
    """
    GOLDEN_MASTER_DIR.mkdir(exist_ok=True, parents=True)

    golden_file = GOLDEN_MASTER_DIR / f"{test_name}.golden.{format}"

    if format == 'json':
        import json
        from decimal import Decimal

        class DecimalEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, Decimal):
                    return str(obj)
                if isinstance(obj, date):
                    return obj.isoformat()
                return super().default(obj)

        with open(golden_file, 'w') as f:
            json.dump(data, f, indent=2, cls=DecimalEncoder)

    elif format == 'csv':
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            data.to_csv(golden_file, index=False)
        else:
            raise ValueError("CSV format requires pandas DataFrame")

    elif format == 'excel':
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            data.to_excel(golden_file, index=False)
        else:
            raise ValueError("Excel format requires pandas DataFrame")

    else:
        raise ValueError(f"Unsupported format: {format}")

    print(f"\n[GOLDEN] Saved golden master: {golden_file.name}")


def assert_golden_match(
    actual: Any,
    test_name: str,
    format: str = 'json',
    tolerance: float = 0.01,
    save_if_missing: bool = True
) -> None:
    """
    Assert that actual data matches golden master.

    Args:
        actual: Actual data from test
        test_name: Unique test name (must match save_golden)
        format: Format ('json', 'csv', 'excel')
        tolerance: Tolerance for float comparisons
        save_if_missing: If True, save golden if it doesn't exist

    Usage:
        result = parser.parse(file)
        assert_golden_match(
            {'count': len(result.transactions)},
            'test_epf_parser'
        )
    """
    golden_file = GOLDEN_MASTER_DIR / f"{test_name}.golden.{format}"

    if not golden_file.exists():
        if save_if_missing:
            print(f"\n[GOLDEN] Golden master not found, creating: {golden_file.name}")
            save_golden(actual, test_name, format)
            return
        else:
            pytest.fail(f"Golden master not found: {golden_file}")

    if format == 'json':
        import json
        from decimal import Decimal

        with open(golden_file) as f:
            expected = json.load(f)

        # Compare with tolerance for floats
        def compare_values(a, e):
            if isinstance(a, (int, float, Decimal)) and isinstance(e, (int, float, str)):
                return abs(float(a) - float(e)) < tolerance
            elif isinstance(a, dict) and isinstance(e, dict):
                return all(k in e and compare_values(a[k], e[k]) for k in a)
            elif isinstance(a, list) and isinstance(e, list):
                return len(a) == len(e) and all(compare_values(av, ev) for av, ev in zip(a, e))
            else:
                return a == e

        if not compare_values(actual, expected):
            print(f"\n[GOLDEN] Mismatch detected!")
            print(f"Expected: {expected}")
            print(f"Actual: {actual}")
            pytest.fail(f"Golden master mismatch for {test_name}")

        print(f"\n[GOLDEN] ✓ Match verified: {golden_file.name}")

    elif format in ['csv', 'excel']:
        import pandas as pd
        import numpy as np

        if format == 'csv':
            expected = pd.read_csv(golden_file)
        else:
            expected = pd.read_excel(golden_file)

        if not isinstance(actual, pd.DataFrame):
            pytest.fail("CSV/Excel comparison requires pandas DataFrame")

        # Compare DataFrames
        try:
            pd.testing.assert_frame_equal(actual, expected, rtol=tolerance, atol=tolerance)
            print(f"\n[GOLDEN] ✓ DataFrame match verified: {golden_file.name}")
        except AssertionError as e:
            print(f"\n[GOLDEN] DataFrame mismatch!")
            print(str(e))
            pytest.fail(f"Golden master DataFrame mismatch for {test_name}")


@pytest.fixture
def golden_master_dir() -> Path:
    """Return path to golden masters directory."""
    GOLDEN_MASTER_DIR.mkdir(exist_ok=True, parents=True)
    return GOLDEN_MASTER_DIR


# =============================================================================
# Utility Fixtures
# =============================================================================

@pytest.fixture
def test_user_id() -> int:
    """Return test user ID (always 1 for in-memory DB)."""
    return 1


@pytest.fixture
def test_password() -> str:
    """Return test database password."""
    return TEST_DB_PASSWORD
```

---

## 2. Refactored Integration Tests

### tests/integration/test_epf_integration.py (COMPLETE REFACTORED)

```python
"""
EPF Parser Integration Test - Refactored

Features:
- PathResolver for paths (NO hardcoding)
- Multi-user support via parameterization
- Golden master comparison
- Graceful skip if no files
- In-memory DB for speed
"""

import pytest
from pathlib import Path
from decimal import Decimal

from pfas.parsers.epf.epf import EPFParser


class TestEPFParser:
    """EPF Parser integration tests with real data."""

    def test_epf_parse_basic(self, epf_file, test_db):
        """Test basic EPF PDF parsing."""
        print(f"\n{'='*70}")
        print(f"EPF Parser - Basic Parse Test")
        print(f"File: {epf_file.name}")
        print(f"{'='*70}")

        # Parse
        parser = EPFParser(test_db)
        result = parser.parse(epf_file)

        # Basic assertions
        assert result.success, f"Parse failed: {result.errors}"
        assert result.account is not None, "No account info extracted"
        assert len(result.transactions) > 0, "No transactions extracted"

        print(f"\n✓ Parsed successfully")
        print(f"  Account: {result.account.uan}")
        print(f"  Transactions: {len(result.transactions)}")
        print(f"  Errors: {len(result.errors)}")
        print(f"  Warnings: {len(result.warnings)}")

    def test_epf_account_details(self, epf_file, test_db):
        """Test EPF account information extraction."""
        parser = EPFParser(test_db)
        result = parser.parse(epf_file)

        assert result.success
        account = result.account

        # Verify account fields
        assert account.uan is not None, "UAN not extracted"
        assert account.member_id is not None, "Member ID not extracted"
        assert account.member_name is not None, "Member name not extracted"

        print(f"\n✓ Account Details:")
        print(f"  UAN: {account.uan}")
        print(f"  Member: {account.member_name}")
        print(f"  Member ID: {account.member_id}")

    def test_epf_transactions_structure(self, epf_file, test_db):
        """Test EPF transaction structure and fields."""
        parser = EPFParser(test_db)
        result = parser.parse(epf_file)

        assert len(result.transactions) > 0

        # Check first transaction has all required fields
        txn = result.transactions[0]

        assert txn.wage_month is not None
        assert txn.transaction_date is not None
        assert txn.employee_contribution is not None
        assert txn.employer_contribution is not None
        assert txn.pension_contribution is not None

        print(f"\n✓ Transaction Structure Valid")
        print(f"  Sample transaction: {txn.wage_month}")
        print(f"  EE: ₹{txn.employee_contribution}")
        print(f"  ER: ₹{txn.employer_contribution}")

    def test_epf_save_to_db(self, epf_file, clean_db, test_user_id):
        """Test EPF data persistence to database."""
        parser = EPFParser(clean_db)
        result = parser.parse(epf_file)

        # Save to database
        count = parser.save_to_db(result, user_id=test_user_id)

        assert count > 0, "No records saved"

        # Verify in database
        cursor = clean_db.execute("SELECT COUNT(*) as cnt FROM epf_transactions")
        db_count = cursor.fetchone()['cnt']

        assert db_count == count, f"Expected {count}, got {db_count}"

        print(f"\n✓ Database Save")
        print(f"  Saved: {count} transactions")
        print(f"  Verified: {db_count} in DB")

    def test_epf_80c_calculation(self, epf_file, test_db):
        """Test 80C deduction calculation."""
        parser = EPFParser(test_db)
        result = parser.parse(epf_file)

        eligible_80c = parser.calculate_80c_eligible(result.transactions)

        assert eligible_80c >= 0, "80C calculation error"
        assert eligible_80c <= Decimal('9999999'), "80C value unrealistic"

        cap = Decimal('150000')
        deductible = min(eligible_80c, cap)

        print(f"\n✓ 80C Calculation")
        print(f"  Eligible: ₹{eligible_80c:,.2f}")
        print(f"  Cap: ₹{cap:,.2f}")
        print(f"  Deductible: ₹{deductible:,.2f}")

    def test_epf_interest_extraction(self, epf_file, test_db):
        """Test EPF interest and TDS extraction."""
        parser = EPFParser(test_db)
        result = parser.parse(epf_file)

        if result.interest:
            interest = result.interest

            assert interest.financial_year is not None
            assert interest.employee_interest >= 0
            assert interest.employer_interest >= 0

            print(f"\n✓ Interest Details")
            print(f"  FY: {interest.financial_year}")
            print(f"  EE Interest: ₹{interest.employee_interest:,.2f}")
            print(f"  ER Interest: ₹{interest.employer_interest:,.2f}")
            print(f"  TDS: ₹{interest.tds_deducted:,.2f}")
        else:
            pytest.skip("No interest data in this EPF file")


# =============================================================================
# Multi-User Parameterized Tests
# =============================================================================

@pytest.mark.parametrize("path_resolver", ["Sanjay"], indirect=True)
class TestEPFMultiUser:
    """Multi-user EPF parser tests."""

    def test_epf_multi_user_parse(self, path_resolver, epf_file, test_db):
        """Test EPF parsing for multiple users."""
        print(f"\nTesting for user: {path_resolver.user_name}")

        parser = EPFParser(test_db)
        result = parser.parse(epf_file)

        assert result.success
        print(f"✓ User {path_resolver.user_name}: {len(result.transactions)} transactions")


# =============================================================================
# Golden Master Tests
# =============================================================================

class TestEPFGoldenMaster:
    """EPF parser golden master tests."""

    def test_epf_totals_golden(self, epf_file, test_db):
        """Test EPF totals against golden master."""
        parser = EPFParser(test_db)
        result = parser.parse(epf_file)

        # Calculate totals
        totals = {
            'total_transactions': len(result.transactions),
            'total_ee_contribution': str(sum(t.employee_contribution for t in result.transactions)),
            'total_er_contribution': str(sum(t.employer_contribution for t in result.transactions)),
            'total_pension': str(sum(t.pension_contribution for t in result.transactions)),
        }

        # Compare with golden master
        from conftest import assert_golden_match
        assert_golden_match(
            totals,
            f'epf_totals_{epf_file.stem}',
            format='json',
            save_if_missing=True
        )
```

Continue in next part due to length...
