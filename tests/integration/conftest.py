"""
Integration Test Fixtures - Scalable, Multi-User, Multi-Asset

Features:
- PathResolver for ALL paths (NO hardcoding)
- Multi-user support (PFAS_TEST_USER env var)
- Multi-asset parameterization
- Configurable archive fallback (inbox first, then archive)
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
from typing import Generator, Dict, Any, List, Optional

from pfas.core.paths import PathResolver
from pfas.core.database import DatabaseManager
from pfas.core.accounts import setup_chart_of_accounts

# Environment Configuration
DEFAULT_TEST_USER = "Sanjay"
TEST_USER = os.getenv("PFAS_TEST_USER", DEFAULT_TEST_USER)
PFAS_TEST_ROOT = os.getenv("PFAS_ROOT", str(Path.cwd()))

# Test configuration - can be overridden via environment or config file
USE_ARCHIVE_FALLBACK = os.getenv("PFAS_TEST_USE_ARCHIVE", "true").lower() == "true"

TEST_ASSETS = [
    "Mutual-Fund", "Bank", "Indian-Stocks", "Salary",
    "EPF", "NPS", "PPF", "SGB", "USA-Stocks", "FD-Bonds"
]

TEST_DB_PASSWORD = "test_password_integration"
GOLDEN_MASTER_DIR = Path(__file__).parent / "golden_masters"


def _load_test_config() -> Dict[str, Any]:
    """Load test configuration from config/test_config.json."""
    config_path = Path(PFAS_TEST_ROOT) / "config" / "test_config.json"
    if config_path.exists():
        try:
            with open(config_path, encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"[WARN] Failed to load test config: {e}")
    return {
        "file_sources": {
            "primary": "inbox",
            "fallback_to_archive": True
        }
    }


# Load test configuration at module level
TEST_CONFIG = _load_test_config()


# Core Fixtures
@pytest.fixture(scope="session")
def test_root() -> Path:
    """Return PFAS test root directory."""
    root = Path(PFAS_TEST_ROOT)
    print(f"\n[FIXTURE] Test Root: {root}")
    return root


@pytest.fixture(scope="session", params=[TEST_USER])
def path_resolver(request, test_root) -> PathResolver:
    """Session-scoped PathResolver with multi-user support."""
    user = request.param
    resolver = PathResolver(root_path=test_root, user_name=user)

    print(f"\n[FIXTURE] PathResolver for user: {user}")
    print(f"   Inbox: {resolver.inbox()}")

    if not resolver.user_dir.exists():
        pytest.skip(
            f"User directory not found: {resolver.user_dir}\n"
            f"Create it or set PFAS_TEST_USER to an existing user"
        )

    return resolver


@pytest.fixture(scope="session")
def test_db() -> Generator:
    """Session-scoped in-memory database."""
    print("\n[FIXTURE] Creating in-memory test database...")

    DatabaseManager.reset_instance()
    db_manager = DatabaseManager()
    conn = db_manager.init(":memory:", TEST_DB_PASSWORD)
    setup_chart_of_accounts(conn)

    conn.execute("""
        INSERT INTO users (id, pan_encrypted, pan_salt, name, email)
        VALUES (1, X'00', X'00', 'Test User', 'test@example.com')
    """)
    conn.commit()

    yield conn

    db_manager.close()
    DatabaseManager.reset_instance()


@pytest.fixture
def clean_db(test_db):
    """Function-scoped fixture that resets database state."""
    # Check if database is still open before attempting operations
    try:
        test_db.execute("SELECT 1")
    except Exception:
        # Database connection already closed - this can happen with session-scoped fixtures
        pytest.skip("Database connection closed - session fixture cleanup in progress")
        return

    tables = [
        'journal_entries', 'journals', 'bank_transactions',
        'mf_transactions', 'mf_holdings', 'stock_trades',
        'epf_transactions', 'nps_transactions', 'ppf_transactions',
        'salary_records', 'ingestion_log'
    ]

    for table in tables:
        try:
            test_db.execute(f"DELETE FROM {table}")
        except Exception:
            pass

    try:
        test_db.commit()
    except Exception:
        # Database already closed, this is fine
        pass

    yield test_db


# Asset-Specific Fixtures
def _search_directory_for_files(
    search_path: Path,
    extensions: List[str],
    pattern: str = '*',
    exclude_patterns: List[str] = None
) -> List[Path]:
    """Search a directory for files matching criteria.

    Args:
        search_path: Directory to search in
        extensions: List of file extensions to search for
        pattern: Glob pattern to match (default: '*' for all files)
        exclude_patterns: List of patterns to exclude from filename

    Returns:
        List of matching file paths
    """
    if not search_path.exists():
        return []

    if exclude_patterns is None:
        exclude_patterns = []

    files = []
    for ext in extensions:
        found = list(search_path.rglob(f"{pattern}{ext}"))
        for file in found:
            # Skip files in 'failed' subdirectory
            if 'failed' in file.parts:
                continue

            # Skip files matching exclusion patterns
            if any(excl.lower() in file.name.lower() for excl in exclude_patterns):
                continue

            files.append(file)

    return files


def _find_latest_file(
    inbox_path: Path,
    extensions: List[str],
    asset_type: str,
    user_name: str,
    pattern: str = '*',
    exclude_patterns: List[str] = None,
    archive_path: Optional[Path] = None,
    use_archive_fallback: Optional[bool] = None
) -> Path:
    """Find latest file with graceful skip, pattern filtering, and archive fallback.

    Args:
        inbox_path: Primary directory to search in (inbox)
        extensions: List of file extensions to search for (e.g., ['.pdf', '.xlsx'])
        asset_type: Asset type name for error messages
        user_name: User name for error messages
        pattern: Glob pattern to match (default: '*' for all files)
        exclude_patterns: List of patterns to exclude from filename (e.g., ['holdings', 'interest'])
        archive_path: Optional archive directory to search if inbox is empty
        use_archive_fallback: Override global fallback setting (None uses config)

    Returns:
        Path to the latest matching file
    """
    if exclude_patterns is None:
        exclude_patterns = []

    # Determine if archive fallback should be used
    if use_archive_fallback is None:
        fallback_config = TEST_CONFIG.get("file_sources", {})
        use_archive_fallback = fallback_config.get("fallback_to_archive", USE_ARCHIVE_FALLBACK)

    # Search inbox first
    files = _search_directory_for_files(inbox_path, extensions, pattern, exclude_patterns)
    source_used = "inbox"

    # Fallback to archive if inbox is empty and fallback is enabled
    if not files and use_archive_fallback and archive_path:
        files = _search_directory_for_files(archive_path, extensions, pattern, exclude_patterns)
        if files:
            source_used = "archive"
            print(f"\n[FIXTURE] Inbox empty, using archive for {asset_type}")

    # If still no files, try archive even if inbox doesn't exist
    if not files and use_archive_fallback and archive_path and not inbox_path.exists():
        files = _search_directory_for_files(archive_path, extensions, pattern, exclude_patterns)
        if files:
            source_used = "archive"
            print(f"\n[FIXTURE] Inbox not found, using archive for {asset_type}")

    if not files:
        # Build helpful error message
        searched_paths = [str(inbox_path)]
        if use_archive_fallback and archive_path:
            searched_paths.append(str(archive_path))

        pytest.skip(
            f"No {asset_type} files found.\n"
            f"User: {user_name}\n"
            f"Searched paths:\n  - " + "\n  - ".join(searched_paths) + "\n"
            f"Expected extensions: {', '.join(extensions)}\n"
            f"Pattern: {pattern}\n"
            f"Excluded patterns: {', '.join(exclude_patterns) if exclude_patterns else 'None'}\n"
            f"Add test files to inbox or archive to run this test."
        )

    latest = max(files, key=lambda p: p.stat().st_mtime)
    print(f"\n[FIXTURE] Selected {asset_type} file from {source_used}: {latest.name}")
    return latest


def _get_archive_path(path_resolver: PathResolver, asset_subpath: str) -> Path:
    """Get the archive path for an asset type.

    Args:
        path_resolver: PathResolver instance
        asset_subpath: Relative path within inbox/archive (e.g., 'Mutual-Fund/CAMS')

    Returns:
        Archive path for the asset
    """
    return path_resolver.archive() / asset_subpath


def get_asset_path(
    path_resolver: PathResolver,
    asset_subpath: str,
    use_archive_fallback: Optional[bool] = None
) -> Path:
    """Get the best available path for an asset (inbox or archive).

    This function checks inbox first, then falls back to archive if configured.

    Args:
        path_resolver: PathResolver instance
        asset_subpath: Relative path within inbox/archive (e.g., 'Mutual-Fund/CAMS')
        use_archive_fallback: Override global fallback setting (None uses config)

    Returns:
        Path to the asset directory (inbox or archive, whichever has files)

    Raises:
        pytest.skip if neither inbox nor archive exist or have files
    """
    inbox_path = path_resolver.inbox() / asset_subpath
    archive_path = path_resolver.archive() / asset_subpath

    # Determine if archive fallback should be used
    if use_archive_fallback is None:
        fallback_config = TEST_CONFIG.get("file_sources", {})
        use_archive_fallback = fallback_config.get("fallback_to_archive", USE_ARCHIVE_FALLBACK)

    # Check inbox first
    if inbox_path.exists() and any(inbox_path.iterdir()):
        return inbox_path

    # Fallback to archive if enabled
    if use_archive_fallback and archive_path.exists() and any(archive_path.iterdir()):
        print(f"\n[PATH] Using archive for {asset_subpath} (inbox empty or missing)")
        return archive_path

    # Return inbox path even if empty (let the test decide what to do)
    return inbox_path


def find_files_in_path(
    path_resolver: PathResolver,
    asset_subpath: str,
    extensions: List[str],
    pattern: str = '*',
    exclude_patterns: List[str] = None,
    use_archive_fallback: Optional[bool] = None
) -> List[Path]:
    """Find files in inbox or archive for an asset type.

    Args:
        path_resolver: PathResolver instance
        asset_subpath: Relative path within inbox/archive
        extensions: List of file extensions to search for
        pattern: Glob pattern to match (default: '*' for all files)
        exclude_patterns: List of patterns to exclude from filename
        use_archive_fallback: Override global fallback setting

    Returns:
        List of matching file paths
    """
    inbox_path = path_resolver.inbox() / asset_subpath
    archive_path = path_resolver.archive() / asset_subpath

    # Determine if archive fallback should be used
    if use_archive_fallback is None:
        fallback_config = TEST_CONFIG.get("file_sources", {})
        use_archive_fallback = fallback_config.get("fallback_to_archive", USE_ARCHIVE_FALLBACK)

    # Search inbox first
    files = _search_directory_for_files(inbox_path, extensions, pattern, exclude_patterns)
    source = "inbox"

    # Fallback to archive if inbox is empty
    if not files and use_archive_fallback:
        files = _search_directory_for_files(archive_path, extensions, pattern, exclude_patterns)
        if files:
            source = "archive"
            print(f"\n[FILES] Using {len(files)} file(s) from archive for {asset_subpath}")

    return files


@pytest.fixture
def epf_file(path_resolver) -> Path:
    """Latest EPF passbook PDF from inbox/archive (excluding interest-only statements)."""
    inbox = path_resolver.inbox() / "EPF"
    archive = _get_archive_path(path_resolver, "EPF")
    # Exclude interest-only statements - we want passbooks with full transaction details
    return _find_latest_file(
        inbox,
        ['.pdf'],
        "EPF",
        path_resolver.user_name,
        exclude_patterns=['interest'],  # Skip interest-only statements
        archive_path=archive
    )


@pytest.fixture
def mutual_fund_file(path_resolver) -> Path:
    """Latest Mutual Fund transaction file from inbox/archive (excluding holdings)."""
    inbox = path_resolver.inbox() / "Mutual-Fund"
    archive = _get_archive_path(path_resolver, "Mutual-Fund")
    # Exclude holdings files - we want transaction files
    return _find_latest_file(
        inbox,
        ['.pdf', '.xlsx', '.xls'],
        "Mutual-Fund",
        path_resolver.user_name,
        exclude_patterns=['holding', 'holdings', 'consolidated'],
        archive_path=archive
    )


# Parser-specific MF fixtures
@pytest.fixture
def cams_file(path_resolver) -> Path:
    """Latest CAMS transaction file from inbox/archive."""
    inbox = path_resolver.inbox() / "Mutual-Fund" / "CAMS"
    archive = _get_archive_path(path_resolver, "Mutual-Fund/CAMS")
    return _find_latest_file(
        inbox,
        ['.pdf', '.xlsx', '.xls'],
        "CAMS",
        path_resolver.user_name,
        exclude_patterns=['holding', 'holdings'],
        archive_path=archive
    )


@pytest.fixture
def karvy_file(path_resolver) -> Path:
    """Latest KARVY/KFintech transaction file from inbox/archive."""
    inbox = path_resolver.inbox() / "Mutual-Fund" / "KARVY"
    archive = _get_archive_path(path_resolver, "Mutual-Fund/KARVY")
    return _find_latest_file(
        inbox,
        ['.pdf', '.xlsx', '.xls'],
        "KARVY",
        path_resolver.user_name,
        exclude_patterns=['holding', 'holdings'],
        archive_path=archive
    )


@pytest.fixture
def bank_file(path_resolver) -> Path:
    """Latest Bank statement from inbox/archive."""
    inbox = path_resolver.inbox() / "Bank"
    archive = _get_archive_path(path_resolver, "Bank")
    return _find_latest_file(
        inbox,
        ['.pdf', '.xlsx', '.xls', '.csv'],
        "Bank",
        path_resolver.user_name,
        archive_path=archive
    )


@pytest.fixture
def nps_file(path_resolver) -> Path:
    """Latest NPS transaction file from inbox/archive."""
    inbox = path_resolver.inbox() / "NPS"
    archive = _get_archive_path(path_resolver, "NPS")
    return _find_latest_file(
        inbox,
        ['.pdf', '.csv', '.xlsx'],
        "NPS",
        path_resolver.user_name,
        archive_path=archive
    )


@pytest.fixture
def ppf_file(path_resolver) -> Path:
    """Latest PPF file from inbox/archive."""
    inbox = path_resolver.inbox() / "PPF"
    archive = _get_archive_path(path_resolver, "PPF")
    return _find_latest_file(
        inbox,
        ['.pdf', '.xlsx'],
        "PPF",
        path_resolver.user_name,
        archive_path=archive
    )


@pytest.fixture
def stock_file(path_resolver) -> Path:
    """Latest Indian Stock trading file from inbox/archive (excluding holdings)."""
    inbox = path_resolver.inbox() / "Indian-Stocks"
    archive = _get_archive_path(path_resolver, "Indian-Stocks")
    # Exclude holdings files - we want trading/P&L files
    return _find_latest_file(
        inbox,
        ['.pdf', '.xlsx', '.csv'],
        "Indian-Stocks",
        path_resolver.user_name,
        exclude_patterns=['holding', 'holdings', 'portfolio'],
        archive_path=archive
    )


# Parser-specific stock fixtures
@pytest.fixture
def zerodha_file(path_resolver) -> Path:
    """Latest Zerodha Tax P&L file from inbox/archive."""
    inbox = path_resolver.inbox() / "Indian-Stocks" / "Zerodha"
    archive = _get_archive_path(path_resolver, "Indian-Stocks/Zerodha")
    # Look for taxpnl files specifically
    return _find_latest_file(
        inbox,
        ['.xlsx', '.csv'],
        "Zerodha",
        path_resolver.user_name,
        pattern='*taxpnl*',
        archive_path=archive
    )


@pytest.fixture
def icici_direct_file(path_resolver) -> Path:
    """Latest ICICI Direct trading file from inbox/archive."""
    inbox = path_resolver.inbox() / "Indian-Stocks" / "ICICIDirect"
    archive = _get_archive_path(path_resolver, "Indian-Stocks/ICICIDirect")
    # Exclude holdings files
    return _find_latest_file(
        inbox,
        ['.csv', '.xlsx'],
        "ICICIDirect",
        path_resolver.user_name,
        exclude_patterns=['holding', 'holdings', 'portfolio'],
        archive_path=archive
    )


@pytest.fixture
def salary_file(path_resolver) -> Path:
    """Latest Salary file from inbox/archive."""
    inbox = path_resolver.inbox() / "Salary"
    archive = _get_archive_path(path_resolver, "Salary")
    return _find_latest_file(
        inbox,
        ['.pdf', '.xlsx'],
        "Salary",
        path_resolver.user_name,
        archive_path=archive
    )


# Parameterized Fixtures
@pytest.fixture(params=TEST_ASSETS)
def asset_type(request) -> str:
    """Parameterized asset type fixture."""
    return request.param


@pytest.fixture
def asset_inbox(path_resolver, asset_type) -> Path:
    """Parameterized asset inbox directory."""
    inbox = path_resolver.inbox() / asset_type
    if not inbox.exists():
        pytest.skip(f"Inbox not found for {asset_type}: {inbox}")
    return inbox


# Golden Master Helpers
def save_golden(data: Any, test_name: str, format: str = 'json') -> None:
    """Save golden master data."""
    GOLDEN_MASTER_DIR.mkdir(exist_ok=True, parents=True)
    golden_file = GOLDEN_MASTER_DIR / f"{test_name}.golden.{format}"

    if format == 'json':
        class DecimalEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, Decimal):
                    return str(obj)
                if isinstance(obj, date):
                    return obj.isoformat()
                return super().default(obj)

        with open(golden_file, 'w') as f:
            json.dump(data, f, indent=2, cls=DecimalEncoder)
    else:
        raise ValueError(f"Unsupported format: {format}")

    print(f"\n[GOLDEN] Saved: {golden_file.name}")


def assert_golden_match(actual: Any, test_name: str, format: str = 'json',
                        tolerance: float = 0.01, save_if_missing: bool = True) -> None:
    """Assert actual data matches golden master."""
    golden_file = GOLDEN_MASTER_DIR / f"{test_name}.golden.{format}"

    if not golden_file.exists():
        if save_if_missing:
            print(f"\n[GOLDEN] Creating: {golden_file.name}")
            save_golden(actual, test_name, format)
            return
        else:
            pytest.fail(f"Golden master not found: {golden_file}")

    if format == 'json':
        with open(golden_file) as f:
            expected = json.load(f)

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
            pytest.fail(f"Golden master mismatch for {test_name}\nExpected: {expected}\nActual: {actual}")

        print(f"\n[GOLDEN] ✓ Match verified: {golden_file.name}")


@pytest.fixture
def test_user_id() -> int:
    """Return test user ID."""
    return 1
