"""
Migration 001: Foundation Hardening

Adds tables and columns for:
- Audit log with source tracking
- NAV history for mutual funds
- Transaction idempotency
- Batch ingestion tracking

Run with:
    python migrations/001_foundation_hardening.py --db-path /path/to/finance.db

Or programmatically:
    from migrations.foundation_hardening import run_migration
    run_migration(conn)
"""

import argparse
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

logger = logging.getLogger(__name__)

MIGRATION_VERSION = "001"
MIGRATION_NAME = "foundation_hardening"


def get_db_connection(db_path: Path, password: str = None):
    """Get database connection with optional encryption."""
    try:
        import sqlcipher3
        conn = sqlcipher3.connect(str(db_path))
        if password:
            conn.execute(f"PRAGMA key='{password}'")
            conn.execute("PRAGMA cipher_compatibility=4")
        conn.execute("SELECT 1").fetchone()  # Test connection
        return conn
    except ImportError:
        return sqlite3.connect(str(db_path))


def check_migration_status(conn: sqlite3.Connection) -> bool:
    """Check if migration has already been applied."""
    try:
        cursor = conn.execute("""
            SELECT 1 FROM schema_migrations WHERE version = ?
        """, (MIGRATION_VERSION,))
        return cursor.fetchone() is not None
    except sqlite3.OperationalError:
        # Table doesn't exist yet
        return False


def record_migration(conn: sqlite3.Connection) -> None:
    """Record that migration was applied."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        INSERT INTO schema_migrations (version, name)
        VALUES (?, ?)
    """, (MIGRATION_VERSION, MIGRATION_NAME))

    conn.commit()


def run_migration(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """
    Run the foundation hardening migration.

    Args:
        conn: Database connection
        dry_run: If True, don't commit changes

    Returns:
        Dictionary with migration results
    """
    results = {
        "version": MIGRATION_VERSION,
        "name": MIGRATION_NAME,
        "tables_created": [],
        "columns_added": [],
        "indexes_created": [],
        "errors": []
    }

    if check_migration_status(conn):
        logger.info(f"Migration {MIGRATION_VERSION} already applied, skipping")
        results["skipped"] = True
        return results

    cursor = conn.cursor()

    try:
        # =====================================================
        # 1. AUDIT LOG - Add source column if missing
        # =====================================================
        cursor.execute("PRAGMA table_info(audit_log)")
        audit_columns = [col[1] for col in cursor.fetchall()]

        if "audit_log" not in get_tables(conn):
            cursor.execute("""
                CREATE TABLE audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    record_id INTEGER,
                    action TEXT CHECK(action IN ('INSERT','UPDATE','DELETE')),
                    old_values JSON,
                    new_values JSON,
                    user_id INTEGER,
                    ip_address TEXT,
                    source TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            results["tables_created"].append("audit_log")
            logger.info("Created audit_log table")

        elif "source" not in audit_columns:
            cursor.execute("ALTER TABLE audit_log ADD COLUMN source TEXT")
            results["columns_added"].append("audit_log.source")
            logger.info("Added source column to audit_log")

        # =====================================================
        # 2. NAV HISTORY TABLE
        # =====================================================
        if "mf_nav_history" not in get_tables(conn):
            cursor.execute("""
                CREATE TABLE mf_nav_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scheme_id INTEGER NOT NULL,
                    nav_date DATE NOT NULL,
                    nav DECIMAL(12, 4) NOT NULL,
                    source TEXT DEFAULT 'unknown',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(scheme_id, nav_date),
                    FOREIGN KEY (scheme_id) REFERENCES mf_schemes(id)
                )
            """)
            results["tables_created"].append("mf_nav_history")
            logger.info("Created mf_nav_history table")

            cursor.execute("""
                CREATE INDEX idx_nav_history_scheme_date
                ON mf_nav_history(scheme_id, nav_date)
            """)
            results["indexes_created"].append("idx_nav_history_scheme_date")

        # =====================================================
        # 3. PROCESSED TRANSACTIONS (Idempotency)
        # =====================================================
        if "processed_transactions" not in get_tables(conn):
            cursor.execute("""
                CREATE TABLE processed_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    idempotency_key TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    journal_id INTEGER,
                    source TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    metadata JSON,
                    FOREIGN KEY (journal_id) REFERENCES journals(id)
                )
            """)
            results["tables_created"].append("processed_transactions")
            logger.info("Created processed_transactions table")

            cursor.execute("""
                CREATE INDEX idx_processed_txn_key
                ON processed_transactions(idempotency_key)
            """)
            results["indexes_created"].append("idx_processed_txn_key")

        # =====================================================
        # 4. PARSER IDEMPOTENCY TABLE
        # =====================================================
        if "parser_idempotency" not in get_tables(conn):
            cursor.execute("""
                CREATE TABLE parser_idempotency (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    idempotency_key TEXT UNIQUE NOT NULL,
                    parser_type TEXT NOT NULL,
                    record_table TEXT,
                    record_id INTEGER,
                    source_file TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            results["tables_created"].append("parser_idempotency")
            logger.info("Created parser_idempotency table")

            cursor.execute("""
                CREATE INDEX idx_parser_idempotency_key
                ON parser_idempotency(idempotency_key)
            """)
            results["indexes_created"].append("idx_parser_idempotency_key")

        # =====================================================
        # 5. PROCESSED FILES (Batch Ingestion)
        # =====================================================
        if "processed_files" not in get_tables(conn):
            cursor.execute("""
                CREATE TABLE processed_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_hash TEXT UNIQUE NOT NULL,
                    file_name TEXT NOT NULL,
                    file_path TEXT,
                    file_size INTEGER,
                    user_id INTEGER NOT NULL,
                    batch_id TEXT,
                    parser_type TEXT,
                    records_count INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'success',
                    error_message TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            results["tables_created"].append("processed_files")
            logger.info("Created processed_files table")

            cursor.execute("""
                CREATE INDEX idx_processed_files_hash
                ON processed_files(file_hash)
            """)
            results["indexes_created"].append("idx_processed_files_hash")

        # =====================================================
        # 6. BATCH RUNS TABLE
        # =====================================================
        if "batch_runs" not in get_tables(conn):
            cursor.execute("""
                CREATE TABLE batch_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    files_count INTEGER,
                    records_count INTEGER,
                    status TEXT DEFAULT 'pending',
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT
                )
            """)
            results["tables_created"].append("batch_runs")
            logger.info("Created batch_runs table")

        # =====================================================
        # 7. SCHEMA MIGRATIONS TABLE
        # =====================================================
        if "schema_migrations" not in get_tables(conn):
            cursor.execute("""
                CREATE TABLE schema_migrations (
                    version TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            results["tables_created"].append("schema_migrations")
            logger.info("Created schema_migrations table")

        # =====================================================
        # 8. BACKFILL NAV HISTORY FROM TRANSACTIONS
        # =====================================================
        if "mf_nav_history" in results["tables_created"]:
            cursor.execute("""
                INSERT OR IGNORE INTO mf_nav_history (scheme_id, nav_date, nav, source)
                SELECT DISTINCT
                    mf.scheme_id,
                    mt.date,
                    mt.nav,
                    'migration_backfill'
                FROM mf_transactions mt
                JOIN mf_folios mf ON mt.folio_id = mf.id
                WHERE mt.nav IS NOT NULL AND mt.nav > 0
            """)
            backfill_count = cursor.rowcount
            if backfill_count > 0:
                logger.info(f"Backfilled {backfill_count} NAV records from transactions")
                results["nav_backfill_count"] = backfill_count

        # Record migration
        if not dry_run:
            record_migration(conn)
            conn.commit()
            logger.info(f"Migration {MIGRATION_VERSION} applied successfully")
        else:
            conn.rollback()
            logger.info(f"Migration {MIGRATION_VERSION} dry run completed (rolled back)")

        results["success"] = True

    except Exception as e:
        conn.rollback()
        results["success"] = False
        results["errors"].append(str(e))
        logger.exception(f"Migration failed: {e}")

    return results


def get_tables(conn: sqlite3.Connection) -> list:
    """Get list of existing tables."""
    cursor = conn.execute("""
        SELECT name FROM sqlite_master WHERE type='table'
    """)
    return [row[0] for row in cursor.fetchall()]


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Run PFAS Foundation Hardening Migration"
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        required=True,
        help="Path to SQLite/SQLCipher database"
    )
    parser.add_argument(
        "--password",
        help="Database encryption password"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without committing"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    if not args.db_path.exists():
        logger.error(f"Database not found: {args.db_path}")
        sys.exit(1)

    conn = get_db_connection(args.db_path, args.password)

    results = run_migration(conn, dry_run=args.dry_run)

    print("\n=== Migration Results ===")
    print(f"Version: {results['version']}")
    print(f"Name: {results['name']}")
    print(f"Success: {results.get('success', False)}")

    if results.get("skipped"):
        print("Status: Already applied (skipped)")
    else:
        if results["tables_created"]:
            print(f"Tables created: {', '.join(results['tables_created'])}")
        if results["columns_added"]:
            print(f"Columns added: {', '.join(results['columns_added'])}")
        if results["indexes_created"]:
            print(f"Indexes created: {', '.join(results['indexes_created'])}")
        if results.get("nav_backfill_count"):
            print(f"NAV records backfilled: {results['nav_backfill_count']}")

    if results.get("errors"):
        print(f"Errors: {results['errors']}")
        sys.exit(1)

    conn.close()


if __name__ == "__main__":
    main()
