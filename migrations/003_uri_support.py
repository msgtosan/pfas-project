#!/usr/bin/env python3
"""
Migration 003: Add URI Support for Multi-User File Isolation.

This migration adds:
1. file_uri column to relevant tables for portable file addressing
2. user_namespace column for multi-tenant isolation
3. relative_path column for environment-independent paths
4. Indexes for efficient URI lookups

Tables affected:
- statement_processing_log
- ingestion_log
- golden_reference

Usage:
    python migrations/003_uri_support.py --db-path Data/Users/Sanjay/db/finance.db --password xxx
    python migrations/003_uri_support.py --db-path Data/Users/Sanjay/db/finance.db --password xxx --dry-run
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

MIGRATION_VERSION = "003"
MIGRATION_NAME = "uri_support"


def get_connection(db_path: str, password: str = None):
    """Get database connection with optional encryption."""
    try:
        import sqlcipher3
        conn = sqlcipher3.connect(db_path)
        if password:
            conn.execute(f"PRAGMA key = '{password}'")
            conn.execute("PRAGMA cipher_compatibility = 4")
        # Verify connection works
        conn.execute("SELECT 1").fetchone()
        logger.info(f"Connected to database: {db_path}")
        return conn
    except ImportError:
        import sqlite3
        conn = sqlite3.connect(db_path)
        logger.info(f"Connected to database (sqlite3): {db_path}")
        return conn


def check_migration_status(conn) -> bool:
    """Check if migration has already been applied."""
    try:
        cursor = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE version = ?",
            (MIGRATION_VERSION,)
        )
        return cursor.fetchone() is not None
    except Exception:
        # Table doesn't exist yet
        return False


def ensure_schema_migrations_table(conn):
    """Ensure schema_migrations table exists."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        )
    """)
    conn.commit()


def column_exists(conn, table: str, column: str) -> bool:
    """Check if a column exists in a table."""
    try:
        cursor = conn.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in cursor.fetchall()]
        return column in columns
    except Exception:
        return False


def table_exists(conn, table: str) -> bool:
    """Check if a table exists."""
    cursor = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    )
    return cursor.fetchone() is not None


def run_migration(conn, dry_run: bool = False):
    """Run the migration."""
    logger.info(f"Running migration {MIGRATION_VERSION}: {MIGRATION_NAME}")

    statements = []

    # 1. Add columns to statement_processing_log
    if table_exists(conn, "statement_processing_log"):
        if not column_exists(conn, "statement_processing_log", "file_uri"):
            statements.append(
                "ALTER TABLE statement_processing_log ADD COLUMN file_uri TEXT"
            )
        if not column_exists(conn, "statement_processing_log", "user_namespace"):
            statements.append(
                "ALTER TABLE statement_processing_log ADD COLUMN user_namespace TEXT"
            )
        if not column_exists(conn, "statement_processing_log", "relative_path"):
            statements.append(
                "ALTER TABLE statement_processing_log ADD COLUMN relative_path TEXT"
            )

    # 2. Add columns to ingestion_log
    if table_exists(conn, "ingestion_log"):
        if not column_exists(conn, "ingestion_log", "file_uri"):
            statements.append(
                "ALTER TABLE ingestion_log ADD COLUMN file_uri TEXT"
            )
        if not column_exists(conn, "ingestion_log", "user_namespace"):
            statements.append(
                "ALTER TABLE ingestion_log ADD COLUMN user_namespace TEXT"
            )
        if not column_exists(conn, "ingestion_log", "relative_path"):
            statements.append(
                "ALTER TABLE ingestion_log ADD COLUMN relative_path TEXT"
            )

    # 3. Add columns to golden_reference
    if table_exists(conn, "golden_reference"):
        if not column_exists(conn, "golden_reference", "file_uri"):
            statements.append(
                "ALTER TABLE golden_reference ADD COLUMN file_uri TEXT"
            )
        if not column_exists(conn, "golden_reference", "user_namespace"):
            statements.append(
                "ALTER TABLE golden_reference ADD COLUMN user_namespace TEXT"
            )

    # 4. Create file_manifests table for tracking manifest metadata
    if not table_exists(conn, "file_manifests"):
        statements.append("""
            CREATE TABLE file_manifests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                category TEXT NOT NULL,
                manifest_version TEXT NOT NULL DEFAULT '1.0',
                total_entries INTEGER DEFAULT 0,
                active_entries INTEGER DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                manifest_hash TEXT,
                UNIQUE(user_id, category),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)

    # 5. Create file_lineage table for tracking supersession chains
    if not table_exists(conn, "file_lineage"):
        statements.append("""
            CREATE TABLE file_lineage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                category TEXT NOT NULL,
                predecessor_hash TEXT NOT NULL,
                successor_hash TEXT NOT NULL,
                superseded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                supersession_reason TEXT,
                UNIQUE(predecessor_hash, successor_hash),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)

    # 6. Create indexes for efficient lookups
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_spl_file_uri ON statement_processing_log(file_uri)",
        "CREATE INDEX IF NOT EXISTS idx_spl_user_namespace ON statement_processing_log(user_namespace)",
        "CREATE INDEX IF NOT EXISTS idx_ingestion_file_uri ON ingestion_log(file_uri)",
        "CREATE INDEX IF NOT EXISTS idx_ingestion_user_namespace ON ingestion_log(user_namespace)",
        "CREATE INDEX IF NOT EXISTS idx_golden_file_uri ON golden_reference(file_uri)",
        "CREATE INDEX IF NOT EXISTS idx_lineage_predecessor ON file_lineage(predecessor_hash)",
        "CREATE INDEX IF NOT EXISTS idx_lineage_successor ON file_lineage(successor_hash)",
    ]

    if dry_run:
        logger.info("DRY RUN - Would execute the following statements:")
        for stmt in statements + index_statements:
            logger.info(f"  {stmt[:100]}...")
        return

    # Execute statements
    for stmt in statements:
        try:
            conn.execute(stmt)
            logger.info(f"Executed: {stmt[:60]}...")
        except Exception as e:
            logger.warning(f"Statement failed (may already exist): {e}")

    # Execute indexes (these are safe to run multiple times)
    for stmt in index_statements:
        try:
            conn.execute(stmt)
        except Exception as e:
            logger.debug(f"Index statement: {e}")

    conn.commit()
    logger.info("Migration statements executed successfully")


def record_migration(conn):
    """Record migration in schema_migrations table."""
    # Check if description column exists
    cursor = conn.execute("PRAGMA table_info(schema_migrations)")
    columns = [row[1] for row in cursor.fetchall()]

    if "description" in columns:
        conn.execute("""
            INSERT OR REPLACE INTO schema_migrations (version, name, applied_at, description)
            VALUES (?, ?, ?, ?)
        """, (
            MIGRATION_VERSION,
            MIGRATION_NAME,
            datetime.now().isoformat(),
            "Add URI support columns for multi-user file isolation and temporal versioning"
        ))
    else:
        conn.execute("""
            INSERT OR REPLACE INTO schema_migrations (version, name, applied_at)
            VALUES (?, ?, ?)
        """, (
            MIGRATION_VERSION,
            MIGRATION_NAME,
            datetime.now().isoformat(),
        ))
    conn.commit()
    logger.info(f"Recorded migration {MIGRATION_VERSION} in schema_migrations")


def main():
    parser = argparse.ArgumentParser(
        description="Migration 003: Add URI Support for Multi-User File Isolation"
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to database file"
    )
    parser.add_argument(
        "--password",
        help="Database encryption password"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even if already applied"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Connect to database
    conn = get_connection(args.db_path, args.password)

    # Ensure schema_migrations table exists
    ensure_schema_migrations_table(conn)

    # Check if already applied
    if check_migration_status(conn) and not args.force:
        logger.info(f"Migration {MIGRATION_VERSION} already applied. Use --force to rerun.")
        return 0

    # Run migration
    run_migration(conn, dry_run=args.dry_run)

    # Record migration
    if not args.dry_run:
        record_migration(conn)

    logger.info(f"Migration {MIGRATION_VERSION} completed successfully")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
