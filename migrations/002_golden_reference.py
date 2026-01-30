"""
Migration 002: Golden Reference Reconciliation Engine

Adds tables for:
- truth_sources: Defines source of truth per metric type
- golden_reference: External authoritative statements (NSDL/CDSL CAS)
- golden_holdings: Parsed holdings from golden sources
- reconciliation_events: Cross-correlation results
- reconciliation_suspense: Unresolved mismatches

Run with:
    python migrations/002_golden_reference.py --db-path /path/to/finance.db

Or programmatically:
    from migrations.golden_reference import run_migration
    run_migration(conn)
"""

import argparse
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

logger = logging.getLogger(__name__)

MIGRATION_VERSION = "002"
MIGRATION_NAME = "golden_reference"


def get_db_connection(db_path: Path, password: str = None):
    """Get database connection with optional encryption."""
    try:
        import sqlcipher3
        conn = sqlcipher3.connect(str(db_path))
        if password:
            conn.execute(f"PRAGMA key='{password}'")
            conn.execute("PRAGMA cipher_compatibility=4")
        conn.execute("SELECT 1").fetchone()
        return conn
    except ImportError:
        return sqlite3.connect(str(db_path))


def check_migration_status(conn) -> bool:
    """Check if migration has already been applied."""
    try:
        cursor = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE version = ?",
            (MIGRATION_VERSION,)
        )
        return cursor.fetchone() is not None
    except Exception:
        # Table doesn't exist yet - sqlcipher3 or sqlite3 OperationalError
        return False


def record_migration(conn) -> None:
    """Record that migration was applied."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(
        "INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
        (MIGRATION_VERSION, MIGRATION_NAME)
    )
    conn.commit()


def get_tables(conn: sqlite3.Connection) -> list:
    """Get list of existing tables."""
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )
    return [row[0] for row in cursor.fetchall()]


def run_migration(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """
    Run the golden reference migration.

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
        "indexes_created": [],
        "data_inserted": [],
        "errors": []
    }

    if check_migration_status(conn):
        logger.info(f"Migration {MIGRATION_VERSION} already applied, skipping")
        results["skipped"] = True
        return results

    cursor = conn.cursor()
    existing_tables = get_tables(conn)

    try:
        # =====================================================
        # 1. TRUTH SOURCES - Defines SoT per metric type
        # =====================================================
        if "truth_sources" not in existing_tables:
            cursor.execute("""
                CREATE TABLE truth_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_type TEXT NOT NULL,
                    asset_class TEXT NOT NULL,
                    source_priority JSON NOT NULL,
                    description TEXT,
                    user_id INTEGER,
                    is_default INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(metric_type, asset_class, user_id)
                )
            """)
            results["tables_created"].append("truth_sources")
            logger.info("Created truth_sources table")

            # Insert default truth source configurations
            default_sources = [
                # Net Worth / Holdings - CAS is authoritative
                ('NET_WORTH', 'MUTUAL_FUND', '["NSDL_CAS", "CDSL_CAS", "RTA_CAS", "SYSTEM"]',
                 'NSDL/CDSL CAS is authoritative for MF net worth'),
                ('NET_WORTH', 'STOCKS', '["NSDL_CAS", "CDSL_CAS", "BROKER", "SYSTEM"]',
                 'NSDL/CDSL CAS is authoritative for stock holdings'),
                ('NET_WORTH', 'NPS', '["NSDL_CAS", "NPS_STATEMENT", "SYSTEM"]',
                 'NSDL CAS or NPS statement for NPS holdings'),
                ('NET_WORTH', 'EPF', '["EPFO_PASSBOOK", "SYSTEM"]',
                 'EPFO passbook is authoritative for EPF'),
                ('NET_WORTH', 'PPF', '["BANK_STATEMENT", "SYSTEM"]',
                 'Bank statement is authoritative for PPF'),
                ('NET_WORTH', 'US_STOCKS', '["BROKER_STATEMENT", "SYSTEM"]',
                 'Broker statement for US stocks'),

                # Capital Gains - Broker/RTA is authoritative
                ('CAPITAL_GAINS', 'MUTUAL_FUND', '["RTA_CAS", "NSDL_CAS", "SYSTEM"]',
                 'RTA CAS for MF capital gains'),
                ('CAPITAL_GAINS', 'STOCKS', '["BROKER", "NSDL_CAS", "SYSTEM"]',
                 'Broker statement for stock capital gains'),
                ('CAPITAL_GAINS', 'US_STOCKS', '["BROKER_STATEMENT", "SYSTEM"]',
                 'Broker statement for US stock capital gains'),

                # Units - RTA is authoritative for MF
                ('UNITS', 'MUTUAL_FUND', '["RTA_CAS", "NSDL_CAS", "SYSTEM"]',
                 'RTA is authoritative for MF units'),
                ('UNITS', 'STOCKS', '["DEPOSITORY", "BROKER", "SYSTEM"]',
                 'Depository records for stock quantity'),

                # Cost Basis - System is authoritative (we track purchases)
                ('COST_BASIS', 'MUTUAL_FUND', '["SYSTEM", "RTA_CAS"]',
                 'System tracks purchase cost; RTA for validation'),
                ('COST_BASIS', 'STOCKS', '["SYSTEM", "BROKER"]',
                 'System tracks purchase cost; broker for validation'),
            ]

            for metric, asset, sources, desc in default_sources:
                cursor.execute("""
                    INSERT INTO truth_sources
                    (metric_type, asset_class, source_priority, description, user_id)
                    VALUES (?, ?, ?, ?, NULL)
                """, (metric, asset, sources, desc))

            results["data_inserted"].append(f"truth_sources: {len(default_sources)} default configs")

        # =====================================================
        # 2. GOLDEN REFERENCE - External statements
        # =====================================================
        if "golden_reference" not in existing_tables:
            cursor.execute("""
                CREATE TABLE golden_reference (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    statement_date DATE NOT NULL,
                    period_start DATE,
                    period_end DATE,
                    file_path TEXT,
                    file_hash TEXT,
                    raw_data JSON,
                    investor_name TEXT,
                    investor_pan TEXT,
                    status TEXT DEFAULT 'ACTIVE',
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    validated_at TIMESTAMP,
                    notes TEXT,
                    UNIQUE(user_id, source_type, file_hash)
                )
            """)
            results["tables_created"].append("golden_reference")

            cursor.execute("""
                CREATE INDEX idx_golden_ref_user_source
                ON golden_reference(user_id, source_type, statement_date)
            """)
            results["indexes_created"].append("idx_golden_ref_user_source")

        # =====================================================
        # 3. GOLDEN HOLDINGS - Parsed holdings from golden sources
        # =====================================================
        if "golden_holdings" not in existing_tables:
            cursor.execute("""
                CREATE TABLE golden_holdings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    golden_ref_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    asset_type TEXT NOT NULL,
                    isin TEXT,
                    symbol TEXT,
                    name TEXT NOT NULL,
                    folio_number TEXT,
                    account_number TEXT,
                    units DECIMAL(18, 6),
                    nav DECIMAL(12, 4),
                    market_value DECIMAL(18, 2),
                    cost_basis DECIMAL(18, 2),
                    unrealized_gain DECIMAL(18, 2),
                    currency TEXT DEFAULT 'INR',
                    exchange_rate DECIMAL(10, 6) DEFAULT 1.0,
                    value_inr DECIMAL(18, 2),
                    as_of_date DATE NOT NULL,
                    financial_year TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (golden_ref_id) REFERENCES golden_reference(id)
                )
            """)
            results["tables_created"].append("golden_holdings")

            cursor.execute("""
                CREATE INDEX idx_golden_holdings_isin
                ON golden_holdings(user_id, isin, as_of_date)
            """)
            cursor.execute("""
                CREATE INDEX idx_golden_holdings_folio
                ON golden_holdings(user_id, folio_number, as_of_date)
            """)
            results["indexes_created"].extend([
                "idx_golden_holdings_isin",
                "idx_golden_holdings_folio"
            ])

        # =====================================================
        # 4. RECONCILIATION EVENTS - Cross-correlation results
        # =====================================================
        if "reconciliation_events" not in existing_tables:
            cursor.execute("""
                CREATE TABLE reconciliation_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    reconciliation_date DATE NOT NULL,
                    metric_type TEXT NOT NULL,
                    asset_class TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    golden_ref_id INTEGER,

                    -- Comparison keys
                    isin TEXT,
                    folio_number TEXT,
                    symbol TEXT,

                    -- Values
                    system_value DECIMAL(18, 6),
                    golden_value DECIMAL(18, 6),
                    difference DECIMAL(18, 6),
                    difference_pct DECIMAL(10, 4),
                    tolerance_used DECIMAL(10, 6),

                    -- Status
                    status TEXT DEFAULT 'PENDING',
                    match_result TEXT,
                    severity TEXT DEFAULT 'INFO',

                    -- Resolution
                    resolved_at TIMESTAMP,
                    resolved_by TEXT,
                    resolution_action TEXT,
                    resolution_notes TEXT,

                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (golden_ref_id) REFERENCES golden_reference(id)
                )
            """)
            results["tables_created"].append("reconciliation_events")

            cursor.execute("""
                CREATE INDEX idx_recon_events_user_date
                ON reconciliation_events(user_id, reconciliation_date, status)
            """)
            cursor.execute("""
                CREATE INDEX idx_recon_events_status
                ON reconciliation_events(status, severity)
            """)
            results["indexes_created"].extend([
                "idx_recon_events_user_date",
                "idx_recon_events_status"
            ])

        # =====================================================
        # 5. RECONCILIATION SUSPENSE - Unresolved items
        # =====================================================
        if "reconciliation_suspense" not in existing_tables:
            cursor.execute("""
                CREATE TABLE reconciliation_suspense (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    event_id INTEGER NOT NULL,

                    -- Asset identification
                    asset_type TEXT NOT NULL,
                    isin TEXT,
                    symbol TEXT,
                    name TEXT,
                    folio_number TEXT,

                    -- Suspense amounts
                    suspense_units DECIMAL(18, 6),
                    suspense_value DECIMAL(18, 2),
                    suspense_currency TEXT DEFAULT 'INR',

                    -- Tracking
                    suspense_reason TEXT,
                    opened_date DATE NOT NULL,
                    target_resolution_date DATE,
                    actual_resolution_date DATE,

                    -- Status
                    status TEXT DEFAULT 'OPEN',
                    priority TEXT DEFAULT 'NORMAL',
                    assigned_to TEXT,

                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (event_id) REFERENCES reconciliation_events(id)
                )
            """)
            results["tables_created"].append("reconciliation_suspense")

            cursor.execute("""
                CREATE INDEX idx_suspense_open
                ON reconciliation_suspense(user_id, status, priority)
            """)
            results["indexes_created"].append("idx_suspense_open")

        # =====================================================
        # 6. GOLDEN CAPITAL GAINS - CG from golden sources
        # =====================================================
        if "golden_capital_gains" not in existing_tables:
            cursor.execute("""
                CREATE TABLE golden_capital_gains (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    golden_ref_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    financial_year TEXT NOT NULL,
                    asset_type TEXT NOT NULL,

                    -- Identification
                    isin TEXT,
                    symbol TEXT,
                    name TEXT,
                    folio_number TEXT,

                    -- Capital gains breakdown
                    stcg_equity DECIMAL(18, 2) DEFAULT 0,
                    stcg_other DECIMAL(18, 2) DEFAULT 0,
                    ltcg_equity DECIMAL(18, 2) DEFAULT 0,
                    ltcg_other DECIMAL(18, 2) DEFAULT 0,
                    total_gain DECIMAL(18, 2),

                    -- Currency for foreign assets
                    currency TEXT DEFAULT 'INR',
                    exchange_rate DECIMAL(10, 6) DEFAULT 1.0,
                    gain_inr DECIMAL(18, 2),

                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (golden_ref_id) REFERENCES golden_reference(id)
                )
            """)
            results["tables_created"].append("golden_capital_gains")

            cursor.execute("""
                CREATE INDEX idx_golden_cg_user_fy
                ON golden_capital_gains(user_id, financial_year, asset_type)
            """)
            results["indexes_created"].append("idx_golden_cg_user_fy")

        # Record migration
        if not dry_run:
            record_migration(conn)
            conn.commit()
            logger.info(f"Migration {MIGRATION_VERSION} applied successfully")
        else:
            conn.rollback()
            logger.info(f"Migration {MIGRATION_VERSION} dry run completed")

        results["success"] = True

    except Exception as e:
        conn.rollback()
        results["success"] = False
        results["errors"].append(str(e))
        logger.exception(f"Migration failed: {e}")

    return results


# SQL Schema as standalone string for documentation
GOLDEN_REFERENCE_SCHEMA = """
-- =====================================================
-- GOLDEN REFERENCE RECONCILIATION ENGINE SCHEMA
-- SQLite with user_id context
-- =====================================================

-- 1. Truth Sources Configuration
CREATE TABLE truth_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_type TEXT NOT NULL,        -- NET_WORTH, CAPITAL_GAINS, UNITS, COST_BASIS
    asset_class TEXT NOT NULL,        -- MUTUAL_FUND, STOCKS, NPS, EPF, PPF, US_STOCKS
    source_priority JSON NOT NULL,    -- ["NSDL_CAS", "CDSL_CAS", "RTA_CAS", "SYSTEM"]
    description TEXT,
    user_id INTEGER,                  -- NULL for defaults, user_id for overrides
    is_default INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(metric_type, asset_class, user_id)
);

-- 2. Golden Reference (External Statements)
CREATE TABLE golden_reference (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    source_type TEXT NOT NULL,        -- NSDL_CAS, CDSL_CAS, RTA_CAS, BROKER, etc.
    statement_date DATE NOT NULL,
    period_start DATE,
    period_end DATE,
    file_path TEXT,
    file_hash TEXT,
    raw_data JSON,                    -- Full parsed data for audit
    investor_name TEXT,
    investor_pan TEXT,
    status TEXT DEFAULT 'ACTIVE',     -- ACTIVE, SUPERSEDED, INVALID
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    validated_at TIMESTAMP,
    notes TEXT,
    UNIQUE(user_id, source_type, file_hash)
);

-- 3. Golden Holdings (Parsed from Golden Sources)
CREATE TABLE golden_holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    golden_ref_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    asset_type TEXT NOT NULL,         -- MUTUAL_FUND, STOCKS, NPS, etc.
    isin TEXT,                        -- Primary matching key
    symbol TEXT,                      -- For stocks
    name TEXT NOT NULL,
    folio_number TEXT,                -- Secondary key for MF
    account_number TEXT,              -- For demat accounts
    units DECIMAL(18, 6),
    nav DECIMAL(12, 4),
    market_value DECIMAL(18, 2),
    cost_basis DECIMAL(18, 2),
    unrealized_gain DECIMAL(18, 2),
    currency TEXT DEFAULT 'INR',
    exchange_rate DECIMAL(10, 6) DEFAULT 1.0,
    value_inr DECIMAL(18, 2),         -- Converted to INR
    as_of_date DATE NOT NULL,
    financial_year TEXT,              -- FY format: "2024-25"
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (golden_ref_id) REFERENCES golden_reference(id)
);

-- 4. Reconciliation Events
CREATE TABLE reconciliation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    reconciliation_date DATE NOT NULL,
    metric_type TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    source_type TEXT NOT NULL,
    golden_ref_id INTEGER,

    -- Matching keys
    isin TEXT,
    folio_number TEXT,
    symbol TEXT,

    -- Comparison values
    system_value DECIMAL(18, 6),
    golden_value DECIMAL(18, 6),
    difference DECIMAL(18, 6),
    difference_pct DECIMAL(10, 4),
    tolerance_used DECIMAL(10, 6),

    -- Result
    status TEXT DEFAULT 'PENDING',    -- PENDING, MATCHED, MISMATCH, RESOLVED
    match_result TEXT,                -- EXACT, WITHIN_TOLERANCE, MISMATCH, MISSING_SYSTEM, MISSING_GOLDEN
    severity TEXT DEFAULT 'INFO',     -- INFO, WARNING, ERROR, CRITICAL

    -- Resolution tracking
    resolved_at TIMESTAMP,
    resolved_by TEXT,
    resolution_action TEXT,           -- ADJUSTED, ACCEPTED, INVESTIGATED, SUSPENSE
    resolution_notes TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (golden_ref_id) REFERENCES golden_reference(id)
);

-- 5. Reconciliation Suspense (Parking account for unresolved)
CREATE TABLE reconciliation_suspense (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    asset_type TEXT NOT NULL,
    isin TEXT,
    symbol TEXT,
    name TEXT,
    folio_number TEXT,
    suspense_units DECIMAL(18, 6),
    suspense_value DECIMAL(18, 2),
    suspense_currency TEXT DEFAULT 'INR',
    suspense_reason TEXT,
    opened_date DATE NOT NULL,
    target_resolution_date DATE,
    actual_resolution_date DATE,
    status TEXT DEFAULT 'OPEN',       -- OPEN, IN_PROGRESS, RESOLVED, WRITTEN_OFF
    priority TEXT DEFAULT 'NORMAL',   -- LOW, NORMAL, HIGH, CRITICAL
    assigned_to TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (event_id) REFERENCES reconciliation_events(id)
);

-- 6. Golden Capital Gains
CREATE TABLE golden_capital_gains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    golden_ref_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    financial_year TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    isin TEXT,
    symbol TEXT,
    name TEXT,
    folio_number TEXT,
    stcg_equity DECIMAL(18, 2) DEFAULT 0,
    stcg_other DECIMAL(18, 2) DEFAULT 0,
    ltcg_equity DECIMAL(18, 2) DEFAULT 0,
    ltcg_other DECIMAL(18, 2) DEFAULT 0,
    total_gain DECIMAL(18, 2),
    currency TEXT DEFAULT 'INR',
    exchange_rate DECIMAL(10, 6) DEFAULT 1.0,
    gain_inr DECIMAL(18, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (golden_ref_id) REFERENCES golden_reference(id)
);

-- Indexes for performance
CREATE INDEX idx_golden_ref_user_source ON golden_reference(user_id, source_type, statement_date);
CREATE INDEX idx_golden_holdings_isin ON golden_holdings(user_id, isin, as_of_date);
CREATE INDEX idx_golden_holdings_folio ON golden_holdings(user_id, folio_number, as_of_date);
CREATE INDEX idx_recon_events_user_date ON reconciliation_events(user_id, reconciliation_date, status);
CREATE INDEX idx_recon_events_status ON reconciliation_events(status, severity);
CREATE INDEX idx_suspense_open ON reconciliation_suspense(user_id, status, priority);
CREATE INDEX idx_golden_cg_user_fy ON golden_capital_gains(user_id, financial_year, asset_type);
"""


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Run PFAS Golden Reference Migration"
    )
    parser.add_argument(
        "--db-path", type=Path, required=True,
        help="Path to SQLite/SQLCipher database"
    )
    parser.add_argument("--password", help="Database encryption password")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

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
        if results["indexes_created"]:
            print(f"Indexes created: {', '.join(results['indexes_created'])}")
        if results["data_inserted"]:
            print(f"Data inserted: {', '.join(results['data_inserted'])}")

    if results.get("errors"):
        print(f"Errors: {results['errors']}")
        sys.exit(1)

    conn.close()


if __name__ == "__main__":
    main()
