"""
NAV History Service for PFAS.

Manages historical NAV data for mutual funds with:
- Backfill from existing transactions
- Point-in-time NAV lookup with interpolation
- Daily update stub for AMFI feed integration
"""

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Tuple
import sqlite3

logger = logging.getLogger(__name__)


@dataclass
class NAVRecord:
    """Represents a NAV record for a scheme on a specific date."""

    scheme_id: int
    nav_date: date
    nav: Decimal
    source: str  # 'transaction', 'amfi', 'manual', 'interpolated'

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "NAVRecord":
        """Create NAVRecord from database row."""
        return cls(
            scheme_id=row["scheme_id"],
            nav_date=date.fromisoformat(row["nav_date"]) if isinstance(row["nav_date"], str) else row["nav_date"],
            nav=Decimal(str(row["nav"])),
            source=row["source"] if "source" in row.keys() else "unknown",
        )


class NAVService:
    """
    Service for managing mutual fund NAV history.

    Provides:
    - Historical NAV storage and retrieval
    - Point-in-time NAV lookup with interpolation fallback
    - Backfill from existing transaction data
    - Stub for daily AMFI feed integration

    Usage:
        service = NAVService(conn)

        # Get NAV at specific date
        nav = service.get_nav_at(scheme_id=123, as_of=date(2024, 3, 15))

        # Backfill from transactions
        count = service.backfill_from_transactions()
    """

    # AMFI NAV URL (for future integration)
    AMFI_NAV_URL = "https://www.amfiindia.com/spages/NAVAll.txt"

    def __init__(self, db_connection: sqlite3.Connection):
        """
        Initialize NAV service.

        Args:
            db_connection: SQLite database connection
        """
        self.conn = db_connection
        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        """Ensure mf_nav_history table exists with proper schema."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS mf_nav_history (
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

        # Create index for faster lookups
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_nav_history_scheme_date
            ON mf_nav_history(scheme_id, nav_date)
        """)
        self.conn.commit()

    def get_nav_at(
        self,
        scheme_id: int,
        as_of: date,
        allow_interpolation: bool = True
    ) -> Optional[Decimal]:
        """
        Get NAV for a scheme at a specific date.

        Lookup order:
        1. Exact match in mf_nav_history
        2. If allow_interpolation, interpolate from nearest dates
        3. Fallback to nearest available NAV

        Args:
            scheme_id: Scheme ID
            as_of: Date to get NAV for
            allow_interpolation: Allow linear interpolation between dates

        Returns:
            NAV value or None if not available
        """
        # 1. Try exact match
        cursor = self.conn.execute(
            "SELECT nav FROM mf_nav_history WHERE scheme_id = ? AND nav_date = ?",
            (scheme_id, as_of.isoformat())
        )
        row = cursor.fetchone()
        if row:
            return Decimal(str(row[0]))

        # 2. Try interpolation
        if allow_interpolation:
            interpolated = self._interpolate_nav(scheme_id, as_of)
            if interpolated is not None:
                return interpolated

        # 3. Fallback to nearest NAV (prefer earlier date)
        cursor = self.conn.execute("""
            SELECT nav, nav_date FROM mf_nav_history
            WHERE scheme_id = ? AND nav_date <= ?
            ORDER BY nav_date DESC
            LIMIT 1
        """, (scheme_id, as_of.isoformat()))
        row = cursor.fetchone()
        if row:
            logger.debug(f"Using NAV from {row[1]} for scheme {scheme_id} on {as_of}")
            return Decimal(str(row[0]))

        return None

    def _interpolate_nav(self, scheme_id: int, as_of: date) -> Optional[Decimal]:
        """
        Linearly interpolate NAV between two known dates.

        Args:
            scheme_id: Scheme ID
            as_of: Target date

        Returns:
            Interpolated NAV or None if interpolation not possible
        """
        # Get closest NAV before
        cursor = self.conn.execute("""
            SELECT nav, nav_date FROM mf_nav_history
            WHERE scheme_id = ? AND nav_date < ?
            ORDER BY nav_date DESC
            LIMIT 1
        """, (scheme_id, as_of.isoformat()))
        before = cursor.fetchone()

        # Get closest NAV after
        cursor = self.conn.execute("""
            SELECT nav, nav_date FROM mf_nav_history
            WHERE scheme_id = ? AND nav_date > ?
            ORDER BY nav_date ASC
            LIMIT 1
        """, (scheme_id, as_of.isoformat()))
        after = cursor.fetchone()

        if not before or not after:
            return None

        nav_before = Decimal(str(before[0]))
        date_before = date.fromisoformat(before[1]) if isinstance(before[1], str) else before[1]
        nav_after = Decimal(str(after[0]))
        date_after = date.fromisoformat(after[1]) if isinstance(after[1], str) else after[1]

        # Only interpolate if dates are within 30 days
        if (date_after - date_before).days > 30:
            logger.debug(f"Gap too large for interpolation: {date_before} to {date_after}")
            return None

        # Linear interpolation
        total_days = (date_after - date_before).days
        days_from_start = (as_of - date_before).days
        ratio = Decimal(days_from_start) / Decimal(total_days)

        interpolated = nav_before + (nav_after - nav_before) * ratio
        logger.debug(
            f"Interpolated NAV for scheme {scheme_id} on {as_of}: "
            f"{nav_before} -> {interpolated} -> {nav_after}"
        )

        return interpolated.quantize(Decimal("0.0001"))

    def get_nav_history(
        self,
        scheme_id: int,
        from_date: date = None,
        to_date: date = None
    ) -> List[NAVRecord]:
        """
        Get NAV history for a scheme within date range.

        Args:
            scheme_id: Scheme ID
            from_date: Start date (optional)
            to_date: End date (optional)

        Returns:
            List of NAVRecord objects in chronological order
        """
        query = "SELECT * FROM mf_nav_history WHERE scheme_id = ?"
        params: list = [scheme_id]

        if from_date:
            query += " AND nav_date >= ?"
            params.append(from_date.isoformat())

        if to_date:
            query += " AND nav_date <= ?"
            params.append(to_date.isoformat())

        query += " ORDER BY nav_date ASC"

        cursor = self.conn.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            # Create a simple object that supports attribute access
            class RowProxy:
                def __init__(self, d):
                    self._dict = d
                def __getitem__(self, key):
                    return self._dict[key]
                def keys(self):
                    return self._dict.keys()
            results.append(NAVRecord.from_row(RowProxy(row_dict)))
        return results

    def store_nav(
        self,
        scheme_id: int,
        nav_date: date,
        nav: Decimal,
        source: str = "manual"
    ) -> bool:
        """
        Store a NAV record.

        Args:
            scheme_id: Scheme ID
            nav_date: NAV date
            nav: NAV value
            source: Source of NAV data

        Returns:
            True if inserted, False if updated existing
        """
        cursor = self.conn.execute("""
            INSERT INTO mf_nav_history (scheme_id, nav_date, nav, source)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(scheme_id, nav_date) DO UPDATE SET
                nav = excluded.nav,
                source = excluded.source
        """, (scheme_id, nav_date.isoformat(), str(nav), source))

        self.conn.commit()
        return cursor.rowcount > 0

    def backfill_from_transactions(self, scheme_id: int = None) -> int:
        """
        Backfill NAV history from existing mf_transactions.

        Extracts unique (scheme_id, date, nav) combinations from transactions
        and populates mf_nav_history table.

        Args:
            scheme_id: Optional specific scheme to backfill (default: all schemes)

        Returns:
            Number of NAV records inserted
        """
        query = """
            INSERT OR IGNORE INTO mf_nav_history (scheme_id, nav_date, nav, source)
            SELECT DISTINCT
                mf.scheme_id,
                mt.date,
                mt.nav,
                'transaction'
            FROM mf_transactions mt
            JOIN mf_folios mf ON mt.folio_id = mf.id
            WHERE mt.nav IS NOT NULL AND mt.nav > 0
        """
        params = []

        if scheme_id:
            query = query.replace(
                "WHERE mt.nav IS NOT NULL",
                "WHERE mf.scheme_id = ? AND mt.nav IS NOT NULL"
            )
            params.append(scheme_id)

        cursor = self.conn.execute(query, params)
        self.conn.commit()

        count = cursor.rowcount
        logger.info(f"Backfilled {count} NAV records from transactions")
        return count

    def get_backfill_stats(self) -> Dict[str, int]:
        """
        Get statistics about NAV history coverage.

        Returns:
            Dictionary with stats: total_records, schemes_covered, date_range
        """
        stats = {}

        cursor = self.conn.execute("SELECT COUNT(*) FROM mf_nav_history")
        stats["total_records"] = cursor.fetchone()[0]

        cursor = self.conn.execute("SELECT COUNT(DISTINCT scheme_id) FROM mf_nav_history")
        stats["schemes_covered"] = cursor.fetchone()[0]

        cursor = self.conn.execute("""
            SELECT MIN(nav_date), MAX(nav_date) FROM mf_nav_history
        """)
        row = cursor.fetchone()
        stats["earliest_date"] = row[0]
        stats["latest_date"] = row[1]

        cursor = self.conn.execute("""
            SELECT COUNT(DISTINCT scheme_id) FROM mf_folios
        """)
        stats["total_schemes"] = cursor.fetchone()[0]

        return stats

    def fetch_amfi_nav(self, as_of: date = None) -> int:
        """
        Fetch NAV data from AMFI website.

        NOTE: This is a stub for future integration with AMFI NAV feed.

        Args:
            as_of: Date to fetch NAV for (default: today)

        Returns:
            Number of NAV records updated
        """
        # TODO: Implement AMFI NAV feed integration
        # 1. Fetch https://www.amfiindia.com/spages/NAVAll.txt
        # 2. Parse semicolon-separated format
        # 3. Match by scheme code or ISIN
        # 4. Store in mf_nav_history with source='amfi'

        logger.warning("AMFI NAV fetch not implemented yet")
        raise NotImplementedError(
            "AMFI NAV feed integration pending. "
            "Use backfill_from_transactions() for now."
        )

    def get_scheme_nav_coverage(self, scheme_id: int) -> Dict[str, any]:
        """
        Get NAV coverage statistics for a specific scheme.

        Args:
            scheme_id: Scheme ID

        Returns:
            Dictionary with coverage stats
        """
        cursor = self.conn.execute("""
            SELECT
                COUNT(*) as nav_count,
                MIN(nav_date) as first_nav,
                MAX(nav_date) as last_nav,
                MIN(nav) as min_nav,
                MAX(nav) as max_nav
            FROM mf_nav_history
            WHERE scheme_id = ?
        """, (scheme_id,))

        row = cursor.fetchone()
        return {
            "scheme_id": scheme_id,
            "nav_count": row[0],
            "first_nav_date": row[1],
            "last_nav_date": row[2],
            "min_nav": Decimal(str(row[3])) if row[3] else None,
            "max_nav": Decimal(str(row[4])) if row[4] else None,
        }


def log_change(
    conn: sqlite3.Connection,
    user_id: int,
    table: str,
    record_id: int,
    action: str,
    old: Dict = None,
    new: Dict = None,
    source: str = None
) -> int:
    """
    Convenience function to log an audit change.

    This is a standalone function for use in scripts and migrations.

    Args:
        conn: Database connection
        user_id: User ID
        table: Table name
        record_id: Record ID
        action: INSERT, UPDATE, or DELETE
        old: Old values (for UPDATE/DELETE)
        new: New values (for INSERT/UPDATE)
        source: Source of change

    Returns:
        Audit log entry ID
    """
    from pfas.core.audit import AuditLogger
    logger = AuditLogger(conn, user_id=user_id, source=source)
    return logger.log_change(
        table_name=table,
        record_id=record_id,
        action=action,
        old_values=old,
        new_values=new,
    )
