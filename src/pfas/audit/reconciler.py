"""
Reconciliation Engine - Compare DB data with statement file data.

Identifies mismatches between database records and source statements,
logs discrepancies to reconciliation_audit table.
"""

import logging
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Dict, Any

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

from .mf_audit_parser import MFAuditParser, AuditData, HoldingRecord

logger = logging.getLogger(__name__)


@dataclass
class Mismatch:
    """Represents a data mismatch between DB and file."""
    scheme_name: str
    folio_number: str
    field_name: str
    db_value: str
    file_value: str
    difference: Optional[float] = None
    severity: str = "WARNING"  # INFO, WARNING, ERROR

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'scheme_name': self.scheme_name,
            'folio_number': self.folio_number,
            'field_name': self.field_name,
            'db_value': self.db_value,
            'file_value': self.file_value,
            'difference': self.difference,
            'severity': self.severity,
        }


@dataclass
class ReconciliationResult:
    """Result of reconciliation process."""
    success: bool = True
    records_compared: int = 0
    records_matched: int = 0
    mismatches: List[Mismatch] = field(default_factory=list)
    missing_in_db: List[str] = field(default_factory=list)
    missing_in_file: List[str] = field(default_factory=list)
    source_file: str = ""
    errors: List[str] = field(default_factory=list)

    @property
    def mismatch_count(self) -> int:
        """Count of mismatches."""
        return len(self.mismatches)

    @property
    def match_rate(self) -> float:
        """Percentage of records that matched."""
        if self.records_compared == 0:
            return 0.0
        return (self.records_matched / self.records_compared) * 100


class Reconciler:
    """
    Compares database records with statement file data.

    Features:
    - Configurable mismatch threshold (default ₹100)
    - Logs mismatches to reconciliation_audit table
    - Supports holdings and capital gains reconciliation

    Usage:
        reconciler = Reconciler(conn, user_id)
        result = reconciler.reconcile_holdings(audit_data)

        for mismatch in result.mismatches:
            print(f"{mismatch.field_name}: DB={mismatch.db_value}, File={mismatch.file_value}")
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        user_id: int,
        mismatch_threshold: Decimal = Decimal("100")
    ):
        """
        Initialize reconciler.

        Args:
            conn: Database connection
            user_id: User ID
            mismatch_threshold: Minimum difference to flag as mismatch (default ₹100)
        """
        self.conn = conn
        self.user_id = user_id
        self.mismatch_threshold = mismatch_threshold

    def reconcile_holdings(self, audit_data: AuditData) -> ReconciliationResult:
        """
        Reconcile holdings from audit file against database.

        Args:
            audit_data: Parsed audit data from MFAuditParser

        Returns:
            ReconciliationResult
        """
        result = ReconciliationResult(source_file=audit_data.source_file)

        if not audit_data.success:
            result.errors.extend(audit_data.errors)
            result.success = False
            return result

        if not audit_data.holdings:
            result.errors.append("No holdings data to reconcile")
            return result

        # Get DB holdings for comparison
        db_holdings = self._get_db_holdings(audit_data.statement_date)

        # Create lookup key: folio_number|scheme_name (normalized)
        db_lookup = {
            self._normalize_key(h['folio_number'], h['scheme_name']): h
            for h in db_holdings
        }

        file_keys = set()

        for file_holding in audit_data.holdings:
            result.records_compared += 1
            key = self._normalize_key(file_holding.folio_number, file_holding.scheme_name)
            file_keys.add(key)

            if key not in db_lookup:
                result.missing_in_db.append(
                    f"{file_holding.folio_number}: {file_holding.scheme_name}"
                )
                continue

            db_holding = db_lookup[key]

            # Compare fields
            mismatches = self._compare_holding(
                db_holding, file_holding,
                audit_data.source_file
            )

            if mismatches:
                result.mismatches.extend(mismatches)
            else:
                result.records_matched += 1

        # Find records in DB but not in file
        for key, db_holding in db_lookup.items():
            if key not in file_keys:
                result.missing_in_file.append(
                    f"{db_holding['folio_number']}: {db_holding['scheme_name']}"
                )

        # Log mismatches to database
        if result.mismatches:
            self._log_mismatches(result.mismatches, audit_data.source_file)

        logger.info(
            f"Reconciliation complete: {result.records_compared} compared, "
            f"{result.records_matched} matched, {result.mismatch_count} mismatches"
        )

        return result

    def _get_db_holdings(self, as_of_date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get holdings from database.

        Args:
            as_of_date: Optional date filter (for point-in-time comparison)

        Returns:
            List of holdings as dicts
        """
        # First try mf_holdings table (snapshot data)
        query = """
            SELECT
                folio_number,
                scheme_name,
                units,
                nav,
                current_value,
                cost_value
            FROM mf_holdings
            WHERE user_id = ?
        """
        params = [self.user_id]

        if as_of_date:
            query += " AND nav_date = ?"
            params.append(as_of_date.isoformat())

        cursor = self.conn.execute(query, params)
        rows = cursor.fetchall()

        if rows:
            return [dict(row) for row in rows]

        # Fallback: Calculate from transactions if no holdings snapshot
        return self._calculate_holdings_from_transactions()

    def _calculate_holdings_from_transactions(self) -> List[Dict[str, Any]]:
        """
        Calculate current holdings from transaction history.

        Returns:
            List of calculated holdings
        """
        query = """
            SELECT
                f.folio_number,
                s.name as scheme_name,
                SUM(CASE
                    WHEN t.transaction_type IN ('PURCHASE', 'SWITCH_IN', 'DIVIDEND_REINVEST')
                    THEN t.units
                    ELSE -t.units
                END) as units,
                0 as nav,
                0 as current_value,
                SUM(CASE
                    WHEN t.transaction_type IN ('PURCHASE', 'SWITCH_IN', 'DIVIDEND_REINVEST')
                    THEN t.amount
                    ELSE 0
                END) as cost_value
            FROM mf_transactions t
            JOIN mf_folios f ON t.folio_id = f.id
            JOIN mf_schemes s ON f.scheme_id = s.id
            WHERE t.user_id = ?
            GROUP BY f.folio_number, s.name
            HAVING units > 0.001
        """

        cursor = self.conn.execute(query, (self.user_id,))
        return [dict(row) for row in cursor.fetchall()]

    def _normalize_key(self, folio: str, scheme: str) -> str:
        """Create normalized lookup key."""
        # Normalize: lowercase, remove spaces, remove common suffixes
        folio_norm = str(folio).lower().strip().replace(' ', '')
        scheme_norm = str(scheme).lower().strip()

        # Remove common variations
        for suffix in [' - direct', ' - regular', ' (d)', ' (g)', ' growth', ' direct']:
            scheme_norm = scheme_norm.replace(suffix, '')

        return f"{folio_norm}|{scheme_norm}"

    def _compare_holding(
        self,
        db_holding: Dict[str, Any],
        file_holding: HoldingRecord,
        source_file: str
    ) -> List[Mismatch]:
        """
        Compare DB holding with file holding.

        Args:
            db_holding: Database record
            file_holding: File record
            source_file: Source file path

        Returns:
            List of mismatches found
        """
        mismatches = []

        # Compare units
        db_units = Decimal(str(db_holding.get('units', 0)))
        file_units = file_holding.units

        if abs(db_units - file_units) > Decimal("0.001"):
            diff = float(file_units - db_units)
            severity = "ERROR" if abs(diff) > 1 else "WARNING"

            mismatches.append(Mismatch(
                scheme_name=file_holding.scheme_name,
                folio_number=file_holding.folio_number,
                field_name='units',
                db_value=str(db_units),
                file_value=str(file_units),
                difference=diff,
                severity=severity
            ))

        # Compare current value
        db_value = Decimal(str(db_holding.get('current_value', 0)))
        file_value = file_holding.current_value

        value_diff = abs(db_value - file_value)
        if value_diff > self.mismatch_threshold:
            severity = "ERROR" if value_diff > Decimal("1000") else "WARNING"

            mismatches.append(Mismatch(
                scheme_name=file_holding.scheme_name,
                folio_number=file_holding.folio_number,
                field_name='current_value',
                db_value=str(db_value),
                file_value=str(file_value),
                difference=float(file_value - db_value),
                severity=severity
            ))

        # Compare cost value (if both available)
        db_cost = Decimal(str(db_holding.get('cost_value') or 0))
        file_cost = file_holding.cost_value or Decimal("0")

        if db_cost > 0 and file_cost > 0:
            cost_diff = abs(db_cost - file_cost)
            if cost_diff > self.mismatch_threshold:
                mismatches.append(Mismatch(
                    scheme_name=file_holding.scheme_name,
                    folio_number=file_holding.folio_number,
                    field_name='cost_value',
                    db_value=str(db_cost),
                    file_value=str(file_cost),
                    difference=float(file_cost - db_cost),
                    severity="WARNING"
                ))

        return mismatches

    def _log_mismatches(self, mismatches: List[Mismatch], source_file: str):
        """
        Log mismatches to reconciliation_audit table.

        Args:
            mismatches: List of mismatches
            source_file: Source file path
        """
        today = date.today().isoformat()

        for m in mismatches:
            try:
                self.conn.execute(
                    """
                    INSERT INTO reconciliation_audit
                    (user_id, asset_type, audit_date, source_file, scheme_name,
                     folio_number, field_name, db_value, file_value, difference, severity)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.user_id,
                        'Mutual-Fund',
                        today,
                        source_file,
                        m.scheme_name,
                        m.folio_number,
                        m.field_name,
                        m.db_value,
                        m.file_value,
                        m.difference,
                        m.severity
                    )
                )
            except Exception as e:
                logger.warning(f"Failed to log mismatch: {e}")

        self.conn.commit()

    def get_unresolved_mismatches(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get unresolved mismatches from database.

        Args:
            limit: Maximum number to return

        Returns:
            List of unresolved mismatches
        """
        cursor = self.conn.execute(
            """
            SELECT
                id, asset_type, audit_date, source_file, scheme_name,
                folio_number, field_name, db_value, file_value,
                difference, severity, created_at
            FROM reconciliation_audit
            WHERE user_id = ? AND resolved = 0
            ORDER BY
                CASE severity WHEN 'ERROR' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
                created_at DESC
            LIMIT ?
            """,
            (self.user_id, limit)
        )

        return [dict(row) for row in cursor.fetchall()]

    def resolve_mismatch(
        self,
        mismatch_id: int,
        resolution_notes: str
    ):
        """
        Mark a mismatch as resolved.

        Args:
            mismatch_id: Mismatch record ID
            resolution_notes: Notes explaining resolution
        """
        self.conn.execute(
            """
            UPDATE reconciliation_audit
            SET resolved = 1, resolution_notes = ?, resolved_at = CURRENT_TIMESTAMP
            WHERE id = ? AND user_id = ?
            """,
            (resolution_notes, mismatch_id, self.user_id)
        )
        self.conn.commit()


def reconcile_mf_holdings(
    conn: sqlite3.Connection,
    user_id: int,
    audit_file: Path,
    mismatch_threshold: Decimal = Decimal("100")
) -> ReconciliationResult:
    """
    Convenience function to reconcile MF holdings.

    Args:
        conn: Database connection
        user_id: User ID
        audit_file: Path to audit/holdings Excel
        mismatch_threshold: Minimum difference to flag

    Returns:
        ReconciliationResult

    Example:
        result = reconcile_mf_holdings(conn, 1, Path("holdings.xlsx"))
        print(f"Match rate: {result.match_rate:.1f}%")
    """
    parser = MFAuditParser()
    audit_data = parser.parse(audit_file)

    reconciler = Reconciler(conn, user_id, mismatch_threshold)
    return reconciler.reconcile_holdings(audit_data)
