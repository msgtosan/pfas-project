"""
MF Analyzer Diagnostics and Audit Module.

Provides:
- Database integrity checks
- Data quality audits
- Duplicate detection
- Value reconciliation
- Verbose diagnostic reports
"""

import logging
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class AuditResult:
    """Result of an audit check."""
    check_name: str
    passed: bool
    message: str
    details: List[str] = field(default_factory=list)
    severity: str = "INFO"  # INFO, WARNING, ERROR


@dataclass
class DiagnosticReport:
    """Complete diagnostic report."""
    user_name: str
    report_date: date
    total_holdings: int = 0
    total_value: Decimal = Decimal("0")
    audits: List[AuditResult] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(a.passed for a in self.audits)

    @property
    def error_count(self) -> int:
        return sum(1 for a in self.audits if not a.passed and a.severity == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for a in self.audits if not a.passed and a.severity == "WARNING")


class MFDiagnostics:
    """
    Diagnostic and audit utilities for MF holdings.

    Usage:
        diagnostics = MFDiagnostics(conn)
        report = diagnostics.run_full_audit(user_id)
        diagnostics.print_report(report)
    """

    def __init__(self, conn):
        """Initialize with database connection."""
        self.conn = conn

    def run_full_audit(self, user_id: int, user_name: str = "") -> DiagnosticReport:
        """
        Run all audit checks and return a complete report.

        Args:
            user_id: User ID to audit
            user_name: User name for report

        Returns:
            DiagnosticReport with all audit results
        """
        report = DiagnosticReport(
            user_name=user_name,
            report_date=date.today()
        )

        # Get basic stats
        stats = self._get_holdings_stats(user_id)
        report.total_holdings = stats.get("total_count", 0)
        report.total_value = Decimal(str(stats.get("total_value", 0)))

        # Run all audits
        report.audits.extend([
            self.audit_duplicate_holdings(user_id),
            self.audit_zero_value_holdings(user_id),
            self.audit_negative_values(user_id),
            self.audit_missing_required_fields(user_id),
            self.audit_rta_consistency(user_id),
            self.audit_date_validity(user_id),
            self.audit_folio_consistency(user_id),
            self.audit_scheme_classification(user_id),
            self.audit_value_reasonableness(user_id),
        ])

        # Collect warnings and errors
        for audit in report.audits:
            if not audit.passed:
                if audit.severity == "ERROR":
                    report.errors.extend(audit.details)
                else:
                    report.warnings.extend(audit.details)

        return report

    def _get_holdings_stats(self, user_id: int) -> Dict[str, Any]:
        """Get basic holdings statistics."""
        cursor = self.conn.execute("""
            SELECT
                COUNT(*) as total_count,
                SUM(CAST(current_value AS REAL)) as total_value,
                SUM(CAST(cost_value AS REAL)) as total_cost,
                COUNT(DISTINCT folio_number) as unique_folios,
                COUNT(DISTINCT scheme_name) as unique_schemes
            FROM mf_holdings
            WHERE user_id = ?
        """, (user_id,))
        row = cursor.fetchone()
        if row:
            return {
                "total_count": row[0] or 0,
                "total_value": row[1] or 0,
                "total_cost": row[2] or 0,
                "unique_folios": row[3] or 0,
                "unique_schemes": row[4] or 0,
            }
        return {}

    def audit_duplicate_holdings(self, user_id: int) -> AuditResult:
        """Check for duplicate holdings (same scheme+folio+nav_date)."""
        cursor = self.conn.execute("""
            SELECT scheme_name, folio_number, nav_date, COUNT(*) as cnt
            FROM mf_holdings
            WHERE user_id = ?
            GROUP BY scheme_name, folio_number, nav_date
            HAVING COUNT(*) > 1
        """, (user_id,))

        duplicates = cursor.fetchall()
        if duplicates:
            details = [
                f"Duplicate: {row[0][:40]}... | Folio: {row[1]} | Date: {row[2]} | Count: {row[3]}"
                for row in duplicates
            ]
            return AuditResult(
                check_name="Duplicate Holdings Check",
                passed=False,
                message=f"Found {len(duplicates)} duplicate holding entries",
                details=details,
                severity="ERROR"
            )

        return AuditResult(
            check_name="Duplicate Holdings Check",
            passed=True,
            message="No duplicate holdings found"
        )

    def audit_zero_value_holdings(self, user_id: int) -> AuditResult:
        """Check for holdings with zero or null current value."""
        cursor = self.conn.execute("""
            SELECT scheme_name, folio_number, current_value, units
            FROM mf_holdings
            WHERE user_id = ?
            AND (current_value IS NULL OR CAST(current_value AS REAL) = 0)
        """, (user_id,))

        zero_holdings = cursor.fetchall()
        if zero_holdings:
            details = [
                f"Zero value: {row[0][:40]}... | Folio: {row[1]} | Value: {row[2]} | Units: {row[3]}"
                for row in zero_holdings
            ]
            return AuditResult(
                check_name="Zero Value Holdings Check",
                passed=False,
                message=f"Found {len(zero_holdings)} holdings with zero value",
                details=details,
                severity="WARNING"
            )

        return AuditResult(
            check_name="Zero Value Holdings Check",
            passed=True,
            message="No zero-value holdings found"
        )

    def audit_negative_values(self, user_id: int) -> AuditResult:
        """Check for negative values in holdings."""
        cursor = self.conn.execute("""
            SELECT scheme_name, folio_number, current_value, units, cost_value
            FROM mf_holdings
            WHERE user_id = ?
            AND (CAST(current_value AS REAL) < 0
                 OR CAST(units AS REAL) < 0
                 OR CAST(cost_value AS REAL) < 0)
        """, (user_id,))

        negative = cursor.fetchall()
        if negative:
            details = [
                f"Negative: {row[0][:40]}... | Value: {row[2]} | Units: {row[3]} | Cost: {row[4]}"
                for row in negative
            ]
            return AuditResult(
                check_name="Negative Values Check",
                passed=False,
                message=f"Found {len(negative)} holdings with negative values",
                details=details,
                severity="ERROR"
            )

        return AuditResult(
            check_name="Negative Values Check",
            passed=True,
            message="No negative values found"
        )

    def audit_missing_required_fields(self, user_id: int) -> AuditResult:
        """Check for missing required fields."""
        cursor = self.conn.execute("""
            SELECT id, scheme_name, folio_number, amc_name, rta
            FROM mf_holdings
            WHERE user_id = ?
            AND (scheme_name IS NULL OR scheme_name = ''
                 OR folio_number IS NULL OR folio_number = ''
                 OR amc_name IS NULL OR amc_name = ''
                 OR rta IS NULL OR rta = '')
        """, (user_id,))

        missing = cursor.fetchall()
        if missing:
            details = []
            for row in missing:
                missing_fields = []
                if not row[1]: missing_fields.append("scheme_name")
                if not row[2]: missing_fields.append("folio_number")
                if not row[3]: missing_fields.append("amc_name")
                if not row[4]: missing_fields.append("rta")
                details.append(f"ID {row[0]}: Missing {', '.join(missing_fields)}")

            return AuditResult(
                check_name="Required Fields Check",
                passed=False,
                message=f"Found {len(missing)} holdings with missing required fields",
                details=details,
                severity="ERROR"
            )

        return AuditResult(
            check_name="Required Fields Check",
            passed=True,
            message="All required fields present"
        )

    def audit_rta_consistency(self, user_id: int) -> AuditResult:
        """Check RTA values are valid."""
        cursor = self.conn.execute("""
            SELECT DISTINCT rta, COUNT(*) as cnt
            FROM mf_holdings
            WHERE user_id = ?
            GROUP BY rta
        """, (user_id,))

        rta_counts = cursor.fetchall()
        valid_rtas = {'CAMS', 'KFINTECH'}
        invalid = []
        details = []

        for row in rta_counts:
            rta, count = row[0], row[1]
            details.append(f"RTA '{rta}': {count} holdings")
            if rta not in valid_rtas:
                invalid.append(rta)

        if invalid:
            return AuditResult(
                check_name="RTA Consistency Check",
                passed=False,
                message=f"Invalid RTA values found: {invalid}",
                details=details,
                severity="WARNING"
            )

        return AuditResult(
            check_name="RTA Consistency Check",
            passed=True,
            message=f"RTA values valid: {[r[0] for r in rta_counts]}",
            details=details
        )

    def audit_date_validity(self, user_id: int) -> AuditResult:
        """Check NAV dates are reasonable."""
        cursor = self.conn.execute("""
            SELECT scheme_name, folio_number, nav_date
            FROM mf_holdings
            WHERE user_id = ?
            AND (nav_date < '2000-01-01' OR nav_date > date('now', '+1 day'))
        """, (user_id,))

        invalid_dates = cursor.fetchall()
        if invalid_dates:
            details = [
                f"Invalid date: {row[0][:40]}... | Folio: {row[1]} | Date: {row[2]}"
                for row in invalid_dates
            ]
            return AuditResult(
                check_name="Date Validity Check",
                passed=False,
                message=f"Found {len(invalid_dates)} holdings with invalid dates",
                details=details,
                severity="WARNING"
            )

        return AuditResult(
            check_name="Date Validity Check",
            passed=True,
            message="All dates are valid"
        )

    def audit_folio_consistency(self, user_id: int) -> AuditResult:
        """Check folio numbers are consistent format."""
        cursor = self.conn.execute("""
            SELECT folio_number, COUNT(*) as cnt
            FROM mf_holdings
            WHERE user_id = ?
            GROUP BY folio_number
        """, (user_id,))

        folios = cursor.fetchall()
        details = []
        warnings = []

        for row in folios:
            folio, count = row[0], row[1]
            details.append(f"Folio '{folio}': {count} schemes")

            # Check for unusual folio formats
            if folio and ('.' in str(folio) or len(str(folio)) > 15):
                warnings.append(f"Unusual folio format: {folio}")

        if warnings:
            return AuditResult(
                check_name="Folio Consistency Check",
                passed=False,
                message=f"Found {len(warnings)} folios with unusual format",
                details=warnings,
                severity="WARNING"
            )

        return AuditResult(
            check_name="Folio Consistency Check",
            passed=True,
            message=f"All {len(folios)} folios have valid format",
            details=details
        )

    def audit_scheme_classification(self, user_id: int) -> AuditResult:
        """Check scheme type classification."""
        cursor = self.conn.execute("""
            SELECT scheme_type, COUNT(*) as cnt,
                   SUM(CAST(current_value AS REAL)) as total_value
            FROM mf_holdings
            WHERE user_id = ?
            GROUP BY scheme_type
        """, (user_id,))

        valid_types = {'EQUITY', 'DEBT', 'HYBRID'}
        details = []
        invalid = []

        for row in cursor.fetchall():
            scheme_type, count, value = row[0], row[1], row[2] or 0
            details.append(f"{scheme_type}: {count} holdings, Rs. {value:,.2f}")
            if scheme_type not in valid_types:
                invalid.append(scheme_type)

        if invalid:
            return AuditResult(
                check_name="Scheme Classification Check",
                passed=False,
                message=f"Invalid scheme types: {invalid}",
                details=details,
                severity="WARNING"
            )

        return AuditResult(
            check_name="Scheme Classification Check",
            passed=True,
            message="All schemes properly classified",
            details=details
        )

    def audit_value_reasonableness(self, user_id: int) -> AuditResult:
        """Check values are reasonable (not abnormally high/low)."""
        cursor = self.conn.execute("""
            SELECT scheme_name, folio_number,
                   CAST(current_value AS REAL) as value,
                   CAST(units AS REAL) as units,
                   CASE WHEN CAST(units AS REAL) > 0
                        THEN CAST(current_value AS REAL) / CAST(units AS REAL)
                        ELSE 0 END as nav
            FROM mf_holdings
            WHERE user_id = ?
        """, (user_id,))

        details = []
        warnings = []

        for row in cursor.fetchall():
            scheme, folio, value, units, nav = row

            # Check for very high single holding (> 50 crore)
            if value > 500000000:
                warnings.append(f"Very high value: {scheme[:30]}... Rs. {value:,.0f}")

            # Check for unusual NAV (< 1 or > 100000)
            if nav and (nav < 1 or nav > 100000):
                warnings.append(f"Unusual NAV: {scheme[:30]}... NAV={nav:.2f}")

        if warnings:
            return AuditResult(
                check_name="Value Reasonableness Check",
                passed=False,
                message=f"Found {len(warnings)} potentially unusual values",
                details=warnings,
                severity="WARNING"
            )

        return AuditResult(
            check_name="Value Reasonableness Check",
            passed=True,
            message="All values within reasonable range"
        )

    def get_holdings_summary(self, user_id: int) -> Dict[str, Any]:
        """Get detailed holdings summary for diagnostics."""
        summary = {}

        # By RTA
        cursor = self.conn.execute("""
            SELECT rta, COUNT(*) as cnt,
                   SUM(CAST(current_value AS REAL)) as value,
                   SUM(CAST(cost_value AS REAL)) as cost
            FROM mf_holdings WHERE user_id = ?
            GROUP BY rta
        """, (user_id,))
        summary['by_rta'] = [
            {'rta': r[0], 'count': r[1], 'value': r[2], 'cost': r[3]}
            for r in cursor.fetchall()
        ]

        # By scheme type
        cursor = self.conn.execute("""
            SELECT scheme_type, COUNT(*) as cnt,
                   SUM(CAST(current_value AS REAL)) as value
            FROM mf_holdings WHERE user_id = ?
            GROUP BY scheme_type
        """, (user_id,))
        summary['by_type'] = [
            {'type': r[0], 'count': r[1], 'value': r[2]}
            for r in cursor.fetchall()
        ]

        # By source file
        cursor = self.conn.execute("""
            SELECT source_file, COUNT(*) as cnt,
                   SUM(CAST(current_value AS REAL)) as value
            FROM mf_holdings WHERE user_id = ?
            GROUP BY source_file
        """, (user_id,))
        summary['by_source'] = [
            {'file': r[0].split('/')[-1] if r[0] else 'Unknown', 'count': r[1], 'value': r[2]}
            for r in cursor.fetchall()
        ]

        return summary

    def print_report(self, report: DiagnosticReport):
        """Print diagnostic report to console."""
        print("\n" + "=" * 70)
        print(f"MF DIAGNOSTIC REPORT - {report.user_name}")
        print(f"Date: {report.report_date}")
        print("=" * 70)

        print(f"\nTotal Holdings: {report.total_holdings}")
        print(f"Total Value: Rs. {float(report.total_value):,.2f}")

        print("\n" + "-" * 70)
        print("AUDIT RESULTS")
        print("-" * 70)

        for audit in report.audits:
            status = "✓ PASS" if audit.passed else f"✗ FAIL ({audit.severity})"
            print(f"\n{audit.check_name}: {status}")
            print(f"  {audit.message}")
            if audit.details and not audit.passed:
                for detail in audit.details[:5]:  # Limit to first 5
                    print(f"    - {detail}")
                if len(audit.details) > 5:
                    print(f"    ... and {len(audit.details) - 5} more")

        print("\n" + "-" * 70)
        print("SUMMARY")
        print("-" * 70)
        total = len(report.audits)
        passed = sum(1 for a in report.audits if a.passed)
        print(f"Checks Passed: {passed}/{total}")
        print(f"Errors: {report.error_count}")
        print(f"Warnings: {report.warning_count}")

        if report.all_passed:
            print("\n✓ All audit checks passed!")
        else:
            print(f"\n⚠ {total - passed} audit checks need attention")

        print("=" * 70 + "\n")
