"""
MF Capital Gains Reconciler - Reconciles calculated vs reported capital gains.

Compares capital gains computed by PFAS with those reported in RTA statements
(CAMS, KFintech) and maintains audit trail for reconciliation.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

logger = logging.getLogger(__name__)


@dataclass
class ReconciliationItem:
    """Individual scheme reconciliation result."""
    scheme_name: str
    folio_number: str
    calc_stcg: Decimal = Decimal("0")
    calc_ltcg: Decimal = Decimal("0")
    reported_stcg: Decimal = Decimal("0")
    reported_ltcg: Decimal = Decimal("0")
    difference: Decimal = Decimal("0")
    match_status: str = "MATCH"  # MATCH, MISMATCH, MISSING_CALC, MISSING_REPORTED
    notes: str = ""

    @property
    def is_matched(self) -> bool:
        return self.match_status == "MATCH"


@dataclass
class ReconciliationResult:
    """Overall reconciliation result for a financial year and RTA."""
    user_id: int
    financial_year: str
    rta: str
    asset_class: str = "ALL"

    # Calculated totals
    calc_stcg: Decimal = Decimal("0")
    calc_ltcg: Decimal = Decimal("0")
    calc_total: Decimal = Decimal("0")

    # Reported totals
    reported_stcg: Decimal = Decimal("0")
    reported_ltcg: Decimal = Decimal("0")
    reported_total: Decimal = Decimal("0")

    # Differences
    stcg_difference: Decimal = Decimal("0")
    ltcg_difference: Decimal = Decimal("0")
    total_difference: Decimal = Decimal("0")

    # Status
    is_reconciled: bool = False
    tolerance_used: Decimal = Decimal("1.00")

    # Detail items
    items: List[ReconciliationItem] = field(default_factory=list)
    source_file: str = ""
    notes: str = ""

    def calculate_differences(self):
        """Calculate difference fields."""
        self.stcg_difference = self.calc_stcg - self.reported_stcg
        self.ltcg_difference = self.calc_ltcg - self.reported_ltcg
        self.calc_total = self.calc_stcg + self.calc_ltcg
        self.reported_total = self.reported_stcg + self.reported_ltcg
        self.total_difference = self.calc_total - self.reported_total

        # Check if within tolerance
        self.is_reconciled = abs(self.total_difference) <= self.tolerance_used


class MFReconciler:
    """
    Reconciles calculated capital gains with RTA-reported capital gains.

    Usage:
        reconciler = MFReconciler(conn, config)
        result = reconciler.reconcile(
            user_id=1,
            financial_year="2024-25",
            rta="CAMS"
        )
        reconciler.save_result(result)
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        config: Optional[Dict[str, Any]] = None
    ):
        self.conn = conn
        self.config = config or {}
        self.tolerance_amount = Decimal(str(
            self.config.get("reconciliation", {}).get("tolerance_amount", 1.00)
        ))
        self.tolerance_pct = Decimal(str(
            self.config.get("reconciliation", {}).get("tolerance_percentage", 0.01)
        ))

    def reconcile(
        self,
        user_id: int,
        financial_year: str,
        rta: str = "CAMS",
        asset_class: str = "ALL",
        reported_cg_file: Optional[Path] = None
    ) -> ReconciliationResult:
        """
        Reconcile capital gains for a user, FY, and RTA.

        Args:
            user_id: User ID
            financial_year: Financial year (e.g., "2024-25")
            rta: RTA source ("CAMS", "KFINTECH", "COMBINED")
            asset_class: Asset class to reconcile ("EQUITY", "DEBT", "ALL")
            reported_cg_file: Optional path to RTA capital gains statement

        Returns:
            ReconciliationResult with comparison details
        """
        result = ReconciliationResult(
            user_id=user_id,
            financial_year=financial_year,
            rta=rta,
            asset_class=asset_class,
            tolerance_used=self.tolerance_amount
        )

        # Get calculated capital gains from our DB
        calc_gains = self._get_calculated_gains(user_id, financial_year, asset_class)
        result.calc_stcg = calc_gains.get("stcg", Decimal("0"))
        result.calc_ltcg = calc_gains.get("ltcg", Decimal("0"))

        # Get reported capital gains
        if reported_cg_file:
            reported_gains = self._parse_cg_statement(reported_cg_file, rta)
            result.reported_stcg = reported_gains.get("stcg", Decimal("0"))
            result.reported_ltcg = reported_gains.get("ltcg", Decimal("0"))
            result.source_file = str(reported_cg_file)
            result.items = reported_gains.get("items", [])
        else:
            # Try to get from previously stored reported values
            reported_gains = self._get_stored_reported_gains(user_id, financial_year, rta)
            result.reported_stcg = reported_gains.get("stcg", Decimal("0"))
            result.reported_ltcg = reported_gains.get("ltcg", Decimal("0"))

        # Calculate differences and determine reconciliation status
        result.calculate_differences()

        # Reconcile at item level if we have items
        if result.items:
            result.items = self._reconcile_items(
                calc_gains.get("by_scheme", {}),
                result.items
            )

        logger.info(
            f"Reconciliation for {user_id}/{financial_year}/{rta}: "
            f"calc={result.calc_total}, reported={result.reported_total}, "
            f"diff={result.total_difference}, reconciled={result.is_reconciled}"
        )

        return result

    def _get_calculated_gains(
        self,
        user_id: int,
        financial_year: str,
        asset_class: str
    ) -> Dict[str, Any]:
        """Get calculated capital gains from mf_capital_gains table."""
        if asset_class == "ALL":
            query = """
                SELECT
                    SUM(stcg_amount) as stcg,
                    SUM(ltcg_amount) as ltcg
                FROM mf_capital_gains
                WHERE user_id = ? AND financial_year = ?
            """
            params = (user_id, financial_year)
        else:
            query = """
                SELECT
                    SUM(stcg_amount) as stcg,
                    SUM(ltcg_amount) as ltcg
                FROM mf_capital_gains
                WHERE user_id = ? AND financial_year = ? AND asset_class = ?
            """
            params = (user_id, financial_year, asset_class)

        cursor = self.conn.execute(query, params)
        row = cursor.fetchone()

        result = {
            "stcg": Decimal(str(row[0] or 0)) if row else Decimal("0"),
            "ltcg": Decimal(str(row[1] or 0)) if row else Decimal("0"),
            "by_scheme": {}
        }

        # Get scheme-level breakdown
        scheme_query = """
            SELECT
                s.name as scheme_name,
                f.folio_number,
                SUM(t.short_term_gain) as stcg,
                SUM(t.long_term_gain) as ltcg
            FROM mf_transactions t
            JOIN mf_folios f ON t.folio_id = f.id
            JOIN mf_schemes s ON f.scheme_id = s.id
            WHERE t.user_id = ? AND t.date >= ? AND t.date <= ?
              AND t.transaction_type = 'REDEMPTION'
            GROUP BY s.name, f.folio_number
        """
        fy_start, fy_end = self._get_fy_dates(financial_year)
        cursor = self.conn.execute(scheme_query, (user_id, fy_start, fy_end))

        for row in cursor.fetchall():
            key = f"{row[0]}|{row[1]}"
            result["by_scheme"][key] = {
                "scheme_name": row[0],
                "folio_number": row[1],
                "stcg": Decimal(str(row[2] or 0)),
                "ltcg": Decimal(str(row[3] or 0))
            }

        return result

    def _get_stored_reported_gains(
        self,
        user_id: int,
        financial_year: str,
        rta: str
    ) -> Dict[str, Decimal]:
        """Get previously stored reported gains."""
        cursor = self.conn.execute("""
            SELECT reported_stcg, reported_ltcg
            FROM mf_cg_reconciliation
            WHERE user_id = ? AND financial_year = ? AND rta = ?
            ORDER BY created_at DESC
            LIMIT 1
        """, (user_id, financial_year, rta))

        row = cursor.fetchone()
        if row:
            return {
                "stcg": Decimal(str(row[0] or 0)),
                "ltcg": Decimal(str(row[1] or 0))
            }
        return {"stcg": Decimal("0"), "ltcg": Decimal("0")}

    def _parse_cg_statement(
        self,
        file_path: Path,
        rta: str
    ) -> Dict[str, Any]:
        """Parse capital gains statement from RTA file."""
        result = {
            "stcg": Decimal("0"),
            "ltcg": Decimal("0"),
            "items": []
        }

        try:
            if file_path.suffix.lower() in ['.xlsx', '.xls']:
                return self._parse_cg_excel(file_path, rta)
            elif file_path.suffix.lower() == '.pdf':
                logger.warning("PDF parsing for CG reconciliation not yet implemented")
                return result
        except Exception as e:
            logger.error(f"Failed to parse CG statement {file_path}: {e}")

        return result

    def _parse_cg_excel(self, file_path: Path, rta: str) -> Dict[str, Any]:
        """Parse Excel capital gains statement."""
        result = {
            "stcg": Decimal("0"),
            "ltcg": Decimal("0"),
            "items": []
        }

        try:
            # Read summary sheet first
            xl = pd.ExcelFile(file_path, engine='calamine')

            # Try to find summary row with totals
            for sheet_name in xl.sheet_names:
                df = pd.read_excel(xl, sheet_name=sheet_name, header=None)

                # Look for "Total" row with STCG/LTCG values
                for idx, row in df.iterrows():
                    row_str = ' '.join(str(v) for v in row if pd.notna(v)).lower()
                    if 'total' in row_str or 'grand total' in row_str:
                        # Try to extract STCG/LTCG values
                        for col_idx, val in enumerate(row):
                            if pd.notna(val):
                                try:
                                    numeric_val = Decimal(str(val).replace(',', '').replace('Rs.', '').strip())
                                    # Heuristic: larger positive value is likely LTCG
                                    if numeric_val > result["ltcg"]:
                                        result["ltcg"] = numeric_val
                                except:
                                    pass

            # Parse transaction details for item-level reconciliation
            detail_sheets = ['TRXN_DETAILS', 'Transaction_Details', 'Trasaction_Details']
            header_row = 3 if rta == "CAMS" else 4

            for sheet_name in detail_sheets:
                if sheet_name in xl.sheet_names:
                    df = pd.read_excel(xl, sheet_name=sheet_name, header=header_row)

                    # Group by scheme and sum gains
                    for scheme_name in df['Scheme Name'].unique() if 'Scheme Name' in df.columns else []:
                        scheme_df = df[df['Scheme Name'] == scheme_name]
                        stcg = Decimal("0")
                        ltcg = Decimal("0")

                        for _, row in scheme_df.iterrows():
                            if 'Short Term' in str(row.get('Gain Type', '')):
                                gain_val = row.get('Gain Amount', row.get('Capital Gain', 0))
                                stcg += self._parse_decimal(gain_val)
                            elif 'Long Term' in str(row.get('Gain Type', '')):
                                gain_val = row.get('Gain Amount', row.get('Capital Gain', 0))
                                ltcg += self._parse_decimal(gain_val)

                        folio = str(scheme_df['Folio No'].iloc[0]) if 'Folio No' in scheme_df.columns else ""

                        result["items"].append(ReconciliationItem(
                            scheme_name=scheme_name,
                            folio_number=folio,
                            reported_stcg=stcg,
                            reported_ltcg=ltcg
                        ))

                    break

        except Exception as e:
            logger.error(f"Error parsing CG Excel: {e}")

        return result

    def _reconcile_items(
        self,
        calc_by_scheme: Dict[str, Dict],
        reported_items: List[ReconciliationItem]
    ) -> List[ReconciliationItem]:
        """Reconcile at scheme level."""
        reconciled = []

        for item in reported_items:
            key = f"{item.scheme_name}|{item.folio_number}"
            calc_data = calc_by_scheme.get(key)

            if calc_data:
                item.calc_stcg = calc_data["stcg"]
                item.calc_ltcg = calc_data["ltcg"]
                item.difference = (
                    (item.calc_stcg + item.calc_ltcg) -
                    (item.reported_stcg + item.reported_ltcg)
                )

                if abs(item.difference) <= self.tolerance_amount:
                    item.match_status = "MATCH"
                else:
                    item.match_status = "MISMATCH"
            else:
                item.match_status = "MISSING_CALC"
                item.notes = "No calculated gains found for this scheme"

            reconciled.append(item)

        # Check for schemes we have calculations for but no reported values
        reported_keys = {f"{i.scheme_name}|{i.folio_number}" for i in reported_items}
        for key, calc_data in calc_by_scheme.items():
            if key not in reported_keys:
                reconciled.append(ReconciliationItem(
                    scheme_name=calc_data["scheme_name"],
                    folio_number=calc_data["folio_number"],
                    calc_stcg=calc_data["stcg"],
                    calc_ltcg=calc_data["ltcg"],
                    match_status="MISSING_REPORTED",
                    notes="No reported gains found for this scheme"
                ))

        return reconciled

    def save_result(self, result: ReconciliationResult) -> int:
        """Save reconciliation result to database."""
        cursor = self.conn.execute("""
            INSERT INTO mf_cg_reconciliation (
                user_id, financial_year, rta, asset_class,
                calc_stcg, calc_ltcg, calc_total_gain,
                reported_stcg, reported_ltcg, reported_total_gain,
                stcg_difference, ltcg_difference, total_difference,
                is_reconciled, tolerance_used, reconciliation_notes,
                source_file, reconciled_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, financial_year, rta, asset_class) DO UPDATE SET
                calc_stcg = excluded.calc_stcg,
                calc_ltcg = excluded.calc_ltcg,
                calc_total_gain = excluded.calc_total_gain,
                reported_stcg = excluded.reported_stcg,
                reported_ltcg = excluded.reported_ltcg,
                reported_total_gain = excluded.reported_total_gain,
                stcg_difference = excluded.stcg_difference,
                ltcg_difference = excluded.ltcg_difference,
                total_difference = excluded.total_difference,
                is_reconciled = excluded.is_reconciled,
                reconciliation_notes = excluded.reconciliation_notes,
                source_file = excluded.source_file,
                reconciled_at = excluded.reconciled_at,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
        """, (
            result.user_id, result.financial_year, result.rta, result.asset_class,
            str(result.calc_stcg), str(result.calc_ltcg), str(result.calc_total),
            str(result.reported_stcg), str(result.reported_ltcg), str(result.reported_total),
            str(result.stcg_difference), str(result.ltcg_difference), str(result.total_difference),
            result.is_reconciled, str(result.tolerance_used), result.notes,
            result.source_file, datetime.now().isoformat() if result.is_reconciled else None
        ))

        row = cursor.fetchone()
        recon_id = row[0] if row else cursor.lastrowid

        # Save detail items
        if result.items and recon_id:
            for item in result.items:
                self.conn.execute("""
                    INSERT INTO mf_cg_reconciliation_items (
                        reconciliation_id, scheme_name, folio_number,
                        calc_stcg, calc_ltcg, reported_stcg, reported_ltcg,
                        difference, match_status, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    recon_id, item.scheme_name, item.folio_number,
                    str(item.calc_stcg), str(item.calc_ltcg),
                    str(item.reported_stcg), str(item.reported_ltcg),
                    str(item.difference), item.match_status, item.notes
                ))

        self.conn.commit()
        return recon_id

    def _get_fy_dates(self, financial_year: str) -> Tuple[str, str]:
        """Get start and end dates for a financial year."""
        # FY format: "2024-25"
        parts = financial_year.split("-")
        start_year = int(parts[0])
        end_year = start_year + 1

        return (f"{start_year}-04-01", f"{end_year}-03-31")

    def _parse_decimal(self, value) -> Decimal:
        """Parse decimal value safely."""
        if pd.isna(value):
            return Decimal("0")
        try:
            cleaned = str(value).replace(',', '').replace('Rs.', '').replace(' ', '')
            return Decimal(cleaned)
        except:
            return Decimal("0")

    def get_reconciliation_report(
        self,
        user_id: int,
        financial_year: str
    ) -> pd.DataFrame:
        """Get reconciliation report as DataFrame."""
        query = """
            SELECT
                financial_year, rta, asset_class,
                calc_stcg, calc_ltcg, calc_total_gain,
                reported_stcg, reported_ltcg, reported_total_gain,
                total_difference, is_reconciled, reconciled_at
            FROM mf_cg_reconciliation
            WHERE user_id = ? AND financial_year = ?
            ORDER BY rta, asset_class
        """
        return pd.read_sql_query(query, self.conn, params=(user_id, financial_year))
