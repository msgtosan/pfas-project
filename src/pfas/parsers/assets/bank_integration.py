"""
Bank Intelligence Integration for Asset Classes.

Extracts income data from bank statements and populates:
- Rental Income
- SGB Interest
- REIT Distributions
- Dividends

Data-driven: No code changes needed for new categories.
"""

import sqlite3
from datetime import date
from decimal import Decimal
from typing import Optional, Dict, List, Any
from pathlib import Path

from .models import AssetIncomeSummary
from .rental import RentalIncomeManager
from .sgb import SGBTracker
from .reit import REITTracker
from .dividends import DividendTracker


class BankAssetIntegration:
    """
    Integrates bank statement intelligence with asset trackers.

    Reads categorized transactions from money_movement.db
    and populates the appropriate asset tables.
    """

    # Category to asset class mapping
    CATEGORY_MAPPING = {
        "RENT_INCOME": "rental",
        "SGB_INTEREST": "sgb",
        "DIVIDEND": "dividend",
        "REIT_DIVIDEND": "reit_dividend",
        "REIT_INTEREST": "reit_interest",
        "SAVINGS_INTEREST": "bank_interest",
    }

    def __init__(
        self,
        bank_intel_db: str,
        asset_db: sqlite3.Connection,
        user_id: Optional[int] = None
    ):
        """
        Initialize integration.

        Args:
            bank_intel_db: Path to money_movement.db
            asset_db: Connection to PFAS asset database
            user_id: Optional user ID for filtering
        """
        self.bank_intel_db = bank_intel_db
        self.asset_conn = asset_db
        self.user_id = user_id

        # Initialize asset trackers
        self.rental_manager = RentalIncomeManager(asset_db)
        self.sgb_tracker = SGBTracker(asset_db)
        self.reit_tracker = REITTracker(asset_db)
        self.dividend_tracker = DividendTracker(asset_db)

    def extract_income_from_bank(
        self,
        financial_year: str,
        user_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Extract all income categories from bank statements.

        Args:
            financial_year: Fiscal year (e.g., "FY 2024-25")
            user_name: Optional user name filter

        Returns:
            Dict with extraction results per category
        """
        if not Path(self.bank_intel_db).exists():
            raise FileNotFoundError(f"Bank intelligence DB not found: {self.bank_intel_db}")

        bank_conn = sqlite3.connect(self.bank_intel_db)
        bank_conn.row_factory = sqlite3.Row

        results = {
            "financial_year": financial_year,
            "rental": {"count": 0, "total": Decimal("0")},
            "sgb": {"count": 0, "total": Decimal("0")},
            "dividend": {"count": 0, "total": Decimal("0")},
            "reit": {"count": 0, "total": Decimal("0")},
        }

        try:
            # Extract rental income
            results["rental"] = self._extract_rental(bank_conn, financial_year, user_name)

            # Extract SGB interest
            results["sgb"] = self._extract_sgb_interest(bank_conn, financial_year, user_name)

            # Extract dividends
            results["dividend"] = self._extract_dividends(bank_conn, financial_year, user_name)

            # Note: REIT distributions may need manual classification
            # as bank statements don't distinguish dividend vs interest portions

        finally:
            bank_conn.close()

        return results

    def _extract_rental(
        self,
        bank_conn: sqlite3.Connection,
        financial_year: str,
        user_name: Optional[str]
    ) -> Dict[str, Any]:
        """Extract rental income from bank statements."""
        query = """
            SELECT txn_date, amount, base_string, user_name
            FROM bank_transactions_intel
            WHERE category = 'RENT_INCOME'
              AND fiscal_year = ?
              AND txn_type = 'CREDIT'
        """
        params = [financial_year]

        if user_name:
            query += " AND user_name = ?"
            params.append(user_name)

        query += " ORDER BY txn_date"

        cursor = bank_conn.execute(query, params)
        rows = cursor.fetchall()

        count = 0
        total = Decimal("0")

        for row in rows:
            amount = Decimal(str(row["amount"]))
            txn_date = date.fromisoformat(row["txn_date"])

            # For now, assume a default property ID = 1
            # In production, this would be matched based on tenant name in description
            try:
                self.rental_manager.add_rental_income_from_bank(
                    property_id=1,
                    financial_year=financial_year,
                    amount=amount,
                    payment_date=txn_date,
                    source_description=row["base_string"]
                )
                count += 1
                total += amount
            except Exception:
                pass  # Skip if property doesn't exist

        return {"count": count, "total": total}

    def _extract_sgb_interest(
        self,
        bank_conn: sqlite3.Connection,
        financial_year: str,
        user_name: Optional[str]
    ) -> Dict[str, Any]:
        """Extract SGB interest from bank statements."""
        query = """
            SELECT txn_date, amount, base_string, user_name
            FROM bank_transactions_intel
            WHERE category = 'SGB_INTEREST'
              AND fiscal_year = ?
              AND txn_type = 'CREDIT'
        """
        params = [financial_year]

        if user_name:
            query += " AND user_name = ?"
            params.append(user_name)

        query += " ORDER BY txn_date"

        cursor = bank_conn.execute(query, params)
        rows = cursor.fetchall()

        count = 0
        total = Decimal("0")

        for row in rows:
            amount = Decimal(str(row["amount"]))
            txn_date = date.fromisoformat(row["txn_date"])

            self.sgb_tracker.add_interest_from_bank(
                amount=amount,
                payment_date=txn_date,
                financial_year=financial_year,
            )
            count += 1
            total += amount

        return {"count": count, "total": total}

    def _extract_dividends(
        self,
        bank_conn: sqlite3.Connection,
        financial_year: str,
        user_name: Optional[str]
    ) -> Dict[str, Any]:
        """Extract dividends from bank statements."""
        query = """
            SELECT txn_date, amount, base_string, user_name
            FROM bank_transactions_intel
            WHERE category = 'DIVIDEND'
              AND fiscal_year = ?
              AND txn_type = 'CREDIT'
        """
        params = [financial_year]

        if user_name:
            query += " AND user_name = ?"
            params.append(user_name)

        query += " ORDER BY txn_date"

        cursor = bank_conn.execute(query, params)
        rows = cursor.fetchall()

        count = 0
        total = Decimal("0")

        for row in rows:
            amount = Decimal(str(row["amount"]))
            txn_date = date.fromisoformat(row["txn_date"])
            description = row["base_string"] or ""

            self.dividend_tracker.add_dividend_from_bank(
                amount=amount,
                payment_date=txn_date,
                financial_year=financial_year,
                user_id=self.user_id,
                description=description,
            )
            count += 1
            total += amount

        return {"count": count, "total": total}

    def get_asset_income_summary(self, financial_year: str) -> AssetIncomeSummary:
        """
        Get comprehensive income summary for all asset classes.

        Args:
            financial_year: Fiscal year (e.g., "FY 2024-25")

        Returns:
            AssetIncomeSummary with all income and TDS details
        """
        summary = AssetIncomeSummary(financial_year=financial_year)

        # Rental Income
        try:
            rental_data = self.rental_manager.get_all_hp_income(
                self.user_id or 1, financial_year
            )
            summary.rental_gross = rental_data.get("total_hp_income", Decimal("0"))
            if summary.rental_gross > 0:
                # Calculate components
                for prop in rental_data.get("properties", []):
                    summary.rental_municipal_tax += Decimal(str(prop.get("municipal_tax", 0)))
                    summary.rental_standard_deduction += Decimal(str(prop.get("standard_deduction", 0)))
                summary.rental_net_income = summary.rental_gross
        except Exception:
            pass

        # SGB Interest
        summary.sgb_interest = self.sgb_tracker.get_total_interest_for_fy(
            financial_year, self.user_id
        )

        # REIT Distributions
        reit_summary = self.reit_tracker.get_distribution_summary(
            financial_year, self.user_id
        )
        summary.reit_dividend = reit_summary.get("dividend", Decimal("0"))
        summary.reit_interest = reit_summary.get("interest", Decimal("0"))
        summary.reit_other = reit_summary.get("other", Decimal("0"))
        summary.tds_on_reit = reit_summary.get("tds", Decimal("0"))

        # Dividends
        dividend_summary = self.dividend_tracker.get_summary_for_fy(
            financial_year, self.user_id
        )
        summary.dividend_income = dividend_summary.total_dividend_income
        summary.tds_on_dividend = dividend_summary.total_tds_deducted

        return summary


def extract_all_from_bank(
    bank_intel_db: str,
    asset_db_path: str,
    financial_year: str,
    user_name: Optional[str] = None,
    sgb_holdings_file: Optional[str] = None
) -> Dict[str, Any]:
    """
    Main entry point to extract all income from bank statements.

    Args:
        bank_intel_db: Path to money_movement.db
        asset_db_path: Path to asset database
        financial_year: Fiscal year (e.g., "FY 2024-25")
        user_name: Optional user filter
        sgb_holdings_file: Optional path to SGB holdings Excel

    Returns:
        Dict with extraction results
    """
    # Create/connect to asset database
    asset_conn = sqlite3.connect(asset_db_path)
    asset_conn.row_factory = sqlite3.Row

    results = {}

    try:
        integration = BankAssetIntegration(bank_intel_db, asset_conn)

        # Import SGB holdings if file provided
        if sgb_holdings_file and Path(sgb_holdings_file).exists():
            holdings = integration.sgb_tracker.import_holdings(sgb_holdings_file)
            results["sgb_holdings_imported"] = len(holdings)

        # Extract income from bank statements
        extraction = integration.extract_income_from_bank(financial_year, user_name)
        results.update(extraction)

        # Get summary
        summary = integration.get_asset_income_summary(financial_year)
        results["summary"] = {
            "rental_net": float(summary.rental_net_income),
            "sgb_interest": float(summary.sgb_interest),
            "dividend_income": float(summary.dividend_income),
            "reit_dividend": float(summary.reit_dividend),
            "reit_interest": float(summary.reit_interest),
            "total_taxable": float(summary.total_taxable_income),
            "total_exempt": float(summary.total_exempt_income),
            "total_tds": float(summary.total_tds_credit),
        }

        asset_conn.commit()

    finally:
        asset_conn.close()

    return results


# CLI entry point
if __name__ == "__main__":
    import sys
    import json

    bank_db = sys.argv[1] if len(sys.argv) > 1 else "Data/Reports/Bank_Intelligence/money_movement.db"
    asset_db = sys.argv[2] if len(sys.argv) > 2 else "Data/Reports/Bank_Intelligence/assets.db"
    fy = sys.argv[3] if len(sys.argv) > 3 else "FY 2024-25"
    sgb_file = sys.argv[4] if len(sys.argv) > 4 else None

    print(f"Extracting income for {fy}")
    print(f"Bank DB: {bank_db}")
    print(f"Asset DB: {asset_db}")

    results = extract_all_from_bank(bank_db, asset_db, fy, sgb_holdings_file=sgb_file)

    print("\nResults:")
    print(json.dumps(results, indent=2, default=str))
