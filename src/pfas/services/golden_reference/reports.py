"""
Reconciliation Report Generator.

Generates reports for reconciliation results in various formats:
- Excel (xlsx)
- CSV
- Text summary
"""

import csv
import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Dict, Any

from .models import (
    AssetClass,
    ReconciliationSummary,
    ReconciliationEvent,
    SuspenseItem,
    MatchResult,
    Severity,
)

logger = logging.getLogger(__name__)


class ReconciliationReporter:
    """
    Generates reconciliation reports.

    Usage:
        reporter = ReconciliationReporter(output_dir=Path("reports"))

        # Generate Excel report
        report_path = reporter.generate_excel(summary)

        # Generate CSV report
        csv_path = reporter.generate_csv(events)

        # Print text summary
        reporter.print_summary(summary)
    """

    def __init__(
        self,
        output_dir: Optional[Path] = None,
        user_name: str = "User"
    ):
        """
        Initialize reporter.

        Args:
            output_dir: Directory for output files
            user_name: User name for reports
        """
        self.output_dir = output_dir or Path(".")
        self.user_name = user_name
        self._xlsxwriter_available = self._check_xlsxwriter()

    def _check_xlsxwriter(self) -> bool:
        """Check if xlsxwriter is available."""
        try:
            import xlsxwriter
            return True
        except ImportError:
            return False

    def generate_excel(
        self,
        summary: ReconciliationSummary,
        events: Optional[List[ReconciliationEvent]] = None,
        suspense: Optional[List[SuspenseItem]] = None,
        filename: Optional[str] = None
    ) -> Optional[Path]:
        """
        Generate Excel reconciliation report.

        Args:
            summary: Reconciliation summary
            events: Optional list of events (if not in summary)
            suspense: Optional list of suspense items
            filename: Optional custom filename

        Returns:
            Path to generated file, or None if xlsxwriter not available
        """
        if not self._xlsxwriter_available:
            logger.warning("xlsxwriter not available, cannot generate Excel report")
            return None

        import xlsxwriter

        # Generate filename
        if not filename:
            filename = f"reconciliation_{summary.asset_class.value}_{summary.reconciliation_date.isoformat()}.xlsx"

        output_path = self.output_dir / filename
        self.output_dir.mkdir(parents=True, exist_ok=True)

        workbook = xlsxwriter.Workbook(str(output_path))

        # Formats
        header_fmt = workbook.add_format({
            'bold': True,
            'bg_color': '#4472C4',
            'font_color': 'white',
            'border': 1
        })
        currency_fmt = workbook.add_format({'num_format': '₹#,##0.00'})
        pct_fmt = workbook.add_format({'num_format': '0.00%'})
        match_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        mismatch_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        warning_fmt = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})

        # Summary sheet
        self._write_summary_sheet(workbook, summary, header_fmt, currency_fmt, pct_fmt)

        # Events sheet
        event_list = events or (summary.events if hasattr(summary, 'events') else [])
        if event_list:
            self._write_events_sheet(
                workbook, event_list, header_fmt, currency_fmt,
                match_fmt, mismatch_fmt, warning_fmt
            )

        # Suspense sheet
        if suspense:
            self._write_suspense_sheet(workbook, suspense, header_fmt, currency_fmt)

        workbook.close()
        logger.info(f"Generated Excel report: {output_path}")
        return output_path

    def _write_summary_sheet(
        self,
        workbook,
        summary: ReconciliationSummary,
        header_fmt,
        currency_fmt,
        pct_fmt
    ):
        """Write summary sheet."""
        sheet = workbook.add_worksheet("Summary")

        # Title
        sheet.write(0, 0, "Golden Reference Reconciliation Report")
        sheet.write(1, 0, f"User: {self.user_name}")
        sheet.write(2, 0, f"Date: {summary.reconciliation_date.isoformat()}")
        sheet.write(3, 0, f"Asset Class: {summary.asset_class.value}")
        sheet.write(4, 0, f"Source: {summary.source_type.value}")

        # Summary metrics
        row = 6
        metrics = [
            ("Total Items", summary.total_items),
            ("Exact Matches", summary.matched_exact),
            ("Within Tolerance", summary.matched_tolerance),
            ("Mismatches", summary.mismatches),
            ("Missing in System", summary.missing_system),
            ("Missing in Golden", summary.missing_golden),
        ]

        sheet.write(row, 0, "Metric", header_fmt)
        sheet.write(row, 1, "Value", header_fmt)
        row += 1

        for metric, value in metrics:
            sheet.write(row, 0, metric)
            sheet.write(row, 1, value)
            row += 1

        # Match rate
        row += 1
        sheet.write(row, 0, "Match Rate")
        sheet.write(row, 1, summary.match_rate / 100, pct_fmt)

        # Value summary
        row += 2
        sheet.write(row, 0, "Value Summary", header_fmt)
        sheet.merge_range(row, 0, row, 1, "Value Summary", header_fmt)
        row += 1

        sheet.write(row, 0, "System Total")
        sheet.write(row, 1, float(summary.total_system_value), currency_fmt)
        row += 1

        sheet.write(row, 0, "Golden Total")
        sheet.write(row, 1, float(summary.total_golden_value), currency_fmt)
        row += 1

        sheet.write(row, 0, "Difference")
        sheet.write(row, 1, float(summary.total_difference), currency_fmt)

        # Adjust column widths
        sheet.set_column(0, 0, 20)
        sheet.set_column(1, 1, 15)

    def _write_events_sheet(
        self,
        workbook,
        events: List[ReconciliationEvent],
        header_fmt,
        currency_fmt,
        match_fmt,
        mismatch_fmt,
        warning_fmt
    ):
        """Write events sheet."""
        sheet = workbook.add_worksheet("Details")

        headers = [
            "ISIN/Folio", "Symbol", "Match Result", "Severity",
            "System Value", "Golden Value", "Difference", "Diff %", "Status"
        ]

        for col, header in enumerate(headers):
            sheet.write(0, col, header, header_fmt)

        for row, event in enumerate(events, start=1):
            identifier = event.isin or event.folio_number or event.symbol or ""
            sheet.write(row, 0, identifier)
            sheet.write(row, 1, event.symbol or "")
            sheet.write(row, 2, event.match_result.value if event.match_result else "")

            severity = event.severity.value if event.severity else ""
            sheet.write(row, 3, severity)

            sheet.write(row, 4, float(event.system_value) if event.system_value else 0, currency_fmt)
            sheet.write(row, 5, float(event.golden_value) if event.golden_value else 0, currency_fmt)
            sheet.write(row, 6, float(event.difference) if event.difference else 0, currency_fmt)
            sheet.write(row, 7, float(event.difference_pct) / 100 if event.difference_pct else 0)
            sheet.write(row, 8, event.status.value if event.status else "")

            # Apply conditional formatting
            if event.match_result in [MatchResult.EXACT, MatchResult.WITHIN_TOLERANCE]:
                sheet.set_row(row, None, match_fmt)
            elif event.match_result == MatchResult.MISMATCH:
                if event.severity == Severity.CRITICAL:
                    sheet.set_row(row, None, mismatch_fmt)
                elif event.severity in [Severity.WARNING, Severity.ERROR]:
                    sheet.set_row(row, None, warning_fmt)

        # Adjust column widths
        sheet.set_column(0, 0, 15)
        sheet.set_column(1, 1, 12)
        sheet.set_column(2, 2, 18)
        sheet.set_column(3, 3, 10)
        sheet.set_column(4, 6, 15)
        sheet.set_column(7, 7, 10)
        sheet.set_column(8, 8, 12)

    def _write_suspense_sheet(
        self,
        workbook,
        items: List[SuspenseItem],
        header_fmt,
        currency_fmt
    ):
        """Write suspense sheet."""
        sheet = workbook.add_worksheet("Suspense")

        headers = [
            "ID", "Asset Type", "ISIN/Folio", "Name",
            "Suspense Value", "Reason", "Opened", "Priority", "Status"
        ]

        for col, header in enumerate(headers):
            sheet.write(0, col, header, header_fmt)

        for row, item in enumerate(items, start=1):
            sheet.write(row, 0, item.id or "")
            sheet.write(row, 1, item.asset_type.value if item.asset_type else "")
            sheet.write(row, 2, item.isin or item.folio_number or "")
            sheet.write(row, 3, item.name or "")
            sheet.write(row, 4, float(item.suspense_value) if item.suspense_value else 0, currency_fmt)
            sheet.write(row, 5, item.suspense_reason or "")
            sheet.write(row, 6, item.opened_date.isoformat() if item.opened_date else "")
            sheet.write(row, 7, item.priority or "")
            sheet.write(row, 8, item.status.value if item.status else "")

        # Adjust column widths
        sheet.set_column(0, 0, 8)
        sheet.set_column(1, 1, 12)
        sheet.set_column(2, 2, 15)
        sheet.set_column(3, 3, 30)
        sheet.set_column(4, 4, 15)
        sheet.set_column(5, 5, 40)
        sheet.set_column(6, 6, 12)
        sheet.set_column(7, 8, 12)

    def generate_csv(
        self,
        events: List[ReconciliationEvent],
        filename: Optional[str] = None
    ) -> Path:
        """
        Generate CSV reconciliation report.

        Args:
            events: List of reconciliation events
            filename: Optional custom filename

        Returns:
            Path to generated file
        """
        if not filename:
            filename = f"reconciliation_events_{date.today().isoformat()}.csv"

        output_path = self.output_dir / filename
        self.output_dir.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # Header
            writer.writerow([
                "ISIN", "Folio Number", "Symbol", "Match Result", "Severity",
                "System Value", "Golden Value", "Difference", "Difference %",
                "Status", "Reconciliation Date"
            ])

            # Data rows
            for event in events:
                writer.writerow([
                    event.isin or "",
                    event.folio_number or "",
                    event.symbol or "",
                    event.match_result.value if event.match_result else "",
                    event.severity.value if event.severity else "",
                    str(event.system_value) if event.system_value else "",
                    str(event.golden_value) if event.golden_value else "",
                    str(event.difference) if event.difference else "",
                    str(event.difference_pct) if event.difference_pct else "",
                    event.status.value if event.status else "",
                    event.reconciliation_date.isoformat() if event.reconciliation_date else "",
                ])

        logger.info(f"Generated CSV report: {output_path}")
        return output_path

    def print_summary(self, summary: ReconciliationSummary) -> None:
        """Print text summary to console."""
        print(f"\n{'=' * 60}")
        print(f"RECONCILIATION REPORT - {summary.asset_class.value}")
        print(f"{'=' * 60}")
        print(f"User: {self.user_name}")
        print(f"Date: {summary.reconciliation_date}")
        print(f"Source: {summary.source_type.value}")
        print()

        print("SUMMARY")
        print("-" * 40)
        print(f"  Total Items:         {summary.total_items:,}")
        print(f"  Exact Matches:       {summary.matched_exact:,}")
        print(f"  Within Tolerance:    {summary.matched_tolerance:,}")
        print(f"  Mismatches:          {summary.mismatches:,}")
        print(f"  Missing in System:   {summary.missing_system:,}")
        print(f"  Missing in Golden:   {summary.missing_golden:,}")
        print(f"  Match Rate:          {summary.match_rate:.1f}%")
        print()

        print("VALUES")
        print("-" * 40)
        print(f"  System Total:        ₹{summary.total_system_value:,.2f}")
        print(f"  Golden Total:        ₹{summary.total_golden_value:,.2f}")
        print(f"  Difference:          ₹{summary.total_difference:,.2f}")
        print()

        if summary.mismatches > 0 or summary.missing_system > 0 or summary.missing_golden > 0:
            print("⚠️  Discrepancies found - review suspense items")
        else:
            print("✓ All items matched successfully")

        print(f"{'=' * 60}\n")
