"""Salary Documents Ingester (Form16, Payslips)."""

import logging
from pathlib import Path
from decimal import Decimal
from datetime import date
from typing import List, Optional, Dict, Any

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

from pfas.services.generic_ingester import GenericAssetIngester, GenericIngestionResult
from .form16 import Form16Parser
from .payslip import PayslipParser

# Ledger integration imports
from pfas.core.transaction_service import TransactionService, TransactionSource
from pfas.parsers.ledger_integration import record_salary

logger = logging.getLogger(__name__)


class SalaryIngester(GenericAssetIngester):
    """Salary documents ingester."""

    def __init__(self, conn: sqlite3.Connection, user_id: int, inbox_path: Path):
        super().__init__(conn, user_id, inbox_path, "Salary")
        self.form16_parser = Form16Parser(conn)
        self.payslip_parser = PayslipParser(conn)

    def get_supported_extensions(self) -> List[str]:
        return ['.pdf', '.xlsx', '.xls']

    def detect_source_from_path(self, file_path: Path) -> Optional[str]:
        name_upper = file_path.name.upper()
        if 'FORM16' in name_upper or 'FORM-16' in name_upper:
            return 'Form16'
        elif 'PAYSLIP' in name_upper or 'SALARY' in name_upper:
            return 'Payslip'
        return 'Generic'

    def parse_file(self, file_path: Path, source: Optional[str]) -> Dict[str, Any]:
        result = {'success': False, 'records': [], 'errors': []}

        try:
            if source == 'Form16':
                data = self.form16_parser.parse(file_path)
            elif source == 'Payslip':
                data = self.payslip_parser.parse(file_path)
            else:
                # Try Form16 first, then payslip
                try:
                    data = self.form16_parser.parse(file_path)
                except:
                    data = self.payslip_parser.parse(file_path)

            result['success'] = True
            result['records'] = [data] if data else []

        except Exception as e:
            result['errors'].append(f"Parse error: {str(e)}")

        return result

    def save_to_db(self, records: List[Any], source_file: str = "") -> int:
        """
        Save salary records to database with double-entry ledger.

        Args:
            records: List of salary records (SalaryRecord or Form16Record)
            source_file: Path to source file for idempotency

        Returns:
            Number of records saved
        """
        if not records:
            return 0

        # Initialize TransactionService for ledger entries
        txn_service = TransactionService(self.conn)

        inserted = 0
        for row_idx, record in enumerate(records):
            try:
                # Handle SalaryRecord objects
                if hasattr(record, 'pay_period'):
                    # It's a SalaryRecord from payslip
                    employer = record.employee_name or "Unknown"
                    pay_period = record.pay_period
                    gross_salary = record.gross_salary
                    net_salary = record.net_pay
                    tds_deducted = record.income_tax_deducted
                    epf_employee = record.pf_employee

                    # Determine transaction date from pay_period or pay_date
                    if record.pay_date:
                        txn_date = record.pay_date
                    else:
                        # Parse pay_period like "June 2024" to last day of month
                        import pandas as pd
                        try:
                            period_date = pd.to_datetime(pay_period, format='%B %Y')
                            # Last day of the month
                            txn_date = (period_date + pd.offsets.MonthEnd(0)).date()
                        except:
                            txn_date = date.today()

                    # Record to double-entry ledger
                    ledger_result = record_salary(
                        txn_service=txn_service,
                        conn=self.conn,
                        user_id=self.user_id,
                        employer=employer,
                        pay_period=pay_period,
                        gross_salary=gross_salary,
                        net_salary=net_salary,
                        tds_deducted=tds_deducted,
                        epf_employee=epf_employee,
                        txn_date=txn_date,
                        source_file=source_file,
                        row_idx=row_idx,
                        source=TransactionSource.PARSER_HDFC,
                    )

                    if ledger_result.is_duplicate:
                        logger.debug(f"Duplicate salary record skipped: {pay_period}")
                        continue

                    inserted += 1

                elif hasattr(record, 'assessment_year'):
                    # It's a Form16Record - these are annual summaries
                    # Form16 typically doesn't create journal entries as it's
                    # a summary of already recorded monthly salaries
                    inserted += 1

                else:
                    # Unknown record type
                    logger.warning(f"Unknown salary record type: {type(record)}")

            except Exception as e:
                logger.warning(f"Failed to save salary record: {e}")
                logger.debug(f"Record data: {record}")

        return inserted


def ingest_salary_documents(
    conn: sqlite3.Connection,
    user_id: int,
    inbox_path: Path,
    force: bool = False
) -> GenericIngestionResult:
    ingester = SalaryIngester(conn, user_id, inbox_path)
    return ingester.ingest(force)
