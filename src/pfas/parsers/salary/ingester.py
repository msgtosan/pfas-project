"""Salary Documents Ingester (Form16, Payslips)."""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

try:
    import sqlcipher3 as sqlite3
except ImportError:
    import sqlite3

from pfas.services.generic_ingester import GenericAssetIngester, GenericIngestionResult
from .form16 import Form16Parser
from .payslip import PayslipParser

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

    def save_to_db(self, records: List[Any]) -> int:
        # Form16/Payslip parsers typically handle DB insertion themselves
        # Return count of records
        return len(records)


def ingest_salary_documents(
    conn: sqlite3.Connection,
    user_id: int,
    inbox_path: Path,
    force: bool = False
) -> GenericIngestionResult:
    ingester = SalaryIngester(conn, user_id, inbox_path)
    return ingester.ingest(force)
