"""
Bank statement parsers for PFAS.

Supports multiple bank formats:
- ICICI Bank (PDF, Excel)
- SBI (Excel)
- HDFC Bank (PDF)
"""

from pfas.parsers.bank.models import (
    BankTransaction,
    BankAccount,
    ParseResult,
    TransactionCategory,
)
from pfas.parsers.bank.base import BankStatementParser
from pfas.parsers.bank.icici import ICICIParser
from pfas.parsers.bank.icici_excel import ICICIExcelParser
from pfas.parsers.bank.sbi import SBIParser
from pfas.parsers.bank.hdfc import HDFCParser
from pfas.parsers.bank.interest import InterestCalculator
from pfas.parsers.bank.utils import consolidate_transactions

__all__ = [
    "BankTransaction",
    "BankAccount",
    "ParseResult",
    "TransactionCategory",
    "BankStatementParser",
    "ICICIParser",
    "ICICIExcelParser",
    "SBIParser",
    "HDFCParser",
    "InterestCalculator",
    "consolidate_transactions",
]
