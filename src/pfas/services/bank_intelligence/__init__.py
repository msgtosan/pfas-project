"""
Bank Intelligence Service for PFAS.

Data-Driven Bank Statement Processing - No code changes required for:
- Adding new statement files (just drop into folder)
- Adding new users/banks (create folder + config.json)
- Modifying categories (edit user_bank_config.json)

Usage:
    python -m src.pfas.services.bank_intelligence.run ingest
    python -m src.pfas.services.bank_intelligence.run report
    python -m src.pfas.services.bank_intelligence.run audit
"""

from .intelligent_analyzer import BankIntelligenceAnalyzer
from .report_generation import FiscalReportGenerator
from .db_audit import DatabaseAuditor
from .models import BankTransactionIntel, UserBankConfig
from .category_rules import CategoryClassifier

__all__ = [
    "BankIntelligenceAnalyzer",
    "FiscalReportGenerator",
    "DatabaseAuditor",
    "BankTransactionIntel",
    "UserBankConfig",
    "CategoryClassifier",
]
