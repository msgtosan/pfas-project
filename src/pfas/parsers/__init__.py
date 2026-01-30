"""
PFAS Parsers - Bank and investment statement parsers.

Supports parsing of:
- Bank statements (ICICI, SBI, HDFC)
- Mutual fund statements (CAMS, Karvy)
- Stock broker statements (Zerodha, ICICI Direct)
- Retirement accounts (EPF, NPS, PPF)
- Salary slips and Form 16

Architecture:
- BaseParser: Abstract base class for all parsers
- ParserRegistry: Plugin registration and discovery
- StagingPipeline: Raw → Normalized → Final table flow
- ColumnMappingConfig: JSON-based column mapping
- LedgerIntegration: Double-entry ledger recording for all parsers
"""

from .base import (
    BaseParser,
    ParsedRecord,
    NormalizationResult,
    ParserRegistry,
    StrictOpenXMLConverter,
    ColumnMappingConfig,
    StagingPipeline,
)

# Ledger integration exports
from .ledger_integration import (
    AccountCode,
    LedgerRecordResult,
    record_mf_purchase,
    record_mf_redemption,
    record_mf_switch,
    record_mf_dividend,
    record_bank_credit,
    record_bank_debit,
    record_stock_buy,
    record_stock_sell,
    record_salary,
    record_epf_contribution,
    record_epf_interest,
    record_ppf_deposit,
    record_ppf_interest,
    record_ppf_withdrawal,
    # Deep accounting functions
    validate_salary_components,
    record_salary_multi_leg,
    record_employer_pf_contribution,
    record_mf_purchase_with_cost_basis,
    record_mf_redemption_with_cost_basis,
    record_stock_buy_with_cost_basis,
    record_stock_sell_with_cost_basis,
    record_rsu_vest,
    record_rsu_sale,
    record_espp_purchase,
    record_foreign_dividend,
    get_sbi_tt_rate,
)

__version__ = "0.2.0"

__all__ = [
    # Base infrastructure
    "BaseParser",
    "ParsedRecord",
    "NormalizationResult",
    "ParserRegistry",
    "StrictOpenXMLConverter",
    "ColumnMappingConfig",
    "StagingPipeline",
    # Ledger integration
    "AccountCode",
    "LedgerRecordResult",
    "record_mf_purchase",
    "record_mf_redemption",
    "record_mf_switch",
    "record_mf_dividend",
    "record_bank_credit",
    "record_bank_debit",
    "record_stock_buy",
    "record_stock_sell",
    "record_salary",
    "record_epf_contribution",
    "record_epf_interest",
    "record_ppf_deposit",
    "record_ppf_interest",
    "record_ppf_withdrawal",
    # Deep accounting
    "validate_salary_components",
    "record_salary_multi_leg",
    "record_employer_pf_contribution",
    "record_mf_purchase_with_cost_basis",
    "record_mf_redemption_with_cost_basis",
    "record_stock_buy_with_cost_basis",
    "record_stock_sell_with_cost_basis",
    "record_rsu_vest",
    "record_rsu_sale",
    "record_espp_purchase",
    "record_foreign_dividend",
    "get_sbi_tt_rate",
]
