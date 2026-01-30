"""Services module for PFAS business logic.

Provides services for:
- Tax Rules: Data-driven tax rate management
- Income Aggregation: Pre-computed income summaries
- Statement Tracking: Prevents re-parsing of processed files
- Advance Tax Calculator: Complete tax computation
- Cash Flow Statement: Cash flow statement generation
- Balance Sheet: Balance sheet snapshot generation
- Portfolio Valuation: Holdings valuation and XIRR
- Liabilities: Loan and liability management
- NAV Service: Mutual fund NAV history with interpolation
- Batch Ingester: Atomic batch file ingestion
"""

from .tax_rules_service import TaxRulesService, TaxSlab, CapitalGainsRate
from .income_aggregation_service import IncomeAggregationService, IncomeRecord
from .statement_tracker import StatementTracker
from .advance_tax_calculator import AdvanceTaxCalculator, AdvanceTaxResult
from .cash_flow_service import CashFlowStatementService
from .balance_sheet_service import BalanceSheetService
from .portfolio_valuation_service import PortfolioValuationService, PortfolioSummary, XIRRResult
from .liabilities_service import LiabilitiesService, LoanSummary, AmortizationEntry
from .nav_service import NAVService, NAVRecord
from .batch_ingester import BatchIngester, BatchResult, FileResult, FileStatus
from .cost_basis_tracker import CostBasisTracker, CostMethod, Lot, CostBasisResult, HoldingSummary

__all__ = [
    # Tax Services
    "TaxRulesService",
    "TaxSlab",
    "CapitalGainsRate",
    "IncomeAggregationService",
    "IncomeRecord",
    "StatementTracker",
    "AdvanceTaxCalculator",
    "AdvanceTaxResult",
    # Financial Statement Services
    "CashFlowStatementService",
    "BalanceSheetService",
    # Portfolio Services
    "PortfolioValuationService",
    "PortfolioSummary",
    "XIRRResult",
    # Liability Services
    "LiabilitiesService",
    "LoanSummary",
    "AmortizationEntry",
    # NAV Services
    "NAVService",
    "NAVRecord",
    # Batch Ingestion
    "BatchIngester",
    "BatchResult",
    "FileResult",
    "FileStatus",
    # Cost Basis Tracking
    "CostBasisTracker",
    "CostMethod",
    "Lot",
    "CostBasisResult",
    "HoldingSummary",
]
