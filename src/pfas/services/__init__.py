"""Services module for PFAS business logic.

Provides services for:
- Tax Rules: Data-driven tax rate management
- Income Aggregation: Pre-computed income summaries
- Statement Tracking: Prevents re-parsing of processed files
- Advance Tax Calculator: Complete tax computation
"""

from .tax_rules_service import TaxRulesService, TaxSlab, CapitalGainsRate
from .income_aggregation_service import IncomeAggregationService, IncomeRecord
from .statement_tracker import StatementTracker
from .advance_tax_calculator import AdvanceTaxCalculator, AdvanceTaxResult

__all__ = [
    "TaxRulesService",
    "TaxSlab",
    "CapitalGainsRate",
    "IncomeAggregationService",
    "IncomeRecord",
    "StatementTracker",
    "AdvanceTaxCalculator",
    "AdvanceTaxResult",
]
