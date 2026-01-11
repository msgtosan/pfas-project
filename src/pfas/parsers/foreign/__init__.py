"""Foreign asset parsers module.

Provides parsers for:
- Morgan Stanley/E*TRADE statements
- RSU vest and sale records
- ESPP purchase records
- Form 1042-S (US withholding)
"""

from .morgan_stanley import MorganStanleyParser
from .models import (
    StockPlanDetails,
    CashFlowActivity,
    RSUVest,
    RSUSale,
    ESPPPurchase,
    ESPPSale,
    ForeignDividend,
    ForeignParseResult,
)

__all__ = [
    "MorganStanleyParser",
    "StockPlanDetails",
    "CashFlowActivity",
    "RSUVest",
    "RSUSale",
    "ESPPPurchase",
    "ESPPSale",
    "ForeignDividend",
    "ForeignParseResult",
]
