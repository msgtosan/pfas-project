"""Foreign asset processing services.

Provides processing for:
- RSU vest and sale calculations
- ESPP purchase and sale calculations
- Foreign dividend processing
- DTAA credit calculation
"""

from .rsu_processor import RSUProcessor
from .espp_processor import ESPPProcessor
from .dtaa_calculator import DTAACalculator

__all__ = [
    "RSUProcessor",
    "ESPPProcessor",
    "DTAACalculator",
]
