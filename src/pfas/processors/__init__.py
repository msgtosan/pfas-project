"""
PFAS Statement Processors

Unified processors for handling multiple file types from various financial institutions.
"""

from pfas.processors.icici_direct_processor import (
    ICICIDirectProcessor,
    ProcessingResult,
    process_icici_direct,
)

__all__ = [
    "ICICIDirectProcessor",
    "ProcessingResult",
    "process_icici_direct",
]
