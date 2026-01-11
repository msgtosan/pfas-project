"""ITR (Income Tax Return) services module.

Provides generators for:
- Schedule FA (Foreign Assets)
- ITR-2 JSON export
"""

from .schedule_fa import ScheduleFAGenerator, ScheduleFAData
from .itr2_exporter import ITR2Exporter

__all__ = [
    "ScheduleFAGenerator",
    "ScheduleFAData",
    "ITR2Exporter",
]
