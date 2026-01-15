"""PFAS Command Line Interface tools.

Available CLI tools:
- reports_cli: Generate financial statements (Balance Sheet, Cash Flow, Income Statement)
- advance_tax_cli: Calculate advance tax liability
- mf_analyzer_cli: Mutual Fund statement analysis and reporting
"""

from .reports_cli import main as reports_main
from .mf_analyzer_cli import main as mf_analyzer_main

__all__ = ["reports_main", "mf_analyzer_main"]
