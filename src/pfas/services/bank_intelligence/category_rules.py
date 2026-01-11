"""
Category Classification Rules for Bank Transactions.

Provides rule-based classification of bank transactions into
income categories and expense categories for PFAS asset extraction.
"""

from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass


@dataclass
class CategoryMapping:
    """Mapping of category to PFAS asset class and table."""
    category: str
    keywords: List[str]
    is_income: bool
    asset_class: Optional[str] = None
    pfas_table: Optional[str] = None
    sub_category: Optional[str] = None
    priority: int = 0


# Income categories with PFAS asset mapping
INCOME_CATEGORIES: List[CategoryMapping] = [
    CategoryMapping(
        category="RENT_INCOME",
        keywords=["RENT FROM", "RENTAL INCOME", "HOUSE RENT", "RENT RECEIVED",
                  "RENTAL", "PROPERTY RENT", "TENANT"],
        is_income=True,
        asset_class="RENTAL",
        pfas_table="rental_income",
        priority=100
    ),
    CategoryMapping(
        category="SGB_INTEREST",
        keywords=["SGB INT", "SOV GOLD BOND", "SOVEREIGN GOLD", "SGB INTEREST",
                  "GOLD BOND INT", "SGBINT"],
        is_income=True,
        asset_class="SGB",
        pfas_table="sgb_interest",
        priority=100
    ),
    CategoryMapping(
        category="DIVIDEND",
        keywords=["DIV-", "DIVIDEND", "DIV CREDIT", "DIV/", "DIVDEND",
                  "INTERIM DIV", "FINAL DIV", "DIVIDEND WARR"],
        is_income=True,
        asset_class="STOCK_DIVIDEND",
        pfas_table="stock_dividends",
        priority=90
    ),
    CategoryMapping(
        category="SAVINGS_INTEREST",
        keywords=["INT PD", "INTEREST CREDIT", "INT.PD", "INT CREDIT",
                  "INTEREST PAID", "INT PAID", "INT.CR", "INT.CREDIT",
                  "INTEREST ON", "INT ON SAV", "QUARTERLY INT"],
        is_income=True,
        asset_class="BANK_INTEREST",
        pfas_table="bank_interest_summary",
        priority=80
    ),
    CategoryMapping(
        category="FD_INTEREST",
        keywords=["FD INT", "FDR INT", "FIXED DEPOSIT INT", "TDR INT",
                  "TERM DEPOSIT INT", "FD INTEREST", "FDR INTEREST"],
        is_income=True,
        asset_class="FD_INTEREST",
        pfas_table="bank_interest_summary",
        sub_category="FD",
        priority=85
    ),
    CategoryMapping(
        category="SALARY",
        keywords=["SALARY", "SAL CREDIT", "PAYROLL", "SAL-", "SALARY CREDIT",
                  "NEFT-SALARY", "RTGS-SALARY", "QUALCOMM", "MICROSOFT",
                  "GOOGLE", "AMAZON", "INFOSYS", "TCS", "WIPRO", "HCL"],
        is_income=True,
        asset_class="SALARY",
        pfas_table="salary_records",
        priority=95
    ),
    CategoryMapping(
        category="MF_REDEMPTION",
        keywords=["MF-SIP", "MUTUAL FUND", "MF REDEMPTION", "MFSS",
                  "AMC-", "FOLIO", "ICICI PRU", "HDFC AMC", "SBI MF",
                  "ADITYA BIRLA", "UTI MF", "AXIS MF", "KOTAK MF"],
        is_income=True,
        asset_class="MF_REDEMPTION",
        pfas_table="mf_transactions",
        priority=70
    ),
    CategoryMapping(
        category="REFUND",
        keywords=["REFUND", "CASHBACK", "REVERSAL", "CREDIT REVERSAL"],
        is_income=True,
        priority=30
    ),
]

# Transfer and payment categories
TRANSFER_CATEGORIES: List[CategoryMapping] = [
    CategoryMapping(
        category="UPI",
        keywords=["UPI/", "UPI-", "UPI:"],
        is_income=False,
        priority=50
    ),
    CategoryMapping(
        category="NEFT",
        keywords=["NEFT", "NEFT/", "NEFT-"],
        is_income=False,
        priority=50
    ),
    CategoryMapping(
        category="RTGS",
        keywords=["RTGS", "RTGS/", "RTGS-"],
        is_income=False,
        priority=50
    ),
    CategoryMapping(
        category="IMPS",
        keywords=["IMPS", "IMPS/", "IMPS-"],
        is_income=False,
        priority=50
    ),
    CategoryMapping(
        category="ATM",
        keywords=["ATM", "ATM WDL", "ATM WITHDRAWAL", "CASH WDL"],
        is_income=False,
        priority=50
    ),
    CategoryMapping(
        category="CARD",
        keywords=["CARD", "DEBIT CARD", "POS", "POS/", "VISA", "MASTERCARD",
                  "RUPAY", "ECOM"],
        is_income=False,
        priority=40
    ),
    CategoryMapping(
        category="CHEQUE",
        keywords=["CHQ", "CHEQUE", "CHECK", "CHQ DEP", "CHQ CLG"],
        is_income=False,
        priority=40
    ),
    CategoryMapping(
        category="CASH_DEPOSIT",
        keywords=["CASH DEP", "CDM", "CASH DEPOSIT", "BY CASH"],
        is_income=False,
        priority=40
    ),
    CategoryMapping(
        category="CASH_WITHDRAWAL",
        keywords=["CASH WD", "CWD", "CASH WITHDRAWAL", "TO CASH"],
        is_income=False,
        priority=40
    ),
    CategoryMapping(
        category="BILL_PAYMENT",
        keywords=["BILL PAY", "BILLPAY", "ELECTRICITY", "WATER BILL",
                  "GAS BILL", "BSNL", "AIRTEL", "JIO", "VODAFONE",
                  "INSURANCE PREM", "LIC", "ICICI PRUD"],
        is_income=False,
        priority=30
    ),
    CategoryMapping(
        category="TRANSFER",
        keywords=["TRANSFER", "TRF", "FT-", "FUND TRANSFER", "SELF TRF"],
        is_income=False,
        priority=20
    ),
]


class CategoryClassifier:
    """
    Classifies bank transactions into categories.

    Uses keyword matching with priority ordering to classify
    transactions. Higher priority categories are checked first.
    """

    def __init__(self, custom_overrides: Optional[Dict[str, str]] = None):
        """
        Initialize classifier with optional custom overrides.

        Args:
            custom_overrides: Dictionary of keyword -> category overrides
        """
        self.custom_overrides = custom_overrides or {}
        self._build_rules()

    def _build_rules(self) -> None:
        """Build sorted rules list from all categories."""
        all_rules = INCOME_CATEGORIES + TRANSFER_CATEGORIES
        # Sort by priority (descending)
        self.rules = sorted(all_rules, key=lambda x: x.priority, reverse=True)

    def classify(self, description: str) -> Tuple[Optional[str], Optional[str], bool]:
        """
        Classify a transaction description.

        Args:
            description: Transaction description/narration

        Returns:
            Tuple of (category, sub_category, is_income)
        """
        description_upper = description.upper()

        # Check custom overrides first
        for keyword, category in self.custom_overrides.items():
            if keyword.upper() in description_upper:
                # Find matching rule for category metadata
                for rule in self.rules:
                    if rule.category == category:
                        return (category, rule.sub_category, rule.is_income)
                return (category, None, False)

        # Check rules by priority
        for rule in self.rules:
            for keyword in rule.keywords:
                if keyword.upper() in description_upper:
                    return (rule.category, rule.sub_category, rule.is_income)

        return ("OTHER", None, False)

    def get_asset_mapping(self, category: str) -> Optional[Tuple[str, str]]:
        """
        Get PFAS asset class and table for a category.

        Args:
            category: Category name

        Returns:
            Tuple of (asset_class, pfas_table) or None
        """
        for rule in self.rules:
            if rule.category == category:
                if rule.asset_class and rule.pfas_table:
                    return (rule.asset_class, rule.pfas_table)
        return None

    def is_income_category(self, category: str) -> bool:
        """Check if category is an income category."""
        for rule in self.rules:
            if rule.category == category:
                return rule.is_income
        return False

    def get_income_categories(self) -> List[str]:
        """Get list of all income category names."""
        return [rule.category for rule in self.rules if rule.is_income]

    def get_pfas_mappable_categories(self) -> List[CategoryMapping]:
        """Get categories that can be mapped to PFAS tables."""
        return [rule for rule in self.rules if rule.pfas_table is not None]
