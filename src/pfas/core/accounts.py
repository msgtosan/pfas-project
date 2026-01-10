"""
Chart of Accounts management for PFAS.

Provides hierarchical account structure for 18 asset classes.
Supports Indian tax reporting requirements.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import sqlite3

from pfas.core.exceptions import AccountNotFoundError


# Chart of Accounts - 18 Asset Classes
CHART_OF_ACCOUNTS: Dict[str, Dict[str, Any]] = {
    # Assets (1xxx)
    "1000": {"name": "Assets", "type": "ASSET", "parent": None},
    "1100": {"name": "Current Assets", "type": "ASSET", "parent": "1000"},
    "1101": {"name": "Bank - Savings", "type": "ASSET", "parent": "1100"},
    "1102": {"name": "Bank - Current", "type": "ASSET", "parent": "1100"},
    "1103": {"name": "Bank - FD", "type": "ASSET", "parent": "1100"},
    "1104": {"name": "Cash in Hand", "type": "ASSET", "parent": "1100"},

    "1200": {"name": "Investments", "type": "ASSET", "parent": "1000"},
    "1201": {"name": "Mutual Funds - Equity", "type": "ASSET", "parent": "1200"},
    "1202": {"name": "Mutual Funds - Debt", "type": "ASSET", "parent": "1200"},
    "1203": {"name": "Indian Stocks", "type": "ASSET", "parent": "1200"},
    "1204": {"name": "SGB", "type": "ASSET", "parent": "1200"},
    "1205": {"name": "RBI Bonds", "type": "ASSET", "parent": "1200"},
    "1206": {"name": "REIT/InvIT", "type": "ASSET", "parent": "1200"},

    "1300": {"name": "Retirement Funds", "type": "ASSET", "parent": "1000"},
    "1301": {"name": "EPF - Employee", "type": "ASSET", "parent": "1300"},
    "1302": {"name": "EPF - Employer", "type": "ASSET", "parent": "1300"},
    "1303": {"name": "PPF", "type": "ASSET", "parent": "1300"},
    "1304": {"name": "NPS - Tier I", "type": "ASSET", "parent": "1300"},
    "1305": {"name": "NPS - Tier II", "type": "ASSET", "parent": "1300"},

    "1400": {"name": "Foreign Assets", "type": "ASSET", "parent": "1000", "currency": "USD"},
    "1401": {"name": "US Stocks - RSU", "type": "ASSET", "parent": "1400", "currency": "USD"},
    "1402": {"name": "US Stocks - ESPP", "type": "ASSET", "parent": "1400", "currency": "USD"},
    "1403": {"name": "US Stocks - DRIP", "type": "ASSET", "parent": "1400", "currency": "USD"},
    "1404": {"name": "US Brokerage Cash", "type": "ASSET", "parent": "1400", "currency": "USD"},

    "1500": {"name": "Other Assets", "type": "ASSET", "parent": "1000"},
    "1501": {"name": "Unlisted Shares", "type": "ASSET", "parent": "1500"},
    "1502": {"name": "Real Estate", "type": "ASSET", "parent": "1500"},

    "1600": {"name": "Tax Assets", "type": "ASSET", "parent": "1000"},
    "1601": {"name": "TDS Receivable", "type": "ASSET", "parent": "1600"},
    "1602": {"name": "TCS Receivable", "type": "ASSET", "parent": "1600"},
    "1603": {"name": "Advance Tax Paid", "type": "ASSET", "parent": "1600"},
    "1604": {"name": "Foreign Tax Credit", "type": "ASSET", "parent": "1600"},

    # Liabilities (2xxx)
    "2000": {"name": "Liabilities", "type": "LIABILITY", "parent": None},
    "2100": {"name": "Tax Payable", "type": "LIABILITY", "parent": "2000"},
    "2101": {"name": "Income Tax Payable", "type": "LIABILITY", "parent": "2100"},
    "2102": {"name": "Professional Tax Payable", "type": "LIABILITY", "parent": "2100"},

    # Equity (3xxx)
    "3000": {"name": "Equity", "type": "EQUITY", "parent": None},
    "3100": {"name": "Opening Balance", "type": "EQUITY", "parent": "3000"},
    "3200": {"name": "Retained Earnings", "type": "EQUITY", "parent": "3000"},

    # Income (4xxx)
    "4000": {"name": "Income", "type": "INCOME", "parent": None},
    "4100": {"name": "Salary Income", "type": "INCOME", "parent": "4000"},
    "4101": {"name": "Basic Salary", "type": "INCOME", "parent": "4100"},
    "4102": {"name": "HRA", "type": "INCOME", "parent": "4100"},
    "4103": {"name": "Special Allowance", "type": "INCOME", "parent": "4100"},
    "4104": {"name": "RSU Perquisite", "type": "INCOME", "parent": "4100"},
    "4105": {"name": "ESPP Perquisite", "type": "INCOME", "parent": "4100"},
    "4106": {"name": "Other Perquisites", "type": "INCOME", "parent": "4100"},

    "4200": {"name": "Investment Income", "type": "INCOME", "parent": "4000"},
    "4201": {"name": "Bank Interest", "type": "INCOME", "parent": "4200"},
    "4202": {"name": "FD Interest", "type": "INCOME", "parent": "4200"},
    "4203": {"name": "Dividend - Indian", "type": "INCOME", "parent": "4200"},
    "4204": {"name": "Dividend - Foreign", "type": "INCOME", "parent": "4200"},
    "4205": {"name": "EPF Interest", "type": "INCOME", "parent": "4200"},
    "4206": {"name": "PPF Interest", "type": "INCOME", "parent": "4200"},
    "4207": {"name": "SGB Interest", "type": "INCOME", "parent": "4200"},

    "4300": {"name": "Capital Gains", "type": "INCOME", "parent": "4000"},
    "4301": {"name": "STCG - Equity 20%", "type": "INCOME", "parent": "4300"},
    "4302": {"name": "LTCG - Equity 12.5%", "type": "INCOME", "parent": "4300"},
    "4303": {"name": "CG - Debt (Slab)", "type": "INCOME", "parent": "4300"},
    "4304": {"name": "STCG - Foreign", "type": "INCOME", "parent": "4300"},
    "4305": {"name": "LTCG - Foreign 12.5%", "type": "INCOME", "parent": "4300"},

    "4400": {"name": "Rental Income", "type": "INCOME", "parent": "4000"},
    "4401": {"name": "Gross Rental Income", "type": "INCOME", "parent": "4400"},

    # Expenses (5xxx)
    "5000": {"name": "Expenses", "type": "EXPENSE", "parent": None},
    "5100": {"name": "Tax Deductions", "type": "EXPENSE", "parent": "5000"},
    "5101": {"name": "Section 80C", "type": "EXPENSE", "parent": "5100"},
    "5102": {"name": "Section 80D", "type": "EXPENSE", "parent": "5100"},
    "5103": {"name": "Section 80CCD(1B)", "type": "EXPENSE", "parent": "5100"},
    "5104": {"name": "Section 80TTA/80TTB", "type": "EXPENSE", "parent": "5100"},
    "5105": {"name": "Section 24 - HP Interest", "type": "EXPENSE", "parent": "5100"},
    "5106": {"name": "Standard Deduction 16(ia)", "type": "EXPENSE", "parent": "5100"},

    "5200": {"name": "Investment Expenses", "type": "EXPENSE", "parent": "5000"},
    "5201": {"name": "STT Paid", "type": "EXPENSE", "parent": "5200"},
    "5202": {"name": "Brokerage", "type": "EXPENSE", "parent": "5200"},
    "5203": {"name": "Rental Standard Deduction", "type": "EXPENSE", "parent": "5200"},
    "5204": {"name": "Municipal Tax", "type": "EXPENSE", "parent": "5200"},
}


@dataclass
class Account:
    """Represents an account in the Chart of Accounts."""

    id: int
    code: str
    name: str
    account_type: str
    parent_id: Optional[int]
    currency: str
    description: Optional[str]
    is_active: bool

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Account":
        """Create Account from database row."""
        return cls(
            id=row["id"],
            code=row["code"],
            name=row["name"],
            account_type=row["account_type"],
            parent_id=row["parent_id"],
            currency=row["currency"],
            description=row["description"],
            is_active=bool(row["is_active"]),
        )


def setup_chart_of_accounts(conn: sqlite3.Connection, user_id: Optional[int] = None) -> int:
    """
    Populate the chart of accounts from CHART_OF_ACCOUNTS.

    Args:
        conn: Database connection
        user_id: User ID to assign to created accounts (for multi-user support)

    Returns:
        Number of accounts created
    """
    cursor = conn.cursor()

    # First pass: Create accounts without parent references
    # (to handle forward references)
    code_to_id: Dict[str, int] = {}

    for code, details in CHART_OF_ACCOUNTS.items():
        cursor.execute(
            """
            INSERT OR IGNORE INTO accounts (code, name, account_type, currency, user_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                code,
                details["name"],
                details["type"],
                details.get("currency", "INR"),
                user_id,
            ),
        )

        # Get the account ID
        cursor.execute("SELECT id FROM accounts WHERE code = ?", (code,))
        row = cursor.fetchone()
        if row:
            code_to_id[code] = row[0]

    # Second pass: Update parent references
    for code, details in CHART_OF_ACCOUNTS.items():
        parent_code = details.get("parent")
        if parent_code and parent_code in code_to_id:
            cursor.execute(
                "UPDATE accounts SET parent_id = ? WHERE code = ?",
                (code_to_id[parent_code], code),
            )

    conn.commit()
    return len(code_to_id)


def get_account_by_code(conn: sqlite3.Connection, code: str) -> Optional[Account]:
    """
    Get an account by its code.

    Args:
        conn: Database connection
        code: Account code (e.g., "1101")

    Returns:
        Account object or None if not found
    """
    cursor = conn.execute(
        "SELECT * FROM accounts WHERE code = ?", (code,)
    )
    row = cursor.fetchone()
    if row:
        return Account.from_row(row)
    return None


def get_account_by_id(conn: sqlite3.Connection, account_id: int) -> Optional[Account]:
    """
    Get an account by its ID.

    Args:
        conn: Database connection
        account_id: Account ID

    Returns:
        Account object or None if not found
    """
    cursor = conn.execute(
        "SELECT * FROM accounts WHERE id = ?", (account_id,)
    )
    row = cursor.fetchone()
    if row:
        return Account.from_row(row)
    return None


def get_accounts_by_type(
    conn: sqlite3.Connection, account_type: str
) -> List[Account]:
    """
    Get all accounts of a given type.

    Args:
        conn: Database connection
        account_type: One of ASSET, LIABILITY, INCOME, EXPENSE, EQUITY

    Returns:
        List of Account objects
    """
    cursor = conn.execute(
        "SELECT * FROM accounts WHERE account_type = ? AND is_active = 1",
        (account_type,),
    )
    return [Account.from_row(row) for row in cursor.fetchall()]


def get_child_accounts(
    conn: sqlite3.Connection, parent_code: str
) -> List[Account]:
    """
    Get all child accounts of a parent account.

    Args:
        conn: Database connection
        parent_code: Parent account code

    Returns:
        List of child Account objects
    """
    parent = get_account_by_code(conn, parent_code)
    if not parent:
        raise AccountNotFoundError(parent_code)

    cursor = conn.execute(
        "SELECT * FROM accounts WHERE parent_id = ? AND is_active = 1",
        (parent.id,),
    )
    return [Account.from_row(row) for row in cursor.fetchall()]


def get_account_hierarchy(
    conn: sqlite3.Connection, root_code: str = None
) -> Dict[str, Any]:
    """
    Get account hierarchy as a nested dictionary.

    Args:
        conn: Database connection
        root_code: Optional root account code (defaults to all roots)

    Returns:
        Nested dictionary of accounts
    """
    def build_tree(parent_id: Optional[int]) -> List[Dict[str, Any]]:
        if parent_id is None:
            cursor = conn.execute(
                "SELECT * FROM accounts WHERE parent_id IS NULL AND is_active = 1"
            )
        else:
            cursor = conn.execute(
                "SELECT * FROM accounts WHERE parent_id = ? AND is_active = 1",
                (parent_id,),
            )

        result = []
        for row in cursor.fetchall():
            account = Account.from_row(row)
            children = build_tree(account.id)
            node = {
                "code": account.code,
                "name": account.name,
                "type": account.account_type,
                "currency": account.currency,
            }
            if children:
                node["children"] = children
            result.append(node)
        return result

    if root_code:
        root = get_account_by_code(conn, root_code)
        if not root:
            raise AccountNotFoundError(root_code)
        children = build_tree(root.id)
        return {
            "code": root.code,
            "name": root.name,
            "type": root.account_type,
            "currency": root.currency,
            "children": children,
        }

    return {"roots": build_tree(None)}
