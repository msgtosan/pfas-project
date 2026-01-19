# MF Module Integration Roadmap

This document outlines how the Mutual Fund module integrates with other PFAS components:
- Balance Sheet (Asset Side)
- Cash Flow Statement (Investing Activities)
- Advance Tax Calculator
- Net Worth Tracker

## 1. Balance Sheet Integration

### Asset Classification

MF holdings are classified under **Investments** in the Balance Sheet:

```
ASSETS
├── Current Assets
│   └── Cash & Bank Balances
├── Investments (← MF Holdings)
│   ├── Mutual Funds - Equity
│   ├── Mutual Funds - Debt
│   └── Mutual Funds - Hybrid
├── Fixed Assets
└── Other Assets
```

### Account Mapping

```python
# Chart of Accounts mapping
MF_ACCOUNTS = {
    "EQUITY": "ASSET.INV.MF.EQUITY",      # Code: 1310
    "DEBT": "ASSET.INV.MF.DEBT",          # Code: 1320
    "HYBRID": "ASSET.INV.MF.HYBRID",      # Code: 1330
}
```

### Data Flow: Holdings → Balance Sheet

```python
from pfas.services.balance_sheet_service import BalanceSheetService
from decimal import Decimal

class MFBalanceSheetIntegration:
    """Integrates MF holdings with Balance Sheet."""

    def sync_to_balance_sheet(self, user_id: int, as_of_date: date):
        """Sync MF holdings to Balance Sheet accounts."""

        # Get holdings by category
        holdings = self._get_holdings_by_category(user_id, as_of_date)

        # Update Balance Sheet accounts
        for category, value in holdings.items():
            account_code = MF_ACCOUNTS[category]

            # Create journal entry for valuation adjustment
            self.journal.create_entry(
                date=as_of_date,
                description=f"MF {category} valuation as of {as_of_date}",
                entries=[
                    {"account": account_code, "debit": value, "credit": Decimal("0")},
                    {"account": "EQUITY.REVAL", "debit": Decimal("0"), "credit": value}
                ]
            )

    def _get_holdings_by_category(self, user_id: int, as_of_date: date) -> dict:
        """Get holdings aggregated by scheme type."""
        query = """
            SELECT scheme_type, SUM(CAST(current_value AS DECIMAL)) as total
            FROM mf_holdings
            WHERE user_id = ? AND nav_date <= ?
            GROUP BY scheme_type
        """
        # Returns: {"EQUITY": 500000, "DEBT": 200000, "HYBRID": 100000}
```

### Balance Sheet Report Query

```sql
-- Get MF holdings for Balance Sheet
SELECT
    CASE scheme_type
        WHEN 'EQUITY' THEN 'Mutual Funds - Equity'
        WHEN 'DEBT' THEN 'Mutual Funds - Debt'
        WHEN 'HYBRID' THEN 'Mutual Funds - Hybrid'
        ELSE 'Mutual Funds - Other'
    END as account_name,
    SUM(CAST(current_value AS DECIMAL)) as balance,
    SUM(CAST(cost_value AS DECIMAL)) as cost_basis,
    SUM(CAST(appreciation AS DECIMAL)) as unrealized_gain
FROM mf_holdings
WHERE user_id = :user_id AND nav_date = :as_of_date
GROUP BY scheme_type
ORDER BY scheme_type;
```

---

## 2. Cash Flow Statement Integration

### Cash Flow Classification

MF transactions are classified under **Investing Activities**:

```
CASH FLOW STATEMENT
├── Operating Activities
│   └── Salary, Business Income, Expenses
├── Investing Activities (← MF Transactions)
│   ├── Purchase of Mutual Funds (Outflow)
│   ├── Sale/Redemption of Mutual Funds (Inflow)
│   ├── Dividends Received (Inflow)
│   └── Switch In/Out (Net)
└── Financing Activities
    └── Loans, EMIs
```

### Transaction Mapping

```python
MF_CASHFLOW_MAPPING = {
    "PURCHASE": "INVESTING_OUTFLOW",
    "REDEMPTION": "INVESTING_INFLOW",
    "SWITCH_IN": "INVESTING_INTERNAL",  # No net cash flow
    "SWITCH_OUT": "INVESTING_INTERNAL",
    "DIVIDEND": "INVESTING_INFLOW",
    "DIVIDEND_REINVEST": "INVESTING_INTERNAL",  # No net cash flow
}
```

### Data Flow: Transactions → Cash Flow

```python
from pfas.services.cash_flow_service import CashFlowService

class MFCashFlowIntegration:
    """Integrates MF transactions with Cash Flow Statement."""

    def get_investing_cash_flow(
        self,
        user_id: int,
        start_date: date,
        end_date: date
    ) -> dict:
        """Get MF-related investing cash flows for a period."""

        query = """
            SELECT
                transaction_type,
                SUM(CAST(amount AS DECIMAL)) as total_amount
            FROM mf_transactions t
            JOIN mf_folios f ON t.folio_id = f.id
            WHERE f.user_id = :user_id
              AND t.date BETWEEN :start_date AND :end_date
              AND t.transaction_type IN ('PURCHASE', 'REDEMPTION', 'DIVIDEND')
            GROUP BY transaction_type
        """

        # Process results
        cash_flows = {
            "mf_purchases": Decimal("0"),     # Cash outflow
            "mf_redemptions": Decimal("0"),   # Cash inflow
            "mf_dividends": Decimal("0"),     # Cash inflow
        }

        for row in results:
            if row.transaction_type == "PURCHASE":
                cash_flows["mf_purchases"] = row.total_amount
            elif row.transaction_type == "REDEMPTION":
                cash_flows["mf_redemptions"] = row.total_amount
            elif row.transaction_type == "DIVIDEND":
                cash_flows["mf_dividends"] = row.total_amount

        # Net investing cash flow from MF
        cash_flows["net_mf_cash_flow"] = (
            cash_flows["mf_redemptions"] +
            cash_flows["mf_dividends"] -
            cash_flows["mf_purchases"]
        )

        return cash_flows
```

### Cash Flow Report Query

```sql
-- MF Cash Flow for a period
WITH mf_flows AS (
    SELECT
        CASE
            WHEN transaction_type = 'PURCHASE' THEN 'Purchase of Mutual Funds'
            WHEN transaction_type = 'REDEMPTION' THEN 'Proceeds from MF Redemption'
            WHEN transaction_type = 'DIVIDEND' THEN 'Dividend Received'
            ELSE 'Other MF Activity'
        END as description,
        CASE
            WHEN transaction_type = 'PURCHASE' THEN -amount
            ELSE amount
        END as cash_flow
    FROM mf_transactions t
    JOIN mf_folios f ON t.folio_id = f.id
    WHERE f.user_id = :user_id
      AND t.date BETWEEN :start_date AND :end_date
      AND transaction_type IN ('PURCHASE', 'REDEMPTION', 'DIVIDEND')
)
SELECT description, SUM(cash_flow) as amount
FROM mf_flows
GROUP BY description
ORDER BY description;
```

---

## 3. Advance Tax Integration

### Taxable Income Components from MF

| Component | Tax Treatment | Tax Rate (FY 2024-25) |
|-----------|--------------|----------------------|
| Equity STCG | Special Rate | 20% |
| Equity LTCG | Special Rate | 12.5% (above Rs.1.25L exemption) |
| Debt STCG | Slab Rate | As per income slab |
| Debt LTCG | Slab Rate | As per income slab |
| Dividends | Slab Rate | As per income slab |

### Advance Tax Calculation Flow

```python
from pfas.services.advance_tax_calculator import AdvanceTaxCalculator

class MFAdvanceTaxIntegration:
    """Integrates MF capital gains with Advance Tax calculation."""

    def get_tax_liability(self, user_id: int, financial_year: str) -> dict:
        """Get MF-related tax liability for advance tax."""

        # Get capital gains
        cg = self._get_capital_gains(user_id, financial_year)

        # Calculate tax
        tax = {
            "equity_stcg_tax": cg["equity_stcg"] * Decimal("0.20"),
            "equity_ltcg_tax": max(
                Decimal("0"),
                (cg["equity_ltcg"] - Decimal("125000")) * Decimal("0.125")
            ),
            "debt_gains_tax": Decimal("0"),  # Added to slab income
            "dividend_tax": Decimal("0"),    # Added to slab income
        }

        # Debt gains and dividends go to slab calculation
        tax["slab_income_from_mf"] = cg["debt_stcg"] + cg["debt_ltcg"] + cg["dividends"]

        return tax

    def _get_capital_gains(self, user_id: int, financial_year: str) -> dict:
        """Get realized capital gains for FY."""
        query = """
            SELECT
                asset_class,
                SUM(stcg_amount) as stcg,
                SUM(ltcg_amount) as ltcg
            FROM mf_capital_gains
            WHERE user_id = ? AND financial_year = ?
            GROUP BY asset_class
        """
        # Returns categorized gains
```

### Quarterly Advance Tax Deadlines

```python
ADVANCE_TAX_SCHEDULE = {
    "Q1": {"due_date": "06-15", "cumulative_pct": 15},  # By June 15
    "Q2": {"due_date": "09-15", "cumulative_pct": 45},  # By Sept 15
    "Q3": {"due_date": "12-15", "cumulative_pct": 75},  # By Dec 15
    "Q4": {"due_date": "03-15", "cumulative_pct": 100}, # By Mar 15
}

def calculate_quarterly_advance_tax(
    total_tax_liability: Decimal,
    quarter: str,
    tax_already_paid: Decimal
) -> Decimal:
    """Calculate advance tax due for a quarter."""
    schedule = ADVANCE_TAX_SCHEDULE[quarter]
    cumulative_due = total_tax_liability * Decimal(schedule["cumulative_pct"]) / 100
    return max(Decimal("0"), cumulative_due - tax_already_paid)
```

### Tax Report Integration

```sql
-- Capital Gains Summary for ITR
SELECT
    financial_year,
    asset_class,
    stcg_amount,
    ltcg_amount,
    ltcg_exemption,
    taxable_stcg,
    taxable_ltcg,
    CASE asset_class
        WHEN 'EQUITY' THEN stcg_amount * 0.20
        ELSE 0
    END as stcg_tax,
    CASE asset_class
        WHEN 'EQUITY' THEN MAX(0, (ltcg_amount - ltcg_exemption)) * 0.125
        ELSE 0
    END as ltcg_tax
FROM mf_capital_gains
WHERE user_id = :user_id AND financial_year = :fy
ORDER BY asset_class;
```

---

## 4. Net Worth Tracker Integration

### Net Worth Components

```
NET WORTH = ASSETS - LIABILITIES

ASSETS
├── Bank Balances
├── Mutual Funds (← from mf_holdings)
│   ├── Equity MF
│   ├── Debt MF
│   └── Hybrid MF
├── Stocks
├── Fixed Deposits
├── EPF/PPF/NPS
├── Real Estate
└── Other Assets

LIABILITIES
├── Home Loan
├── Car Loan
├── Credit Card
└── Other Liabilities
```

### Net Worth Calculation

```python
class NetWorthTracker:
    """Tracks overall net worth including MF holdings."""

    def get_net_worth(self, user_id: int, as_of_date: date) -> dict:
        """Calculate net worth as of a date."""

        # Get MF holdings
        mf_value = self._get_mf_value(user_id, as_of_date)

        # Get other assets (stocks, bank, FD, etc.)
        other_assets = self._get_other_assets(user_id, as_of_date)

        # Get liabilities
        liabilities = self._get_liabilities(user_id, as_of_date)

        total_assets = mf_value["total"] + sum(other_assets.values())
        total_liabilities = sum(liabilities.values())

        return {
            "as_of_date": as_of_date,
            "assets": {
                "mutual_funds": mf_value,
                **other_assets
            },
            "total_assets": total_assets,
            "liabilities": liabilities,
            "total_liabilities": total_liabilities,
            "net_worth": total_assets - total_liabilities,
            "mf_percentage": (mf_value["total"] / total_assets * 100) if total_assets else 0
        }

    def _get_mf_value(self, user_id: int, as_of_date: date) -> dict:
        """Get MF holdings value."""
        query = """
            SELECT
                scheme_type,
                SUM(CAST(current_value AS DECIMAL)) as value,
                SUM(CAST(cost_value AS DECIMAL)) as cost
            FROM mf_holdings
            WHERE user_id = ? AND nav_date <= ?
            GROUP BY scheme_type
        """
        # Process and return
```

### Net Worth Trend Query

```sql
-- Net Worth trend from MF snapshots
SELECT
    snapshot_date,
    total_value as mf_value,
    equity_value,
    debt_value,
    hybrid_value,
    total_appreciation as unrealized_gain,
    weighted_xirr as portfolio_xirr
FROM mf_holdings_snapshot
WHERE user_id = :user_id
ORDER BY snapshot_date DESC
LIMIT 24;  -- Last 24 months
```

---

## 5. Implementation Phases

### Phase 1: Core Integration (Current)
- [x] MF Holdings → mf_holdings table
- [x] MF Transactions → mf_transactions table
- [x] Capital Gains → mf_capital_gains table
- [x] FY Summaries → mf_fy_summary table
- [x] Holdings Snapshots → mf_holdings_snapshot table

### Phase 2: Balance Sheet Integration
- [ ] Create journal entries for MF valuations
- [ ] Map scheme types to account codes
- [ ] Generate Balance Sheet with MF breakdown
- [ ] Handle unrealized gains/losses

### Phase 3: Cash Flow Integration
- [ ] Classify MF transactions as investing activities
- [ ] Generate Cash Flow Statement with MF section
- [ ] Handle dividend reinvestment (no cash flow)
- [ ] Track net investment/divestment

### Phase 4: Tax Integration
- [ ] Feed capital gains to Advance Tax Calculator
- [ ] Generate Schedule 112A data (Equity LTCG)
- [ ] Generate Schedule CG data (Other CG)
- [ ] Calculate quarterly advance tax from MF

### Phase 5: Net Worth Dashboard
- [ ] Aggregate MF with other assets
- [ ] Track net worth over time
- [ ] Asset allocation analysis
- [ ] Goal-based tracking

---

## 6. API Reference

### Balance Sheet API

```python
# Get MF contribution to Balance Sheet
from pfas.services.balance_sheet_service import get_mf_balance

mf_balance = get_mf_balance(user_id=1, as_of_date=date(2024, 3, 31))
# Returns: {"equity": 500000, "debt": 200000, "hybrid": 100000, "total": 800000}
```

### Cash Flow API

```python
# Get MF cash flows
from pfas.services.cash_flow_service import get_mf_cash_flows

flows = get_mf_cash_flows(user_id=1, fy="2024-25")
# Returns: {"purchases": -200000, "redemptions": 50000, "dividends": 5000, "net": -145000}
```

### Advance Tax API

```python
# Get MF tax liability
from pfas.services.advance_tax_calculator import get_mf_tax

tax = get_mf_tax(user_id=1, fy="2024-25")
# Returns: {"equity_stcg_tax": 2000, "equity_ltcg_tax": 6250, "slab_income": 15000}
```

### Net Worth API

```python
# Get net worth with MF breakdown
from pfas.services.net_worth_service import get_net_worth

nw = get_net_worth(user_id=1, as_of_date=date(2024, 3, 31))
# Returns: {"net_worth": 5000000, "mf_value": 800000, "mf_pct": 16.0}
```

---

## 7. Configuration

### Integration Config (config/integration_config.json)

```json
{
  "balance_sheet": {
    "mf_account_mapping": {
      "EQUITY": "ASSET.INV.MF.EQUITY",
      "DEBT": "ASSET.INV.MF.DEBT",
      "HYBRID": "ASSET.INV.MF.HYBRID"
    },
    "revaluation_account": "EQUITY.RETAINED.UNREALIZED"
  },
  "cash_flow": {
    "mf_activity_type": "INVESTING",
    "include_switch_as_cashflow": false
  },
  "advance_tax": {
    "equity_stcg_rate": 0.20,
    "equity_ltcg_rate": 0.125,
    "equity_ltcg_exemption": 125000,
    "debt_to_slab": true
  },
  "net_worth": {
    "include_unrealized_gains": true,
    "snapshot_frequency": "MONTHLY"
  }
}
```

---

## 8. Data Freshness

| Integration | Data Source | Update Frequency |
|-------------|-------------|------------------|
| Balance Sheet | mf_holdings | On statement import |
| Cash Flow | mf_transactions | On statement import |
| Advance Tax | mf_capital_gains | On CG statement import |
| Net Worth | mf_holdings_snapshot | Manual or FY_END |

---

## 9. Error Handling

### Common Integration Errors

1. **Missing Holdings Data**
   - Run MF Analyzer before integration
   - Check inbox for statements

2. **Account Mapping Failure**
   - Verify accounts exist in chart of accounts
   - Run accounts setup script

3. **Tax Calculation Mismatch**
   - Reconcile capital gains first
   - Verify asset class classification

4. **Net Worth Inconsistency**
   - Ensure all asset classes are mapped
   - Check for duplicate holdings

---

This roadmap provides a clear path for integrating the MF module with all major PFAS financial reporting and analysis components.
