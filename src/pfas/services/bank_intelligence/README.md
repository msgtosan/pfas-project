# Bank Intelligence Suite

**Data-Driven Bank Statement Processing for PFAS**

Version 6.3 | January 2026

---

## Overview

The Bank Intelligence Suite is a **fully data-driven** solution for processing bank statements. It requires **no code changes** when:
- Adding new bank statement files
- Adding new users
- Adding new banks
- Modifying category classification rules
- Changing column mappings or date formats

All configuration is done through JSON files in the data directory.

---

## Quick Start

### 1. Run Ingestion (Process All Bank Statements)

```bash
# From project root
python -m src.pfas.services.bank_intelligence.run ingest

# Or specify custom paths
python -m src.pfas.services.bank_intelligence.run ingest \
    --data-root Data/Users \
    --db-path Data/Reports/Bank_Intelligence/money_movement.db
```

### 2. Generate Excel Report

```bash
python -m src.pfas.services.bank_intelligence.run report

# Filter by fiscal year or user
python -m src.pfas.services.bank_intelligence.run report \
    --fiscal-year "FY 2024-25" \
    --user Sanjay
```

### 3. Audit Database

```bash
python -m src.pfas.services.bank_intelligence.run audit

# Show specific options
python -m src.pfas.services.bank_intelligence.run audit --stats
python -m src.pfas.services.bank_intelligence.run audit --income --fy "FY 2024-25"
```

---

## Directory Structure

```
Data/
├── Users/
│   ├── <UserName>/                    # One folder per user
│   │   └── Bank/
│   │       ├── <BankName>/            # One folder per bank
│   │       │   ├── user_bank_config.json   # Bank-specific config
│   │       │   ├── Statement1.xls          # Bank statements
│   │       │   ├── Statement2.xlsx
│   │       │   └── Statement3.csv
│   │       └── <AnotherBank>/
│   │           └── ...
│   └── <AnotherUser>/
│       └── Bank/
│           └── ...
│
└── Reports/
    └── Bank_Intelligence/
        ├── money_movement.db          # SQLite database
        └── Master_Report.xlsx         # Excel report
```

---

## Configuration

### User Bank Config (`user_bank_config.json`)

Each bank folder must have a `user_bank_config.json` file. If not present, one will be created with defaults.

**Location:** `Data/Users/<UserName>/Bank/<BankName>/user_bank_config.json`

```json
{
    "user_name": "Sanjay",
    "bank_name": "ICICI",
    "account_type": "SAVINGS",
    "statement_format": "XLS",

    "header_search_keywords": [
        "Value Date",
        "Transaction Date",
        "Transaction Remarks",
        "Withdrawal Amount",
        "Deposit Amount",
        "Balance"
    ],

    "date_column_names": [
        "Value Date",
        "Transaction Date",
        "Date",
        "Txn Date"
    ],

    "amount_column_patterns": {
        "debit": ["Withdrawal Amount(INR)", "Withdrawal", "DR", "Debit"],
        "credit": ["Deposit Amount(INR)", "Deposit", "CR", "Credit"]
    },

    "skip_rows_top": 0,
    "date_format": "%d/%m/%Y",

    "category_overrides": {
        "QUALCOMM": "SALARY",
        "RENT FROM": "RENT_INCOME",
        "RAMAKRISHNAN": "RENT_INCOME",
        "DIV-": "DIVIDEND",
        "INT.PD": "SAVINGS_INTEREST",
        "NATIONAL SECURITIES": "SGB_INTEREST",
        "ZERODHA": "STOCK_INVESTMENT",
        "ZOMATO": "FOOD_ORDER",
        "RATNADEEP": "GROCERY"
    }
}
```

### Configuration Fields

| Field | Description | Required |
|-------|-------------|----------|
| `user_name` | User identifier | Yes |
| `bank_name` | Bank identifier | Yes |
| `account_type` | SAVINGS, CURRENT, etc. | No (default: SAVINGS) |
| `statement_format` | XLS, XLSX, CSV | No (default: XLS) |
| `header_search_keywords` | Keywords to find header row | No (uses defaults) |
| `date_column_names` | Possible date column names | No (uses defaults) |
| `amount_column_patterns` | Debit/credit column patterns | No (uses defaults) |
| `skip_rows_top` | Rows to skip at top | No (default: 0) |
| `date_format` | Date parsing format | No (default: %d/%m/%Y) |
| `category_overrides` | Custom keyword → category mapping | No |

---

## Adding New Data (No Code Changes Required)

### Adding New Statement Files

Simply drop new `.xls`, `.xlsx`, or `.csv` files into the bank folder:

```
Data/Users/Sanjay/Bank/ICICI/
├── user_bank_config.json
├── SanjaySB_FY24-25.xls        # Existing
├── SanjaySB_FY25-26_Q1.xls     # NEW - Just add the file!
└── SanjaySB_FY25-26_Q2.xls     # NEW - Just add the file!
```

Then run:
```bash
python -m src.pfas.services.bank_intelligence.run ingest
```

**Deduplication:** Existing transactions are automatically skipped based on unique ID (SHA256 hash of user + bank + date + description + amount).

### Adding New Users

Create a new folder structure:

```bash
mkdir -p Data/Users/NewUser/Bank/HDFC
```

Create `user_bank_config.json`:
```json
{
    "user_name": "NewUser",
    "bank_name": "HDFC",
    "account_type": "SAVINGS",
    "date_format": "%d/%m/%y",
    "category_overrides": {
        "SALARY": "SALARY",
        "RENT": "RENT_INCOME"
    }
}
```

Add statement files and run ingestion.

### Adding New Banks

Create a new bank folder under the user:

```bash
mkdir -p Data/Users/Sanjay/Bank/SBI
```

Create bank-specific config and add statements.

### Modifying Category Rules

Edit the `category_overrides` in `user_bank_config.json`:

```json
{
    "category_overrides": {
        "ACME CORP": "SALARY",           # Add new salary keyword
        "PROPERTY RENT": "RENT_INCOME",  # Add new rent keyword
        "NSDL": "DIVIDEND",              # Add dividend keyword
        "PPF": "PPF_DEPOSIT"             # Add investment category
    }
}
```

No code changes needed - just re-run ingestion.

---

## Category System

### Built-in Income Categories (PFAS Asset Mapping)

| Category | Keywords (Default) | PFAS Asset Class | PFAS Table |
|----------|-------------------|------------------|------------|
| `SALARY` | SALARY, PAYROLL, SAL CREDIT | Salary | salary_records |
| `RENT_INCOME` | RENT FROM, RENTAL | Rental Income | rental_income |
| `DIVIDEND` | DIV-, DIVIDEND | Stock Dividends | stock_dividends |
| `SAVINGS_INTEREST` | INT PD, INTEREST CREDIT | Bank Interest | bank_interest_summary |
| `SGB_INTEREST` | SGB INT, SOVEREIGN GOLD | SGB Holdings | sgb_interest |
| `FD_INTEREST` | FD INT, FIXED DEPOSIT INT | Fixed Deposits | bank_interest_summary |
| `MF_REDEMPTION` | MUTUAL FUND, AMC- | Mutual Funds | mf_transactions |

### Built-in Transfer Categories

| Category | Keywords (Default) |
|----------|-------------------|
| `UPI` | UPI/, UPI- |
| `NEFT` | NEFT |
| `RTGS` | RTGS |
| `IMPS` | IMPS |
| `ATM` | ATM, ATM WDL |
| `CARD` | CARD, DEBIT CARD, POS |
| `CHEQUE` | CHQ, CHEQUE |

### Custom Categories

Add any custom category in `category_overrides`:

```json
{
    "category_overrides": {
        "NETFLIX": "ENTERTAINMENT",
        "GYM FEES": "FITNESS",
        "SCHOOL FEES": "EDUCATION",
        "PETROL": "TRANSPORT"
    }
}
```

---

## Output Reports

### Excel Master Report

Generated sheets:
1. **Detailed_Ledger** - All transactions with UID, user, base_string
2. **FY_Summary** - Pivot by fiscal year and category
3. **Category_Analysis** - Category-wise breakdown with totals
4. **Income_Summary** - Income for PFAS asset classes

**Features:**
- Auto-filters on all columns
- Dynamic SUBTOTAL formulas (update when filtered)
- Color-coded amounts (green=credit, red=debit)
- Frozen header row

### SQLite Database

**Tables:**
- `bank_transactions_intel` - All transactions with full metadata
- `categories` - Category definitions
- `ingestion_log` - Processing history

**Query Examples:**
```sql
-- Get income by fiscal year
SELECT fiscal_year, category, SUM(CAST(amount AS REAL)) as total
FROM bank_transactions_intel
WHERE txn_type = 'CREDIT'
  AND category IN ('SALARY', 'RENT_INCOME', 'DIVIDEND', 'SAVINGS_INTEREST')
GROUP BY fiscal_year, category;

-- Get monthly spending by category
SELECT strftime('%Y-%m', txn_date) as month, category,
       SUM(ABS(CAST(amount AS REAL))) as total
FROM bank_transactions_intel
WHERE txn_type = 'DEBIT'
GROUP BY month, category
ORDER BY month DESC;
```

---

## Fiscal Year Logic

Indian fiscal year (April 1 - March 31):

| Transaction Date | Fiscal Year |
|-----------------|-------------|
| 2024-03-15 | FY 2023-24 |
| 2024-04-01 | FY 2024-25 |
| 2024-12-25 | FY 2024-25 |
| 2025-01-15 | FY 2024-25 |
| 2025-04-01 | FY 2025-26 |

---

## Troubleshooting

### Header Not Detected

If the analyzer can't find headers, update `header_search_keywords`:

```json
{
    "header_search_keywords": [
        "S No.",
        "Value Date",
        "Transaction Remarks",
        "Withdrawal Amount",
        "Deposit Amount"
    ]
}
```

### Date Parsing Errors

Update `date_format` to match your statement:

```json
{
    "date_format": "%d-%m-%Y"
}
```

Common formats:
- `%d/%m/%Y` - 31/12/2024
- `%d-%m-%Y` - 31-12-2024
- `%Y-%m-%d` - 2024-12-31
- `%d/%m/%y` - 31/12/24
- `%d %b %Y` - 31 Dec 2024

### Column Not Found

Update `amount_column_patterns`:

```json
{
    "amount_column_patterns": {
        "debit": ["Withdrawal Amt", "Dr Amount", "Debit"],
        "credit": ["Deposit Amt", "Cr Amount", "Credit"]
    }
}
```

### Verify Data Integrity

```bash
python -m src.pfas.services.bank_intelligence.run audit --validate
```

---

## API Usage

```python
from src.pfas.services.bank_intelligence import (
    BankIntelligenceAnalyzer,
    FiscalReportGenerator,
    DatabaseAuditor,
    UserBankConfig,
)

# Ingest all statements
with BankIntelligenceAnalyzer("money_movement.db", "Data/Users") as analyzer:
    result = analyzer.scan_and_ingest_all()
    print(f"Processed: {result.transactions_processed}")
    print(f"Inserted: {result.transactions_inserted}")

# Generate report
with FiscalReportGenerator("money_movement.db") as generator:
    generator.generate_master_report("Master_Report.xlsx")

    # Get income for PFAS
    income = generator.get_income_for_pfas("FY 2024-25", "Sanjay")
    print(income)

# Audit database
with DatabaseAuditor("money_movement.db") as auditor:
    auditor.audit_recent_records(10)
    auditor.print_statistics()
    auditor.print_income_summary("FY 2024-25")
```

---

## File Formats Supported

| Format | Extension | Notes |
|--------|-----------|-------|
| Excel (Old) | .xls | xlrd library |
| Excel (New) | .xlsx | openpyxl library |
| CSV | .csv | pandas library |

---

## Performance

| Metric | Typical Value |
|--------|---------------|
| Ingestion Speed | ~1000 txn/second |
| Memory Usage | ~50 MB for 10K transactions |
| Database Size | ~0.5 KB per transaction |

---

## License

Proprietary - Personal Financial Accounting System for Indian Tax Residents
