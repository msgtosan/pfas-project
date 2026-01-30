# Golden Reference Reconciliation Engine

## Overview

The Golden Reference Reconciliation Engine provides a robust framework for comparing PFAS system data against authoritative external sources like NSDL/CDSL Consolidated Account Statements (CAS). It implements a "Truth Resolver" pattern to determine which source should be considered authoritative for different metrics and asset classes.

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────────┐
│                    Golden Reference Engine                       │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐        │
│  │   NSDL CAS   │   │  Truth       │   │   Cross      │        │
│  │   Parser     │──▶│  Resolver    │──▶│  Correlator  │        │
│  └──────────────┘   └──────────────┘   └──────────────┘        │
│         │                 │                    │                │
│         ▼                 ▼                    ▼                │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐        │
│  │   Golden     │   │   Source     │   │   Recon      │        │
│  │   Ingester   │   │   Priority   │   │   Events     │        │
│  └──────────────┘   └──────────────┘   └──────────────┘        │
│         │                                      │                │
│         ▼                                      ▼                │
│  ┌──────────────────────────────────────────────────┐          │
│  │              SQLite Database                      │          │
│  │  ┌─────────────┐ ┌─────────────┐ ┌────────────┐  │          │
│  │  │golden_ref   │ │golden_hold  │ │recon_events│  │          │
│  │  └─────────────┘ └─────────────┘ └────────────┘  │          │
│  └──────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

### Truth Hierarchy

The Truth Resolver determines which data source is authoritative based on metric type:

| Metric Type | Asset Class | Primary Source | Fallback Sources |
|-------------|-------------|----------------|------------------|
| **NET_WORTH** | Mutual Funds | NSDL_CAS | CDSL_CAS → RTA_CAS → SYSTEM |
| **NET_WORTH** | Stocks | NSDL_CAS | CDSL_CAS → BROKER → SYSTEM |
| **NET_WORTH** | NPS | NSDL_CAS | NPS_STATEMENT → SYSTEM |
| **CAPITAL_GAINS** | Mutual Funds | RTA_CAS | NSDL_CAS → SYSTEM |
| **CAPITAL_GAINS** | Stocks | BROKER | NSDL_CAS → SYSTEM |
| **UNITS** | Mutual Funds | RTA_CAS | NSDL_CAS → SYSTEM |
| **COST_BASIS** | All | SYSTEM | External sources for validation |

### Reconciliation Process

1. **Ingest Golden Reference**: Parse NSDL/CDSL CAS PDF into normalized holdings
2. **Load System Holdings**: Query database for current system state
3. **Match by Key**: Use ISIN/FolioNumber/Symbol as matching keys
4. **Compare Values**: Apply tolerance (default 0.01) for minor differences
5. **Generate Events**: Create reconciliation events for each comparison
6. **Handle Suspense**: Park unresolved mismatches in suspense account

## Quick Start

### 1. Ingest NSDL CAS

```bash
# Ingest with password from config
python -m pfas.cli.golden_cli -i -u Sanjay -f golden/nsdl/NSDLe-CAS_100980467_DEC_2025.PDF

# Ingest with explicit password
python -m pfas.cli.golden_cli -i -u Sanjay -f cas.pdf --password AAPPS0793R

# Ingest and auto-reconcile
python -m pfas.cli.golden_cli -i -u Sanjay -f cas.pdf --auto-reconcile
```

### 2. Run Reconciliation

```bash
# Reconcile all asset classes
python -m pfas.cli.golden_cli -r -u Sanjay

# Reconcile specific asset class
python -m pfas.cli.golden_cli -r -u Sanjay -a MUTUAL_FUND
```

### 3. Check Status

```bash
# View golden reference status
python -m pfas.cli.golden_cli -s -u Sanjay

# View open suspense items
python -m pfas.cli.golden_cli --suspense -u Sanjay
```

## Programmatic Usage

### Parse NSDL CAS

```python
from pfas.services.golden_reference import NSDLCASParser, GoldenReferenceIngester

# Parse CAS PDF
parser = NSDLCASParser()
cas_data = parser.parse("/path/to/nsdl_cas.pdf", password="secret")

print(f"Statement date: {cas_data.statement_date}")
print(f"Holdings: {len(cas_data.all_holdings)}")
print(f"Total value: ₹{cas_data.total_value:,.2f}")

# Ingest into database
ingester = GoldenReferenceIngester(conn, user_id=1)
ref_id = ingester.ingest_nsdl_cas(
    cas_data,
    file_path="/path/to/nsdl_cas.pdf",
    file_hash=parser.calculate_file_hash("/path/to/nsdl_cas.pdf")
)
```

### Run Reconciliation

```python
from pfas.services.golden_reference import CrossCorrelator, AssetClass

correlator = CrossCorrelator(conn, user_id=1)

# Reconcile MF holdings
summary = correlator.reconcile_holdings(
    asset_class=AssetClass.MUTUAL_FUND,
    golden_ref_id=ref_id
)

print(f"Match rate: {summary.match_rate:.1f}%")
print(f"Mismatches: {summary.mismatches}")
print(f"Total difference: ₹{summary.total_difference:,.2f}")
```

### Configure Truth Sources

```python
from pfas.services.golden_reference import TruthResolver, MetricType, AssetClass, SourceType

resolver = TruthResolver(conn, user_id=1)

# Check current source
source = resolver.get_truth_source(MetricType.NET_WORTH, AssetClass.MUTUAL_FUND)
print(f"Authoritative source: {source.value}")

# Set user override
resolver.set_user_override(
    MetricType.NET_WORTH,
    AssetClass.MUTUAL_FUND,
    [SourceType.RTA_CAS, SourceType.NSDL_CAS, SourceType.SYSTEM],
    "User prefers RTA for MF valuation"
)
```

## Database Schema

### Core Tables

```sql
-- Golden Reference (external statements)
CREATE TABLE golden_reference (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    source_type TEXT NOT NULL,      -- NSDL_CAS, CDSL_CAS, RTA_CAS, etc.
    statement_date DATE NOT NULL,
    file_path TEXT,
    file_hash TEXT,
    status TEXT DEFAULT 'ACTIVE'    -- ACTIVE, SUPERSEDED, INVALID
);

-- Golden Holdings (parsed from golden sources)
CREATE TABLE golden_holdings (
    id INTEGER PRIMARY KEY,
    golden_ref_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    asset_type TEXT NOT NULL,       -- MUTUAL_FUND, STOCKS, NPS, etc.
    isin TEXT,                      -- Primary matching key
    folio_number TEXT,              -- Secondary key for MF
    units DECIMAL(18, 6),
    market_value DECIMAL(18, 2),
    currency TEXT DEFAULT 'INR',
    exchange_rate DECIMAL(10, 6)    -- For USD holdings
);

-- Reconciliation Events
CREATE TABLE reconciliation_events (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    reconciliation_date DATE,
    metric_type TEXT,
    asset_class TEXT,
    isin TEXT,
    system_value DECIMAL(18, 6),
    golden_value DECIMAL(18, 6),
    difference DECIMAL(18, 6),
    tolerance_used DECIMAL(10, 6),
    status TEXT DEFAULT 'PENDING',  -- MATCHED, MISMATCH, RESOLVED
    match_result TEXT               -- EXACT, WITHIN_TOLERANCE, MISMATCH
);

-- Suspense Account (unresolved items)
CREATE TABLE reconciliation_suspense (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    suspense_value DECIMAL(18, 2),
    suspense_reason TEXT,
    status TEXT DEFAULT 'OPEN'      -- OPEN, RESOLVED, WRITTEN_OFF
);
```

## Configuration

### Reconciliation Mode

The reconciliation engine supports three execution modes:

1. **MANUAL** (default): User triggers reconciliation explicitly
2. **SCHEDULED**: System runs reconciliation on a schedule (daily/weekly/monthly)
3. **ON_INGEST**: Auto-run reconciliation after ingesting golden reference

Configure in `Data/Users/{user}/config/reconciliation.json`:

```json
{
  "mode": "MANUAL",
  "frequency": "MONTHLY",
  "auto_reconcile_on_ingest": false,
  "tolerances": {
    "absolute": "0.01",
    "percentage": "0.1"
  },
  "severity_thresholds": {
    "warning": "100",
    "error": "1000",
    "critical": "10000"
  },
  "notifications": {
    "on_mismatch": true,
    "on_critical": true,
    "email": false
  },
  "asset_classes": ["MUTUAL_FUND", "STOCKS", "NPS", "SGB"],
  "create_suspense_on_mismatch": true,
  "auto_resolve_within_tolerance": true
}
```

View current settings:
```bash
python -m pfas.cli.golden_cli -c -u Sanjay
```

### Password Configuration

Store passwords in `Data/Users/{user}/config/passwords.json`:

```json
{
  "database": {
    "password": "db_encryption_key"
  },
  "golden": {
    "nsdl": "NSDL_CAS_PASSWORD",
    "cdsl": "CDSL_CAS_PASSWORD"
  }
}
```

### Truth Source Override

Store user-specific truth source overrides in `Data/Users/{user}/config/truth_sources.json`:

```json
{
  "overrides": [
    {
      "metric_type": "NET_WORTH",
      "asset_class": "MUTUAL_FUND",
      "source_priority": ["RTA_CAS", "NSDL_CAS", "SYSTEM"],
      "description": "Prefer RTA for MF valuation"
    }
  ]
}
```

### Reconciliation Tolerance

Configure tolerance in reconciliation config:

```python
from pfas.services.golden_reference import ReconciliationConfig

config = ReconciliationConfig(
    absolute_tolerance=Decimal("0.01"),      # 0.01 unit tolerance
    percentage_tolerance=Decimal("0.001"),   # 0.1% percentage tolerance
    warning_threshold=Decimal("100"),        # ₹100 for WARNING
    error_threshold=Decimal("1000"),         # ₹1000 for ERROR
    critical_threshold=Decimal("10000"),     # ₹10000 for CRITICAL
)

correlator = CrossCorrelator(conn, user_id=1, config=config)
```

## Directory Structure

```
Data/Users/{user}/
├── golden/
│   ├── nsdl/                 # NSDL CAS statements
│   │   ├── NSDLe-CAS_*.PDF
│   │   └── ...
│   ├── cdsl/                 # CDSL CAS statements
│   └── rta/                  # RTA CAS statements (CAMS, KFintech)
├── config/
│   ├── passwords.json        # File passwords
│   └── truth_sources.json    # Source priority overrides
└── db/
    └── finance.db            # SQLite database
```

## Multi-Currency Support (US Stocks)

For US stocks and other foreign holdings, the engine handles currency conversion:

```python
# GoldenHolding with USD values
holding = GoldenHolding(
    asset_type=AssetClass.US_STOCKS,
    isin="US0378331005",
    symbol="AAPL",
    name="Apple Inc",
    units=Decimal("10"),
    market_value=Decimal("1750"),    # USD
    currency="USD",
    exchange_rate=Decimal("83.25")   # USD/INR rate
)

# value_inr is auto-calculated: 1750 * 83.25 = ₹145,687.50
print(f"INR Value: ₹{holding.value_inr:,.2f}")
```

## Workflow Diagram

```
┌───────────────────────────────────────────────────────────────────┐
│                       RECONCILIATION WORKFLOW                      │
└───────────────────────────────────────────────────────────────────┘

[1] INGEST GOLDEN SOURCE
    ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
    │   PDF File   │─────▶│   Parser     │─────▶│   Database   │
    │ (NSDL CAS)   │      │ (Extract)    │      │ (Store)      │
    └──────────────┘      └──────────────┘      └──────────────┘

[2] DETERMINE TRUTH SOURCE
    ┌──────────────┐      ┌──────────────┐
    │   Metric +   │─────▶│   Source     │
    │   Asset      │      │   Priority   │
    └──────────────┘      └──────────────┘

[3] LOAD & MATCH
    ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
    │   Golden     │      │              │      │   System     │
    │   Holdings   │─────▶│   MATCH BY   │◀─────│   Holdings   │
    │              │      │ ISIN/FOLIO   │      │              │
    └──────────────┘      └──────────────┘      └──────────────┘

[4] COMPARE & CLASSIFY
    ┌──────────────┐
    │  EXACT       │──▶ Status: MATCHED
    │  MATCH       │
    └──────────────┘

    ┌──────────────┐
    │  WITHIN      │──▶ Status: MATCHED (auto-resolved)
    │  TOLERANCE   │
    └──────────────┘

    ┌──────────────┐      ┌──────────────┐
    │  MISMATCH    │─────▶│   SUSPENSE   │
    │              │      │   ACCOUNT    │
    └──────────────┘      └──────────────┘

    ┌──────────────┐
    │  MISSING     │──▶ Flag for investigation
    └──────────────┘

[5] GENERATE REPORT
    ┌──────────────────────────────────────────────────────────┐
    │  Summary: 95% match rate, 3 mismatches, ₹15,000 suspense │
    └──────────────────────────────────────────────────────────┘
```

## Migration

Run the migration to create required tables:

```bash
python migrations/002_golden_reference.py --db-path Data/Users/Sanjay/db/finance.db
```

## Reports

### Exporting Reconciliation Reports

Generate reports in various formats:

```bash
# Excel report (default)
python -m pfas.cli.golden_cli -e -u Sanjay --format excel

# CSV export
python -m pfas.cli.golden_cli -e -u Sanjay --format csv -a MUTUAL_FUND

# Text summary
python -m pfas.cli.golden_cli -e -u Sanjay --format text

# Custom output directory
python -m pfas.cli.golden_cli -e -u Sanjay -o /path/to/output
```

### Report Types

1. **Excel Report** (default)
   - Summary sheet with overview metrics
   - Details sheet with all reconciliation events
   - Suspense sheet with open items
   - Color-coded match status

2. **CSV Export**
   - Raw event data for further analysis
   - Compatible with Excel, Google Sheets, etc.

3. **Text Summary**
   - Console output with key metrics
   - Quick overview of reconciliation status

## Best Practices

1. **Regular Reconciliation**: Run reconciliation monthly after receiving CAS
2. **Resolve Suspense Promptly**: Investigate and resolve suspense items within 30 days
3. **Archive Old Statements**: Mark superseded golden references to maintain history
4. **Configure Tolerances**: Adjust tolerance based on typical rounding differences
5. **User Overrides**: Use per-user truth source overrides sparingly

## Troubleshooting

### Password Issues
- Check `passwords.json` has correct password for NSDL files
- Use `--password` flag for explicit password
- Ensure password is case-sensitive

### Missing Holdings
- Verify system has all transactions ingested
- Check date ranges match between system and CAS
- Validate ISIN/folio matching

### Large Differences
- Review individual holding details
- Check for corporate actions (splits, bonuses)
- Verify NAV dates match
