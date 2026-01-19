# PFAS User Preferences Configuration

## Overview

Each user has a `config/preferences.json` file that controls their personal settings for report generation, file formats, and display preferences.

## File Location

```
Data/Users/<username>/config/preferences.json
```

## Schema

```json
{
  "$schema": "user_preferences_v1",
  "version": "1.0",

  "reports": {
    "default_format": "xlsx",
    "formats_by_type": {
      "balance_sheet": ["xlsx", "pdf"],
      "cash_flow": ["xlsx", "pdf"],
      "income_statement": ["xlsx", "pdf"],
      "capital_gains": ["xlsx", "json"],
      "portfolio": ["xlsx"],
      "tax_computation": ["xlsx", "json", "pdf"]
    },
    "naming": {
      "include_timestamp": true,
      "include_fy": true,
      "pattern": "{report_type}_{fy}_{date}"
    },
    "auto_open": false
  },

  "financial_year": {
    "default": "2024-25",
    "start_month": 4
  },

  "display": {
    "currency_symbol": "₹",
    "decimal_places": 2,
    "date_format": "DD-MMM-YYYY",
    "negative_in_brackets": true
  },

  "parsers": {
    "auto_archive": true,
    "duplicate_handling": "skip",
    "default_sources": {
      "Mutual-Fund": "CAMS",
      "Indian-Stocks": "Zerodha",
      "Bank": "ICICI"
    }
  },

  "notifications": {
    "on_parse_complete": true,
    "on_error": true,
    "summary_after_ingest": true
  }
}
```

## Field Descriptions

### reports

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `default_format` | string | `"xlsx"` | Default output format for all reports |
| `formats_by_type` | object | - | Override formats per report type (array = generate multiple) |
| `naming.include_timestamp` | bool | `true` | Add timestamp to filenames |
| `naming.include_fy` | bool | `true` | Add FY to filenames |
| `naming.pattern` | string | `"{report_type}_{fy}_{date}"` | Filename pattern |
| `auto_open` | bool | `false` | Auto-open generated reports |

### Supported Formats

| Format | Extension | Use Case |
|--------|-----------|----------|
| `xlsx` | `.xlsx` | Primary format, multi-sheet support |
| `pdf` | `.pdf` | Print-ready, official submissions |
| `json` | `.json` | API integration, ITR upload |
| `csv` | `.csv` | Data exchange, import to other tools |
| `html` | `.html` | Web viewing, email embedding |

### financial_year

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `default` | string | Current FY | Default FY for reports |
| `start_month` | int | `4` | FY start month (4 = April for India) |

### parsers

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_archive` | bool | `true` | Move processed files to archive |
| `duplicate_handling` | string | `"skip"` | `skip`, `update`, `error` |
| `default_sources` | object | - | Preferred parser per asset type |

## Example: Sanjay's Preferences

```json
{
  "$schema": "user_preferences_v1",
  "version": "1.0",

  "reports": {
    "default_format": "xlsx",
    "formats_by_type": {
      "balance_sheet": ["xlsx", "pdf"],
      "tax_computation": ["xlsx", "json"]
    },
    "naming": {
      "include_timestamp": true,
      "include_fy": true,
      "pattern": "Sanjay_{report_type}_FY{fy}"
    }
  },

  "financial_year": {
    "default": "2024-25"
  },

  "display": {
    "currency_symbol": "₹",
    "decimal_places": 2,
    "negative_in_brackets": true
  },

  "parsers": {
    "auto_archive": true,
    "duplicate_handling": "skip",
    "default_sources": {
      "Mutual-Fund": "CAMS",
      "Indian-Stocks": "Zerodha",
      "Bank": "ICICI"
    }
  }
}
```

## Loading Priority

1. User's `preferences.json` (highest priority)
2. Global `config/defaults.json` (fallback)
3. Hardcoded defaults in code (last resort)

## Validation

Preferences are validated on load:
- Unknown fields are warned but ignored (forward compatibility)
- Invalid format types raise errors
- Missing required fields use defaults

## Migration

When schema version changes:
1. Old preferences are backed up as `preferences.json.v{old_version}.bak`
2. Automatic migration is attempted
3. User is notified of manual changes needed
