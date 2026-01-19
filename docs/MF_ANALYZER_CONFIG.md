# MF Analyzer Configuration Guide

## Overview

The MF Analyzer CLI uses a hierarchical configuration system that allows flexible customization at multiple levels:

1. **Built-in defaults** (lowest priority)
2. **Global project config** (`config/mf_analyzer_config_v2.json`)
3. **User preferences** (`Data/Users/{user}/config/preferences.json`)
4. **User passwords** (`Data/Users/{user}/config/passwords.json`)
5. **Command-line arguments** (highest priority)

Values from higher priority sources override lower priority ones.

## Configuration Files

### Global Project Config

**Location:** `config/mf_analyzer_config_v2.json`

Contains project-wide settings:
- File patterns for CAMS/Karvy statements
- Field mappings for Excel parsing
- Scheme classification rules
- Report settings
- Tax rates by financial year

### User Preferences

**Location:** `Data/Users/{user}/config/preferences.json`

User-specific preferences:
```json
{
  "reports": {
    "default_format": "xlsx",
    "naming": {
      "include_timestamp": true,
      "pattern": "{user}_{report_type}_FY{fy}_{date}"
    }
  },
  "financial_year": {
    "default": "2024-25"
  },
  "cas": {
    "consolidate_folios": true,
    "clean_scheme_names": true
  }
}
```

### User Passwords

**Location:** `Data/Users/{user}/config/passwords.json`

Sensitive credentials (keep secure):
```json
{
  "database": {
    "password": "your_secure_password",
    "encryption": "sqlcipher"
  },
  "files": {
    "Sanjay_CAS.pdf": "PDF_PASSWORD",
    "*.pdf": "default_pdf_password"
  },
  "patterns": {
    "CAMS": "common_cams_password",
    "KARVY": "common_karvy_password"
  }
}
```

## Command-Line Overrides

All config values can be overridden via CLI arguments:

```bash
# Use specific config file
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --config my_config.json

# Override database path
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --db /path/to/finance.db

# Override database password
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --db-password "my_password"

# Override project root
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --root /path/to/project
```

## Usage Examples

### Basic Usage (uses all defaults from config)
```bash
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --fy 2024-25
```

### With Verbose Logging
```bash
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --fy 2024-25 --verbose
```

### Ingest Only (skip analysis and reports)
```bash
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --ingest-only
```

### Generate Reports Only
```bash
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --fy 2024-25 --report-only
```

---

## Database Management

### Database Location

Default: `Data/Users/{user}/db/finance.db`

The database is SQLCipher encrypted. Password sources (in order of precedence):
1. `--db-password` CLI argument
2. `passwords.json` > `database.password`
3. Built-in default: `pfas_secure_2024`

### When to Delete the Database

Delete the database to:
- **Start fresh** after major code changes
- **Verify data integrity** with clean import
- **Fix corruption** from interrupted operations
- **Test regression** with known baseline

### Database Reset Procedure

**WARNING:** This permanently deletes all imported data. Ensure you have the original source files.

```bash
# 1. Stop any running processes
pkill -f mf_analyzer

# 2. Backup current database (optional)
cp Data/Users/Sanjay/db/finance.db Data/Users/Sanjay/db/finance.db.backup

# 3. Delete the database
rm Data/Users/Sanjay/db/finance.db

# 4. Run fresh import
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --fy 2024-25 --verbose

# 5. Verify results
# Compare output with expected values
```

### Verify Data Integrity

After a fresh import, verify:

1. **Transaction counts** match source files
2. **Holdings totals** match latest statements
3. **Capital gains** match RTA statements

```bash
# Run with verbose to see counts
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --fy 2024-25 --verbose 2>&1 | grep -E "Files|Records|Holdings|Total"
```

### Force Re-Import

To re-import files without deleting the database:

```bash
# Force flag re-processes already ingested files
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --fy 2024-25 --force
```

---

## Troubleshooting

### Config Not Loading

Check config file exists and is valid JSON:
```bash
# Validate JSON syntax
python -m json.tool config/mf_analyzer_config_v2.json > /dev/null && echo "Valid JSON"

# Check user config directory
ls -la Data/Users/Sanjay/config/
```

### Database Connection Errors

```bash
# Check database file permissions
ls -la Data/Users/Sanjay/db/

# Test with explicit password
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --db-password "pfas_secure_2024"
```

### Config Precedence Issues

Use `--verbose` to see which config files are loaded:
```bash
python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --verbose 2>&1 | grep -E "Loaded|Config sources"
```

---

## Security Notes

1. **Never commit `passwords.json`** to version control
2. Add to `.gitignore`:
   ```
   Data/Users/*/config/passwords.json
   ```
3. Use environment variables for CI/CD:
   ```bash
   export PFAS_DB_PASSWORD="secure_password"
   python -m pfas.cli.mf_analyzer_v2_cli --user Sanjay --db-password "$PFAS_DB_PASSWORD"
   ```
