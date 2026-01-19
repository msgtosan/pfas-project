# PFAS Configuration Best Practices

## Architecture Overview

```
Data/
├── config/                          # Global configuration
│   ├── paths.json                   # Path structure (rarely changes)
│   ├── defaults.json                # Default preferences for new users
│   └── formats.json                 # Supported format definitions
│
└── Users/
    └── <username>/
        ├── config/
        │   ├── preferences.json     # User-specific preferences
        │   └── passwords.json       # File passwords (gitignored)
        ├── db/
        │   └── finance.db           # User's encrypted database
        ├── inbox/                   # Drop files here
        │   ├── Mutual-Fund/
        │   ├── Bank/
        │   └── ...
        ├── archive/                 # Processed files
        └── reports/                 # Generated reports (per user!)
            ├── balance_sheet/
            ├── cash_flow/
            ├── capital_gains/
            └── ...
```

## Key Principles

### 1. Configuration Hierarchy

```
Code Defaults → Global Config → User Preferences → CLI Arguments
    (lowest)                                        (highest)
```

Each layer overrides the previous:
- **Code Defaults**: Hardcoded fallbacks (always work)
- **Global Config**: `Data/config/defaults.json` (admin-controlled)
- **User Preferences**: `Data/Users/<user>/config/preferences.json` (user-controlled)
- **CLI Arguments**: `--format xlsx` (per-invocation override)

### 2. Never Hardcode Paths

**Bad:**
```python
report_path = Path("Data/Reports/balance_sheet.xlsx")
```

**Good:**
```python
report_path = path_resolver.get_report_path("balance_sheet", "2024-25", "xlsx")
```

### 3. All Reports Go to User Directory

Reports must NEVER go to a global `Data/Reports/` folder. Always use:

```python
# Correct: User-specific reports folder
reports_dir = path_resolver.reports()  # Returns Data/Users/<user>/reports/
```

### 4. Format Preferences Are User-Specific

```python
# Load user's preferred format
prefs = path_resolver.get_preferences()
formats = prefs.reports.get_formats("balance_sheet")  # e.g., ["xlsx", "pdf"]

# Generate in all preferred formats
for fmt in formats:
    output_path = path_resolver.get_report_path("balance_sheet", fy, fmt)
    generate_report(output_path, fmt)
```

## Configuration Files

### Global: `config/paths.json`

Defines directory structure. Rarely changed after initial setup.

```json
{
  "users_base": "Users",
  "global": {
    "config_dir": "config",
    "shared_masters": "shared/masters"
  },
  "per_user": {
    "db_file": "db/finance.db",
    "inbox": "inbox",
    "archive": "archive",
    "reports": "reports",
    "user_config_dir": "config"
  }
}
```

### Global: `config/defaults.json`

Default preferences for new users. Admin can customize for organization.

```json
{
  "reports": {
    "default_format": "xlsx",
    "formats_by_type": {
      "tax_computation": ["xlsx", "json"]
    }
  },
  "display": {
    "currency_symbol": "₹",
    "decimal_places": 2
  }
}
```

### Per-User: `config/preferences.json`

User's personal preferences (overrides defaults).

```json
{
  "reports": {
    "default_format": "xlsx",
    "formats_by_type": {
      "balance_sheet": ["xlsx", "pdf"],
      "tax_computation": ["xlsx", "json", "pdf"]
    }
  }
}
```

## Multi-User Scalability

### User Isolation

Each user has completely isolated:
- Database (encrypted per-user)
- Configuration files
- Input/output directories
- Report outputs

### Adding New Users

```python
from pfas.core.paths import PathResolver
from pfas.core.preferences import create_default_preferences

def setup_new_user(data_root: Path, user_name: str):
    resolver = PathResolver(data_root, user_name)

    # Create directory structure
    resolver.ensure_user_structure()

    # Create default preferences
    create_default_preferences(resolver.user_config_dir(), user_name)

    # Initialize database
    from pfas.core.database import DatabaseManager
    db = DatabaseManager()
    db.init(str(resolver.db_path()), password)
```

### User Enumeration

```python
def list_users(data_root: Path) -> List[str]:
    users_dir = data_root / "Users"
    return [d.name for d in users_dir.iterdir() if d.is_dir()]
```

## Format Support Matrix

### Input Formats (Parsers)

| Asset Type | PDF | Excel | CSV | JSON |
|------------|-----|-------|-----|------|
| Mutual-Fund (CAMS) | ✅ | ✅ | - | - |
| Mutual-Fund (KARVY) | ✅ | ✅ | - | - |
| Bank (ICICI) | - | ✅ | ✅ | - |
| Indian-Stocks (Zerodha) | - | ✅ | - | - |
| Indian-Stocks (ICICI Direct) | - | - | ✅ | - |
| EPF | ✅ | - | - | - |
| NPS | - | ✅ | ✅ | - |
| PPF | - | ✅ | ✅ | - |
| Salary (Form16) | ✅ | - | - | - |

### Output Formats (Reports)

| Report Type | Excel | PDF | JSON | CSV | HTML |
|-------------|-------|-----|------|-----|------|
| Balance Sheet | ✅ | ✅ | ✅ | - | ✅ |
| Cash Flow | ✅ | ✅ | ✅ | - | ✅ |
| Income Statement | ✅ | ✅ | ✅ | - | ✅ |
| Capital Gains | ✅ | ✅ | ✅ | ✅ | - |
| Portfolio | ✅ | ✅ | ✅ | ✅ | - |
| Tax Computation | ✅ | ✅ | ✅ | - | - |
| ITR-2 Schedule | - | - | ✅ | - | - |

## Implementation Checklist

### For New CLI Commands

```python
def my_command(args):
    # 1. Always use PathResolver
    resolver = PathResolver(args.data_root, args.user)

    # 2. Load user preferences
    prefs = resolver.get_preferences()

    # 3. Get format from args or preferences
    fmt = args.format or prefs.reports.default_format

    # 4. Generate output path from resolver (NOT hardcoded)
    output_path = resolver.get_report_path("my_report", args.fy, fmt)

    # 5. Ensure parent directories exist
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 6. Generate report
    generate_report(output_path, fmt)
```

### For New Parsers

```python
class MyParser:
    def __init__(self, db_connection, path_resolver: PathResolver):
        self.conn = db_connection
        self.resolver = path_resolver
        self.prefs = path_resolver.get_preferences()

    def parse(self, file_path: Path) -> ParseResult:
        # Use preferences for default behavior
        auto_archive = self.prefs.parsers.auto_archive
        duplicate_handling = self.prefs.parsers.duplicate_handling
        ...
```

### For New Report Generators

```python
class ReportGenerator:
    def __init__(self, path_resolver: PathResolver):
        self.resolver = path_resolver
        self.prefs = path_resolver.get_preferences()

    def generate(self, report_type: str, fy: str):
        # Get all formats user wants for this report type
        formats = self.prefs.reports.get_formats(report_type)

        outputs = []
        for fmt in formats:
            output_path = self.resolver.get_report_path(report_type, fy, fmt)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if fmt == "xlsx":
                self._generate_excel(output_path)
            elif fmt == "pdf":
                self._generate_pdf(output_path)
            elif fmt == "json":
                self._generate_json(output_path)

            outputs.append(output_path)

        return outputs
```

## Migration Guide

### From Hardcoded Paths to PathResolver

**Before:**
```python
db_path = "Data/pfas.db"
report_dir = "Data/Reports"
```

**After:**
```python
resolver = PathResolver(data_root, user_name)
db_path = resolver.db_path()
report_dir = resolver.reports()
```

### From Global Reports to Per-User Reports

**Before:**
```python
output = Path("Data/Reports/balance_sheet.xlsx")
```

**After:**
```python
output = resolver.get_report_path("balance_sheet", "2024-25", "xlsx")
# Returns: Data/Users/Sanjay/reports/balance_sheet/Sanjay_balance_sheet_FY202425_2025-01-17.xlsx
```

## Testing Configuration

### Unit Tests

```python
def test_user_preferences_loading(tmp_path):
    # Setup
    user_config = tmp_path / "config"
    user_config.mkdir()
    (user_config / "preferences.json").write_text('{"reports": {"default_format": "pdf"}}')

    # Load
    prefs = UserPreferences.load(user_config)

    # Assert
    assert prefs.reports.default_format == "pdf"
```

### Integration Tests

```python
def test_report_goes_to_user_directory(path_resolver):
    report_path = path_resolver.get_report_path("balance_sheet", "2024-25", "xlsx")

    # Verify it's under user's reports directory
    assert path_resolver.user_name in str(report_path)
    assert "reports" in str(report_path)
    assert "balance_sheet" in str(report_path)
```

## Common Mistakes to Avoid

1. **Don't create `Data/Reports/`** - All reports go under user directory
2. **Don't read config without fallbacks** - Always use UserPreferences.load()
3. **Don't ignore CLI overrides** - Args should override preferences
4. **Don't store sensitive data in preferences** - Passwords go in passwords.json (gitignored)
5. **Don't assume format** - Always check user preferences
