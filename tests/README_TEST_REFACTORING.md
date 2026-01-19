# PFAS Test Suite - Refactored for Scale

## Overview

The PFAS test suite has been completely refactored to support:
- ✅ **NO hardcoded paths** - Uses PathResolver exclusively
- ✅ **Multi-user testing** - Via environment variables and parameterization
- ✅ **Multi-asset testing** - Parameterized fixtures for all asset types
- ✅ **Golden master comparison** - Regression testing for outputs
- ✅ **Graceful skips** - Helpful messages when files not found
- ✅ **In-memory DB** - Fast, isolated tests
- ✅ **CI/CD ready** - Environment variable configuration

## Quick Start

### Run All Integration Tests

```bash
# Run all integration tests
PYTHONPATH=src pytest tests/integration/ -v

# Run specific test file
PYTHONPATH=src pytest tests/integration/test_epf_integration.py -v

# Run with detailed output
PYTHONPATH=src pytest tests/integration/ -v -s
```

### Run for Different Users

```bash
# Test with default user (Sanjay)
PYTHONPATH=src pytest tests/integration/ -v

# Test with different user
PFAS_TEST_USER=Priya PYTHONPATH=src pytest tests/integration/ -v

# Test with custom root path
PFAS_ROOT=/path/to/data PYTHONPATH=src pytest tests/integration/ -v
```

### Run Specific Asset Tests

```bash
# Run only EPF tests
PYTHONPATH=src pytest tests/integration/test_epf_integration.py -v

# Run only MF tests
PYTHONPATH=src pytest tests/integration/test_mf_cams_integration.py -v

# Run only NPS tests
PYTHONPATH=src pytest tests/integration/test_nps_integration.py -v
```

## Environment Variables

### PFAS_TEST_USER
- **Purpose:** Select which user's data to test
- **Default:** `Sanjay`
- **Usage:** `PFAS_TEST_USER=Priya pytest tests/integration/`

### PFAS_ROOT
- **Purpose:** Set PFAS data root directory
- **Default:** Current working directory
- **Usage:** `PFAS_ROOT=/mnt/data/PFAS pytest tests/integration/`

## CI/CD Configuration

### GitHub Actions Example

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: pip install -e .[dev]

      - name: Run integration tests
        env:
          PFAS_TEST_USER: TestUser
          PFAS_ROOT: ${{ github.workspace }}/test_data
        run: pytest tests/integration/ -v
```

### GitLab CI Example

```yaml
test:integration:
  script:
    - pip install -e .[dev]
    - export PFAS_TEST_USER=TestUser
    - export PFAS_ROOT=$CI_PROJECT_DIR/test_data
    - pytest tests/integration/ -v
```

## Fixtures Reference

### Core Fixtures

#### `path_resolver`
- **Scope:** Session
- **Returns:** PathResolver instance
- **Usage:**
  ```python
  def test_something(path_resolver):
      inbox = path_resolver.inbox() / "Mutual-Fund"
      assert inbox.exists()
  ```

#### `test_db`
- **Scope:** Session
- **Returns:** In-memory SQLite connection
- **Usage:**
  ```python
  def test_parser(test_db):
      cursor = test_db.execute("SELECT * FROM users")
  ```

#### `clean_db`
- **Scope:** Function
- **Returns:** Clean database (tables cleared)
- **Usage:**
  ```python
  def test_ingestion(clean_db):
      # Fresh database for each test
  ```

### Asset-Specific Fixtures

All fixtures automatically skip if no files found:

- `epf_file` - Latest EPF PDF
- `mutual_fund_file` - Latest MF statement
- `bank_file` - Latest bank statement
- `nps_file` - Latest NPS file
- `ppf_file` - Latest PPF file
- `stock_file` - Latest stock file
- `salary_file` - Latest salary file

**Example:**
```python
def test_epf_parse(epf_file, test_db):
    # epf_file is automatically selected
    # Test skips gracefully if no EPF files found
    parser = EPFParser(test_db)
    result = parser.parse(epf_file)
    assert result.success
```

### Parameterized Fixtures

#### `asset_type`
- **Parameterized:** All TEST_ASSETS
- **Usage:**
  ```python
  def test_all_assets(asset_type, path_resolver):
      inbox = path_resolver.inbox() / asset_type
      # Runs once for each asset type
  ```

## Golden Master Testing

### Saving Golden Masters

```python
from conftest import save_golden

def test_parser_output(epf_file, test_db):
    parser = EPFParser(test_db)
    result = parser.parse(epf_file)

    # Save results as golden master
    totals = {'count': len(result.transactions)}
    save_golden(totals, 'epf_parser_totals', format='json')
```

### Comparing with Golden Masters

```python
from conftest import assert_golden_match

def test_parser_regression(epf_file, test_db):
    parser = EPFParser(test_db)
    result = parser.parse(epf_file)

    totals = {'count': len(result.transactions)}

    # Assert matches golden master (or creates if missing)
    assert_golden_match(
        totals,
        'epf_parser_totals',
        format='json',
        save_if_missing=True
    )
```

Golden masters are stored in: `tests/integration/golden_masters/`

## Multi-User Testing

### Parameterized Multi-User Tests

```python
@pytest.mark.parametrize("path_resolver", ["Sanjay", "Priya"], indirect=True)
def test_multi_user_parse(path_resolver, epf_file, test_db):
    """Test runs for both Sanjay and Priya."""
    parser = EPFParser(test_db)
    result = parser.parse(epf_file)
    assert result.success
    print(f"User: {path_resolver.user_name}")
```

### Running Multi-User Tests

```bash
# Set up test data for multiple users
mkdir -p Data/Users/Sanjay/inbox/EPF
mkdir -p Data/Users/Priya/inbox/EPF

# Copy test files
cp test_epf.pdf Data/Users/Sanjay/inbox/EPF/
cp test_epf.pdf Data/Users/Priya/inbox/EPF/

# Run tests for both users
pytest tests/integration/ -v
```

## Writing New Tests

### Template for New Integration Test

```python
"""New Asset Integration Test"""

import pytest
from pfas.parsers.new_asset.parser import NewAssetParser


class TestNewAssetParser:
    """Integration tests for new asset."""

    def test_basic_parse(self, new_asset_file, test_db):
        """Test basic parsing."""
        parser = NewAssetParser(test_db)
        result = parser.parse(new_asset_file)

        assert result.success
        assert len(result.records) > 0

    def test_save_to_db(self, new_asset_file, clean_db, test_user_id):
        """Test database persistence."""
        parser = NewAssetParser(clean_db)
        result = parser.parse(new_asset_file)

        count = parser.save_to_db(result, user_id=test_user_id)
        assert count > 0
```

### Add New Asset Fixture

In `tests/integration/conftest.py`:

```python
@pytest.fixture
def new_asset_file(path_resolver) -> Path:
    """Latest New Asset file from inbox."""
    inbox = path_resolver.inbox() / "New-Asset"
    return _find_latest_file(
        inbox,
        ['.pdf', '.xlsx'],  # Supported extensions
        "New-Asset",
        path_resolver.user_name
    )
```

## Best Practices

### 1. Always Use PathResolver
```python
# ✓ Good
def test_something(path_resolver):
    inbox = path_resolver.inbox() / "EPF"

# ✗ Bad
def test_something():
    inbox = Path("/home/user/Data/Users/Sanjay/inbox/EPF")
```

### 2. Use clean_db for Isolation
```python
# ✓ Good
def test_ingestion(clean_db):
    # Database is fresh for each test

# ✗ Bad
def test_ingestion(test_db):
    # May have data from previous tests
```

### 3. Graceful Skips
Fixtures automatically skip if files not found. No manual checks needed:

```python
# ✓ Good - automatic skip
def test_parser(epf_file, test_db):
    # epf_file fixture handles skip if no files

# ✗ Bad - manual check
def test_parser(path_resolver, test_db):
    files = list(path_resolver.inbox().glob("*.pdf"))
    if not files:
        pytest.skip("No files")
```

### 4. Use Golden Masters for Regression
```python
# Prevent regressions in parser output
from conftest import assert_golden_match

def test_parser_regression(epf_file, test_db):
    result = parse(epf_file)
    assert_golden_match(result.summary, 'epf_summary')
```

## Troubleshooting

### Tests Skipped - "No files found"
**Solution:** Add test files to user's inbox

```bash
# Check inbox
ls Data/Users/Sanjay/inbox/EPF/

# Add test files
cp sample.pdf Data/Users/Sanjay/inbox/EPF/
```

### Tests Skipped - "User directory not found"
**Solution:** Create user directory or set PFAS_TEST_USER

```bash
# Create user
mkdir -p Data/Users/Sanjay/inbox

# Or use existing user
PFAS_TEST_USER=ExistingUser pytest tests/integration/
```

### Import Errors
**Solution:** Set PYTHONPATH

```bash
PYTHONPATH=src pytest tests/integration/
```

### Database Errors
**Solution:** Ensure DatabaseManager is properly initialized

```bash
# Check if all tables exist
pytest tests/integration/ -v -s
```

## Summary

The refactored test suite provides:
- **Scalability:** Easy to add new users, assets, tests
- **Maintainability:** No hardcoded paths, DRY principles
- **Reliability:** In-memory DB, isolated tests
- **Flexibility:** Environment variables, parameterization
- **Debuggability:** Graceful skips with helpful messages
- **Regression testing:** Golden masters for outputs

**No hardcoded paths. All PathResolver. Future-proof.**
