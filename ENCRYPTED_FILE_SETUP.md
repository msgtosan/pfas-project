# Encrypted File Handling - Setup Complete

## Summary

Comprehensive encrypted file handling has been integrated into the PFAS project. The system provides centralized password management for encrypted PDFs and other protected files.

## What Was Created/Modified

### 1. Core Path Resolver Enhancement
**File:** `src/pfas/core/paths.py`

Added methods:
- `password_config_file()` - Get path to passwords.json
- `get_file_password()` - Retrieve password with priority matching
- `_prompt_for_password()` - Interactive password prompt

**Key Features:**
- Priority-based password matching (exact > pattern > wildcard > prompt)
- Support for exact filenames, substring patterns, extension patterns
- Non-interactive mode support

### 2. Encrypted File Handler Service
**File:** `src/pfas/services/encrypted_file_handler.py` (NEW)

**Classes:**
- `EncryptedFileHandler` - Main handler class
- `create_encrypted_file_handler()` - Factory function

**Capabilities:**
- Automatic password lookup from config
- Session-level password caching
- Context managers for pdfplumber and PyPDF2
- Password encryption status checking
- Multi-password testing

**Methods:**
- `get_password()` - Get password with caching
- `open_pdf_pdfplumber()` - Context manager for pdfplumber
- `open_pdf_pypdf2()` - Context manager for PyPDF2
- `check_pdf_encrypted()` - Check encryption status
- `try_pdf_passwords()` - Test multiple passwords
- `clear_cache()` - Clear cached passwords

### 3. MF Ingester Integration
**File:** `src/pfas/parsers/mf/ingester.py`

**Changes:**
- Added `encrypted_file_handler` parameter to `__init__()`
- Integrated encrypted file handler in password retrieval logic
- Falls back to `password_callback` for backward compatibility
- Updated convenience function `ingest_mf_statements()`

### 4. CLI Integration
**File:** `src/pfas/cli/main.py`

**Changes:**
- Creates `EncryptedFileHandler` automatically in `cmd_ingest()`
- Passes handler to MFIngester
- Supports both interactive and non-interactive modes

### 5. Comprehensive Tests
**File:** `tests/unit/test_services/test_encrypted_file_handler.py` (NEW)

**Test Coverage:**
- PathResolver password methods (exact match, patterns, priority)
- EncryptedFileHandler initialization and password retrieval
- Password caching (enable/disable, clear)
- Non-interactive mode
- Factory function

**Test Classes:**
- `TestPathResolverPasswordMethods` - 8 tests
- `TestEncryptedFileHandler` - 6 tests
- `TestEncryptedFileHandlerPDFIntegration` - Integration tests (mocked)
- `TestFactoryFunction` - 2 tests

### 6. Documentation
**File:** `docs/ENCRYPTED_FILES.md` (NEW)

**Contents:**
- Password configuration file format
- Password matching priority
- Common password patterns (Indian & USA institutions)
- Usage examples (basic, parsers, CLI, advanced)
- Security considerations
- Troubleshooting guide
- Migration guide

## Password Configuration File Format

### Location
```
Data/Users/<UserName>/config/passwords.json
```

### Example Configuration
```json
{
  "files": {
    "Sanjay_CAMS_Karvy_CAS_FY24-25.pdf": "AAPPS0793R",
    "Sanjay_CAS.pdf": "AAPPS0793R",
    "XXXPS0793X_2024-25_AIS.pdf": "aapps0793r20071971"
  },
  "patterns": {
    "CAMS": "AAPPS0793R",
    "KARVY": "AAPPS0793R",
    "*.pdf": "default_pdf_password"
  }
}
```

## Password Matching Priority

1. **Exact filename match** (highest)
2. **Pattern match** (substring, extension)
3. **Wildcard match** (`*`)
4. **Interactive prompt** (if enabled)
5. **None** (if non-interactive)

## Usage Examples

### Basic Usage

```python
from pfas.core.paths import PathResolver
from pfas.services.encrypted_file_handler import create_encrypted_file_handler

# Create resolver and handler
resolver = PathResolver(root_path, "Sanjay")
handler = create_encrypted_file_handler(resolver, interactive=True)

# Get password
password = handler.get_password(file_path)
```

### With MF Ingester

```python
from pfas.parsers.mf.ingester import MFIngester

handler = create_encrypted_file_handler(resolver)
ingester = MFIngester(
    conn=db_connection,
    user_id=1,
    inbox_path=resolver.inbox() / "Mutual-Fund",
    encrypted_file_handler=handler
)

result = ingester.ingest()
```

### CLI Usage

```bash
# Automatic password lookup from passwords.json
pfas --user Sanjay ingest --asset Mutual-Fund

# Non-interactive mode (config only, no prompts)
pfas --user Sanjay ingest --asset Mutual-Fund --no-prompt

# Interactive mode (prompts if password not in config)
pfas --user Sanjay ingest --asset Mutual-Fund --archive
```

## Security Best Practices

1. **File Permissions**
   ```bash
   chmod 600 Data/Users/Sanjay/config/passwords.json
   ```

2. **Git Exclusion**
   - Ensure `passwords.json` is in `.gitignore`
   - Never commit passwords to version control

3. **Backup Security**
   - Store backups in encrypted locations
   - Use password managers for the passwords themselves

## Common Password Patterns

### Indian Financial Institutions

**CAMS/Karvy MF Statements:**
- Usually: PAN number in uppercase (e.g., `AAPPS0793R`)

**AIS (Annual Information Statement):**
- Format: PAN lowercase + DOB (DDMMYYYY)
- Example: `aapps0793r20071971` for PAN `AAPPS0793R` + DOB `20-07-1971`

**EPF Passbooks:**
- UAN number or member ID

### USA Financial Institutions

**Brokerage Statements:**
- Account number
- Last 4 digits of SSN
- Custom password set by user

## Features

### ✅ Implemented

- Centralized password configuration (`passwords.json`)
- Priority-based password matching (exact > pattern > wildcard)
- Automatic password lookup
- Interactive fallback prompts
- Session-level password caching
- Support for pdfplumber and PyPDF2
- Context managers for PDF opening
- Encryption status checking
- Multi-password testing
- Non-interactive mode
- Backward compatibility with `password_callback`
- Comprehensive tests (16 test cases)
- Complete documentation

### 🎯 Integration Points

1. **PathResolver** - Password configuration and retrieval
2. **EncryptedFileHandler** - Centralized password management
3. **MFIngester** - Mutual fund statement ingestion
4. **CLI** - Command-line interface
5. **Parsers** - CAMS, Karvy, and future PDF parsers

## Testing

### Run Unit Tests

```bash
# Test encrypted file handler
pytest tests/unit/test_services/test_encrypted_file_handler.py -v

# All tests
pytest tests/unit/test_services/ -v
```

### Test Coverage

```bash
pytest tests/unit/test_services/test_encrypted_file_handler.py --cov=src/pfas/services/encrypted_file_handler --cov-report=term-missing
```

## Files Modified

1. `src/pfas/core/paths.py` - Added password methods
2. `src/pfas/parsers/mf/ingester.py` - Integrated encrypted file handler
3. `src/pfas/cli/main.py` - Added encrypted file handler creation

## Files Created

1. `src/pfas/services/encrypted_file_handler.py` - Main service
2. `tests/unit/test_services/test_encrypted_file_handler.py` - Tests
3. `docs/ENCRYPTED_FILES.md` - Documentation
4. `ENCRYPTED_FILE_SETUP.md` - This summary

## Migration Guide

### From Password Callback

**Old:**
```python
def callback(path):
    return "password"

ingester = MFIngester(conn, user_id, inbox, password_callback=callback)
```

**New:**
```python
# Create passwords.json with patterns/files

handler = create_encrypted_file_handler(resolver)
ingester = MFIngester(conn, user_id, inbox, encrypted_file_handler=handler)
```

## Troubleshooting

### Password Not Working

1. Check filename matches exactly in config
2. Verify password case (PAN usually uppercase)
3. Test password manually with pdfplumber

### No Password Prompt

1. Ensure `interactive=True` when creating handler
2. Check `--no-prompt` flag not used
3. Verify terminal supports interactive input

### File Not Found in Config

Check matching logic:
```python
password = resolver.get_file_password(file_path, interactive=False)
print(f"Password: {password}")
```

## Next Steps

### Recommended

1. **Create `passwords.json`** for your user
   ```bash
   cp Data/Users/Sanjay/config/passwords.json Data/Users/<YourName>/config/
   # Edit with your passwords
   ```

2. **Update `.gitignore`**
   ```gitignore
   **/passwords.json
   Data/Users/*/config/passwords.json
   ```

3. **Set file permissions**
   ```bash
   chmod 600 Data/Users/*/config/passwords.json
   ```

4. **Test with real files**
   ```bash
   pfas --user Sanjay ingest --asset Mutual-Fund
   ```

### Future Enhancements

- [ ] Encrypted storage of passwords.json
- [ ] Keyring integration for system password manager
- [ ] Support for other encrypted file types (Excel, ZIP)
- [ ] Password rotation/expiry tracking
- [ ] Multi-user password sharing (with encryption)

## Documentation

- **Full Guide:** `docs/ENCRYPTED_FILES.md`
- **API Documentation:** Docstrings in `src/pfas/services/encrypted_file_handler.py`
- **Tests:** `tests/unit/test_services/test_encrypted_file_handler.py`

## Summary

The PFAS project now has enterprise-grade encrypted file handling with:

- ✅ Centralized configuration
- ✅ Automatic password lookup
- ✅ Multiple matching strategies
- ✅ Session caching
- ✅ Interactive fallback
- ✅ Comprehensive tests
- ✅ Complete documentation
- ✅ Backward compatibility
- ✅ Security-conscious design

All encrypted PDF files can now be processed automatically using the passwords.json configuration file!
