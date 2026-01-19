# Encrypted File Handling in PFAS

## Overview

PFAS provides robust support for handling password-protected files, particularly encrypted PDFs from financial institutions. The system uses a centralized configuration file to manage passwords, eliminating the need for manual entry while maintaining security.

## Password Configuration File

### Location

```
Data/Users/<UserName>/config/passwords.json
```

Example: `Data/Users/Sanjay/config/passwords.json`

### File Format

```json
{
  "files": {
    "exact_filename.pdf": "password_for_this_file",
    "Sanjay_CAMS_Karvy_CAS_FY24-25.pdf": "AAPPS0793R",
    "XXXPS0793X_2024-25_AIS.pdf": "aapps0793r20071971"
  },
  "patterns": {
    "CAMS": "common_cams_password",
    "KARVY": "common_karvy_password",
    "*.pdf": "default_pdf_password",
    "*": "fallback_password_for_all"
  }
}
```

## Password Matching Priority

The system uses the following priority order when looking up passwords:

1. **Exact filename match** (highest priority)
   - Matches the complete filename in the `files` section
   - Example: `"Sanjay_CAS.pdf"` matches exactly

2. **Pattern match**
   - Matches patterns defined in the `patterns` section
   - Supports multiple pattern types:
     - **Substring match**: `"CAMS"` matches any filename containing "CAMS"
     - **Extension match**: `"*.pdf"` matches any PDF file
     - **Wildcard**: `"*"` matches any file (used as last resort)

3. **Interactive prompt**
   - If no match found and `interactive=True`, prompts user for password
   - Password is securely entered (hidden input)

4. **None**
   - If no match and `interactive=False`, returns None
   - Parser will attempt without password and may fail if file is encrypted

## Common Password Patterns

### Indian Financial Institutions

#### Mutual Fund CAS Statements
- **CAMS**: Often uses PAN number in uppercase
- **Karvy/KFintech**: Often uses PAN number or DOB

```json
{
  "files": {
    "Sanjay_CAMS_CAS.pdf": "AAPPS0793R"
  },
  "patterns": {
    "CAMS": "AAPPS0793R",
    "KARVY": "AAPPS0793R"
  }
}
```

#### AIS (Annual Information Statement)
- Format: PAN in lowercase + DOB (DDMMYYYY)
- Example: PAN `AAPPS0793R` + DOB `20-07-1971` = `aapps0793r20071971`

```json
{
  "files": {
    "AAPPS0793R_2024-25_AIS.pdf": "aapps0793r20071971"
  }
}
```

#### Bank Statements
- Usually no password or customer ID

#### EPF Passbooks
- UAN number or member ID

### USA Financial Institutions

#### Brokerage Statements
- Account number
- Last 4 digits of SSN
- Custom password set by user

```json
{
  "patterns": {
    "MorganStanley": "MS_ACCOUNT_PWD",
    "ETrade": "ETRADE_PWD"
  }
}
```

## Usage in Code

### Basic Usage

```python
from pathlib import Path
from pfas.core.paths import PathResolver
from pfas.services.encrypted_file_handler import create_encrypted_file_handler

# Initialize path resolver
resolver = PathResolver(root_path, "Sanjay")

# Create encrypted file handler
handler = create_encrypted_file_handler(resolver, interactive=True)

# Get password for a file
file_path = Path("Data/Users/Sanjay/inbox/Mutual-Fund/CAMS_statement.pdf")
password = handler.get_password(file_path)

print(f"Password: {password}")
```

### Using with PDF Parsers

```python
from pfas.parsers.mf.ingester import MFIngester
from pfas.services.encrypted_file_handler import create_encrypted_file_handler

# Create handler
handler = create_encrypted_file_handler(resolver, interactive=True)

# Create ingester with encrypted file handler
ingester = MFIngester(
    conn=db_connection,
    user_id=1,
    inbox_path=resolver.inbox() / "Mutual-Fund",
    encrypted_file_handler=handler
)

# Run ingestion - passwords handled automatically
result = ingester.ingest()
```

### CLI Usage

The CLI automatically uses the encrypted file handler:

```bash
# With password configuration file
pfas --user Sanjay ingest --asset Mutual-Fund

# Non-interactive mode (uses config only, no prompts)
pfas --user Sanjay ingest --asset Mutual-Fund --no-prompt

# Interactive mode will prompt for passwords not in config
pfas --user Sanjay ingest --asset Mutual-Fund --archive
```

## Advanced Features

### Password Caching

Passwords are cached per session to avoid repeated lookups:

```python
# First call - reads from config
password1 = handler.get_password(file_path)

# Second call - uses cache
password2 = handler.get_password(file_path)  # No config read

# Disable caching
password3 = handler.get_password(file_path, use_cache=False)

# Clear cache for specific file
handler.clear_cache(file_path)

# Clear all cached passwords
handler.clear_cache()
```

### Opening Encrypted PDFs

The handler provides context managers for opening encrypted PDFs:

```python
# Using pdfplumber
with handler.open_pdf_pdfplumber(file_path) as pdf:
    for page in pdf.pages:
        text = page.extract_text()
        print(text)

# Using PyPDF2
with handler.open_pdf_pypdf2(file_path) as reader:
    for page in reader.pages:
        text = page.extract_text()
        print(text)
```

### Checking Encryption Status

```python
# Check if PDF is encrypted
is_encrypted = handler.check_pdf_encrypted(file_path)

if is_encrypted:
    print(f"{file_path.name} is encrypted")
else:
    print(f"{file_path.name} is not encrypted")
```

### Testing Multiple Passwords

```python
# Try multiple passwords
passwords_to_try = [
    "AAPPS0793R",
    "aapps0793r20071971",
    "alternative_password"
]

working_password = handler.try_pdf_passwords(
    file_path,
    passwords_to_try,
    library="pdfplumber"
)

if working_password:
    print(f"Found working password: {working_password}")
else:
    print("None of the passwords worked")
```

## Security Considerations

### Password File Security

⚠️ **Important**: The `passwords.json` file contains sensitive information in plain text.

**Best Practices:**

1. **File Permissions**
   ```bash
   # Linux/Mac - Restrict to user only
   chmod 600 Data/Users/Sanjay/config/passwords.json

   # Windows - Use file properties to restrict access
   ```

2. **Git Exclusion**
   - Ensure `passwords.json` is in `.gitignore`
   - Never commit to version control

   ```gitignore
   # In .gitignore
   **/passwords.json
   Data/Users/*/config/passwords.json
   ```

3. **Backup Security**
   - Store backups in encrypted locations
   - Use password managers for the passwords themselves
   - Consider encrypting the passwords.json file

4. **Shared Systems**
   - Use separate user directories for each person
   - Don't share password configurations
   - Each user should maintain their own `passwords.json`

### Alternative: Environment Variables

For automated/CI environments, use environment variables:

```python
import os
from pfas.core.paths import PathResolver

# Override password callback
def env_password_callback(file_path: Path) -> Optional[str]:
    # Try environment variable first
    env_var = f"PFAS_PWD_{file_path.stem.upper()}"
    return os.getenv(env_var)

# Use with ingester
ingester = MFIngester(
    conn=conn,
    user_id=1,
    inbox_path=inbox_path,
    password_callback=env_password_callback
)
```

## Troubleshooting

### Password Not Working

1. **Check file path**
   - Ensure filename in config matches exactly
   - File paths are case-sensitive on Linux/Mac

2. **Test password manually**
   ```python
   from pathlib import Path
   import pdfplumber

   with pdfplumber.open(str(file_path), password="your_password") as pdf:
       print(f"Pages: {len(pdf.pages)}")
   ```

3. **Common issues**
   - PAN number: Ensure correct case (usually uppercase)
   - DOB format: Check DDMMYYYY format
   - Spaces: Remove any leading/trailing spaces

### File Not Found in Config

Check the matching logic:

```python
from pfas.core.paths import PathResolver

resolver = PathResolver(root_path, "Sanjay")
file_path = Path("/path/to/file.pdf")

password = resolver.get_file_password(file_path, interactive=False)

if password:
    print(f"Found password: {password}")
else:
    print("No password found - check passwords.json")
```

### Interactive Prompt Not Appearing

1. Ensure `interactive=True` when creating handler
2. Check that `--no-prompt` flag is not used in CLI
3. Verify terminal supports interactive input

### Multiple Files with Same Pattern

Use exact filename matches for priority:

```json
{
  "files": {
    "specific_file.pdf": "specific_password"
  },
  "patterns": {
    "generic": "generic_password"
  }
}
```

## Examples

### Example 1: CAMS Mutual Fund Statement

File: `Sanjay_CAMS_Karvy_CAS_FY24-25.pdf`
Password: PAN number `AAPPS0793R`

```json
{
  "files": {
    "Sanjay_CAMS_Karvy_CAS_FY24-25.pdf": "AAPPS0793R"
  }
}
```

### Example 2: Multiple Patterns

```json
{
  "files": {
    "important_statement_2024.pdf": "VerySecurePassword123"
  },
  "patterns": {
    "CAMS": "AAPPS0793R",
    "KARVY": "AAPPS0793R",
    "AIS": "aapps0793r20071971",
    "MorganStanley": "MS_Account_Password",
    "*.pdf": "DefaultPDFPassword"
  }
}
```

### Example 3: Progressive Fallback

```json
{
  "files": {
    "critical_doc.pdf": "password1"
  },
  "patterns": {
    "bank": "bank_password",
    "*.pdf": "generic_pdf_password",
    "*": "last_resort_password"
  }
}
```

## Migration from Password Callback

If you were using the old `password_callback` approach:

**Old Code:**
```python
def my_password_callback(file_path: Path) -> str:
    if "CAMS" in file_path.name:
        return "AAPPS0793R"
    return "default"

ingester = MFIngester(conn, user_id, inbox_path, password_callback=my_password_callback)
```

**New Code:**
```python
# Create passwords.json
{
  "patterns": {
    "CAMS": "AAPPS0793R",
    "*": "default"
  }
}

# Use encrypted file handler
handler = create_encrypted_file_handler(resolver)
ingester = MFIngester(conn, user_id, inbox_path, encrypted_file_handler=handler)
```

## Summary

The PFAS encrypted file handling system provides:

- ✅ Centralized password configuration
- ✅ Priority-based password matching
- ✅ Automatic password lookup
- ✅ Interactive fallback prompts
- ✅ Session-level caching
- ✅ Support for multiple PDF libraries
- ✅ Security-conscious design

For questions or issues, refer to the test suite in `tests/unit/test_services/test_encrypted_file_handler.py`.

