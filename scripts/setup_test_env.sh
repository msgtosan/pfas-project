#!/bin/bash
# PFAS Test Environment Setup Script
# Run this before executing tests to ensure proper configuration

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=== PFAS Test Environment Setup ==="
echo "Project root: $PROJECT_ROOT"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

check_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    ERRORS=$((ERRORS + 1))
}

check_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

ERRORS=0

# 1. Check Python environment
echo "--- Python Environment ---"
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version 2>&1)
    check_pass "Python found: $PYTHON_VERSION"
else
    check_fail "Python3 not found"
fi

# 2. Check virtual environment
if [[ -n "$VIRTUAL_ENV" ]]; then
    check_pass "Virtual environment active: $VIRTUAL_ENV"
else
    check_warn "No virtual environment active. Consider activating one."
fi

# 3. Check PYTHONPATH includes src
echo ""
echo "--- Path Configuration ---"
if [[ ":$PYTHONPATH:" == *":$PROJECT_ROOT/src:"* ]]; then
    check_pass "PYTHONPATH includes src/"
else
    check_warn "PYTHONPATH does not include src/. Setting it now."
    export PYTHONPATH="$PROJECT_ROOT/src:$PYTHONPATH"
    echo "  -> PYTHONPATH=$PYTHONPATH"
fi

# 4. Check Data symlink
echo ""
echo "--- Data Directory ---"
DATA_LINK="$PROJECT_ROOT/Data"
if [[ -L "$DATA_LINK" ]]; then
    DATA_TARGET=$(readlink -f "$DATA_LINK" 2>/dev/null || readlink "$DATA_LINK")
    if [[ -d "$DATA_TARGET" ]]; then
        check_pass "Data symlink exists and points to: $DATA_TARGET"
    else
        check_fail "Data symlink target does not exist: $DATA_TARGET"
    fi
elif [[ -d "$DATA_LINK" ]]; then
    check_pass "Data directory exists (not a symlink)"
else
    check_fail "Data directory/symlink not found at $DATA_LINK"
    echo "  -> Create symlink: ln -s /path/to/your/data $DATA_LINK"
fi

# 5. Check Users directory structure
echo ""
echo "--- User Directory Structure ---"

# Check paths.json configuration
PATHS_CONFIG="$PROJECT_ROOT/config/paths.json"
if [[ -f "$PATHS_CONFIG" ]]; then
    USERS_BASE=$(grep -o '"users_base"[[:space:]]*:[[:space:]]*"[^"]*"' "$PATHS_CONFIG" | cut -d'"' -f4)
    if [[ -n "$USERS_BASE" ]]; then
        check_pass "paths.json users_base configured: $USERS_BASE"
    else
        USERS_BASE="Data/Users"
        check_warn "users_base not found in paths.json, using default: $USERS_BASE"
    fi
else
    USERS_BASE="Data/Users"
    check_warn "paths.json not found, using default users_base: $USERS_BASE"
fi

USERS_DIR="$PROJECT_ROOT/$USERS_BASE"
if [[ -d "$USERS_DIR" ]]; then
    check_pass "Users directory exists: $USERS_DIR"

    # List available users
    echo "  Available users:"
    for user_dir in "$USERS_DIR"/*/; do
        if [[ -d "$user_dir" ]]; then
            user_name=$(basename "$user_dir")
            echo "    - $user_name"
        fi
    done
else
    check_fail "Users directory not found: $USERS_DIR"
    echo "  -> Ensure paths.json users_base points to correct location"
fi

# 6. Check default test user (Sanjay or PFAS_TEST_USER)
echo ""
echo "--- Test User Configuration ---"
TEST_USER="${PFAS_TEST_USER:-Sanjay}"
USER_DIR="$USERS_DIR/$TEST_USER"

if [[ -d "$USER_DIR" ]]; then
    check_pass "Test user directory exists: $USER_DIR"

    # Check required subdirectories
    for subdir in inbox archive reports config db; do
        if [[ -d "$USER_DIR/$subdir" ]]; then
            check_pass "  $subdir/ exists"
        else
            check_warn "  $subdir/ missing - will be created on first use"
        fi
    done
else
    check_fail "Test user directory not found: $USER_DIR"
    echo "  -> Set PFAS_TEST_USER to an existing user, or create the directory"
fi

# 7. Check configuration files
echo ""
echo "--- Configuration Files ---"

# Extract global config dir from paths.json
if [[ -f "$PATHS_CONFIG" ]]; then
    GLOBAL_CONFIG_BASE=$(grep -o '"config_dir"[[:space:]]*:[[:space:]]*"[^"]*"' "$PATHS_CONFIG" | head -1 | cut -d'"' -f4)
    if [[ -z "$GLOBAL_CONFIG_BASE" ]]; then
        GLOBAL_CONFIG_BASE="Data/config"
    fi
else
    GLOBAL_CONFIG_BASE="Data/config"
fi
GLOBAL_CONFIG="$PROJECT_ROOT/$GLOBAL_CONFIG_BASE"
if [[ -d "$GLOBAL_CONFIG" ]]; then
    check_pass "Global config directory: $GLOBAL_CONFIG"

    for config_file in defaults.json paths.json; do
        if [[ -f "$GLOBAL_CONFIG/$config_file" ]]; then
            check_pass "  $config_file exists"
        else
            check_warn "  $config_file missing"
        fi
    done
else
    check_warn "Global config directory not found: $GLOBAL_CONFIG"
fi

USER_CONFIG="$USER_DIR/config"
if [[ -d "$USER_CONFIG" ]]; then
    check_pass "User config directory: $USER_CONFIG"

    for config_file in preferences.json passwords.json; do
        if [[ -f "$USER_CONFIG/$config_file" ]]; then
            check_pass "  $config_file exists"
        else
            check_warn "  $config_file missing (optional)"
        fi
    done
fi

# 8. Check pytest configuration
echo ""
echo "--- Pytest Configuration ---"
if [[ -f "$PROJECT_ROOT/pytest.ini" ]]; then
    check_pass "pytest.ini exists"
elif [[ -f "$PROJECT_ROOT/pyproject.toml" ]]; then
    check_pass "pyproject.toml exists (pytest config)"
else
    check_warn "No pytest configuration found"
fi

# 9. Summary and environment export
echo ""
echo "=========================================="
if [[ $ERRORS -eq 0 ]]; then
    echo -e "${GREEN}Environment setup complete. No errors found.${NC}"
else
    echo -e "${RED}Environment setup found $ERRORS error(s).${NC}"
    echo "Fix the issues above before running tests."
fi

echo ""
echo "Environment variables to use:"
echo "  export PYTHONPATH=\"$PROJECT_ROOT/src:\$PYTHONPATH\""
echo "  export PFAS_TEST_USER=\"$TEST_USER\"       # Default user for tests"
echo "  export PFAS_ROOT=\"$PROJECT_ROOT\""
echo "  export PFAS_TEST_USE_ARCHIVE=\"true\"      # Enable/disable archive fallback"
echo "  export PFAS_TEST_BANK=\"ICICI\"            # Default bank for bank intelligence tests"
echo ""
echo "Configuration file: config/test_config.json"
echo "  - file_sources.fallback_to_archive: true/false (default: true)"
echo "  - When enabled, tests look in inbox first, then archive if inbox is empty"
echo ""
echo "To run tests:"
echo "  source scripts/setup_test_env.sh && pytest tests/ -v"
echo ""
echo "To run tests for a different user:"
echo "  PFAS_TEST_USER=Priya source scripts/setup_test_env.sh && pytest tests/ -v"
echo ""
echo "To run bank intelligence tests for a different bank:"
echo "  PFAS_TEST_BANK=HDFC pytest tests/integration/test_bank_intelligence/ -v"
echo ""

# Export variables for current session
export PYTHONPATH="$PROJECT_ROOT/src:$PYTHONPATH"
export PFAS_TEST_USER="$TEST_USER"
export PFAS_ROOT="$PROJECT_ROOT"

exit $ERRORS
