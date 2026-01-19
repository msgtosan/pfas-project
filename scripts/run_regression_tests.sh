#!/bin/bash
#
# Regression Test Runner for PFAS Project
#
# Usage:
#   ./scripts/run_regression_tests.sh [--quick|--full|--user USER_NAME]
#
# Options:
#   --quick      Run only quick smoke tests (< 10 seconds)
#   --full       Run full regression suite (default)
#   --user NAME  Run tests for specific user (default: Sanjay)
#   --coverage   Generate coverage report
#

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
MODE="full"
USER_NAME="Sanjay"
COVERAGE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            MODE="quick"
            shift
            ;;
        --full)
            MODE="full"
            shift
            ;;
        --user)
            USER_NAME="$2"
            shift 2
            ;;
        --coverage)
            COVERAGE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--quick|--full|--user USER_NAME|--coverage]"
            exit 1
            ;;
    esac
done

# Change to project root
cd "$(dirname "$0")/.."

echo -e "${YELLOW}=====================================${NC}"
echo -e "${YELLOW}  PFAS Regression Test Suite${NC}"
echo -e "${YELLOW}=====================================${NC}"
echo ""
echo "Mode: $MODE"
echo "User: $USER_NAME"
echo ""

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Set PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Run tests based on mode
if [ "$MODE" == "quick" ]; then
    echo -e "${YELLOW}Running quick smoke tests...${NC}"
    pytest tests/regression/test_sanjay_regression.py::TestQuickSmokeTest -v --tb=short
elif [ "$MODE" == "full" ]; then
    echo -e "${YELLOW}Running full regression suite...${NC}"

    if [ "$COVERAGE" == true ]; then
        pytest tests/regression/test_sanjay_regression.py -v --tb=short \
            --cov=src/pfas \
            --cov-report=term-missing \
            --cov-report=html:coverage_html

        echo ""
        echo -e "${GREEN}Coverage report generated in coverage_html/index.html${NC}"
    else
        pytest tests/regression/test_sanjay_regression.py -v --tb=short
    fi
fi

# Check exit status
if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}=====================================${NC}"
    echo -e "${GREEN}  ✓ All regression tests passed!${NC}"
    echo -e "${GREEN}=====================================${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}=====================================${NC}"
    echo -e "${RED}  ✗ Regression tests failed!${NC}"
    echo -e "${RED}=====================================${NC}"
    exit 1
fi
