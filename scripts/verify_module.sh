#!/bin/bash
MODULE=$1

echo "=== Verifying Module: $MODULE ==="

# Step 1: Lint
echo ">>> Running linter..."
ruff check src/pfas/parsers/$MODULE/

# Step 2: Type check
echo ">>> Running type checker..."
mypy src/pfas/parsers/$MODULE/

# Step 3: Unit tests
echo ">>> Running unit tests..."
pytest tests/unit/test_parsers/test_$MODULE/ -v --cov=src/pfas/parsers/$MODULE --cov-fail-under=80

# Step 4: Integration tests (if exist)
if [ -f "tests/integration/test_${MODULE}_integration.py" ]; then
    echo ">>> Running integration tests..."
    pytest tests/integration/test_${MODULE}_integration.py -v
fi

echo "=== Module $MODULE Verified ==="