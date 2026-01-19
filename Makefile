# Makefile for PFAS Project
#
# Common development tasks

.PHONY: help test test-quick test-full test-regression test-unit test-integration coverage install-hooks clean

help:
	@echo "PFAS Project - Development Commands"
	@echo ""
	@echo "Testing:"
	@echo "  make test              Run all tests"
	@echo "  make test-quick        Run quick smoke tests only"
	@echo "  make test-regression   Run full regression suite"
	@echo "  make test-unit         Run unit tests only"
	@echo "  make test-integration  Run integration tests only"
	@echo "  make coverage          Generate test coverage report"
	@echo ""
	@echo "Setup:"
	@echo "  make install-hooks     Install git pre-commit hooks"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean             Remove generated files"

test: test-unit test-regression
	@echo "✓ All tests passed"

test-quick:
	@./scripts/run_regression_tests.sh --quick

test-regression:
	@./scripts/run_regression_tests.sh --full

test-unit:
	@echo "Running unit tests..."
	@PYTHONPATH=src pytest tests/unit -v --tb=short

test-integration:
	@echo "Running integration tests..."
	@PYTHONPATH=src pytest tests/integration -v --tb=short

coverage:
	@./scripts/run_regression_tests.sh --full --coverage

install-hooks:
	@./scripts/install_hooks.sh

clean:
	@echo "Cleaning up generated files..."
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf .pytest_cache coverage_html .coverage
	@echo "✓ Cleanup complete"
