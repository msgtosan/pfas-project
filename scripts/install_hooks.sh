#!/bin/bash
#
# Install Git Hooks for PFAS Project
#
# This script installs the pre-commit hook that runs regression tests
# before each commit.
#

set -e

# Change to repository root
cd "$(git rev-parse --show-toplevel)"

echo "Installing git hooks for PFAS project..."

# Create symbolic link for pre-commit hook
if [ -L .git/hooks/pre-commit ]; then
    echo "  Removing existing pre-commit hook symlink..."
    rm .git/hooks/pre-commit
elif [ -f .git/hooks/pre-commit ]; then
    echo "  Backing up existing pre-commit hook to .git/hooks/pre-commit.backup..."
    mv .git/hooks/pre-commit .git/hooks/pre-commit.backup
fi

ln -s ../../.pre-commit-hook .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
chmod +x .pre-commit-hook

echo "  ✓ Pre-commit hook installed"
echo ""
echo "The hook will run quick smoke tests before each commit."
echo ""
echo "To skip the hook for a specific commit (use sparingly):"
echo "  git commit --no-verify"
echo ""
echo "To uninstall the hook:"
echo "  rm .git/hooks/pre-commit"
echo ""
