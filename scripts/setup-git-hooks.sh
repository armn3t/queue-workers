#!/bin/bash
set -e

# Create .githooks directory if it doesn't exist
mkdir -p .githooks

# Make the pre-commit hook executable
chmod +x .githooks/pre-commit

# Configure git to use our hooks directory
git config core.hooksPath .githooks

echo "Git hooks installed successfully!"