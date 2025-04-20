#!/bin/bash
set -e

# Compile and run the version updater
echo "Compiling version updater..."
rustc -o target/update_version scripts/update_version.rs

echo "Running version updater..."
./target/update_version

echo "Version update complete!"
