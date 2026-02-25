#!/bin/bash
set -e

echo "=== Running Medicaid Fraud Detection ==="
echo ""

# Check if data exists
if [ ! -f "data/medicaid-provider-spending.parquet" ]; then
    echo "Data not found. Run ./setup.sh first."
    exit 1
fi

# Run detection
python3 src/main.py

echo ""
echo "=== Complete ==="
echo "Output: fraud_signals.json"
