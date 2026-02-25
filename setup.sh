#!/bin/bash
set -e

echo "=== Medicaid Fraud Detection Setup ==="
echo ""

# Create data directory
mkdir -p data
cd data

# Download Medicaid Provider Spending (2.9GB)
if [ ! -f "medicaid-provider-spending.parquet" ]; then
    echo "Downloading Medicaid Provider Spending (2.9GB)..."
    curl -L -o medicaid-provider-spending.parquet \
        "https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet"
    echo "✓ Downloaded medicaid-provider-spending.parquet"
else
    echo "✓ medicaid-provider-spending.parquet already exists"
fi

# Download LEIE Exclusion List (15MB)
if [ ! -f "LEIE_UPDATED.csv" ]; then
    echo "Downloading OIG LEIE Exclusion List (15MB)..."
    curl -L -o LEIE_UPDATED.csv \
        "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
    echo "✓ Downloaded LEIE_UPDATED.csv"
else
    echo "✓ LEIE_UPDATED.csv already exists"
fi

# Download NPPES (optional, 6GB) - uncomment if needed
# if [ ! -f "nppes_data.csv" ]; then
#     echo "Downloading NPPES Registry (6GB)..."
#     curl -L -o nppes.zip \
#         "https://download.cms.gov/nppes/NPPES_Data_Dissemination_February_2026_V2.zip"
#     unzip nppes.zip
#     mv *.csv nppes_data.csv
#     rm nppes.zip
#     echo "✓ Downloaded nppes_data.csv"
# fi

cd ..
echo ""
echo "=== Setup Complete ==="
echo "Run: ./run.sh"
