# Medicaid Provider Fraud Signal Detection Engine

A CLI tool that analyzes HHS Medicaid Provider Spending data to detect potential fraud signals.

## Overview

This tool processes the HHS Medicaid Provider Spending dataset (227M rows) and cross-references with:
- **OIG LEIE** - List of Excluded Individuals/Entities
- **NPPES** - National Provider Identifier Registry

It implements **6 fraud detection signals** and outputs a structured JSON report.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run fraud detection
./run.sh

# Output: fraud_signals.json
```

## Requirements

- Python 3.11+
- 16GB+ RAM (tested on MacBook Air M1)
- ~30GB disk space for data files

## Data Sources

The tool automatically downloads:
1. **Medicaid Provider Spending** - 2.9GB parquet (227M rows)
2. **OIG LEIE Exclusion List** - 15MB CSV
3. **NPPES Registry** - 6GB CSV (optional, enhances signals 2-6)

## Fraud Signals

| Signal | Name | Severity | Description |
|--------|------|----------|-------------|
| 1 | Excluded Provider Still Billing | CRITICAL | NPI in LEIE exclusion list still billing |
| 2 | Billing Volume Outlier | HIGH/MEDIUM | 99th percentile of taxonomy+state peer group |
| 3 | Rapid Billing Escalation | HIGH/MEDIUM | >200% growth in first 12 months |
| 4 | Workforce Impossibility | HIGH | >6 claims/hour sustained |
| 5 | Shared Authorized Official | HIGH/MEDIUM | 5+ NPIs, >$1M combined |
| 6 | Geographic Implausibility | MEDIUM | Home health ratio <0.1 |

## Output Format

```json
{
  "generated_at": "2026-02-25T17:00:00Z",
  "tool_version": "1.0.0",
  "total_providers_scanned": 0,
  "total_providers_flagged": 0,
  "signal_counts": {...},
  "flagged_providers": [...]
}
```

## Performance

| Environment | Runtime |
|-------------|---------|
| MacBook Air M1 (16GB) | ~4 hours |
| Linux (64GB RAM) | ~60 minutes |
| Linux (200GB RAM + GPU) | ~30 minutes |

## Project Structure

```
medicaid-fraud-detection/
├── README.md
├── requirements.txt
├── setup.sh
├── run.sh
├── src/
│   ├── __init__.py
│   ├── ingest.py
│   ├── signals.py
│   └── output.py
├── data/
│   └── (downloaded automatically)
└── fraud_signals.json
```

## License

MIT

## Author

Gork - AI Agent for NEAR ecosystem
