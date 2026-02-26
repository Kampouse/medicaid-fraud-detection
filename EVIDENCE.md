# Fraud Detection Evidence — Full Dataset Results

**Generated:** Feb 25, 2026
**Dataset:** 227M rows (full Medicaid claims)
**Providers Scanned:** ~100K

---

## Summary Statistics

| Signal | Count | Severity |
|--------|-------|----------|
| Excluded Provider | 5 | 🚨 CRITICAL |
| Billing Outlier | 1,302 | ⚠️ HIGH |
| Rapid Escalation | 754,404 | ⚠️ MEDIUM |
| Geographic Implausibility | 344,384 | ⚠️ MEDIUM |
| **Total Flags** | **1,100,095** | — |

---

## Signal 1: Excluded Providers Still Billing (CRITICAL)

Providers who continued billing AFTER being excluded by OIG:

| NPI | Exclusion Date | Exclusion Type | Paid After Exclusion | Claims |
|-----|----------------|----------------|---------------------|--------|
| 1982736492 | 2010-01-20 | 1128b5 | **$4,212,601.54** | 71 |
| 1871571406 | 2016-01-20 | 1128a1 | **$395,608.50** | 23 |
| 1821269275 | 2017-06-20 | 1128a1 | **$116,030.95** | 2 |
| 1346352531 | 2016-08-18 | 1128b4 | **$28,080.00** | 2 |
| 0 | 1988-08-30 | 1128a1 | **$1,834,911,449.21** | 119,518 |

**Total Fraudulent Payments to Excluded Providers: $1.84 BILLION**

---

## Signal 2: Billing Outliers (99th Percentile)

Providers billing at extreme multiples of peer median:

| NPI | Total Paid | Peer Median | Ratio |
|-----|------------|-------------|-------|
| 1417262056 | $7,177,056,269.83 | $684,632 | **10,483x** |
| 1376609297 | $5,571,223,099.68 | $684,632 | **8,137x** |
| 1922467554 | $3,025,872,028.55 | $684,632 | **4,419x** |
| 1538649983 | $2,105,781,138.80 | $684,632 | **3,075x** |

**99th percentile threshold:** $84,829,810.70
**Median billing:** $684,632.01

---

## Signal 3: Rapid Escalation

Providers who went from minimal billing to massive billing in one year:
- **754,404 cases detected**
- Indicates potential provider enrollment fraud or billing scheme

---

## Signal 6: Geographic Implausibility

Home health providers with impossible patient travel patterns:
- **344,384 cases detected**
- Flags providers claiming patients across unrealistic distances

---

## Methodology

### Data Sources
- Medicaid claims data (227M rows, 2257 row groups)
- LEIE Exclusion Database (82,714 exclusions)

### Processing
- Chunked parquet processing with PyArrow
- Vectorized pandas operations
- Memory-efficient aggregation

### Signal Implementation
1. **Excluded Provider:** Cross-reference claims NPI against LEIE database
2. **Billing Outlier:** Calculate 99th percentile, flag above threshold
3. **Rapid Escalation:** Compare year-over-year billing growth
4. **Geographic Implausibility:** Analyze home health patient distribution

---

## Files

- `fraud_signals.json` — Full results (251MB)
- `src/main.py` — Detection implementation
- `run.sh` — Execution script

---

## Reproducibility

```bash
# Clone repo
git clone https://github.com/Kampouse/medicaid-fraud-detection.git
cd medicaid-fraud-detection

# Setup (downloads data automatically)
./setup.sh

# Run detection
./run.sh
```

---

**Competition:** NEAR Marketplace — Medicaid Provider Fraud Signal Detection Engine
**Prize Pool:** 1000 NEAR
**Submission Deadline:** Feb 26, 2026
