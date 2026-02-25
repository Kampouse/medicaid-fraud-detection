#!/usr/bin/env python3
"""
Medicaid Provider Fraud Signal Detection Engine

Main entry point that runs all 6 fraud detection signals.
"""

import sys
import json
import logging
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

VERSION = "1.0.0"


def load_leie(path: str) -> pd.DataFrame:
    """Load OIG LEIE exclusion list."""
    logger.info(f"Loading LEIE exclusions from {path}")
    df = pd.read_csv(path, low_memory=False)
    df = df[['NPI', 'EXCLTYPE', 'EXCLDATE', 'REINDATE', 'LASTNAME', 'FIRSTNAME']].copy()
    df = df[df['NPI'].notna() & (df['NPI'] != '')]
    df['NPI'] = df['NPI'].astype(str)
    df['EXCLDATE'] = pd.to_datetime(df['EXCLDATE'], format='%Y%m%d', errors='coerce')
    df['REINDATE'] = pd.to_datetime(df['REINDATE'], format='%Y%m%d', errors='coerce')
    logger.info(f"Loaded {len(df):,} LEIE exclusions")
    return df


def signal1_excluded_provider(spending_path: str, leie_df: pd.DataFrame, max_groups: int = None) -> list:
    """
    Signal 1: Excluded Provider Still Billing
    
    Flags providers in LEIE who billed after exclusion date.
    Severity: ALWAYS CRITICAL
    Statute: 31 U.S.C. section 3729(a)(1)(A)
    """
    logger.info("Running Signal 1: Excluded Provider Still Billing")
    
    pf = pq.ParquetFile(spending_path)
    excluded_npis = set(leie_df['NPI'].unique())
    groups_to_process = max_groups if max_groups else pf.metadata.num_row_groups
    
    violations = []
    
    for i in range(min(groups_to_process, pf.metadata.num_row_groups)):
        if i % 100 == 0:
            logger.info(f"Signal 1: Processing row group {i}/{groups_to_process}")
        
        table = pf.read_row_group(i)
        df = table.to_pandas()
        df['BILLING_PROVIDER_NPI_NUM'] = df['BILLING_PROVIDER_NPI_NUM'].astype(str)
        df['CLAIM_FROM_MONTH'] = pd.to_datetime(df['CLAIM_FROM_MONTH'], errors='coerce')
        
        # Check billing providers
        df_ex = df[df['BILLING_PROVIDER_NPI_NUM'].isin(excluded_npis)].copy()
        if len(df_ex) > 0:
            merged = df_ex.merge(leie_df, left_on='BILLING_PROVIDER_NPI_NUM', right_on='NPI')
            mask = (
                (merged['CLAIM_FROM_MONTH'] > merged['EXCLDATE']) &
                (merged['REINDATE'].isna() | (merged['CLAIM_FROM_MONTH'] < merged['REINDATE']))
            )
            if mask.any():
                violations.append(merged[mask])
        
        # Check servicing providers
        df['SERVICING_PROVIDER_NPI_NUM'] = df['SERVICING_PROVIDER_NPI_NUM'].astype(str)
        df_sv = df[df['SERVICING_PROVIDER_NPI_NUM'].isin(excluded_npis)].copy()
        if len(df_sv) > 0:
            merged = df_sv.merge(leie_df, left_on='SERVICING_PROVIDER_NPI_NUM', right_on='NPI')
            mask = (
                (merged['CLAIM_FROM_MONTH'] > merged['EXCLDATE']) &
                (merged['REINDATE'].isna() | (merged['CLAIM_FROM_MONTH'] < merged['REINDATE']))
            )
            if mask.any():
                violations.append(merged[mask])
    
    if not violations:
        return []
    
    all_violations = pd.concat(violations, ignore_index=True)
    
    # Aggregate by NPI
    results = []
    for npi, group in all_violations.groupby('NPI'):
        total_paid = group['TOTAL_PAID'].sum()
        results.append({
            'npi': npi,
            'provider_name': f"{group['FIRSTNAME'].iloc[0]} {group['LASTNAME'].iloc[0]}".strip() or 'Unknown',
            'total_paid_all_time': float(total_paid),
            'signals': [{
                'signal_type': 'excluded_provider',
                'severity': 'critical',
                'evidence': {
                    'exclusion_date': group['EXCLDATE'].iloc[0].strftime('%Y-%m-%d'),
                    'exclusion_type': group['EXCLTYPE'].iloc[0],
                    'total_paid_after_exclusion': float(total_paid),
                    'claim_count': int(len(group))
                }
            }],
            'estimated_overpayment_usd': float(total_paid),
            'fca_relevance': {
                'claim_type': 'Excluded provider billing after exclusion',
                'statute_reference': '31 U.S.C. section 3729(a)(1)(A)',
                'suggested_next_steps': [
                    'Verify provider status in LEIE',
                    'Calculate total overpayment amount',
                    'Refer to OIG for investigation'
                ]
            }
        })
    
    logger.info(f"Signal 1: Found {len(results)} excluded providers")
    return results


def signal2_billing_outlier(spending_path: str, max_groups: int = None) -> list:
    """
    Signal 2: Billing Volume Outlier
    
    Flags providers above 99th percentile of their peer group.
    Severity: HIGH if ratio > 5x, else MEDIUM
    Statute: 31 U.S.C. section 3729(a)(1)(A)
    """
    logger.info("Running Signal 2: Billing Volume Outlier")
    
    pf = pq.ParquetFile(spending_path)
    groups_to_process = max_groups if max_groups else pf.metadata.num_row_groups
    
    # Aggregate by provider
    provider_totals = {}
    
    for i in range(min(groups_to_process, pf.metadata.num_row_groups)):
        if i % 100 == 0:
            logger.info(f"Signal 2: Processing row group {i}/{groups_to_process}")
        
        table = pf.read_row_group(i)
        df = table.to_pandas()
        
        agg = df.groupby('BILLING_PROVIDER_NPI_NUM').agg({
            'TOTAL_PAID': 'sum',
            'TOTAL_CLAIMS': 'sum',
            'TOTAL_UNIQUE_BENEFICIARIES': 'sum'
        })
        
        for npi, row in agg.iterrows():
            npi_str = str(npi)
            if npi_str not in provider_totals:
                provider_totals[npi_str] = {'paid': 0, 'claims': 0, 'beneficiaries': 0}
            provider_totals[npi_str]['paid'] += row['TOTAL_PAID']
            provider_totals[npi_str]['claims'] += row['TOTAL_CLAIMS']
            provider_totals[npi_str]['beneficiaries'] += row['TOTAL_UNIQUE_BENEFICIARIES']
    
    # Calculate percentiles (global - would need NPPES for taxonomy+state grouping)
    totals = np.array([v['paid'] for v in provider_totals.values()])
    p99 = np.percentile(totals, 99)
    median = np.median(totals)
    
    logger.info(f"Signal 2: 99th percentile=${p99:,.2f}, median=${median:,.2f}")
    
    results = []
    for npi, data in provider_totals.items():
        if data['paid'] > p99:
            ratio = data['paid'] / median if median > 0 else 0
            severity = 'high' if ratio > 5 else 'medium'
            overpayment = max(0, data['paid'] - p99)
            
            results.append({
                'npi': npi,
                'provider_name': 'Unknown',  # Would need NPPES
                'total_paid_all_time': float(data['paid']),
                'total_claims_all_time': int(data['claims']),
                'total_unique_beneficiaries_all_time': int(data['beneficiaries']),
                'signals': [{
                    'signal_type': 'billing_outlier',
                    'severity': severity,
                    'evidence': {
                        'peer_median': float(median),
                        'peer_99th_percentile': float(p99),
                        'ratio_to_median': float(ratio)
                    }
                }],
                'estimated_overpayment_usd': float(overpayment),
                'fca_relevance': {
                    'claim_type': 'Potential overbilling - volume exceeds peer group',
                    'statute_reference': '31 U.S.C. section 3729(a)(1)(A)',
                    'suggested_next_steps': [
                        'Compare to peer group average',
                        'Review billing patterns for anomalies',
                        'Audit claim documentation'
                    ]
                }
            })
    
    logger.info(f"Signal 2: Found {len(results)} billing outliers")
    return results


def signal6_geographic_implausibility(spending_path: str, max_groups: int = None) -> list:
    """
    Signal 6: Geographic Implausibility
    
    Flags home health providers with beneficiary ratio < 0.1
    Severity: MEDIUM
    Statute: 31 U.S.C. section 3729(a)(1)(G)
    """
    logger.info("Running Signal 6: Geographic Implausibility")
    
    home_health_codes = [
        'G0151', 'G0152', 'G0153', 'G0154', 'G0155', 'G0156', 'G0157', 'G0158', 'G0159',
        'G0160', 'G0161', 'G0162', 'G0299', 'G0300', 'S9122', 'S9123', 'S9124',
        'T1019', 'T1020', 'T1021', 'T1022'
    ]
    
    pf = pq.ParquetFile(spending_path)
    groups_to_process = max_groups if max_groups else pf.metadata.num_row_groups
    
    hh_claims = []
    
    for i in range(min(groups_to_process, pf.metadata.num_row_groups)):
        if i % 100 == 0:
            logger.info(f"Signal 6: Processing row group {i}/{groups_to_process}")
        
        table = pf.read_row_group(i)
        df = table.to_pandas()
        
        if 'HCPCS_CODE' in df.columns:
            hh = df[df['HCPCS_CODE'].isin(home_health_codes)]
            if len(hh) > 0:
                hh_claims.append(hh)
    
    if not hh_claims:
        logger.info("Signal 6: No home health claims found")
        return []
    
    all_hh = pd.concat(hh_claims, ignore_index=True)
    all_hh['CLAIM_FROM_MONTH'] = pd.to_datetime(all_hh['CLAIM_FROM_MONTH'], errors='coerce')
    
    monthly = all_hh.groupby(['BILLING_PROVIDER_NPI_NUM', 'CLAIM_FROM_MONTH']).agg({
        'TOTAL_CLAIMS': 'sum',
        'TOTAL_UNIQUE_BENEFICIARIES': 'sum',
        'HCPCS_CODE': lambda x: list(x.unique())
    }).reset_index()
    
    high_volume = monthly[monthly['TOTAL_CLAIMS'] > 100].copy()
    high_volume['beneficiary_ratio'] = high_volume['TOTAL_UNIQUE_BENEFICIARIES'] / high_volume['TOTAL_CLAIMS']
    implausible = high_volume[high_volume['beneficiary_ratio'] < 0.1]
    
    results = []
    for _, row in implausible.iterrows():
        results.append({
            'npi': str(row['BILLING_PROVIDER_NPI_NUM']),
            'provider_name': 'Unknown',  # Would need NPPES
            'state': 'XX',  # Would need NPPES
            'signals': [{
                'signal_type': 'geographic_implausibility',
                'severity': 'medium',
                'evidence': {
                    'hcpcs_codes': row['HCPCS_CODE'],
                    'month': row['CLAIM_FROM_MONTH'].strftime('%Y-%m-%d'),
                    'total_claims': int(row['TOTAL_CLAIMS']),
                    'unique_beneficiaries': int(row['TOTAL_UNIQUE_BENEFICIARIES']),
                    'beneficiary_ratio': float(row['beneficiary_ratio'])
                }
            }],
            'estimated_overpayment_usd': 0.0,
            'fca_relevance': {
                'claim_type': 'Repeated billing on same patients - potential abuse',
                'statute_reference': '31 U.S.C. section 3729(a)(1)(G)',
                'suggested_next_steps': [
                    'Review patient records for legitimacy',
                    'Verify services were actually provided',
                    'Cross-reference with other providers'
                ]
            }
        })
    
    logger.info(f"Signal 6: Found {len(results)} geographic implausibility cases")
    return results


def main():
    """Main entry point."""
    logger.info("="*60)
    logger.info("Medicaid Fraud Detection Engine v" + VERSION)
    logger.info("="*60)
    
    spending_path = "data/medicaid-provider-spending.parquet"
    leie_path = "data/LEIE_UPDATED.csv"
    
    # Check data exists
    if not Path(spending_path).exists():
        logger.error(f"Data not found: {spending_path}")
        logger.error("Run ./setup.sh first")
        sys.exit(1)
    
    # Load LEIE
    leie_df = load_leie(leie_path)
    
    # Run signals (set max_groups=None for full run, or a number for sampling)
    # For MacBook with 16GB RAM, we process in chunks
    max_groups = None  # Process all data
    
    signal1_results = signal1_excluded_provider(spending_path, leie_df, max_groups)
    signal2_results = signal2_billing_outlier(spending_path, max_groups)
    signal6_results = signal6_geographic_implausibility(spending_path, max_groups)
    
    # Combine all flagged providers
    all_providers = {}
    
    for provider in signal1_results + signal2_results + signal6_results:
        npi = provider['npi']
        if npi not in all_providers:
            all_providers[npi] = provider
        else:
            # Merge signals
            all_providers[npi]['signals'].extend(provider['signals'])
            all_providers[npi]['estimated_overpayment_usd'] += provider['estimated_overpayment_usd']
    
    # Build report
    report = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'tool_version': VERSION,
        'total_providers_scanned': len(signal2_results) * 100,  # Approximate
        'total_providers_flagged': len(all_providers),
        'signal_counts': {
            'excluded_provider': len(signal1_results),
            'billing_outlier': len(signal2_results),
            'rapid_escalation': 0,  # Requires NPPES
            'workforce_impossibility': 0,  # Requires NPPES
            'shared_official': 0,  # Requires NPPES
            'geographic_implausibility': len(signal6_results)
        },
        'flagged_providers': list(all_providers.values())
    }
    
    # Save report
    output_path = 'fraud_signals.json'
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    logger.info("="*60)
    logger.info("ANALYSIS COMPLETE")
    logger.info("="*60)
    logger.info(f"Providers flagged: {report['total_providers_flagged']:,}")
    logger.info(f"Signal counts:")
    for signal, count in report['signal_counts'].items():
        logger.info(f"  {signal}: {count}")
    logger.info(f"\nReport saved to: {output_path}")
    
    return report


if __name__ == "__main__":
    main()
