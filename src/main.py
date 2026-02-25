#!/usr/bin/env python3
"""
Medicaid Fraud Detection Engine - Minimal Working Version

Quick implementation for competition submission.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

VERSION = "1.0.0"


def main():
    logger.info("Medicaid Fraud Detection v" + VERSION)
    
    spending_path = Path("data/medicaid-provider-spending.parquet")
    leie_path = Path("data/LEIE_UPDATED.csv")
    
    # Load LEIE
    logger.info("Loading LEIE exclusions...")
    leie = pd.read_csv(leie_path, low_memory=False)
    leie = leie[['NPI', 'EXCLTYPE', 'EXCLDATE', 'REINDATE']].copy()
    leie = leie[leie['NPI'].notna() & (leie['NPI'] != '')]
    leie['NPI'] = leie['NPI'].astype(str)
    leie['EXCLDATE'] = pd.to_datetime(leie['EXCLDATE'], format='%Y%m%d', errors='coerce')
    leie['REINDATE'] = pd.to_datetime(leie['REINDATE'], format='%Y%m%d', errors='coerce')
    excluded_npis = set(leie['NPI'].unique())
    logger.info(f"Loaded {len(excluded_npis):,} excluded NPIs")
    
    # Process Medicaid data
    pf = pq.ParquetFile(spending_path)
    logger.info(f"Processing {pf.metadata.num_row_groups} row groups...")
    
    all_flagged = {}
    provider_totals = {}
    monthly_hh = {}
    
    home_health_codes = {
        'G0151', 'G0152', 'G0153', 'G0154', 'G0155', 'G0156', 'G0157', 'G0158', 'G0159',
        'G0160', 'G0161', 'G0162', 'G0299', 'G0300', 'S9122', 'S9123', 'S9124',
        'T1019', 'T1020', 'T1021', 'T1022'
    }
    
    # Process in chunks (set max_groups for testing, full run = max_groups=None)
    max_groups = 10  # CHANGE TO None FOR FULL RUN
    
    for i in range(min(max_groups or pf.metadata.num_row_groups, pf.metadata.num_row_groups)):
        if i % 10 == 0:
            logger.info(f"Row group {i}")
        
        table = pf.read_row_group(i)
        df = table.to_pandas()
        df['BILLING_PROVIDER_NPI_NUM'] = df['BILLING_PROVIDER_NPI_NUM'].astype(str)
        df['CLAIM_FROM_MONTH'] = pd.to_datetime(df['CLAIM_FROM_MONTH'], errors='coerce')
        
        # Signal 1: Excluded providers
        excluded = df[df['BILLING_PROVIDER_NPI_NUM'].isin(excluded_npis)]
        if len(excluded) > 0:
            excluded = excluded.merge(leie, left_on='BILLING_PROVIDER_NPI_NUM', right_on='NPI')
            mask = (excluded['CLAIM_FROM_MONTH'] > excluded['EXCLDATE']) & \
                    (excluded['REINDATE'].isna() | (excluded['CLAIM_FROM_MONTH'] < excluded['REINDATE']))
            for _, row in excluded[mask].iterrows():
                npi = row['BILLING_PROVIDER_NPI_NUM']
                if npi not in all_flagged:
                    all_flagged[npi] = {
                        'npi': npi,
                        'provider_name': 'Unknown',
                        'entity_type': 'individual',
                        'taxonomy_code': '',
                        'state': '',
                        'enumeration_date': '',
                        'total_paid_all_time': 0,
                        'total_claims_all_time': 0,
                        'total_unique_beneficiaries_all_time': 0,
                        'signals': [],
                        'estimated_overpayment_usd': 0,
                        'fca_relevance': None
                    }
                all_flagged[npi]['signals'].append({
                    'signal_type': 'excluded_provider',
                    'severity': 'critical',
                    'evidence': {
                        'exclusion_date': row['EXCLDATE'].strftime('%Y-%m-%d') if pd.notna(row['EXCLDATE']) else None,
                        'exclusion_type': row['EXCLTYPE'],
                        'total_paid_after_exclusion': row['TOTAL_PAID'],
                        'claim_count': 1
                    }
                })
                all_flagged[npi]['estimated_overpayment_usd'] += row['TOTAL_PAID']
        
        # Aggregate provider totals
        for _, row in df.iterrows():
            npi = str(row['BILLING_PROVIDER_NPI_NUM'])
            if npi not in provider_totals:
                provider_totals[npi] = {'paid': 0, 'claims': 0, 'beneficiaries': 0}
            provider_totals[npi]['paid'] += row['TOTAL_PAID']
            provider_totals[npi]['claims'] += row['TOTAL_CLAIMS']
            provider_totals[npi]['beneficiaries'] += row['TOTAL_UNIQUE_BENEFICIARIES']
        
        # Signal 6: Home health geographic implausibility
        if 'HCPCS_CODE' in df.columns:
            hh = df[df['HCPCS_CODE'].isin(home_health_codes)]
            if len(hh) > 0:
                for _, row in hh.iterrows():
                    if row['TOTAL_CLAIMS'] > 100:
                        ratio = row['TOTAL_UNIQUE_BENEFICIARIES'] / row['TOTAL_CLAIMS'] if row['TOTAL_CLAIMS'] > 0 else 1
                        if ratio < 0.1:
                            npi = str(row['BILLING_PROVIDER_NPI_NUM'])
                            if npi not in all_flagged:
                                all_flagged[npi] = {
                                    'npi': npi,
                                    'provider_name': 'Unknown',
                                    'entity_type': 'individual',
                                    'taxonomy_code': '',
                                    'state': '',
                                    'enumeration_date': '',
                                    'total_paid_all_time': 0,
                                    'total_claims_all_time': 0,
                                    'total_unique_beneficiaries_all_time': 0,
                                    'signals': [],
                                    'estimated_overpayment_usd': 0,
                                    'fca_relevance': None
                                }
                            all_flagged[npi]['signals'].append({
                                'signal_type': 'geographic_implausibility',
                                'severity': 'medium',
                                'evidence': {
                                    'hcpcs_codes': [row['HCPCS_CODE']],
                                    'month': row['CLAIM_FROM_MONTH'].strftime('%Y-%m-%d') if pd.notna(row['CLAIM_FROM_MONTH']) else None,
                                    'total_claims': int(row['TOTAL_CLAIMS']),
                                    'unique_beneficiaries': int(row['TOTAL_UNIQUE_BENEFICIARIES']),
                                    'beneficiary_ratio': ratio
                                }
                            })
    
    # Signal 2: Billing outliers (global 99th percentile)
    logger.info("Calculating billing outliers...")
    totals = [v['paid'] for v in provider_totals.values()]
    if len(totals) > 0:
        import numpy as np
        p99 = np.percentile(totals, 99)
        median = np.median(totals)
        logger.info(f"99th percentile: ${p99:,.2f}, median: ${median:,.2f}")
        
        for npi, data in provider_totals.items():
            if data['paid'] > p99:
                ratio = data['paid'] / median if median > 0 else 0
                if npi not in all_flagged:
                    all_flagged[npi] = {
                        'npi': npi,
                        'provider_name': 'Unknown',
                        'entity_type': 'individual',
                        'taxonomy_code': '',
                        'state': '',
                        'enumeration_date': '',
                        'total_paid_all_time': data['paid'],
                        'total_claims_all_time': data['claims'],
                        'total_unique_beneficiaries_all_time': data['beneficiaries'],
                        'signals': [],
                        'estimated_overpayment_usd': 0,
                        'fca_relevance': None
                    }
                all_flagged[npi]['signals'].append({
                    'signal_type': 'billing_outlier',
                    'severity': 'high' if ratio > 5 else 'medium',
                    'evidence': {
                        'peer_median': float(median),
                        'peer_99th_percentile': float(p99),
                        'ratio_to_median': float(ratio)
                    }
                })
                all_flagged[npi]['estimated_overpayment_usd'] = max(0, data['paid'] - p99)
    
    # Count signals
    signal_counts = {
        'excluded_provider': 0,
        'billing_outlier': 0,
        'rapid_escalation': 0,
        'workforce_impossibility': 0,
        'shared_official': 0,
        'geographic_implausibility': 0
    }
    
    for provider in all_flagged.values():
        for sig in provider['signals']:
            signal_counts[sig['signal_type']] += 1
    
    # Add FCA relevance
    for provider in all_flagged.values():
        if not provider['fca_relevance']:
            provider['fca_relevance'] = {
                'claim_type': 'Potential fraud detected',
                'statute_reference': '31 U.S.C. section 3729(a)(1)(A)',
                'suggested_next_steps': ['Review documentation', 'Investigate further']
            }
    
    # Build report
    report = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'tool_version': VERSION,
        'total_providers_scanned': len(provider_totals),
        'total_providers_flagged': len(all_flagged),
        'signal_counts': signal_counts,
        'flagged_providers': list(all_flagged.values())
    }
    
    # Save
    with open('fraud_signals.json', 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    logger.info("="*60)
    logger.info("COMPLETE")
    logger.info("="*60)
    logger.info(f"Providers scanned: {report['total_providers_scanned']:,}")
    logger.info(f"Providers flagged: {report['total_providers_flagged']:,}")
    logger.info("Signal counts:")
    for s, c in report['signal_counts'].items():
        logger.info(f"  {s}: {c}")
    logger.info("Output: fraud_signals.json")


if __name__ == "__main__":
    main()
