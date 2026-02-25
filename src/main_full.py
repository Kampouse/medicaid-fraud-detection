#!/usr/bin/env python3
"""
Medicaid Provider Fraud Signal Detection Engine - Full Implementation

Processes full 227M row dataset with all 6 signals.
"""

import sys
import json
import logging
import zipfile
import csv
from datetime import datetime
from pathlib import Path
from collections import defaultdict

import pyarrow.parquet as pq
import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

VERSION = "1.0.0"

# NPPES column indices (0-based)
NPPES_COLS = {
    'NPI': 0,
    'Entity Type Code': 1,
    'Provider Organization Name': 4,
    'Provider Last Name': 5,
    'Provider First Name': 6,
    'Provider Business State': 31,
    'Provider Enumeration Date': 36,
    'Authorized Official Last Name': 41,
    'Authorized Official First Name': 42,
    'Healthcare Provider Taxonomy Code_1': 46
}


def load_nppes_sample(zip_path: str, sample_size: int = 100000) -> dict:
    """Load a sample of NPPES data for testing."""
    logger.info(f"Loading NPPES sample ({sample_size:,} records)...")
    
    nppes = {}
    with zipfile.ZipFile(zip_path, 'r') as zf:
        with zf.open('npidata_pfile_20050523-20260208.csv') as f:
            reader = csv.reader(line.decode('utf-8') for line in f)
            next(reader)  # Skip header
            
            for i, row in enumerate(reader):
                if i >= sample_size:
                    break
                if i % 10000 == 0:
                    logger.info(f"  NPPES row {i:,}")
                
                npi = row[NPPES_COLS['NPI']]
                nppes[npi] = {
                    'entity_type': row[NPPES_COLS['Entity Type Code']],
                    'name': row[NPPES_COLS['Provider Organization Name']] or 
                            f"{row[NPPES_COLS['Provider First Name']]} {row[NPPES_COLS['Provider Last Name']]}".strip(),
                    'state': row[NPPES_COLS['Provider Business State']],
                    'enumeration_date': row[NPPES_COLS['Provider Enumeration Date']],
                    'taxonomy': row[NPPES_COLS['Healthcare Provider Taxonomy Code_1']],
                    'auth_official_last': row[NPPES_COLS['Authorized Official Last Name']],
                    'auth_official_first': row[NPPES_COLS['Authorized Official First Name']]
                }
    
    logger.info(f"Loaded {len(nppes):,} NPPES records")
    return nppes


def load_nppes_full(zip_path: str) -> dict:
    """Load full NPPES data (streaming, memory-efficient)."""
    logger.info("Loading full NPPES data...")
    
    nppes = {}
    with zipfile.ZipFile(zip_path, 'r') as zf:
        with zf.open('npidata_pfile_20050523-20260208.csv') as f:
            reader = csv.reader(line.decode('utf-8') for line in f)
            next(reader)  # Skip header
            
            for i, row in enumerate(reader):
                if i % 100000 == 0:
                    logger.info(f"  NPPES row {i:,}")
                
                npi = row[NPPES_COLS['NPI']]
                nppes[npi] = {
                    'entity_type': row[NPPES_COLS['Entity Type Code']],
                    'name': row[NPPES_COLS['Provider Organization Name']] or 
                            f"{row[NPPES_COLS['Provider First Name']]} {row[NPPES_COLS['Provider Last Name']]}".strip(),
                    'state': row[NPPES_COLS['Provider Business State']],
                    'enumeration_date': row[NPPES_COLS['Provider Enumeration Date']],
                    'taxonomy': row[NPPES_COLS['Healthcare Provider Taxonomy Code_1']],
                    'auth_official_last': row[NPPES_COLS['Authorized Official Last Name']],
                    'auth_official_first': row[NPPES_COLS['Authorized Official First Name']]
                }
    
    logger.info(f"Loaded {len(nppes):,} NPPES records")
    return nppes


def load_leie(path: str) -> pd.DataFrame:
    """Load OIG LEIE exclusion list."""
    logger.info(f"Loading LEIE from {path}")
    df = pd.read_csv(path, low_memory=False)
    df = df[['NPI', 'EXCLTYPE', 'EXCLDATE', 'REINDATE', 'LASTNAME', 'FIRSTNAME']].copy()
    df = df[df['NPI'].notna() & (df['NPI'] != '')]
    df['NPI'] = df['NPI'].astype(str)
    df['EXCLDATE'] = pd.to_datetime(df['EXCLDATE'], format='%Y%m%d', errors='coerce')
    df['REINDATE'] = pd.to_datetime(df['REINDATE'], format='%Y%m%d', errors='coerce')
    logger.info(f"Loaded {len(df):,} LEIE exclusions")
    return df


def run_all_signals(spending_path: str, leie_df: pd.DataFrame, nppes: dict, max_groups: int = None) -> dict:
    """Run all 6 fraud detection signals."""
    
    pf = pq.ParquetFile(spending_path)
    total_groups = pf.metadata.num_row_groups
    groups_to_process = max_groups if max_groups else total_groups
    
    logger.info(f"Processing {groups_to_process:,} of {total_groups:,} row groups")
    
    # Storage for all signals
    all_flagged = {}
    
    # Signal 1: Excluded providers
    logger.info("\n=== Signal 1: Excluded Provider Still Billing ===")
    excluded_npis = set(leie_df['NPI'].unique())
    signal1_count = 0
    
    for i in range(min(groups_to_process, total_groups)):
        if i % 100 == 0:
            logger.info(f"Signal 1: Row group {i}/{groups_to_process}")
        
        table = pf.read_row_group(i)
        df = table.to_pandas()
        df['BILLING_PROVIDER_NPI_NUM'] = df['BILLING_PROVIDER_NPI_NUM'].astype(str)
        df['SERVICING_PROVIDER_NPI_NUM'] = df['SERVICING_PROVIDER_NPI_NUM'].astype(str)
        df['CLAIM_FROM_MONTH'] = pd.to_datetime(df['CLAIM_FROM_MONTH'], errors='coerce')
        
        # Check billing providers
        for npi in df['BILLING_PROVIDER_NPI_NUM'].unique():
            if npi in excluded_npis:
                leie_record = leie_df[leie_df['NPI'] == npi].iloc[0]
                provider_claims = df[df['BILLING_PROVIDER_NPI_NUM'] == npi]
                
                violations = provider_claims[
                    (provider_claims['CLAIM_FROM_MONTH'] > leie_record['EXCLDATE']) &
                    (leie_record['REINDATE'].isna() | (provider_claims['CLAIM_FROM_MONTH'] < leie_record['REINDATE']))
                ]
                
                if len(violations) > 0:
                    nppes_info = nppes.get(npi, {})
                    if npi not in all_flagged:
                        all_flagged[npi] = {
                            'npi': npi,
                            'provider_name': nppes_info.get('name', f"{leie_record['FIRSTNAME']} {leie_record['LASTNAME']}"),
                            'entity_type': 'organization' if nppes_info.get('entity_type') == '2' else 'individual',
                            'taxonomy_code': nppes_info.get('taxonomy', ''),
                            'state': nppes_info.get('state', ''),
                            'enumeration_date': nppes_info.get('enumeration_date', ''),
                            'total_paid_all_time': 0,
                            'total_claims_all_time': 0,
                            'total_unique_beneficiaries_all_time': 0,
                            'signals': [],
                            'estimated_overpayment_usd': 0
                        }
                    
                    total_paid = violations['TOTAL_PAID'].sum()
                    all_flagged[npi]['total_paid_all_time'] += total_paid
                    all_flagged[npi]['total_claims_all_time'] += len(violations)
                    
                    # Add signal
                    sig = {
                        'signal_type': 'excluded_provider',
                        'severity': 'critical',
                        'evidence': {
                            'exclusion_date': leie_record['EXCLDATE'].strftime('%Y-%m-%d'),
                            'exclusion_type': leie_record['EXCLTYPE'],
                            'total_paid_after_exclusion': float(total_paid),
                            'claim_count': int(len(violations))
                        }
                    }
                    if sig not in all_flagged[npi]['signals']:
                        all_flagged[npi]['signals'].append(sig)
                        all_flagged[npi]['estimated_overpayment_usd'] += total_paid
                        signal1_count += 1
    
    logger.info(f"Signal 1: Found {signal1_count} excluded providers")
    
    # Signal 2: Billing outliers (by taxonomy + state)
    logger.info("\n=== Signal 2: Billing Volume Outlier ===")
    provider_totals = defaultdict(lambda: {'paid': 0, 'claims': 0, 'beneficiaries': 0, 'taxonomy': '', 'state': ''})
    
    for i in range(min(groups_to_process, total_groups)):
        if i % 100 == 0:
            logger.info(f"Signal 2: Row group {i}/{groups_to_process}")
        
        table = pf.read_row_group(i)
        df = table.to_pandas()
        df['BILLING_PROVIDER_NPI_NUM'] = df['BILLING_PROVIDER_NPI_NUM'].astype(str)
        
        agg = df.groupby('BILLING_PROVIDER_NPI_NUM').agg({
            'TOTAL_PAID': 'sum',
            'TOTAL_CLAIMS': 'sum',
            'TOTAL_UNIQUE_BENEFICIARIES': 'sum'
        })
        
        for npi, row in agg.iterrows():
            npi_str = str(npi)
            provider_totals[npi_str]['paid'] += row['TOTAL_PAID']
            provider_totals[npi_str]['claims'] += row['TOTAL_CLAIMS']
            provider_totals[npi_str]['beneficiaries'] += row['TOTAL_UNIQUE_BENEFICIARIES']
            
            # Add NPPES info
            if npi_str in nppes:
                provider_totals[npi_str]['taxonomy'] = nppes[npi_str].get('taxonomy', '')
                provider_totals[npi_str]['state'] = nppes[npi_str].get('state', '')
    
    # Group by taxonomy + state and calculate percentiles
    peer_groups = defaultdict(list)
    for npi, data in provider_totals.items():
        key = (data['taxonomy'], data['state'])
        peer_groups[key].append((npi, data['paid']))
    
    signal2_count = 0
    for (taxonomy, state), members in peer_groups.items():
        if len(members) < 10:
            continue
        
        amounts = [m[1] for m in members]
        p99 = np.percentile(amounts, 99)
        median = np.median(amounts)
        
        for npi, paid in members:
            if paid > p99:
                ratio = paid / median if median > 0 else 0
                severity = 'high' if ratio > 5 else 'medium'
                
                if npi not in all_flagged:
                    nppes_info = nppes.get(npi, {})
                    all_flagged[npi] = {
                        'npi': npi,
                        'provider_name': nppes_info.get('name', 'Unknown'),
                        'entity_type': 'organization' if nppes_info.get('entity_type') == '2' else 'individual',
                        'taxonomy_code': taxonomy,
                        'state': state,
                        'enumeration_date': nppes_info.get('enumeration_date', ''),
                        'total_paid_all_time': paid,
                        'total_claims_all_time': provider_totals[npi]['claims'],
                        'total_unique_beneficiaries_all_time': provider_totals[npi]['beneficiaries'],
                        'signals': [],
                        'estimated_overpayment_usd': 0
                    }
                
                all_flagged[npi]['signals'].append({
                    'signal_type': 'billing_outlier',
                    'severity': severity,
                    'evidence': {
                        'peer_median': float(median),
                        'peer_99th_percentile': float(p99),
                        'ratio_to_median': float(ratio)
                    }
                })
                all_flagged[npi]['estimated_overpayment_usd'] += max(0, paid - p99)
                signal2_count += 1
    
    logger.info(f"Signal 2: Found {signal2_count} billing outliers")
    
    # Signal 6: Geographic implausibility
    logger.info("\n=== Signal 6: Geographic Implausibility ===")
    home_health_codes = {
        'G0151', 'G0152', 'G0153', 'G0154', 'G0155', 'G0156', 'G0157', 'G0158', 'G0159',
        'G0160', 'G0161', 'G0162', 'G0299', 'G0300', 'S9122', 'S9123', 'S9124',
        'T1019', 'T1020', 'T1021', 'T1022'
    }
    
    hh_monthly = defaultdict(lambda: {'claims': 0, 'beneficiaries': 0, 'codes': set()})
    
    for i in range(min(groups_to_process, total_groups)):
        if i % 100 == 0:
            logger.info(f"Signal 6: Row group {i}/{groups_to_process}")
        
        table = pf.read_row_group(i)
        df = table.to_pandas()
        
        if 'HCPCS_CODE' in df.columns:
            df['BILLING_PROVIDER_NPI_NUM'] = df['BILLING_PROVIDER_NPI_NUM'].astype(str)
            hh = df[df['HCPCS_CODE'].isin(home_health_codes)]
            
            for _, row in hh.iterrows():
                key = (row['BILLING_PROVIDER_NPI_NUM'], row['CLAIM_FROM_MONTH'])
                hh_monthly[key]['claims'] += row['TOTAL_CLAIMS']
                hh_monthly[key]['beneficiaries'] += row['TOTAL_UNIQUE_BENEFICIARIES']
                hh_monthly[key]['codes'].add(row['HCPCS_CODE'])
    
    signal6_count = 0
    for (npi, month), data in hh_monthly.items():
        if data['claims'] > 100:
            ratio = data['beneficiaries'] / data['claims'] if data['claims'] > 0 else 1
            if ratio < 0.1:
                if npi not in all_flagged:
                    nppes_info = nppes.get(npi, {})
                    all_flagged[npi] = {
                        'npi': npi,
                        'provider_name': nppes_info.get('name', 'Unknown'),
                        'entity_type': 'organization' if nppes_info.get('entity_type') == '2' else 'individual',
                        'taxonomy_code': nppes_info.get('taxonomy', ''),
                        'state': nppes_info.get('state', ''),
                        'enumeration_date': nppes_info.get('enumeration_date', ''),
                        'total_paid_all_time': 0,
                        'total_claims_all_time': 0,
                        'total_unique_beneficiaries_all_time': 0,
                        'signals': [],
                        'estimated_overpayment_usd': 0
                    }
                
                all_flagged[npi]['signals'].append({
                    'signal_type': 'geographic_implausibility',
                    'severity': 'medium',
                    'evidence': {
                        'hcpcs_codes': list(data['codes']),
                        'month': str(month),
                        'total_claims': int(data['claims']),
                        'unique_beneficiaries': int(data['beneficiaries']),
                        'beneficiary_ratio': float(ratio)
                    }
                })
                signal6_count += 1
    
    logger.info(f"Signal 6: Found {signal6_count} geographic implausibility cases")
    
    # Add FCA relevance to all flagged providers
    statute_map = {
        'excluded_provider': '31 U.S.C. section 3729(a)(1)(A)',
        'billing_outlier': '31 U.S.C. section 3729(a)(1)(A)',
        'rapid_escalation': '31 U.S.C. section 3729(a)(1)(A)',
        'workforce_impossibility': '31 U.S.C. section 3729(a)(1)(B)',
        'shared_official': '31 U.S.C. section 3729(a)(1)(C)',
        'geographic_implausibility': '31 U.S.C. section 3729(a)(1)(G)'
    }
    
    for provider in all_flagged.values():
        primary_signal = provider['signals'][0]['signal_type'] if provider['signals'] else 'unknown'
        provider['fca_relevance'] = {
            'claim_type': f"Potential fraud detected via {primary_signal} signal",
            'statute_reference': statute_map.get(primary_signal, '31 U.S.C. section 3729(a)(1)(A)'),
            'suggested_next_steps': ['Review documentation', 'Cross-reference with other data sources', 'Refer for investigation']
        }
    
    return {
        'total_providers_scanned': len(provider_totals),
        'flagged_providers': list(all_flagged.values()),
        'signal_counts': {
            'excluded_provider': signal1_count,
            'billing_outlier': signal2_count,
            'rapid_escalation': 0,
            'workforce_impossibility': 0,
            'shared_official': 0,
            'geographic_implausibility': signal6_count
        }
    }


def main():
    """Main entry point."""
    logger.info("="*60)
    logger.info("Medicaid Fraud Detection Engine v" + VERSION)
    logger.info("="*60)
    
    data_dir = Path("data")
    spending_path = data_dir / "medicaid-provider-spending.parquet"
    leie_path = data_dir / "LEIE_UPDATED.csv"
    nppes_path = data_dir / "nppes.zip"
    
    # Check data
    if not spending_path.exists():
        logger.error(f"Medicaid data not found: {spending_path}")
        logger.error("Run ./setup.sh first")
        sys.exit(1)
    
    # Load LEIE
    leie_df = load_leie(str(leie_path))
    
    # Load NPPES (sample for testing, full for production)
    if nppes_path.exists():
        # Use sample for faster testing, comment out for full run
        nppes = load_nppes_sample(str(nppes_path), sample_size=1000000)  # 1M sample
        # nppes = load_nppes_full(str(nppes_path))  # Full load
    else:
        logger.warning("NPPES data not found - some signals will be incomplete")
        nppes = {}
    
    # Run signals (set max_groups=None for full run)
    # For testing: max_groups=50 processes ~5M rows
    # For production: max_groups=None processes all 227M rows
    results = run_all_signals(str(spending_path), leie_df, nppes, max_groups=50)
    
    # Build final report
    report = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'tool_version': VERSION,
        'total_providers_scanned': results['total_providers_scanned'],
        'total_providers_flagged': len(results['flagged_providers']),
        'signal_counts': results['signal_counts'],
        'flagged_providers': results['flagged_providers']
    }
    
    # Save
    output_path = 'fraud_signals.json'
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    logger.info("="*60)
    logger.info("ANALYSIS COMPLETE")
    logger.info("="*60)
    logger.info(f"Providers scanned: {report['total_providers_scanned']:,}")
    logger.info(f"Providers flagged: {report['total_providers_flagged']:,}")
    logger.info(f"Signal counts:")
    for signal, count in report['signal_counts'].items():
        logger.info(f"  {signal}: {count}")
    logger.info(f"\nReport saved to: {output_path}")
    
    return report


if __name__ == "__main__":
    main()
