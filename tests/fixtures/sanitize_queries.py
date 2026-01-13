#!/usr/bin/env python3
"""
Sanitize queries.csv by replacing customer-specific IDs with test IDs and sampling representative queries.

This script:
1. Reads queries.csv
2. Replaces account IDs and page/media IDs based on component type
3. Maps all legacy component names to their V2 equivalents
4. Samples queries based on structural features (async vs nested, breakdowns, filtering, etc.)
5. Outputs sampled sanitized queries to queries_sanitized.csv
"""

import csv
import json
import re
import random
from pathlib import Path
from typing import Dict, List, Any

# Test data for replacement
INSTAGRAM_ACCOUNTS = {
    "177057932317550": {
        "instagram_business_account": {
            "id": "17841403584244541"
        },
        "name": "Keboola",
        "category": "Information technology company",
        "id": "177057932317550"
    },
    "473032723088889": {
        "name": "Keboola UK",
        "category": "Software",
        "id": "473032723088889"
    }
}

PAGE_ACCOUNTS = {
    "177057932317550": {
        "id": "177057932317550",
        "name": "Keboola",
        "category": "Information technology company"
    },
    "473032723088889": {
        "id": "473032723088889",
        "name": "Keboola UK",
        "category": "Software"
    }
}


def load_config_secrets() -> Dict[str, Any]:
    """Load the config.secrets.json file to get ads account IDs."""
    config_path = Path(__file__).parent / "config.secrets.json"
    if config_path.exists():
        with open(config_path, 'r') as f:
            return json.load(f)
    return {}


def get_replacement_id(category: str, original_id: str, ads_accounts: List[str]) -> str:
    """Get the replacement ID based on query category."""
    if category == 'ads':
        # Use ads account from config or default placeholder
        return random.choice(ads_accounts) if ads_accounts else "act_186649832776475"
    elif category == 'instagram':
        # Use instagram account
        account = random.choice(list(INSTAGRAM_ACCOUNTS.values()))
        if 'instagram_business_account' in account:
            return account['instagram_business_account']['id']
        return account['id']
    else:  # pages
        # Use page account
        account = random.choice(list(PAGE_ACCOUNTS.values()))
        return account['id']


def sanitize_query_json(query_obj: Dict[str, Any], category: str, ads_accounts: List[str]) -> Dict[str, Any]:
    """
    Sanitize a query object by replacing customer-specific IDs and filter values.
    """
    query_str = json.dumps(query_obj)
    
    # Replace Facebook Ad Account IDs (act_XXXXXXXXXX)
    act_pattern = r'act_\d+'
    act_matches = re.findall(act_pattern, query_str)
    for match in set(act_matches):
        replacement = get_replacement_id('ads', match, ads_accounts)
        query_str = query_str.replace(match, replacement)
    
    # Replace numeric IDs (10-18 digits) - matches page/media/adset/campaign IDs
    id_pattern = r'\b\d{10,18}\b'
    id_matches = re.findall(id_pattern, query_str)
    for match in set(id_matches):
        # Specific handling for Instagram IDs if they start with 17841
        if match.startswith('17841'):
            replacement = get_replacement_id('instagram', match, ads_accounts)
            query_str = query_str.replace(match, replacement)
        else:
            # General replacement based on component category
            replacement = get_replacement_id(category, match, ads_accounts)
            query_str = query_str.replace(match, replacement)
    
    # Sanitize filtering values (campaign names etc in filters)
    def sanitize_filter_values(text):
        def replacer(m):
            prefix = m.group(1)
            quote = m.group(2)
            content = m.group(3)
            suffix = m.group(4)
            if content.strip().isdigit():
                return m.group(0)
            return f'{prefix}{quote}campaign_placeholder{suffix}'

        text = re.sub(r'(value\s*[:=]\s*)((?:\\+)?[\"\'])([^\\\"\']+?)((?:\\+)?[\"\'])', replacer, text)
        return text

    query_str = sanitize_filter_values(query_str)
    
    return json.loads(query_str)


def get_sampling_fingerprint(row):
    """Create a structural fingerprint for sampling."""
    comp_id = row['kbc_component_id']
    query_type = row['query_type']
    query = json.loads(row['query_json'])
    
    # Handle flat vs nested structure for feature detection
    if 'query' in query and isinstance(query['query'], dict):
        params = query['query']
    else:
        params = query
        
    path = params.get('path', '')
    level = params.get('level', '')
    
    all_content = str(query).lower()
    features = {
        'breakdowns': 'breakdown' in all_content,
        'filtering': 'filtering' in all_content or 'filter' in all_content,
        'action_breakdowns': 'action_breakdown' in all_content,
        'attribution': 'attribution' in all_content,
        'time_increment': 'time_increment' in all_content,
        'summary': 'summary' in all_content,
        'nested': 'insights' in all_content and ('{' in all_content or '.' in all_content),
        'async': 'async' in query_type.lower(),
        'split_by_day': 'split-query-time-range-by-day' in all_content,
        'stop_empty': 'stop-on-empty-response' in all_content,
        'time_pagination': 'time-based-pagination' in all_content
    }
    
    active_features = tuple(sorted([k for k, v in features.items() if v]))
    
    return (comp_id, query_type, path, level, active_features)


def main():
    """Main sanitization process."""
    input_file = Path(__file__).parent / "queries.csv"
    output_file = Path(__file__).parent / "queries_sanitized.csv"
    
    if not input_file.exists():
        print(f"Input file not found: {input_file}")
        return

    # Load config to get ads accounts
    config = load_config_secrets()
    ads_accounts = []
    if 'parameters' in config and 'accounts' in config['parameters']:
        for acc_id in config['parameters']['accounts'].keys():
            if acc_id.startswith('act_'):
                ads_accounts.append(acc_id)
            else:
                ads_accounts.append(f"act_{acc_id}")
    
    if not ads_accounts:
        ads_accounts = ["act_186649832776475"]
    
    print(f"Using ads accounts for replacement: {ads_accounts}")
    
    ID_MAPPING = {
        "Facebook Ads": "Facebook Ads V2",
        "Facebook Pages": "Facebook Pages V2",
        "Instagram": "Instagram V2"
    }

    def get_category(comp_id: str) -> str:
        comp_id_lower = comp_id.lower()
        if 'instagram' in comp_id_lower: return 'instagram'
        if 'pages' in comp_id_lower: return 'pages'
        return 'ads'
    
    groups = {}
    row_count = 0
    with open(input_file, 'r', encoding='utf-8') as f_in:
        reader = csv.DictReader(f_in)
        for row in reader:
            row_count += 1
            original_comp_id = row.get('kbc_component_id', 'Facebook Ads')
            row['kbc_component_id'] = ID_MAPPING.get(original_comp_id, original_comp_id)
            
            fingerprint = get_sampling_fingerprint(row)
            if fingerprint not in groups:
                groups[fingerprint] = []
            groups[fingerprint].append(row)

    # Sampling: Take 2 examples per structural category
    SAMPLES_PER_CATEGORY = 2
    sampled_rows = []
    for fp in sorted(groups.keys(), key=lambda x: str(x)):
        # Optionally random sample, or just take first N for determinism
        sampled_rows.extend(groups[fp][:SAMPLES_PER_CATEGORY])

    print(f"Sampled {len(sampled_rows)} queries from {len(groups)} structural categories.")
    
    sanitized_count = 0
    with open(output_file, 'w', encoding='utf-8', newline='') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(['kbc_component_id', 'query_type', 'query_json'])
        
        for row in sampled_rows:
            comp_id = row['kbc_component_id']
            category = get_category(comp_id)
            query_type = row.get('query_type', 'nested-query')
            query_json_str = row.get('query_json', '')
            
            if not query_json_str: continue
            
            try:
                query_obj = json.loads(query_json_str)
                sanitized = sanitize_query_json(query_obj, category, ads_accounts)
                
                writer.writerow([comp_id, query_type, json.dumps(sanitized)])
                sanitized_count += 1
            except json.JSONDecodeError:
                continue

    print(f"\nSanitization complete!")
    print(f"Processed {sanitized_count} sampled queries.")
    print(f"Output written to: {output_file}")


if __name__ == "__main__":
    main()
