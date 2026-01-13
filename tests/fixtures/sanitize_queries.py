#!/usr/bin/env python3
"""
Sanitize queries.csv by replacing customer-specific IDs with test IDs.

This script:
1. Reads queries.csv
2. Replaces account IDs and page/media IDs based on component type
3. Maps all legacy component names to their V2 equivalents
4. Outputs ALL sanitized queries to queries_sanitized.csv
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
            
            # If it's a number, leave it
            if content.strip().isdigit():
                return m.group(0)
            
            # If it's a known non-sensitive keyword, leave it
            # But "contain" etc are usually the operator, not the value.
            # The value is what we want to sanitize.
            return f'{prefix}{quote}campaign_placeholder{suffix}'

        # Pattern: value, followed by optional spaces/colons, then quotes (possibly escaped), 
        # then content, then closing quotes.
        # We handle double quotes and potentially multiple levels of escaping in JSON
        # This regex matches value: "something" or value: \"something\" or value: \\\"something\\\"
        text = re.sub(r'(value\s*[:=]\s*)((?:\\+)?[\"\'])([^\\\"\']+?)((?:\\+)?[\"\'])', replacer, text)
        return text

    query_str = sanitize_filter_values(query_str)
    
    return json.loads(query_str)


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
    
    # Mapping to map ALL components to V2 equivalents
    ID_MAPPING = {
        "Facebook Ads": "Facebook Ads V2",
        "Facebook Pages": "Facebook Pages V2",
        "Instagram": "Instagram V2"
    }

    def get_category(comp_id: str) -> str:
        comp_id_lower = comp_id.lower()
        if 'instagram' in comp_id_lower:
            return 'instagram'
        if 'pages' in comp_id_lower:
            return 'pages'
        return 'ads'
    
    sanitized_count = 0
    with open(input_file, 'r', encoding='utf-8') as f_in, \
         open(output_file, 'w', encoding='utf-8', newline='') as f_out:
        
        reader = csv.DictReader(f_in)
        writer = csv.writer(f_out)
        
        # Write header
        writer.writerow(['kbc_component_id', 'query_type', 'query_json'])
        
        for row in reader:
            original_comp_id = row.get('kbc_component_id', 'Facebook Ads')
            # Map everything to V2
            comp_id = ID_MAPPING.get(original_comp_id, original_comp_id)

            category = get_category(comp_id)
            query_type = row.get('query_type', 'nested-query')
            query_json_str = row.get('query_json', '')
            
            if not query_json_str:
                continue
            
            try:
                query_obj = json.loads(query_json_str)
                sanitized = sanitize_query_json(query_obj, category, ads_accounts)
                
                writer.writerow([
                    comp_id,
                    query_type,
                    json.dumps(sanitized)
                ])
                sanitized_count += 1
                
            except json.JSONDecodeError:
                continue

    print(f"\nSanitization complete!")
    print(f"Processed {sanitized_count} queries.")
    print(f"Output written to: {output_file}")


if __name__ == "__main__":
    main()
