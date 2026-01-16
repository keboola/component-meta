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
        # Use instagram business account - filter to only accounts with instagram_business_account
        ig_accounts = [acc for acc in INSTAGRAM_ACCOUNTS.values()
                      if 'instagram_business_account' in acc]
        if ig_accounts:
            account = random.choice(ig_accounts)
            return account['instagram_business_account']['id']
        # Fallback if no IG accounts found
        return "17841403584244541"
    else:  # pages
        # Use page account
        account = random.choice(list(PAGE_ACCOUNTS.values()))
        return account['id']


def determine_required_id_type(query_obj: Dict[str, Any], component_category: str) -> str:
    """
    Determine the required ID type based on the query structure and endpoint.

    Returns: 'ads', 'instagram', or 'pages'
    """
    # Page-specific endpoints that require Page IDs
    PAGE_ENDPOINTS = [
        'feed', 'posts', 'published_posts', 'ratings',
        'video', 'video_reels', 'likes', 'conversations',
        'tagged', 'albums', 'events', 'live_videos'
    ]

    # Instagram-specific endpoints that require Instagram Business Account IDs
    INSTAGRAM_ENDPOINTS = [
        'stories', 'media', 'recently_searched_hashtags',
        'available_catalogs', 'catalog_product_search'
    ]

    # Instagram-specific fields that require Instagram Business Account IDs
    INSTAGRAM_FIELDS = [
        'biography', 'followers_count', 'username', 'media_count',
        'profile_picture_url', 'ig_id', 'media_product_type',
        'caption', 'comments_count', 'like_count', 'media_type',
        'follower_demographics', 'engaged_audience_demographics',
        'accounts_engaged', 'total_interactions', 'impressions', 'reach',
        'saved', 'shares', 'profile_views'
    ]

    # Page-specific fields that require Page IDs
    PAGE_FIELDS = [
        'fan_count', 'talking_about_count', 'were_here_count',
        'checkins', 'page_token', 'access_token'
    ]

    # Extract query parameters from nested or flat structure
    if 'query' in query_obj and isinstance(query_obj['query'], dict):
        params = query_obj['query']
    else:
        params = query_obj

    # Check the path field
    path = params.get('path', '').lower()

    # Check if this is an async-insights-query (always needs Ad Account ID)
    query_type = query_obj.get('type', '')
    if query_type == 'async-insights-query':
        return 'ads'

    # Check if path matches page-specific endpoints
    if path in PAGE_ENDPOINTS:
        return 'pages'

    # Check if path matches instagram-specific endpoints
    if path in INSTAGRAM_ENDPOINTS:
        return 'instagram'

    # Check fields for Instagram-specific indicators
    fields = str(params.get('fields', '')).lower()
    if any(field in fields for field in INSTAGRAM_FIELDS):
        return 'instagram'

    # Check fields for Page-specific indicators
    if any(field in fields for field in PAGE_FIELDS):
        return 'pages'

    # Check parameters string for endpoint indicators
    parameters = str(params.get('parameters', '')).lower()
    if any(endpoint in parameters for endpoint in PAGE_ENDPOINTS):
        return 'pages'
    if any(endpoint in parameters for endpoint in INSTAGRAM_ENDPOINTS):
        return 'instagram'

    # Check the entire query string for endpoint clues
    query_str = json.dumps(query_obj).lower()

    # If we find page-specific paths in the query string
    for endpoint in PAGE_ENDPOINTS:
        if f'/{endpoint}' in query_str or f'"{endpoint}"' in query_str:
            return 'pages'

    # If we find instagram-specific paths in the query string
    for endpoint in INSTAGRAM_ENDPOINTS:
        if f'/{endpoint}' in query_str or f'"{endpoint}"' in query_str:
            return 'instagram'

    # Look for insights patterns with nested syntax
    if 'insights' in query_str:
        # Check for page-specific insights patterns
        for endpoint in PAGE_ENDPOINTS:
            if endpoint in query_str:
                return 'pages'
        # Check for instagram-specific insights patterns
        for endpoint in INSTAGRAM_ENDPOINTS:
            if endpoint in query_str:
                return 'instagram'
        # Insights without specific endpoint indicators default to ads
        if 'action_type' in query_str or 'breakdown' in query_str or 'action_breakdown' in query_str:
            return 'ads'

    # Default to component category
    return component_category


def sanitize_query_json(query_obj: Dict[str, Any], category: str, ads_accounts: List[str]) -> Dict[str, Any]:
    """
    Sanitize a query object by replacing customer-specific IDs and filter values.

    Now endpoint-aware: determines the required ID type based on the query structure
    (page endpoints need Page IDs, Instagram endpoints need Instagram IDs, etc.)
    """
    # Determine what type of ID this query actually needs based on its endpoint
    required_id_type = determine_required_id_type(query_obj, category)

    query_str = json.dumps(query_obj)

    # Replace Facebook Ad Account IDs (act_XXXXXXXXXX)
    # These should stay as ads accounts if the query needs ads, otherwise convert to appropriate type
    act_pattern = r'act_\d+'
    act_matches = re.findall(act_pattern, query_str)
    for match in set(act_matches):
        # If this query needs ads IDs, keep as ads account
        # Otherwise, replace with the appropriate ID type
        replacement = get_replacement_id(required_id_type, match, ads_accounts)
        query_str = query_str.replace(match, replacement)

    # Replace numeric IDs (10-18 digits) - matches page/media/adset/campaign IDs
    id_pattern = r'\b\d{10,18}\b'
    id_matches = re.findall(id_pattern, query_str)
    for match in set(id_matches):
        # Specific handling for Instagram IDs if they start with 17841
        if match.startswith('17841'):
            # Keep as Instagram ID if query needs it, otherwise use required type
            if required_id_type == 'instagram':
                replacement = get_replacement_id('instagram', match, ads_accounts)
            else:
                replacement = get_replacement_id(required_id_type, match, ads_accounts)
            query_str = query_str.replace(match, replacement)
        else:
            # Use the endpoint-aware ID type instead of just component category
            replacement = get_replacement_id(required_id_type, match, ads_accounts)
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

    # Parse back to JSON to fill empty ids fields
    result = json.loads(query_str)

    # Fill empty ids field with appropriate ID type if this is a query that needs one
    # Check both flat structure and nested query structure
    if 'query' in result and isinstance(result['query'], dict):
        params = result['query']
    else:
        params = result

    # If ids field exists and is empty, fill it with the appropriate ID type
    # This is critical: empty IDs cause the component to default to AdAccount IDs,
    # which breaks queries for Page and Instagram endpoints
    if 'ids' in params and not params['ids']:
        # Fill with appropriate ID type based on the endpoint
        params['ids'] = get_replacement_id(required_id_type, "", ads_accounts)

    return result


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

    # Smart sampling: Take 3 examples per category (2 working + 1 failing if possible)
    SAMPLES_PER_CATEGORY = 3
    sampled_rows = []

    for fp in sorted(groups.keys(), key=lambda x: str(x)):
        category_rows = groups[fp]

        # Separate working vs failing queries
        working = []
        failing = []

        for row in category_rows:
            has_any_failure = (
                row.get('has_failure_last_3_days') == 'true' or
                row.get('has_failure_last_15_days') == 'true' or
                row.get('has_failure_last_30_days') == 'true' or
                row.get('has_failure_last_90_days') == 'true'
            )

            if has_any_failure:
                failing.append(row)
            else:
                working.append(row)

        # Prioritize: 2 working + 1 failing
        selected = []
        selected.extend(working[:2])  # Take up to 2 working
        if len(selected) < SAMPLES_PER_CATEGORY:
            selected.extend(failing[:SAMPLES_PER_CATEGORY - len(selected)])  # Fill remaining with failing

        sampled_rows.extend(selected)

    print(f"Sampled {len(sampled_rows)} queries from {len(groups)} structural categories.")

    sanitized_count = 0
    with open(output_file, 'w', encoding='utf-8', newline='') as f_out:
        writer = csv.writer(f_out)
        # Add metadata columns
        writer.writerow([
            'kbc_component_id',
            'query_type',
            'query_json',
            'production_working',
            'has_failure_last_3_days',
            'has_failure_last_15_days',
            'has_failure_last_30_days',
            'has_failure_last_90_days'
        ])
        
        for row in sampled_rows:
            comp_id = row['kbc_component_id']
            category = get_category(comp_id)
            query_type = row.get('query_type', 'nested-query')
            query_json_str = row.get('query_json', '')

            if not query_json_str: continue

            # Determine production status
            has_any_failure = (
                row.get('has_failure_last_3_days') == 'true' or
                row.get('has_failure_last_15_days') == 'true' or
                row.get('has_failure_last_30_days') == 'true' or
                row.get('has_failure_last_90_days') == 'true'
            )
            production_working = 'false' if has_any_failure else 'true'

            try:
                query_obj = json.loads(query_json_str)
                sanitized = sanitize_query_json(query_obj, category, ads_accounts)

                writer.writerow([
                    comp_id,
                    query_type,
                    json.dumps(sanitized),
                    production_working,
                    row.get('has_failure_last_3_days', 'false'),
                    row.get('has_failure_last_15_days', 'false'),
                    row.get('has_failure_last_30_days', 'false'),
                    row.get('has_failure_last_90_days', 'false')
                ])
                sanitized_count += 1
            except json.JSONDecodeError:
                continue

    # Calculate statistics
    total_working = sum(1 for row in sampled_rows if not (
        row.get('has_failure_last_3_days') == 'true' or
        row.get('has_failure_last_15_days') == 'true' or
        row.get('has_failure_last_30_days') == 'true' or
        row.get('has_failure_last_90_days') == 'true'
    ))
    total_failing = len(sampled_rows) - total_working

    print(f"\nSanitization complete!")
    print(f"Processed {sanitized_count} sampled queries from {len(groups)} structural categories.")
    print(f"  - Working in production: {total_working} ({100*total_working/len(sampled_rows):.1f}%)")
    print(f"  - Failing in production: {total_failing} ({100*total_failing/len(sampled_rows):.1f}%)")
    print(f"Output written to: {output_file}")


if __name__ == "__main__":
    main()
