#!/usr/bin/env python3
"""
Sanitize queries.csv by replacing customer-specific IDs with test IDs and sampling representative queries.

This script:
1. Reads queries.csv
2. Replaces all IDs with component-specific test IDs (simple 1-1 mapping):
   - Facebook Ads V2 -> act_186649832776475
   - Facebook Pages V2 -> 177057932317550
   - Instagram V2 -> 17841403584244541
3. Maps all legacy component names to their V2 equivalents
4. Samples queries based on structural features (async vs nested, breakdowns, filtering, etc.)
5. Outputs sampled sanitized queries to queries_sanitized.csv
"""

import csv
import json
import re
from pathlib import Path
from typing import Dict, Any

# Simple 1-1 component ID to test account mapping
# Each component type gets exactly one test account ID
# Supports both V1 (legacy) and V2 component IDs
COMPONENT_TEST_IDS = {
    # V2 components
    "Facebook Ads V2": "act_186649832776475",
    "Facebook Pages V2": "177057932317550",
    "Instagram V2": "683499195074649",
    # V1 components (legacy)
    "Facebook Ads": "act_186649832776475",
    "Facebook Pages": "177057932317550",
    "Instagram": "683499195074649",
}


def get_replacement_id(component_id: str) -> str:
    """Get the replacement ID based on component ID.

    Simple 1-1 mapping:
    - Facebook Ads V2 -> act_186649832776475
    - Facebook Pages V2 -> 177057932317550
    - Instagram V2 -> 17841403584244541
    """
    return COMPONENT_TEST_IDS.get(component_id, "act_186649832776475")


def sanitize_query_json(query_obj: Dict[str, Any], component_id: str) -> Dict[str, Any]:
    """
    Sanitize a query object by replacing customer-specific IDs with the test ID for this component.

    Simple approach: Use the component_id to determine which test ID to use.
    - Facebook Ads V2 -> act_186649832776475
    - Facebook Pages V2 -> 177057932317550
    - Instagram V2 -> 17841403584244541
    """
    replacement_id = get_replacement_id(component_id)
    query_str = json.dumps(query_obj)

    # Replace all Facebook Ad Account IDs (act_XXXXXXXXXX) with the replacement ID
    act_pattern = r"act_\d+"
    query_str = re.sub(act_pattern, replacement_id, query_str)

    # Replace all numeric IDs (10-18 digits) with the replacement ID
    # This covers Page IDs, Instagram IDs, ad IDs, campaign IDs, etc.
    id_pattern = r"\b\d{10,18}\b"
    query_str = re.sub(id_pattern, replacement_id, query_str)

    # Sanitize filtering values (campaign names etc in filters)
    def sanitize_filter_values(text):
        def replacer(m):
            prefix = m.group(1)
            quote = m.group(2)
            content = m.group(3)
            suffix = m.group(4)
            if content.strip().isdigit():
                return m.group(0)
            return f"{prefix}{quote}campaign_placeholder{suffix}"

        text = re.sub(
            r"(value\s*[:=]\s*)((?:\\+)?[\"\'])([^\\\"\']+?)((?:\\+)?[\"\'])",
            replacer,
            text,
        )
        return text

    query_str = sanitize_filter_values(query_str)

    # Parse back to JSON
    result = json.loads(query_str)

    # Fill empty ids field if it exists
    # Check both flat structure and nested query structure
    if "query" in result and isinstance(result["query"], dict):
        params = result["query"]
    else:
        params = result

    if "ids" in params and not params["ids"]:
        params["ids"] = replacement_id

    return result


def get_sampling_fingerprint(row):
    """Create a structural fingerprint for sampling."""
    comp_id = row["kbc_component_id"]
    query_type = row["query_type"]
    query = json.loads(row["query_json"])

    # Handle flat vs nested structure for feature detection
    if "query" in query and isinstance(query["query"], dict):
        params = query["query"]
    else:
        params = query

    path = params.get("path", "")
    level = params.get("level", "")

    all_content = str(query).lower()
    features = {
        "breakdowns": "breakdown" in all_content,
        "filtering": "filtering" in all_content or "filter" in all_content,
        "action_breakdowns": "action_breakdown" in all_content,
        "attribution": "attribution" in all_content,
        "time_increment": "time_increment" in all_content,
        "summary": "summary" in all_content,
        "nested": "insights" in all_content
        and ("{" in all_content or "." in all_content),
        "async": "async" in query_type.lower(),
        "split_by_day": "split-query-time-range-by-day" in all_content,
        "stop_empty": "stop-on-empty-response" in all_content,
        "time_pagination": "time-based-pagination" in all_content,
    }

    active_features = tuple(sorted([k for k, v in features.items() if v]))

    return (comp_id, query_type, path, level, active_features)


def main():
    """Main sanitization process."""
    # Input and output files are in tests/fixtures/
    fixtures_dir = Path(__file__).parent.parent / "tests" / "fixtures"
    input_file = fixtures_dir / "queries.csv"
    output_file = fixtures_dir / "queries_sanitized.csv"

    if not input_file.exists():
        print(f"Input file not found: {input_file}")
        return

    # Map legacy V1 component IDs to V2
    ID_MAPPING = {
        "Facebook Ads": "Facebook Ads V2",
        "Facebook Pages": "Facebook Pages V2",
        "Instagram": "Instagram V2",
    }

    groups = {}
    row_count = 0
    with open(input_file, "r", encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in)
        for row in reader:
            row_count += 1
            original_comp_id = row.get("kbc_component_id", "Facebook Ads")
            row["kbc_component_id"] = ID_MAPPING.get(original_comp_id, original_comp_id)

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
                row.get("has_failure_last_3_days") == "true"
                or row.get("has_failure_last_15_days") == "true"
                or row.get("has_failure_last_30_days") == "true"
                or row.get("has_failure_last_90_days") == "true"
            )

            if has_any_failure:
                failing.append(row)
            else:
                working.append(row)

        # Prioritize: 2 working + 1 failing
        selected = []
        selected.extend(working[:2])  # Take up to 2 working
        if len(selected) < SAMPLES_PER_CATEGORY:
            selected.extend(
                failing[: SAMPLES_PER_CATEGORY - len(selected)]
            )  # Fill remaining with failing

        sampled_rows.extend(selected)

    print(
        f"Sampled {len(sampled_rows)} queries from {len(groups)} structural categories."
    )

    sanitized_count = 0
    with open(output_file, "w", encoding="utf-8", newline="") as f_out:
        writer = csv.writer(f_out)
        # Add metadata columns
        writer.writerow(
            [
                "kbc_component_id",
                "query_type",
                "query_json",
                "production_working",
                "has_failure_last_3_days",
                "has_failure_last_15_days",
                "has_failure_last_30_days",
                "has_failure_last_90_days",
            ]
        )

        for row in sampled_rows:
            comp_id = row["kbc_component_id"]
            query_type = row.get("query_type", "nested-query")
            query_json_str = row.get("query_json", "")

            if not query_json_str:
                continue

            # Determine production status
            has_any_failure = (
                row.get("has_failure_last_3_days") == "true"
                or row.get("has_failure_last_15_days") == "true"
                or row.get("has_failure_last_30_days") == "true"
                or row.get("has_failure_last_90_days") == "true"
            )
            production_working = "false" if has_any_failure else "true"

            try:
                query_obj = json.loads(query_json_str)
                sanitized = sanitize_query_json(query_obj, comp_id)

                writer.writerow(
                    [
                        comp_id,
                        query_type,
                        json.dumps(sanitized),
                        production_working,
                        row.get("has_failure_last_3_days", "false"),
                        row.get("has_failure_last_15_days", "false"),
                        row.get("has_failure_last_30_days", "false"),
                        row.get("has_failure_last_90_days", "false"),
                    ]
                )
                sanitized_count += 1
            except json.JSONDecodeError:
                continue

    # Calculate statistics
    total_working = sum(
        1
        for row in sampled_rows
        if not (
            row.get("has_failure_last_3_days") == "true"
            or row.get("has_failure_last_15_days") == "true"
            or row.get("has_failure_last_30_days") == "true"
            or row.get("has_failure_last_90_days") == "true"
        )
    )
    total_failing = len(sampled_rows) - total_working

    print("\nSanitization complete!")
    print(
        f"Processed {sanitized_count} sampled queries from {len(groups)} structural categories."
    )
    print(
        f"  - Working in production: {total_working} ({100 * total_working / len(sampled_rows):.1f}%)"
    )
    print(
        f"  - Failing in production: {total_failing} ({100 * total_failing / len(sampled_rows):.1f}%)"
    )
    print(f"Output written to: {output_file}")


if __name__ == "__main__":
    main()
