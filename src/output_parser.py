import json
from typing import Any, Optional
from collections.abc import Iterator


class OutputParser:
    # Facebook Ads action stats fields that need special handling
    ADS_ACTION_STATS_ROW = [
        "actions",
        "properties",
        "conversion_values",
        "action_values",
        "canvas_component_avg_pct_view",
        "cost_per_10_sec_video_view",
        "cost_per_action_type",
        "cost_per_unique_action_type",
        "unique_actions",
        "video_10_sec_watched_actions",
        "video_15_sec_watched_actions",
        "video_30_sec_watched_actions",
        "video_avg_pct_watched_actions",
        "video_avg_percent_watched_actions",
        "video_avg_sec_watched_actions",
        "video_avg_time_watched_actions",
        "video_complete_watched_actions",
        "video_p100_watched_actions",
        "video_p25_watched_actions",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
        "cost_per_conversion",
        "cost_per_outbound_click",
        "video_p95_watched_actions",
        "website_ctr",
        "website_purchase_roas",
        "purchase_roas",
        "outbound_clicks",
        "conversions",
        "video_play_actions",
        "video_thruplay_watched_actions",
    ]

    # Fields that should be JSON encoded instead of flattened
    SERIALIZED_LISTS_TYPES = [
        "issues_info",
        "frequency_control_specs",
    ]

    def __init__(self, page_loader, page_id: str, row_config):
        self.page_loader = page_loader
        self.page_id = page_id
        self.row_config = row_config

    def parse_data(
        self,
        response: dict,
        fb_node: str,
        parent_id: str,
        table_name: Optional[str] = None,
    ) -> dict:
        result = {}

        for page_response in self._iter_paginated_responses(response):
            rows = self._extract_rows(page_response)
            if not rows:
                break

            for row in rows:
                self._process_row(row, fb_node, parent_id, table_name, result)

        return result

    def _iter_paginated_responses(self, response: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """Yield the current response and any subsequent paginated responses."""
        current = response or {}
        current_url = None

        while isinstance(current, dict) and current:
            yield current

            paging = current.get("paging") or {}
            next_url = paging.get("next")
            if not next_url or next_url == current_url:
                break

            current_url = next_url
            current = self.page_loader.load_page_from_url(next_url)

    def _extract_rows(self, response: dict[str, Any]) -> list[dict[str, Any]]:
        """Normalize response payload into a list of rows to process."""
        data = response.get("insights", response).get("data")

        if not data and isinstance(response, dict) and "id" in response:
            return [response]

        return data or []

    def _process_row(
        self,
        row: dict[str, Any],
        fb_graph_node: str,
        parent_id: str,
        table_name: Optional[str],
        result: dict[str, list[dict[str, Any]]],
    ) -> None:
        """Process a single row from the API response."""
        table_name = self._get_table_name(table_name or getattr(self.row_config.query, "path", ""))

        # Create base row with metadata
        base_row = self._create_base_row(fb_graph_node, parent_id)

        # Process all fields and extract special data types
        processed_data = self._process_fields(row)

        # Combine base row with regular fields
        full_row_data = {**base_row, **processed_data["regular_fields"]}

        # Check if this is an action breakdown query (action_reaction or action_type)
        is_action_breakdown_query = hasattr(self.row_config.query, "parameters") and (
            "action_breakdowns=action_reaction" in str(self.row_config.query.parameters)
            or "action_breakdowns=action_type" in str(self.row_config.query.parameters)
        )

        # For action breakdown queries, only create main row if there are no action stats to process
        # Otherwise, actions become the main rows
        if not is_action_breakdown_query or not processed_data["action_stats"]:
            # For normal queries (not action breakdown), include action stats in main table
            if not is_action_breakdown_query and processed_data["action_stats"]:
                # Process action stats as main table rows
                self._add_action_stats_to_main_table(result, table_name, full_row_data, processed_data["action_stats"])
            else:
                # Always create the main row (even if it has action stats)
                # This matches the original behavior where both main and action rows are created
                if processed_data["values"]:
                    self._add_value_rows(result, table_name, full_row_data, processed_data["values"])
                else:
                    # Always add the main row, even if it only has basic fields
                    self._add_row(result, table_name, full_row_data)

        # Process action stats as separate tables (only for action breakdown queries)
        if is_action_breakdown_query:
            self._process_action_stats(processed_data["action_stats"], row, fb_graph_node, result)

        # Process nested tables recursively
        self._process_nested_data(processed_data["nested_tables"], row, fb_graph_node, result)

    def _create_base_row(self, fb_graph_node: str, parent_id: str) -> dict[str, Any]:
        """Create base row with standard metadata."""
        return {
            "ex_account_id": self.page_id,
            "fb_graph_node": fb_graph_node,
            "parent_id": parent_id,
        }

    def _process_fields(self, row: dict[str, Any]) -> dict[str, Any]:
        """Process all fields in a row and categorize them."""
        processed = {
            "regular_fields": {},
            "nested_tables": {},
            "action_stats": {},
            "values": None,
        }

        for key, value in row.items():
            if key == "values":
                processed["values"] = value
            elif isinstance(value, dict) and "data" in value:
                processed["nested_tables"][key] = value
                # Also check if this nested object has summary alongside data
                if "summary" in value:
                    fake_nested = {"data": [value["summary"]]}
                    processed["nested_tables"]["summary"] = fake_nested
            elif isinstance(value, dict) and "summary" in value:
                fake_nested = {"data": [value["summary"]]}
                processed["nested_tables"]["summary"] = fake_nested
            elif key in self.ADS_ACTION_STATS_ROW and isinstance(value, list):
                # Handle Facebook Ads action stats as separate table
                processed["action_stats"][key] = value
            else:
                # Process regular field based on its type and key
                processed_field = self._process_single_field(key, value)
                processed["regular_fields"].update(processed_field)

        return processed

    def _process_single_field(self, key: str, value: Any) -> dict[str, Any]:
        """Process a single field based on its type and special handling rules."""
        if key in self.SERIALIZED_LISTS_TYPES:
            return {key: json.dumps(value)}
        elif key in self.ADS_ACTION_STATS_ROW and isinstance(value, list):
            # Handle Facebook Ads action stats fields - these should create separate table entries
            # Return empty dict for regular fields since these will be processed as separate tables
            return {}
        elif isinstance(value, (dict, list)):
            return self._flatten_array(key, value)
        else:
            return {key: value}

    def _add_value_rows(
        self,
        result: dict[str, list[dict[str, Any]]],
        table_name: str,
        full_row_data: dict[str, Any],
        values: list[dict[str, Any]],
    ) -> None:
        """Add multiple rows from values array."""
        for value_data in values:
            # Skip values without meaningful content
            if not self._has_meaningful_value(value_data):
                continue

            row = self._create_value_row(full_row_data, value_data)
            self._add_row(result, table_name, row)

    def _create_value_row(self, base_row: dict[str, Any], value_data: dict[str, Any]) -> dict[str, Any]:
        """Create a row from value data."""
        row = base_row.copy()
        row.update({"key1": "", "key2": "", "value": value_data["value"]})

        # Handle end_time for backward compatibility
        if hasattr(self.row_config.query, "fields") and "insights" in str(self.row_config.query.fields):
            row["end_time"] = value_data.get("end_time", None)
        elif "end_time" in value_data:
            row["end_time"] = value_data["end_time"]

        return row

    def _has_meaningful_value(self, value_data: dict[str, Any]) -> bool:
        """Check if value data contains meaningful content."""
        return "value" in value_data and value_data["value"] is not None and value_data["value"] != ""

    def _add_row(
        self,
        result: dict[str, list[dict[str, Any]]],
        table_name: str,
        row_data: dict[str, Any],
    ) -> None:
        """Add a row to the result if it has meaningful data."""
        if not self._has_meaningful_data(row_data):
            return

        if table_name not in result:
            result[table_name] = []
        result[table_name].append(row_data)

    def _add_action_stats_to_main_table(
        self,
        result: dict[str, list[dict[str, Any]]],
        table_name: str,
        base_row: dict[str, Any],
        action_stats: dict[str, Any],
    ) -> None:
        """Add action stats rows to main table (like old implementation)."""
        for stats_field_name, stats_data in action_stats.items():
            if not isinstance(stats_data, list):
                continue

            for action in stats_data:
                if not isinstance(action, dict):
                    continue

                # Create row with action data (similar to old implementation)
                action_row = base_row.copy()

                # Process action_type (same logic as _populate_action_row)
                raw_action_type = action.get("action_type", "")
                action_type = raw_action_type.split(".")[-1]
                if action_type == "post_save":
                    action_type = "post_reaction"

                action_row.update(
                    {
                        "ads_action_name": stats_field_name,
                        "action_type": action_type,
                        "value": action.get("value", ""),
                    }
                )

                # Add all other fields from action (except the ones we already handled)
                for key, value in action.items():
                    if key not in ["action_type", "value"]:
                        action_row[key] = value

                self._add_row(result, table_name, action_row)

    def _has_meaningful_data(self, row_data: dict[str, Any]) -> bool:
        """Check if row contains meaningful data beyond basic identifiers."""
        basic_identifiers = {"id", "parent_id", "ex_account_id", "fb_graph_node"}

        # If we have any data beyond just the basic identifiers, it's meaningful
        has_additional_data = any(
            key not in basic_identifiers and value is not None and value != "" for key, value in row_data.items()
        )

        return has_additional_data

    def _process_action_stats(
        self,
        action_stats: dict[str, Any],
        original_row: dict[str, Any],
        fb_graph_node: str,
        result: dict[str, list[dict[str, Any]]],
    ) -> None:
        """Process action stats as separate tables with _insights suffix or flatten for breakdowns."""
        is_action_breakdown = hasattr(self.row_config.query, "parameters") and any(
            b in str(self.row_config.query.parameters)
            for b in [
                "action_breakdowns=action_reaction",
                "action_breakdowns=action_type",
            ]
        )

        for stats_field_name, stats_data in action_stats.items():
            if not isinstance(stats_data, list):
                continue

            table_name = (
                self._get_table_name("") if is_action_breakdown else self._get_action_stats_table_name(stats_field_name)
            )

            base_row = self._create_base_row(fb_graph_node, self.page_id)
            self._copy_common_fields(base_row, original_row, extended=is_action_breakdown)

            for action in stats_data:
                if not isinstance(action, dict):
                    continue

                action_row = base_row.copy()
                self._populate_action_row(
                    action_row,
                    action,
                    stats_field_name,
                    original_row,
                    is_action_breakdown,
                )
                self._add_row(result, table_name, action_row)

    def _copy_common_fields(self, base_row: dict, original_row: dict, extended: bool) -> None:
        fields = [
            "account_id",
            "ad_id",
            "adset_id",
            "campaign_id",
            "date_start",
            "date_stop",
            "publisher_platform",
        ]
        if extended:
            fields += ["account_name", "campaign_name"]

        for field in fields:
            if field in original_row:
                base_row[field] = original_row[field]

    def _populate_action_row(
        self,
        action_row: dict,
        action: dict,
        stats_field_name: str,
        original_row: dict,
        is_action_breakdown: bool,
    ) -> None:
        action_type = action.get("action_type", "")
        if action_type == "post_save":
            action_type = "post_reaction"

        action_row.update(
            {
                "ads_action_name": stats_field_name,
                "action_type": action_type,
                "value": action.get("value", ""),
            }
        )

        if is_action_breakdown and "action_breakdowns=action_reaction" in str(self.row_config.query.parameters):
            action_reaction = action.get("action_reaction", original_row.get("action_reaction", ""))
            action_row["action_reaction"] = action_reaction

        for key, value in action.items():
            if key not in ["action_type", "value", "action_reaction"]:
                action_row[key] = value

    def _get_action_stats_table_name(self, stats_field_name: str) -> str:
        """Get the proper table name for action stats based on Clojure logic."""
        # Check if the query name already ends with the stats field name
        if self.row_config.name.endswith(f"_{stats_field_name}"):
            return f"{self.row_config.name}_insights"
        else:
            return f"{self.row_config.name}_{stats_field_name}_insights"

    def _process_nested_data(self, nested_tables: dict, original_row: dict, fb_graph_node: str, result: dict) -> None:
        """Process nested table data recursively."""
        for table_name, table_data in nested_tables.items():
            nested_graph_node = f"{fb_graph_node}_{table_name}"
            nested_row_id = original_row.get("id")

            nested_result = self.parse_data(table_data, nested_graph_node, nested_row_id, table_name)

            # Merge nested results
            for nested_table, nested_rows in nested_result.items():
                if nested_table not in result:
                    result[nested_table] = []
                result[nested_table].extend(nested_rows)

    def _flatten_array(self, parent_key: str, values: Any) -> dict[str, Any]:
        """
        Flatten arrays and objects into key-value pairs.
        """
        result = {}

        if isinstance(values, dict):
            for k, v in values.items():
                nested = self._flatten_array(f"{parent_key}_{k}", v)
                result.update(nested)
        elif isinstance(values, list):
            for i, v in enumerate(values):
                nested = self._flatten_array(f"{parent_key}_{i}", v)
                result.update(nested)
        else:
            result[parent_key] = values

        return result

    def _get_table_name(self, table_name: str) -> str:
        """
        Determine the final table name based on row configuration and query context,
        following Clojure-inspired logic.
        """
        row_name = self.row_config.name
        is_async = hasattr(self.row_config, "type") and self.row_config.type == "async-insights-query"

        # Check if this is an insights query by examining the fields
        is_insights_query = str(getattr(self.row_config.query, "fields", "")).startswith("insights")

        final_name = row_name

        if not table_name:
            # For both async insights queries and nested insights queries, add _insights suffix
            if (is_async or is_insights_query) and not row_name.endswith("_insights"):
                final_name = f"{row_name}_insights"
        else:
            if table_name not in row_name and not row_name.endswith(f"_{table_name}"):
                final_name = f"{row_name}_{table_name}"

        return final_name
