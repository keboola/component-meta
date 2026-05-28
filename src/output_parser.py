import json
import logging
from collections.abc import Iterator
from typing import Any

from page_loader import resolve_query_window

logger = logging.getLogger(__name__)


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

    # Parser-synthesized output column names that are NEVER valid FB Graph bare field
    # names. Users who declared these as bare tokens in their config field list were
    # copying parser child-row outputs back into the request — the FB API silently
    # ignored those tokens, returning rows that didn't contain them. Pre-PR #50 the
    # parser then emitted no column at all; PR #50's backfill injected them as empty
    # strings on parent rows, which surfaced as ``Extra columns found: "from_id, ..."``
    # Storage failures (CFTL-656 / SUPPORT-16397).
    #
    # Runtime detection in ``_observed_connections`` catches connection-shaped values
    # the API actually returns (``{"data": [...]}``, list-of-dicts), so we deliberately
    # do NOT keep a general FB connection-edge allowlist — that risks dropping a name
    # that is a legitimate scalar field somewhere. This allowlist is limited to names
    # the parser itself synthesizes when flattening nested ``from{...}`` expansions and
    # is therefore safe: no resource type uses these as flat scalar fields, the API
    # never returns them as keys, and users only encountered them as legacy CSV output
    # column names.
    PARSER_SYNTHESIZED_FIELDS = frozenset(
        {
            "from_id",
            "from_name",
            "from_full_name",
            "from_username",
        }
    )

    def __init__(self, page_loader, page_id: str, row_config):
        self.page_loader = page_loader
        self.page_id = page_id
        self.row_config = row_config
        # Parse the declared field list once — used by _backfill_declared_fields to keep
        # the output CSV schema stable when the FB API omits a field for the period (CFTL-630).
        self._declared_fields = self._parse_declared_fields(getattr(row_config, "query", None))
        # FB Graph connection names observed in API responses for THIS query are tracked here
        # and excluded from backfill on subsequent rows. Catches connection edges that aren't
        # in the static allowlist (e.g. account-specific custom edges). Populated by
        # ``_process_fields`` as it routes ``{"data": [...]}`` keys to child tables.
        self._observed_connections: set[str] = set()

    # Minimum rows buffered before yielding a streaming batch (CFTL-473).
    # Page-sized yields made test suites ~6x slower because every batch pays
    # per-yield overhead in the Component's write loop; accumulating across pages
    # keeps small queries on the old single-yield path while still bounding memory
    # for insights queries with hourly breakdowns + action-stats expansion, which
    # inflate well past the threshold within a handful of pages.
    DEFAULT_STREAM_ROW_THRESHOLD = 5000

    def iter_parsed_data(
        self,
        response: dict,
        fb_node: str,
        parent_id: str,
        table_name: str | None = None,
        row_threshold: int | None = None,
        apply_backfill: bool = True,
    ) -> Iterator[dict[str, list[dict[str, Any]]]]:
        """Stream parsed rows as ``{table: [rows...]}`` batches (CFTL-473 / SUPPORT-15993).

        Rows accumulate across paginated pages until ``row_threshold`` is reached, then
        the batch is yielded and the buffer resets. Small queries (< threshold total
        rows) yield exactly once — identical shape to the pre-CFTL-473 ``parse_data``
        and the same per-account Component write cost. Large queries (hourly
        breakdowns × many accounts × action-stats expansion) flush in bounded chunks
        so peak memory is capped at ≈ threshold × row size.

        Nested-field pagination is still resolved synchronously inside each outer
        row to preserve existing row ordering guarantees.

        ``apply_backfill`` gates the declared-field backfill on each emitted row. It is
        ``True`` for the outer query (so CFTL-630 stays fixed) and ``False`` when the
        parser recurses into nested child tables via ``_process_nested_data`` — child
        rows must not receive the parent query's declared-field schema (CFTL-656).
        """
        threshold = row_threshold if row_threshold is not None else self.DEFAULT_STREAM_ROW_THRESHOLD
        if not isinstance(threshold, int) or threshold <= 0:
            raise ValueError("row_threshold must be a positive integer")
        result: dict[str, list[dict[str, Any]]] = {}
        buffered_rows = 0

        for page_response in self._iter_paginated_responses(response):
            rows = self._extract_rows(page_response)
            if not rows:
                break

            # Pre-scan: observe connection-shaped keys across all rows in this page
            # BEFORE running backfill on any of them. Without this, a connection that
            # appears in row N can't influence backfill on rows 1..N-1 (CFTL-656).
            if apply_backfill:
                for row in rows:
                    self._observe_connection_keys(row)

            for row in rows:
                self._process_row(row, fb_node, parent_id, table_name, result, apply_backfill=apply_backfill)
                buffered_rows = sum(len(v) for v in result.values())
                if buffered_rows >= threshold:
                    yield result
                    result = {}
                    buffered_rows = 0

        if result:
            yield result

    def _observe_connection_keys(self, row: dict[str, Any]) -> None:
        """Record any key whose value is a FB Graph connection-shaped payload.

        Values that look like ``{"data": [...]}``, ``{"summary": {...}}``, or a
        list whose first element is a dict identify the key as a connection edge —
        it will be extracted as a child table by ``_process_fields`` and must not
        also be backfilled onto the parent row as an empty string (CFTL-656).
        """
        for key, value in row.items():
            if isinstance(value, dict) and ("data" in value or "summary" in value):
                self._observed_connections.add(key)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                self._observed_connections.add(key)

    def parse_data(
        self,
        response: dict,
        fb_node: str,
        parent_id: str,
        table_name: str | None = None,
        apply_backfill: bool = True,
    ) -> dict[str, list[dict[str, Any]]]:
        """Fully accumulate rows into a single dict (pre-CFTL-473 behavior).

        Kept for recursive nested-field processing, where synchronous accumulation is
        intentional: nested responses share the outer row's lifetime and must be merged
        into the page's batch in deterministic order.

        ``apply_backfill`` is forwarded to ``iter_parsed_data``; ``_process_nested_data``
        sets it to ``False`` so child-table rows do not receive the parent's declared
        fields (CFTL-656).
        """
        result: dict[str, list[dict[str, Any]]] = {}
        for batch in self.iter_parsed_data(response, fb_node, parent_id, table_name, apply_backfill=apply_backfill):
            for k, v in batch.items():
                if k in result:
                    result[k].extend(v)
                else:
                    result[k] = v
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

            logger.debug("Following pagination URL: %s", next_url)
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
        table_name: str | None,
        result: dict[str, list[dict[str, Any]]],
        apply_backfill: bool = True,
    ) -> None:
        """Process a single row from the API response.

        ``apply_backfill`` defaults to ``True`` for the outer query (preserves
        CFTL-630). Recursive calls from ``_process_nested_data`` pass ``False`` so
        child-table rows do not receive the parent's declared-field schema
        (CFTL-656).
        """
        table_name = self._get_table_name(table_name or getattr(self.row_config.query, "path", ""))

        # Backfill fields the user explicitly requested but that FB omitted from this row.
        # Stops Storage loads from failing with "Missing columns: impressions" when the
        # API returns no rows containing a metric for the queried period (CFTL-630).
        if apply_backfill:
            row = self._backfill_declared_fields(row)

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

        # Process nested tables recursively — resolved synchronously into the current batch.
        self._process_nested_data(processed_data["nested_tables"], row, fb_graph_node, result)

    def _backfill_declared_fields(self, row: dict[str, Any]) -> dict[str, Any]:
        """Return a row that contains every field the user asked for.

        Facebook's API omits a field entirely from the response when there is no data
        for it in the queried period. Downstream Storage loads then fail with
        ``Missing columns: ...`` because the destination table already has the column
        from a previous run. Filling absent declared fields with ``""`` keeps the
        output CSV schema stable regardless of API response content (CFTL-630).

        ``_observed_connections`` excludes any declared field name the parser has
        already seen in this query as a connection-shaped value (``{"data": [...]}``
        / ``{"summary": ...}`` / list-of-dicts). Injecting those as empty strings on
        the parent row would produce phantom columns and the "Extra columns found"
        Storage failure documented in SUPPORT-16397 (CFTL-656).
        """
        if not self._declared_fields:
            return row
        excluded = self.PARSER_SYNTHESIZED_FIELDS | self._observed_connections
        missing = [field for field in self._declared_fields if field not in row and field not in excluded]
        if not missing:
            return row
        filled = dict(row)
        for field in missing:
            filled[field] = ""
        return filled

    @staticmethod
    def _parse_declared_fields(query) -> list[str]:
        """Return the explicit field list the user declared in the query config.

        Mirrors the three formats accepted by ``PageLoader._build_query_params``:
        DSL ``insights.<...>{a,b,c}``, plain CSV ``fields = "a,b,c"``, and a
        ``fields=...`` entry inside ``parameters`` (string or dict).

        The FB Graph DSL supports field expansion (``comments{message,from{name}}``)
        and modifier calls (``comments.limit(0).summary(true)``) inside a single
        field token, both of which contain commas that must NOT split the token.
        We split brace- and paren-aware, then take the base field name so the
        backfill column matches what flows through the response normalizer.
        """
        if query is None:
            return []

        fields_attr = str(getattr(query, "fields", "") or "")
        if fields_attr.startswith("insights"):
            if "{" in fields_attr and "}" in fields_attr:
                inner = fields_attr.split("{", 1)[1].rsplit("}", 1)[0]
                return OutputParser._split_field_dsl(inner)
        elif fields_attr:
            return OutputParser._split_field_dsl(fields_attr)

        parameters = getattr(query, "parameters", None)
        if isinstance(parameters, str):
            for pair in parameters.split("&"):
                if pair.startswith("fields="):
                    return OutputParser._split_field_dsl(pair[len("fields=") :])
        elif isinstance(parameters, dict):
            fields_val = parameters.get("fields")
            if isinstance(fields_val, str):
                return OutputParser._split_field_dsl(fields_val)
            if isinstance(fields_val, list):
                names = [OutputParser._base_field_name(str(f)) for f in fields_val if str(f).strip()]
                return [name for name in names if name]

        return []

    @staticmethod
    def _split_field_dsl(fields_str: str) -> list[str]:
        """Split a FB Graph field DSL list into base field names.

        Splits on commas that sit at depth 0 of both ``{}`` and ``()``, then
        reduces each token to its base field name (anything before the first
        ``{``, ``.``, or ``(``).
        """
        tokens: list[str] = []
        depth_brace = 0
        depth_paren = 0
        current: list[str] = []
        for ch in fields_str:
            if ch == "{":
                depth_brace += 1
                current.append(ch)
            elif ch == "}":
                depth_brace -= 1
                current.append(ch)
            elif ch == "(":
                depth_paren += 1
                current.append(ch)
            elif ch == ")":
                depth_paren -= 1
                current.append(ch)
            elif ch == "," and depth_brace == 0 and depth_paren == 0:
                tokens.append("".join(current))
                current = []
            else:
                current.append(ch)
        if current:
            tokens.append("".join(current))
        return [name for name in (OutputParser._base_field_name(t) for t in tokens) if name]

    @staticmethod
    def _base_field_name(field_dsl: str) -> str:
        """Return the token if it is a flat scalar field; empty string otherwise.

        Only flat scalar names (``impressions``, ``ad_id``, ``shares``) are columns
        on the parent row and need backfilling when the API omits them. Tokens with
        ``{...}`` expansion (``comments{message,from}``) or ``.modifier(...)`` calls
        (``comments.limit(0).summary(true)``, ``reactions.type(SAD)``) are nested-edge
        traversals that produce CHILD tables and have no parent-row column — backfilling
        them as empty strings on the parent injects phantom columns and breaks Storage
        loads with "Extra columns found" (CFTL-656 / SUPPORT-16397).
        """
        token = field_dsl.replace("\n", "").strip()
        if not token:
            return ""
        if any(c in token for c in "{.("):
            return ""
        return token

    def _create_base_row(self, fb_graph_node: str, parent_id: str) -> dict[str, Any]:
        """Create base row with standard metadata."""
        base: dict[str, Any] = {
            "ex_account_id": self.page_id,
            "fb_graph_node": fb_graph_node,
            "parent_id": parent_id,
        }

        # metric_type=total_value responses carry no per-row end_time, so anchor the row to
        # the requested window. Scoped to total_value to avoid adding columns to existing
        # time-series insights schemas (which carry end_time in each values[] entry).
        # Covers both DSL form .metric_type(total_value) and URL form metric_type=total_value.
        query_config = getattr(self.row_config, "query", None)
        if query_config is not None:
            query_fields = str(getattr(query_config, "fields", "") or "")
            query_parameters = str(getattr(query_config, "parameters", "") or "")
            is_total_value = "metric_type(total_value)" in query_fields or "metric_type=total_value" in query_parameters
            if is_total_value:
                since, until = resolve_query_window(query_config)
                if since:
                    base["date_start"] = since
                if until:
                    base["date_stop"] = until

        return base

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
                # Once we have seen a key as a connection edge in any row, exclude it
                # from declared-field backfill on every subsequent row of this query
                # (CFTL-656 dynamic detection — complements FB_CONNECTION_EDGES).
                self._observed_connections.add(key)
                # Also check if this nested object has summary alongside data
                if "summary" in value:
                    fake_nested = {"data": [value["summary"]]}
                    processed["nested_tables"]["summary"] = fake_nested
            elif isinstance(value, dict) and "summary" in value:
                fake_nested = {"data": [value["summary"]]}
                processed["nested_tables"]["summary"] = fake_nested
                self._observed_connections.add(key)
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
        elif isinstance(value, dict | list):
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
        """Create a row from value data.

        Supports the new Facebook API breakdown format where breakdown fields
        appear as siblings of 'value' and 'end_time' in the values array entry.
        These sibling breakdown fields are extracted and mapped to key1/key2.
        """
        row = base_row.copy()
        breakdown_fields = self._extract_breakdown_fields(value_data)
        breakdown_keys = sorted(breakdown_fields.keys())
        key1 = breakdown_fields[breakdown_keys[0]] if len(breakdown_keys) > 0 else ""
        key2 = breakdown_fields[breakdown_keys[1]] if len(breakdown_keys) > 1 else ""
        row.update({"key1": key1, "key2": key2, "value": value_data["value"]})

        # Handle end_time for backward compatibility
        if hasattr(self.row_config.query, "fields") and "insights" in str(self.row_config.query.fields):
            row["end_time"] = value_data.get("end_time", None)
        elif "end_time" in value_data:
            row["end_time"] = value_data["end_time"]

        return row

    @staticmethod
    def _extract_breakdown_fields(value_data: dict[str, Any]) -> dict[str, str]:
        """Extract breakdown fields from a values array entry.

        Breakdown fields are any fields that are not 'value' or 'end_time'.
        Returns a map of breakdown field names to their string values.
        """
        known_keys = {"value", "end_time"}
        return {k: str(v) for k, v in value_data.items() if k not in known_keys}

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
        """Copy fields from the originating insights row onto a per-action row.

        For action-breakdown queries (``extended=True``) the documented metric fields
        from CFTL-630/SUPPORT-16160 are appended so they flow into per-action rows
        instead of being silently dropped. The list is deliberately narrow rather than
        "copy every scalar" to avoid surprising existing V2 customers with unrelated
        new columns that their destination Storage tables don't have.
        """
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
            fields += [
                "account_name",
                "campaign_name",
                "ad_name",
                "impressions",
                "clicks",
                "spend",
                "reach",
            ]
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
        """Process nested table data recursively into the current batch.

        The recursive parse runs with ``apply_backfill=False`` because the parent
        query's declared field list describes the parent row's schema, not the
        child table's. Without this gate, every nested row got the parent's
        declared fields injected as empty strings — surfaced as
        ``Extra columns found`` Storage failures on child tables like
        ``feed_attachments`` / ``feed_comments`` (CFTL-656 / SUPPORT-16397).
        """
        for table_name, table_data in nested_tables.items():
            nested_graph_node = f"{fb_graph_node}_{table_name}"
            nested_row_id = original_row.get("id")

            nested_result = self.parse_data(
                table_data, nested_graph_node, nested_row_id, table_name, apply_backfill=False
            )

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
