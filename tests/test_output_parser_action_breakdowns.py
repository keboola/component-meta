"""Regression tests for the FB Ads V2 action-breakdown output (CFTL-630 / SUPPORT-16160).

Two bugs are guarded here:

* **Bug 1** — when a query carries ``action_breakdowns=action_type`` (or
  ``action_reaction``), the per-action rows emitted by ``OutputParser`` must include
  the user-facing metric fields (``ad_name``, ``impressions``, ``clicks``, ``spend``,
  ``reach``, …) copied from the originating insights row. Previously the parser
  copied only a hard-coded subset of identifier fields, silently dropping every
  other column.

* **Bug 2** — when the Facebook API omits a requested field from the response
  entirely (zero data for the period), the output CSV must still expose that field
  as a column so subsequent Storage loads keep matching the existing table schema.
"""

import unittest
from unittest.mock import MagicMock

from output_parser import OutputParser


class _FakePageLoader:
    def load_page_from_url(self, url: str) -> dict:
        return {}


def _make_row_config(*, fields: str = "insights", parameters: str | None = None) -> MagicMock:
    row_config = MagicMock()
    row_config.name = "ads"
    row_config.type = "regular"
    row_config.query.path = ""
    row_config.query.fields = fields
    row_config.query.parameters = parameters
    return row_config


def _ad_insights_row(**overrides) -> dict:
    row = {
        "account_id": "123",
        "ad_id": "ad_1",
        "ad_name": "Spring promo",
        "campaign_id": "c_1",
        "campaign_name": "Spring 2026",
        "impressions": "1000",
        "clicks": "50",
        "spend": "12.34",
        "reach": "900",
        "date_start": "2026-01-01",
        "date_stop": "2026-01-01",
        "actions": [
            {"action_type": "link_click", "value": "5"},
            {"action_type": "post_save", "value": "2"},
            {"action_type": "landing_page_view", "value": "3"},
        ],
    }
    row.update(overrides)
    return row


class TestActionBreakdownCopiesAllScalarFields(unittest.TestCase):
    """Bug 1: every scalar metric column the user requested lands on every action row."""

    def test_per_action_rows_carry_all_scalar_metric_fields(self):
        row_config = _make_row_config(parameters="action_breakdowns=action_type")
        parser = OutputParser(_FakePageLoader(), page_id="act_123", row_config=row_config)

        response = {"data": [_ad_insights_row()]}
        result = parser.parse_data(response, fb_node="adaccount", parent_id="act_123")

        self.assertIn("ads_insights", result, "action-breakdown rows go into the main insights table")
        rows = result["ads_insights"]
        self.assertEqual(len(rows), 3, "one row per action_type returned by the API")

        for row in rows:
            for field in ("ad_name", "impressions", "clicks", "spend", "reach", "campaign_name"):
                self.assertIn(field, row, f"{field} must be present on every per-action row")
                self.assertEqual(row[field], _ad_insights_row()[field])

    def test_action_reaction_breakdown_also_copies_metric_fields(self):
        row_config = _make_row_config(parameters="action_breakdowns=action_reaction")
        parser = OutputParser(_FakePageLoader(), page_id="act_123", row_config=row_config)

        response = {"data": [_ad_insights_row()]}
        result = parser.parse_data(response, fb_node="adaccount", parent_id="act_123")

        rows = result["ads_insights"]
        self.assertTrue(rows)
        for row in rows:
            self.assertEqual(row["ad_name"], "Spring promo")
            self.assertEqual(row["impressions"], "1000")

    def test_nested_action_lists_are_not_copied_as_scalars(self):
        """The amplifier fields themselves (``actions`` etc.) stay out of per-action rows."""
        row_config = _make_row_config(parameters="action_breakdowns=action_type")
        parser = OutputParser(_FakePageLoader(), page_id="act_123", row_config=row_config)

        response = {"data": [_ad_insights_row()]}
        result = parser.parse_data(response, fb_node="adaccount", parent_id="act_123")

        for row in result["ads_insights"]:
            self.assertNotIn("actions", row)

    def test_non_extended_copy_keeps_restricted_field_set(self):
        """The legacy identifier-only field list still gates ``extended=False`` callers."""
        row_config = _make_row_config()
        parser = OutputParser(_FakePageLoader(), page_id="act_123", row_config=row_config)

        base_row: dict = {}
        parser._copy_common_fields(base_row, _ad_insights_row(), extended=False)

        # The restricted set is intentionally identifier-only.
        for field in ("account_id", "ad_id", "campaign_id", "date_start", "date_stop"):
            self.assertIn(field, base_row)
        # Metric fields and human-readable names must not leak in via the non-extended path.
        for field in ("ad_name", "campaign_name", "impressions", "spend", "reach"):
            self.assertNotIn(field, base_row)


class TestDeclaredFieldBackfill(unittest.TestCase):
    """Bug 2: requested fields appear as columns even when FB omits them entirely."""

    def test_dsl_fields_backfilled_when_api_omits_them(self):
        fields = (
            "insights.level(ad).action_breakdowns(action_type)"
            ".date_preset(last_3d){ad_id,ad_name,impressions,clicks,spend,reach}"
        )
        row_config = _make_row_config(fields=fields, parameters="action_breakdowns=action_type")
        parser = OutputParser(_FakePageLoader(), page_id="act_123", row_config=row_config)

        # API returned rows but omitted `impressions` and `reach` entirely — the bug scenario.
        api_row = {
            "account_id": "123",
            "ad_id": "ad_1",
            "ad_name": "Spring promo",
            "clicks": "0",
            "spend": "0",
            "date_start": "2026-01-01",
            "date_stop": "2026-01-01",
            "actions": [{"action_type": "link_click", "value": "1"}],
        }
        result = parser.parse_data({"data": [api_row]}, fb_node="adaccount", parent_id="act_123")

        rows = result["ads_insights"]
        self.assertTrue(rows)
        for row in rows:
            self.assertIn("impressions", row, "declared but missing impressions must appear as a column")
            self.assertIn("reach", row, "declared but missing reach must appear as a column")
            self.assertEqual(row["impressions"], "")
            self.assertEqual(row["reach"], "")

    def test_parameters_fields_backfilled_for_regular_query(self):
        row_config = _make_row_config(fields="id,name,description", parameters=None)
        row_config.query.path = "campaigns"
        parser = OutputParser(_FakePageLoader(), page_id="act_123", row_config=row_config)

        # FB omits `description` entirely.
        api_row = {"id": "c_1", "name": "Spring 2026"}
        result = parser.parse_data({"data": [api_row]}, fb_node="adaccount", parent_id="act_123")

        # Regular (non-insights) query routes through path-based table naming.
        self.assertTrue(result, "regular query produced no output")
        table_name, rows = next(iter(result.items()))
        self.assertTrue(rows)
        for row in rows:
            self.assertIn("description", row)
            self.assertEqual(row["description"], "")

    def test_no_declared_fields_leaves_row_untouched(self):
        row_config = _make_row_config(fields="insights", parameters=None)
        parser = OutputParser(_FakePageLoader(), page_id="act_123", row_config=row_config)

        api_row = {"id": "x", "spend": "1.00", "date_start": "2026-01-01", "date_stop": "2026-01-01"}
        result = parser.parse_data({"data": [api_row]}, fb_node="adaccount", parent_id="act_123")

        rows = result["ads_insights"]
        self.assertEqual(len(rows), 1)
        # No fabricated columns when the user didn't enumerate any.
        self.assertNotIn("impressions", rows[0])


if __name__ == "__main__":
    unittest.main()
