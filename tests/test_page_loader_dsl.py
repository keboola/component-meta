"""
Unit tests for DSL parameter parsing in PageLoader.

Tests the fix for SUPPORT-14107: Facebook Ads V2 Extractor - DSL Parameter Parsing Issue
"""

import unittest
from unittest.mock import MagicMock

from src.page_loader import PageLoader


class TestDSLParameterParsing(unittest.TestCase):
    """Test DSL parameter parsing in PageLoader._build_params()"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = MagicMock()
        self.loader = PageLoader(client=self.client, query_type="regular", api_version="v20.0")

    def test_parse_level_parameter(self):
        """Test parsing .level(ad) from DSL syntax"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = "insights.level(ad)"
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        self.assertEqual(params["level"], "ad")
        self.assertEqual(params["limit"], 500)

    def test_parse_action_breakdowns_parameter(self):
        """Test parsing .action_breakdowns(action_type) from DSL syntax"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = "insights.action_breakdowns(action_type)"
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        self.assertEqual(params["action_breakdowns"], "action_type")

    def test_parse_date_preset_parameter(self):
        """Test parsing .date_preset(last_3d) from DSL syntax"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = "insights.date_preset(last_3d)"
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        self.assertEqual(params["date_preset"], "last_3d")

    def test_parse_time_increment_parameter(self):
        """Test parsing .time_increment(1) from DSL syntax"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = "insights.time_increment(1)"
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        self.assertEqual(params["time_increment"], "1")

    def test_parse_breakdowns_parameter(self):
        """Test parsing .breakdowns(age,gender) from DSL syntax"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = "insights.breakdowns(age,gender)"
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        self.assertEqual(params["breakdowns"], "age,gender")

    def test_parse_fields_from_curly_braces(self):
        """Test parsing fields from {field1,field2,...} syntax"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = "insights{ad_id,ad_name,spend,clicks}"
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        # Should include all specified fields plus account_id
        self.assertIn("fields", params)
        fields = params["fields"].split(",")
        self.assertIn("ad_id", fields)
        self.assertIn("ad_name", fields)
        self.assertIn("spend", fields)
        self.assertIn("clicks", fields)
        self.assertIn("account_id", fields)  # Always added for backwards compatibility

    def test_parse_customer_full_dsl_query(self):
        """Test parsing the full customer DSL query from SUPPORT-14107"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = (
            "insights.level(ad).action_breakdowns(action_type).date_preset(last_3d)"
            ".time_increment(1){ad_id,ad_name,campaign_id,campaign_name,"
            "cost_per_action_type,actions,impressions,reach,clicks,spend}"
        )
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        # Verify all DSL parameters are parsed correctly
        self.assertEqual(params["level"], "ad")
        self.assertEqual(params["action_breakdowns"], "action_type")
        self.assertEqual(params["date_preset"], "last_3d")
        self.assertEqual(params["time_increment"], "1")

        # Verify fields are extracted and account_id is added
        self.assertIn("fields", params)
        fields = params["fields"].split(",")
        self.assertIn("ad_id", fields)
        self.assertIn("ad_name", fields)
        self.assertIn("campaign_id", fields)
        self.assertIn("campaign_name", fields)
        self.assertIn("cost_per_action_type", fields)
        self.assertIn("actions", fields)
        self.assertIn("impressions", fields)
        self.assertIn("reach", fields)
        self.assertIn("clicks", fields)
        self.assertIn("spend", fields)
        self.assertIn("account_id", fields)  # Always added

    def test_account_id_not_duplicated(self):
        """Test that account_id is not duplicated if already present"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = "insights{account_id,ad_id,spend}"
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        # Count occurrences of account_id
        fields = params["fields"].split(",")
        account_id_count = fields.count("account_id")
        self.assertEqual(account_id_count, 1, "account_id should appear exactly once")

    def test_legacy_metric_and_period_still_work(self):
        """Test that existing .metric() and .period() parsing still works"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = "insights.metric(page_fans).period(day)"
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        self.assertEqual(params["metric"], "page_fans")
        self.assertEqual(params["period"], "day")


if __name__ == "__main__":
    unittest.main()
