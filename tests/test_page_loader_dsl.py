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

    def test_parse_action_attribution_windows_parameter(self):
        """Test parsing .action_attribution_windows(7d_click,1d_view) from DSL syntax"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = "insights.action_attribution_windows(7d_click,1d_view)"
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        self.assertEqual(params["action_attribution_windows"], "7d_click,1d_view")

    def test_parse_action_report_time_parameter(self):
        """Test parsing .action_report_time(impression) from DSL syntax"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = "insights.action_report_time(impression)"
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        self.assertEqual(params["action_report_time"], "impression")

    def test_parse_use_account_attribution_setting_parameter(self):
        """Test parsing .use_account_attribution_setting(true) from DSL syntax"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = "insights.use_account_attribution_setting(true)"
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        self.assertEqual(params["use_account_attribution_setting"], "true")

    def test_parse_filtering_parameter(self):
        """Test parsing .filtering([{...}]) from DSL syntax"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = (
            'insights.filtering([{"field":"action_type","operator":"IN","value":["offsite_conversion"]}])'
        )
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        self.assertIn("filtering", params)
        # The filtering value should be extracted (without the outer parentheses)
        self.assertIn("action_type", params["filtering"])

    def test_parse_summary_action_breakdowns_parameter(self):
        """Test parsing .summary_action_breakdowns(action_type) from DSL syntax"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = "insights.summary_action_breakdowns(action_type)"
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        self.assertEqual(params["summary_action_breakdowns"], "action_type")

    def test_parse_complex_query_with_attribution_and_filtering(self):
        """Test parsing a complex query with multiple attribution and filtering parameters"""
        query_config = MagicMock()
        query_config.limit = 500
        query_config.path = None
        query_config.fields = (
            "insights.level(ad).action_breakdowns(action_type)"
            ".action_attribution_windows(7d_click,1d_view)"
            ".action_report_time(impression)"
            ".date_preset(last_30d).time_increment(1)"
            "{ad_id,ad_name,spend,impressions,actions}"
        )
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        # Verify all parameters are parsed
        self.assertEqual(params["level"], "ad")
        self.assertEqual(params["action_breakdowns"], "action_type")
        self.assertEqual(params["action_attribution_windows"], "7d_click,1d_view")
        self.assertEqual(params["action_report_time"], "impression")
        self.assertEqual(params["date_preset"], "last_30d")
        self.assertEqual(params["time_increment"], "1")

        # Verify fields
        fields = params["fields"].split(",")
        self.assertIn("ad_id", fields)
        self.assertIn("ad_name", fields)
        self.assertIn("spend", fields)
        self.assertIn("impressions", fields)
        self.assertIn("actions", fields)
        self.assertIn("account_id", fields)

    def test_dsl_not_parsed_when_path_set(self):
        """Test that DSL syntax is NOT parsed when path is set (nested field expansion)"""
        query_config = MagicMock()
        query_config.limit = 100
        query_config.path = "ads"  # Custom path set
        query_config.fields = (
            "insights.level(ad).breakdowns(publisher_platform)"
            ".date_preset(last_90d).time_increment(1)"
            "{ad_id,impressions,reach,clicks,spend}"
        )
        query_config.since = ""
        query_config.until = ""
        query_config.parameters = None

        params = self.loader._build_params(query_config)

        # When path is set, DSL should NOT be parsed into parameters
        # Instead, fields should contain the full DSL string
        self.assertNotIn("level", params)
        self.assertNotIn("breakdowns", params)
        self.assertNotIn("date_preset", params)
        self.assertNotIn("time_increment", params)

        # The fields parameter should contain the original DSL string
        self.assertEqual(params["fields"], query_config.fields)


if __name__ == "__main__":
    unittest.main()
