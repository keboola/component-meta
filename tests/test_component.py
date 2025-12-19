"""
Functional tests for Meta (Facebook/Instagram/Pages) component.

Tests cover:
- Facebook Ads: sync insights, DSL parsing, long date ranges (30-day guard should NOT trigger)
- Facebook Pages: feed queries
- Instagram: insights with valid date range, account filtering

@author: esner
"""
import os
import unittest
import mock
from freezegun import freeze_time
from component import Component


class TestComponent(unittest.TestCase):
    def comparedict(self, actual, expected, msg=None):
        """Helper method to compare dictionaries with clear error messages."""
        for key, expected_value in expected.items():
            self.assertIn(key, actual, f"{msg}: Key '{key}' not found in actual dict")
            self.assertEqual(actual[key], expected_value, f"{msg}: Value mismatch for key '{key}'")

    @freeze_time("2010-10-10")
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        """Test that component fails with non-existing config directory."""
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()


class TestPageLoader(unittest.TestCase):
    """Unit tests for PageLoader class - specifically testing component_id guards."""

    @freeze_time("2024-01-15 10:00:00")
    def test_is_instagram_component_true(self):
        """Test _is_instagram_component returns True for Instagram component."""
        from page_loader import PageLoader

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-instagram-v2")
        self.assertTrue(loader._is_instagram_component())

    @freeze_time("2024-01-15 10:00:00")
    def test_is_instagram_component_false_for_ads(self):
        """Test _is_instagram_component returns False for Facebook Ads component."""
        from page_loader import PageLoader

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-facebook-ads-v2")
        self.assertFalse(loader._is_instagram_component())

    @freeze_time("2024-01-15 10:00:00")
    def test_is_instagram_component_false_for_pages(self):
        """Test _is_instagram_component returns False for Facebook Pages component."""
        from page_loader import PageLoader

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-facebook-pages")
        self.assertFalse(loader._is_instagram_component())

    @freeze_time("2024-01-15 10:00:00")
    def test_is_instagram_component_false_for_none(self):
        """Test _is_instagram_component returns False when component_id is None."""
        from page_loader import PageLoader

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", None)
        self.assertFalse(loader._is_instagram_component())

    @freeze_time("2024-01-15 10:00:00")
    def test_30day_validation_skipped_for_ads(self):
        """
        Test that 30-day validation is skipped for Facebook Ads component.

        This is a critical regression test - Facebook Ads should NOT get
        the Instagram-specific 30-day validation error.
        """
        from page_loader import PageLoader
        from configuration import QueryConfig

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-facebook-ads-v2")
        query_config = QueryConfig(
            path="",
            fields="insights.metric(reach,impressions){ad_id,reach,impressions}",
            since="2024-09-01",
            until="2024-11-30",
        )
        params = loader._build_params(query_config)
        self.assertIn("since", params)
        self.assertIn("until", params)
        self.assertEqual(params["since"], "2024-09-01")
        self.assertEqual(params["until"], "2024-11-30")

    @freeze_time("2024-01-15 10:00:00")
    def test_30day_validation_skipped_for_pages(self):
        """
        Test that 30-day validation is skipped for Facebook Pages component.

        This is a critical regression test - Facebook Pages should NOT get
        the Instagram-specific 30-day validation error.
        """
        from page_loader import PageLoader
        from configuration import QueryConfig

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-facebook-pages")
        query_config = QueryConfig(
            path="",
            fields="insights.metric(reach,impressions){page_id,reach,impressions}",
            since="2024-09-01",
            until="2024-11-30",
        )
        params = loader._build_params(query_config)
        self.assertIn("since", params)
        self.assertIn("until", params)
        self.assertEqual(params["since"], "2024-09-01")
        self.assertEqual(params["until"], "2024-11-30")

    @freeze_time("2024-01-15 10:00:00")
    def test_30day_validation_triggers_for_instagram(self):
        """
        Test that 30-day validation triggers for Instagram component.

        Instagram insights queries with >30 day range should raise UserException.
        """
        from page_loader import PageLoader
        from configuration import QueryConfig
        from keboola.component.exceptions import UserException

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-instagram-v2")
        query_config = QueryConfig(
            path="",
            fields="insights.metric(follower_count,profile_views){value,end_time}",
            since="2024-09-01",
            until="2024-11-30",
        )
        with self.assertRaises(UserException) as context:
            loader._build_params(query_config)
        self.assertIn("30 days", str(context.exception))
        self.assertIn("Instagram", str(context.exception))

    @freeze_time("2024-01-15 10:00:00")
    def test_instagram_valid_date_range_no_error(self):
        """
        Test that Instagram component with valid date range (<=30 days) works.
        """
        from page_loader import PageLoader
        from configuration import QueryConfig

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-instagram-v2")
        query_config = QueryConfig(
            path="",
            fields="insights.metric(follower_count,profile_views){value,end_time}",
            since="2024-01-01",
            until="2024-01-15",
        )
        params = loader._build_params(query_config)
        self.assertIn("since", params)
        self.assertIn("until", params)
        self.assertEqual(params["since"], "2024-01-01")
        self.assertEqual(params["until"], "2024-01-15")

    @freeze_time("2024-01-15 10:00:00")
    def test_dsl_parsing_action_breakdowns(self):
        """Test DSL parsing extracts action_breakdowns parameter."""
        from page_loader import PageLoader
        from configuration import QueryConfig

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-facebook-ads-v2")
        query_config = QueryConfig(
            path="",
            fields="insights.action_breakdowns(action_type).level(ad){ad_id,actions}",
        )
        params = loader._build_params(query_config)
        self.assertEqual(params.get("action_breakdowns"), "action_type")
        self.assertEqual(params.get("level"), "ad")

    @freeze_time("2024-01-15 10:00:00")
    def test_dsl_parsing_breakdowns(self):
        """Test DSL parsing extracts breakdowns parameter."""
        from page_loader import PageLoader
        from configuration import QueryConfig

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-facebook-ads-v2")
        query_config = QueryConfig(
            path="",
            fields="insights.breakdowns(country).level(ad){ad_id,impressions,country}",
        )
        params = loader._build_params(query_config)
        self.assertEqual(params.get("breakdowns"), "country")
        self.assertEqual(params.get("level"), "ad")

    @freeze_time("2024-01-15 10:00:00")
    def test_dsl_parsing_time_increment(self):
        """Test DSL parsing extracts time_increment parameter."""
        from page_loader import PageLoader
        from configuration import QueryConfig

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-facebook-ads-v2")
        query_config = QueryConfig(
            path="",
            fields="insights.time_increment(1).level(ad){ad_id,impressions}",
        )
        params = loader._build_params(query_config)
        self.assertEqual(params.get("time_increment"), "1")
        self.assertEqual(params.get("level"), "ad")

    @freeze_time("2024-01-15 10:00:00")
    def test_dsl_parsing_date_preset(self):
        """Test DSL parsing extracts date_preset parameter."""
        from page_loader import PageLoader
        from configuration import QueryConfig

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-facebook-ads-v2")
        query_config = QueryConfig(
            path="",
            fields="insights.date_preset(last_7d).level(ad){ad_id,impressions}",
        )
        params = loader._build_params(query_config)
        self.assertEqual(params.get("date_preset"), "last_7d")
        self.assertEqual(params.get("level"), "ad")

    @freeze_time("2024-01-15 10:00:00")
    def test_dsl_parsing_metric(self):
        """Test DSL parsing extracts and normalizes metric parameter."""
        from page_loader import PageLoader
        from configuration import QueryConfig

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-facebook-ads-v2")
        query_config = QueryConfig(
            path="",
            fields="insights.metric(reach, impressions, clicks){ad_id}",
        )
        params = loader._build_params(query_config)
        self.assertEqual(params.get("metric"), "reach,impressions,clicks")

    @freeze_time("2024-01-15 10:00:00")
    def test_account_id_auto_included_in_fields(self):
        """Test that account_id is automatically added to explicit fields list."""
        from page_loader import PageLoader
        from configuration import QueryConfig

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-facebook-ads-v2")
        query_config = QueryConfig(
            path="",
            fields="insights.level(ad){ad_id,ad_name,impressions,clicks}",
        )
        params = loader._build_params(query_config)
        fields = params.get("fields", "")
        self.assertIn("account_id", fields, "account_id should be auto-included")

    @freeze_time("2024-01-15 10:00:00")
    def test_account_id_not_duplicated_if_present(self):
        """Test that account_id is not duplicated if already in fields list."""
        from page_loader import PageLoader
        from configuration import QueryConfig

        mock_client = mock.MagicMock()
        loader = PageLoader(mock_client, "nested-query", "v23.0", "keboola.ex-facebook-ads-v2")
        query_config = QueryConfig(
            path="",
            fields="insights.level(ad){ad_id,account_id,impressions}",
        )
        params = loader._build_params(query_config)
        fields = params.get("fields", "")
        self.assertEqual(fields.count("account_id"), 1, "account_id should appear exactly once")


class TestFacebookClient(unittest.TestCase):
    """Unit tests for FacebookClient class - specifically testing component_id guards."""

    def test_is_instagram_component_true(self):
        """Test _is_instagram_component returns True for Instagram component."""
        from client import FacebookClient

        mock_oauth = mock.MagicMock()
        mock_oauth.data = {"access_token": "test-token"}
        client = FacebookClient(mock_oauth, "v23.0", "keboola.ex-instagram-v2")
        self.assertTrue(client._is_instagram_component())

    def test_is_instagram_component_false_for_ads(self):
        """Test _is_instagram_component returns False for Facebook Ads component."""
        from client import FacebookClient

        mock_oauth = mock.MagicMock()
        mock_oauth.data = {"access_token": "test-token"}
        client = FacebookClient(mock_oauth, "v23.0", "keboola.ex-facebook-ads-v2")
        self.assertFalse(client._is_instagram_component())

    def test_is_instagram_component_false_for_pages(self):
        """Test _is_instagram_component returns False for Facebook Pages component."""
        from client import FacebookClient

        mock_oauth = mock.MagicMock()
        mock_oauth.data = {"access_token": "test-token"}
        client = FacebookClient(mock_oauth, "v23.0", "keboola.ex-facebook-pages")
        self.assertFalse(client._is_instagram_component())

    def test_is_instagram_insights_query_true(self):
        """Test _is_instagram_insights_query returns True for IG insights query."""
        from client import FacebookClient
        from configuration import QueryRow, QueryConfig

        mock_oauth = mock.MagicMock()
        mock_oauth.data = {"access_token": "test-token"}
        client = FacebookClient(mock_oauth, "v23.0", "keboola.ex-instagram-v2")
        row_config = QueryRow(
            id=1,
            type="nested-query",
            name="test",
            query=QueryConfig(
                path="",
                fields="insights.metric(follower_count,profile_views){value,end_time}",
            ),
        )
        self.assertTrue(client._is_instagram_insights_query(row_config))

    def test_is_instagram_insights_query_false_for_non_insights(self):
        """Test _is_instagram_insights_query returns False for non-insights query."""
        from client import FacebookClient
        from configuration import QueryRow, QueryConfig

        mock_oauth = mock.MagicMock()
        mock_oauth.data = {"access_token": "test-token"}
        client = FacebookClient(mock_oauth, "v23.0", "keboola.ex-instagram-v2")
        row_config = QueryRow(
            id=1,
            type="nested-query",
            name="test",
            query=QueryConfig(
                path="media",
                fields="id,caption,media_type",
            ),
        )
        self.assertFalse(client._is_instagram_insights_query(row_config))


if __name__ == "__main__":
    unittest.main()
