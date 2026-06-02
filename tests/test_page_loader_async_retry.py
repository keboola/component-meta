"""Unit tests for transient-failure retry of async insights reports in PageLoader.

Facebook returns "Job Failed"/"Job Skipped"/timeout transiently under load; the loader
must re-submit the report instead of failing the whole job (FB error code 2 spike).
"""

import unittest
from unittest.mock import MagicMock, patch

from keboola.component.exceptions import UserException

from src.page_loader import AsyncInsightsJobTransientError, PageLoader


class TestAsyncInsightsTransientRetry(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.loader = PageLoader(client=self.client, query_type="async-insights-query", api_version="v25.0")
        self.query_config = MagicMock()

    @patch("src.page_loader.time.sleep", return_value=None)
    def test_resubmits_after_transient_failure_then_succeeds(self, _sleep):
        """A transient 'Job Failed' triggers a re-submit; the second attempt succeeds."""
        self.loader.start_async_insights_job = MagicMock(side_effect=["report-1", "report-2"])
        self.loader.poll_async_job = MagicMock(
            side_effect=[
                AsyncInsightsJobTransientError("async_status=Job Failed"),
                {"data": [{"impressions": "5"}]},
            ]
        )

        result = self.loader._load_async_insights(self.query_config, "act_123")

        self.assertEqual(result, {"data": [{"impressions": "5"}]})
        self.assertEqual(self.loader.start_async_insights_job.call_count, 2)
        self.assertEqual(self.loader.poll_async_job.call_count, 2)

    @patch("src.page_loader.time.sleep", return_value=None)
    def test_raises_user_exception_after_exhausting_retries(self, _sleep):
        """Persistent transient failures eventually surface as a UserException (not silent)."""
        self.loader.start_async_insights_job = MagicMock(return_value="report-x")
        self.loader.poll_async_job = MagicMock(side_effect=AsyncInsightsJobTransientError("async_status=Job Failed"))

        with self.assertRaises(UserException):
            self.loader._load_async_insights(self.query_config, "act_123")

        # initial attempt + _FB_TRANSIENT_ERROR_MAX_RETRIES (3) = 4 submissions
        self.assertEqual(self.loader.start_async_insights_job.call_count, 4)

    @patch("src.page_loader.time.sleep", return_value=None)
    def test_no_report_id_returns_empty_without_retry(self, _sleep):
        """If the report cannot be started at all, return empty data (existing behaviour)."""
        self.loader.start_async_insights_job = MagicMock(return_value=None)
        self.loader.poll_async_job = MagicMock()

        result = self.loader._load_async_insights(self.query_config, "act_123")

        self.assertEqual(result, {"data": []})
        self.loader.poll_async_job.assert_not_called()


if __name__ == "__main__":
    unittest.main()
