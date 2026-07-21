"""Resilience tests for FacebookClient's *production* execution paths.

These drive the code that real jobs actually run:

* async insights → ``_poll_and_process_async_jobs`` (parallel-start, then poll),
* sync queries   → ``_process_single_sync_query`` (per-object pagination).

The earlier async-retry test exercised ``PageLoader._load_async_insights`` directly,
which production never calls — so the resubmit/backoff it asserted was dead code.
Every test here goes through a client method that a job invokes.
"""

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from keboola.component.exceptions import UserException
from requests import HTTPError

from client import FacebookClient, breakdowns_requiring_enablement
from page_loader import _FB_TRANSIENT_ERROR_MAX_RETRIES, AsyncInsightsJobTransientError


def make_client() -> FacebookClient:
    oauth = MagicMock()
    oauth.data = {"access_token": "user-token"}
    return FacebookClient(oauth, "v25.0")


def make_async_job_details(loader: MagicMock, parser: MagicMock, page_id: str = "act_123") -> dict:
    """Build the per-report job_details dict that _start_async_jobs_for_query produces."""
    return {
        "report-1": {
            "page_id": page_id,
            "page_loader": loader,
            "output_parser": parser,
            "fb_graph_node": "ad_account",
            "access_token": "tok",
            "row_config": MagicMock(),
            "start_params": {"access_token": "tok"},
        }
    }


@patch("client.time.sleep", return_value=None)
class TestAsyncInsightsResubmit(unittest.TestCase):
    def test_resubmits_after_transient_failure_then_succeeds(self, _sleep):
        """A transient 'Job Failed' on poll triggers a real re-submit + re-poll in production."""
        loader = MagicMock()
        loader.poll_async_job.side_effect = [
            AsyncInsightsJobTransientError("async_status=Job Failed"),
            {"data": [{"impressions": "5"}]},
        ]
        loader.start_async_insights_job.return_value = "report-2"
        parser = MagicMock()
        parser.iter_parsed_data.return_value = iter([{"query_x": [{"impressions": "5"}]}])

        client = make_client()
        results = list(client._poll_and_process_async_jobs(make_async_job_details(loader, parser)))

        self.assertEqual(results, [{"query_x": [{"impressions": "5"}]}])
        self.assertEqual(loader.poll_async_job.call_count, 2)
        self.assertEqual(loader.start_async_insights_job.call_count, 1)  # one resubmit
        self.assertEqual(client.skipped_objects, 0)

    def test_exhausted_retries_skip_and_count_without_crashing(self, _sleep):
        """Persistent transient failures skip that one report and increment the counter (contain + warn)."""
        loader = MagicMock()
        loader.poll_async_job.side_effect = AsyncInsightsJobTransientError("async_status=Job Failed")
        loader.start_async_insights_job.return_value = "report-n"
        parser = MagicMock()

        client = make_client()
        results = list(client._poll_and_process_async_jobs(make_async_job_details(loader, parser)))

        self.assertEqual(results, [])
        self.assertEqual(client.skipped_objects, 1)
        self.assertEqual(loader.poll_async_job.call_count, _FB_TRANSIENT_ERROR_MAX_RETRIES + 1)
        parser.iter_parsed_data.assert_not_called()

    def test_pagination_httperror_skips_and_counts(self, _sleep):
        """An HTTPError while parsing/paginating the async result is contained, not fatal."""
        loader = MagicMock()
        loader.poll_async_job.return_value = {"data": [{"impressions": "5"}]}
        parser = MagicMock()
        parser.iter_parsed_data.side_effect = HTTPError("500 Server Error")

        client = make_client()
        results = list(client._poll_and_process_async_jobs(make_async_job_details(loader, parser)))

        self.assertEqual(results, [])
        self.assertEqual(client.skipped_objects, 1)

    def test_programming_error_propagates_not_swallowed(self, _sleep):
        """A KeyError (real bug) must crash the job, not be masked as a skipped object."""
        loader = MagicMock()
        loader.poll_async_job.return_value = {"data": [{"impressions": "5"}]}
        parser = MagicMock()
        parser.iter_parsed_data.side_effect = KeyError("regression")

        client = make_client()
        with self.assertRaises(KeyError):
            list(client._poll_and_process_async_jobs(make_async_job_details(loader, parser)))


def make_sync_row(path: str = "feed") -> MagicMock:
    row = MagicMock()
    row.name = "my_query"
    row.type = "query"
    row.query.path = path
    row.query.ids = None
    row.query.fields = "message,created_time"
    return row


@patch("client.OutputParser")
@patch("client.PageLoader")
class TestSyncPaginationResilience(unittest.TestCase):
    def _prepare(self, MockPL, MockOP):
        client = make_client()
        client._request_require_page_token = MagicMock(return_value=False)
        client._get_fb_graph_node = MagicMock(return_value="page")
        loader = MockPL.return_value
        loader.load_page.return_value = {"data": [{"id": "1"}]}
        parser = MockOP.return_value
        account = MagicMock()
        account.id = "act_1"
        return client, parser, [account]

    def test_pagination_httperror_skips_and_counts(self, MockPL, MockOP):
        client, parser, accounts = self._prepare(MockPL, MockOP)
        parser.iter_parsed_data.side_effect = HTTPError("500 Server Error")

        results = list(client._process_single_sync_query(accounts, make_sync_row()))

        self.assertEqual(results, [])
        self.assertEqual(client.skipped_objects, 1)

    def test_programming_error_propagates(self, MockPL, MockOP):
        client, parser, accounts = self._prepare(MockPL, MockOP)
        parser.iter_parsed_data.side_effect = AttributeError("regression")

        with self.assertRaises(AttributeError):
            list(client._process_single_sync_query(accounts, make_sync_row()))

    def test_user_actionable_error_propagates(self, MockPL, MockOP):
        """UserException (e.g. misconfigured metric) must surface, not be silently skipped."""
        client, parser, accounts = self._prepare(MockPL, MockOP)
        parser.iter_parsed_data.side_effect = UserException("Add 'metric_type(total_value)' ...")

        with self.assertRaises(UserException):
            list(client._process_single_sync_query(accounts, make_sync_row()))


def make_async_row(parameters: str = "", fields: str = "", name: str = "ad_perf") -> SimpleNamespace:
    """A minimal async-insights row_config with a real query for breakdown detection."""
    return SimpleNamespace(
        name=name,
        type="async-insights-query",
        query=SimpleNamespace(parameters=parameters, fields=fields),
    )


class TestBreakdownsRequiringEnablement(unittest.TestCase):
    """The pure detector for Aug-6-2026 enablement-gated Ads breakdowns (SUPPORT-17071)."""

    def test_detects_gated_breakdown_in_url_parameters(self):
        query = SimpleNamespace(parameters="level=ad&breakdowns=impression_device,publisher_platform", fields="")
        self.assertEqual(breakdowns_requiring_enablement(query), ["impression_device"])

    def test_detects_gated_breakdown_in_dsl_fields(self):
        query = SimpleNamespace(parameters="", fields="insights.breakdowns(frequency_value){reach}")
        self.assertEqual(breakdowns_requiring_enablement(query), ["frequency_value"])

    def test_ignores_ungated_advertiser_timezone_variant(self):
        # audience != advertiser — only the audience variant is gated.
        query = SimpleNamespace(parameters="breakdowns=hourly_stats_aggregated_by_advertiser_time_zone", fields="")
        self.assertEqual(breakdowns_requiring_enablement(query), [])

    def test_no_breakdowns_returns_empty(self):
        query = SimpleNamespace(parameters="level=ad&breakdowns=publisher_platform", fields="")
        self.assertEqual(breakdowns_requiring_enablement(query), [])

    def test_none_query_returns_empty(self):
        self.assertEqual(breakdowns_requiring_enablement(None), [])


@patch("client.time.sleep", return_value=None)
class TestBreakdownEnablementWarning(unittest.TestCase):
    """An empty async result for a gated-breakdown query must be explained, not silent."""

    def _run_empty(self, row_config):
        loader = MagicMock()
        loader.poll_async_job.return_value = {"data": []}
        parser = MagicMock()
        details = make_async_job_details(loader, parser)
        details["report-1"]["row_config"] = row_config
        client = make_client()
        results = list(client._poll_and_process_async_jobs(details))
        # Empty result is never a parse; nothing is yielded and nothing is counted as skipped.
        self.assertEqual(results, [])
        self.assertEqual(client.skipped_objects, 0)
        parser.iter_parsed_data.assert_not_called()

    def test_warns_on_empty_result_for_gated_breakdown(self, _sleep):
        row = make_async_row(parameters="level=ad&breakdowns=impression_device", name="ad_perf")
        with self.assertLogs("client", level="WARNING") as cm:
            self._run_empty(row)
        self.assertTrue(
            any("impression_device" in msg and "Ads Manager" in msg for msg in cm.output),
            cm.output,
        )

    def test_no_warning_on_empty_result_without_gated_breakdown(self, _sleep):
        row = make_async_row(parameters="level=ad&breakdowns=publisher_platform")
        with self.assertNoLogs("client", level="WARNING"):
            self._run_empty(row)


if __name__ == "__main__":
    unittest.main()
