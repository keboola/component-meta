"""Resilience tests for FacebookClient's *production* execution paths.

These drive the code that real jobs actually run:

* async insights → ``_poll_and_process_async_jobs`` (parallel-start, then poll),
* sync queries   → ``_process_single_sync_query`` (per-object pagination).

The earlier async-retry test exercised ``PageLoader._load_async_insights`` directly,
which production never calls — so the resubmit/backoff it asserted was dead code.
Every test here goes through a client method that a job invokes.
"""

import unittest
from unittest.mock import MagicMock, patch

from keboola.component.exceptions import UserException
from requests import HTTPError

from client import FacebookClient
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


if __name__ == "__main__":
    unittest.main()
