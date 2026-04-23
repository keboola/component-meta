"""Unit tests for the threshold-based streaming behavior of ``OutputParser.iter_parsed_data``.

CFTL-473 / SUPPORT-15993: the previous ``parse_data`` accumulated every paginated row
into a single dict before returning. These tests verify that:

* rows flush in bounded batches once the configured threshold is reached,
* small queries still yield a single accumulated batch (matching the pre-CFTL-473
  Component write path so per-batch overhead is not multiplied), and
* action-stats expansion — the real memory amplifier for insights queries with
  ``actions``/``action_values`` — participates in the threshold-based flushing.
"""

import sys
import tracemalloc
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output_parser import OutputParser  # noqa: E402


class _FakePageLoader:
    """Minimal stand-in for ``PageLoader`` that replays pre-built JSON pages."""

    def __init__(self, follow_up_pages: list[dict]):
        self._pages = list(follow_up_pages)
        self.load_calls: list[str] = []

    def load_page_from_url(self, url: str) -> dict:
        self.load_calls.append(url)
        return self._pages.pop(0) if self._pages else {}


def _make_row_config(fields: str = "insights", path: str = "", parameters=None):
    row_config = MagicMock()
    row_config.name = "my_query"
    row_config.type = "regular"
    row_config.query.path = path
    row_config.query.fields = fields
    row_config.query.parameters = parameters
    return row_config


def _make_insights_page(
    n_rows: int,
    next_url: str | None = None,
    with_actions: bool = False,
    row_offset: int = 0,
) -> dict:
    """Build a single ``/insights`` page with ``n_rows`` rows, optional paging.next."""
    data = []
    for i in range(n_rows):
        row = {
            "account_id": "act_1",
            "campaign_id": f"c-{row_offset + i}",
            "spend": f"{row_offset + i}.00",
            "date_start": "2026-01-01",
            "date_stop": "2026-01-01",
        }
        if with_actions:
            row["actions"] = [
                {"action_type": "link_click", "value": "10"},
                {"action_type": "post_reaction", "value": "5"},
                {"action_type": "landing_page_view", "value": "3"},
            ]
        data.append(row)

    page = {"data": data}
    if next_url:
        page["paging"] = {"next": next_url}
    return page


class TestThresholdBasedStreaming(unittest.TestCase):
    def test_small_query_yields_single_batch(self):
        """Queries below the threshold yield exactly one batch — old Component write path."""
        row_config = _make_row_config()
        page1 = _make_insights_page(
            n_rows=5, next_url="https://graph.facebook.com/v20.0/act_1/insights?after=A", row_offset=0
        )
        page2 = _make_insights_page(
            n_rows=5, next_url="https://graph.facebook.com/v20.0/act_1/insights?after=B", row_offset=5
        )
        page3 = _make_insights_page(n_rows=5, row_offset=10)

        loader = _FakePageLoader(follow_up_pages=[page2, page3])
        parser = OutputParser(loader, page_id="act_1", row_config=row_config)

        batches = list(parser.iter_parsed_data(page1, fb_node="page_insights", parent_id="act_1", row_threshold=10_000))
        self.assertEqual(len(batches), 1, "15 rows should collapse into a single batch below threshold")
        self.assertEqual(len(batches[0]["my_query_insights"]), 15)

    def test_large_query_flushes_in_bounded_batches(self):
        """Crossing the threshold forces a flush; no batch exceeds ≈ threshold rows."""
        row_config = _make_row_config()
        threshold = 100
        rows_per_page = 40  # 3 pages exceed threshold
        urls = [f"https://graph.facebook.com/v20.0/act_1/insights?after={i}" for i in range(5)]

        pages = [
            _make_insights_page(n_rows=rows_per_page, next_url=urls[i + 1], row_offset=i * rows_per_page)
            for i in range(4)
        ]
        pages.append(_make_insights_page(n_rows=rows_per_page, row_offset=4 * rows_per_page))

        loader = _FakePageLoader(follow_up_pages=pages[1:])
        parser = OutputParser(loader, page_id="act_1", row_config=row_config)

        batches = list(
            parser.iter_parsed_data(pages[0], fb_node="page_insights", parent_id="act_1", row_threshold=threshold)
        )

        total_rows = sum(len(b["my_query_insights"]) for b in batches)
        self.assertEqual(total_rows, 5 * rows_per_page, "no rows lost across batches")
        self.assertGreater(len(batches), 1, "large query should flush more than once")
        # Each intermediate batch clears when it hits the threshold; final batch holds the tail.
        for batch in batches[:-1]:
            self.assertGreaterEqual(len(batch["my_query_insights"]), threshold)

    def test_action_stats_expansion_counts_toward_threshold(self):
        """Action-stats rows (the real amplifier) trigger flushes, not just raw API rows."""
        row_config = _make_row_config()
        threshold = 50
        # 1 page × 10 rows × 3 action expansions = 30 rows — below threshold
        single_page = _make_insights_page(n_rows=10, with_actions=True)
        parser = OutputParser(_FakePageLoader([]), page_id="act_1", row_config=row_config)
        batches = list(
            parser.iter_parsed_data(single_page, fb_node="page_insights", parent_id="act_1", row_threshold=threshold)
        )
        self.assertEqual(len(batches), 1)
        self.assertEqual(len(batches[0]["my_query_insights"]), 30)

        # 3 pages × 10 rows × 3 expansions = 90 rows with threshold=50:
        # end of page 2 (60 rows) crosses threshold → flush; page 3 (30 rows) goes to final yield.
        p1 = _make_insights_page(
            n_rows=10,
            next_url="https://graph.facebook.com/v20.0/act_1/insights?after=A",
            with_actions=True,
            row_offset=0,
        )
        p2 = _make_insights_page(
            n_rows=10,
            next_url="https://graph.facebook.com/v20.0/act_1/insights?after=B",
            with_actions=True,
            row_offset=10,
        )
        p3 = _make_insights_page(n_rows=10, with_actions=True, row_offset=20)
        parser = OutputParser(_FakePageLoader(follow_up_pages=[p2, p3]), page_id="act_1", row_config=row_config)
        batches = list(parser.iter_parsed_data(p1, fb_node="page_insights", parent_id="act_1", row_threshold=threshold))
        total = sum(len(b["my_query_insights"]) for b in batches)
        self.assertEqual(total, 90)
        self.assertEqual(len(batches), 2)

    def test_peak_memory_is_bounded_by_threshold_not_total_rows(self):
        """Regression guard: peak memory stays ≈ threshold, not proportional to total rows."""
        row_config = _make_row_config()
        total_pages = 50
        rows_per_page = 200
        threshold = 1000  # 5x rows_per_page × 3 actions ≈ 3000, flushes roughly every 2 pages
        urls = [f"https://graph.facebook.com/v20.0/act_1/insights?after=P{i}" for i in range(total_pages)]

        pages = [
            _make_insights_page(
                n_rows=rows_per_page, next_url=urls[i + 1], with_actions=True, row_offset=i * rows_per_page
            )
            for i in range(total_pages - 1)
        ]
        pages.append(
            _make_insights_page(n_rows=rows_per_page, with_actions=True, row_offset=(total_pages - 1) * rows_per_page)
        )

        loader = _FakePageLoader(follow_up_pages=pages[1:])
        parser = OutputParser(loader, page_id="act_1", row_config=row_config)

        tracemalloc.start()
        tracemalloc.reset_peak()
        total_rows = 0
        for batch in parser.iter_parsed_data(
            pages[0], fb_node="page_insights", parent_id="act_1", row_threshold=threshold
        ):
            total_rows += sum(len(v) for v in batch.values())
            del batch  # emulate the Component flushing the batch to CSV
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        self.assertEqual(total_rows, total_pages * rows_per_page * 3)
        # 30k total rows would be several MB if fully materialized; bounded streaming keeps
        # the working set tied to threshold size. Generous ceiling for noise tolerance.
        self.assertLess(
            peak_bytes,
            4 * 1024 * 1024,
            f"threshold-bounded streaming should cap peak memory, saw peak={peak_bytes} bytes",
        )


class TestParseDataCompatibility(unittest.TestCase):
    def test_parse_data_returns_fully_accumulated_dict(self):
        """Nested-field callers still use the accumulating ``parse_data`` wrapper unchanged."""
        row_config = _make_row_config()
        page1 = _make_insights_page(
            n_rows=3, next_url="https://graph.facebook.com/v20.0/act_1/insights?after=A", row_offset=0
        )
        page2 = _make_insights_page(n_rows=3, row_offset=3)

        parser = OutputParser(_FakePageLoader(follow_up_pages=[page2]), page_id="act_1", row_config=row_config)
        result = parser.parse_data(page1, fb_node="page_insights", parent_id="act_1")

        self.assertIn("my_query_insights", result)
        self.assertEqual(len(result["my_query_insights"]), 6)


if __name__ == "__main__":
    unittest.main()
