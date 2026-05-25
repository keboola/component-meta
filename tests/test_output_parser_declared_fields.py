"""Regression tests for ``OutputParser`` field-list parsing (CFTL-656 / SUPPORT-16397).

PR #50 introduced ``_backfill_declared_fields`` to fix CFTL-630 — when the Facebook API
omits a requested flat field from a row (zero data for the period), the function writes
``""`` so the output CSV keeps a stable column set against the existing Storage schema.

That fix was correct for flat scalar names (``impressions``, ``spend``, ``shares``) but
the field parser also returned a base name for **nested-edge DSL tokens** such as
``comments{message,from}`` or ``comments.limit(0).summary(true)``. Those names identify
*child tables*, not parent-row columns. Backfilling them as empty strings injected
phantom ``comments`` / ``messages`` / ``likes`` columns onto parent rows, and Storage
rejected the load with::

    Extra columns found: "comments, shares".
    During the import new columns can't be added.

These tests pin down:

* nested-edge tokens (``{...}`` expansion, ``.modifier(...)``) are NOT backfilled;
* flat scalar tokens still ARE backfilled (the original CFTL-630 case still passes);
* mixed field lists (flat + nested) keep only the flat names;
* the bug scenario itself — a row missing the nested key — leaves the parent row
  free of any phantom column.
"""

from unittest.mock import MagicMock

import pytest

from output_parser import OutputParser


def _query(fields: str = "", parameters=None) -> MagicMock:
    q = MagicMock()
    q.fields = fields
    q.parameters = parameters
    return q


# ---------- _base_field_name ----------


@pytest.mark.parametrize(
    "token,expected",
    [
        ("impressions", "impressions"),
        ("ad_id", "ad_id"),
        ("shares", "shares"),
        ("  spend  ", "spend"),
        ("", ""),
        ("   ", ""),
    ],
)
def test_base_field_name_keeps_flat_scalars(token: str, expected: str) -> None:
    assert OutputParser._base_field_name(token) == expected


@pytest.mark.parametrize(
    "token",
    [
        "comments{message,from}",
        "comments{message,from{name}}",
        "attachments{caption,type,description}",
        "messages{created_time,from,to,message}",
        "likes{name,username}",
        "comments.limit(0).summary(true)",
        "comments.summary(total_count, can_comment).limit(0)",
        "reactions.type(SAD).summary(total_count).limit(0)",
        "reactions.type(SAD)",
    ],
)
def test_base_field_name_drops_nested_edges(token: str) -> None:
    assert OutputParser._base_field_name(token) == ""


# ---------- _split_field_dsl ----------


def test_split_field_dsl_keeps_flat_drops_nested() -> None:
    fields = "attachments{caption, type, description},message,created_time,shares"
    assert OutputParser._split_field_dsl(fields) == ["message", "created_time", "shares"]


def test_split_field_dsl_all_nested_returns_empty() -> None:
    fields = "comments{message,created_time,from,comments{message,created_time,from}}"
    assert OutputParser._split_field_dsl(fields) == []


def test_split_field_dsl_all_flat_unchanged() -> None:
    fields = "ad_id,ad_name,spend,impressions,reach"
    assert OutputParser._split_field_dsl(fields) == ["ad_id", "ad_name", "spend", "impressions", "reach"]


# ---------- _parse_declared_fields ----------


def test_parse_declared_fields_insights_flat_only() -> None:
    query = _query("insights.since(today).metric(impressions,spend){ad_id,impressions,spend,reach}")
    assert OutputParser._parse_declared_fields(query) == ["ad_id", "impressions", "spend", "reach"]


def test_parse_declared_fields_path_query_with_mixed_fields() -> None:
    # Real shape: query_14 from Matyas's test config, also typical of GRPN configs.
    query = _query("attachments{caption, type, description},message,created_time,shares")
    assert OutputParser._parse_declared_fields(query) == ["message", "created_time", "shares"]


def test_parse_declared_fields_pure_nested_returns_empty() -> None:
    # The exact GRPN bug shape — query 23 in Matyas's test config.
    query = _query("comments{message,created_time,from,comments{message,created_time,from}}")
    assert OutputParser._parse_declared_fields(query) == []


def test_parse_declared_fields_modifier_only_returns_empty() -> None:
    # query 42 in Matyas's test config.
    query = _query("comments.limit(0).summary(true)")
    assert OutputParser._parse_declared_fields(query) == []


def test_parse_declared_fields_none_query() -> None:
    assert OutputParser._parse_declared_fields(None) == []


def test_parse_declared_fields_empty_fields() -> None:
    assert OutputParser._parse_declared_fields(_query("")) == []


def test_parse_declared_fields_parameters_list_filters_nested() -> None:
    query = _query(fields="", parameters={"fields": ["impressions", "comments{message}"]})
    assert OutputParser._parse_declared_fields(query) == ["impressions"]


def test_parse_declared_fields_parameters_string_filters_nested() -> None:
    query = _query(fields="", parameters="fields=attachments{type},message,shares")
    assert OutputParser._parse_declared_fields(query) == ["message", "shares"]


# ---------- end-to-end: _backfill_declared_fields no longer adds phantom columns ----------


def _row_config_for(fields: str) -> MagicMock:
    row_config = MagicMock()
    row_config.name = "q"
    row_config.type = "regular"
    row_config.query.path = "feed"
    row_config.query.fields = fields
    row_config.query.parameters = None
    return row_config


def test_backfill_keeps_flat_field_behavior() -> None:
    # Original CFTL-630 case: API omitted `shares` for this row, backfill must add "".
    row_config = _row_config_for("message,created_time,shares")
    parser = OutputParser(page_loader=None, page_id="page1", row_config=row_config)
    out = parser._backfill_declared_fields({"id": "post1", "message": "hi", "created_time": "2026-01-01"})
    assert out == {"id": "post1", "message": "hi", "created_time": "2026-01-01", "shares": ""}


def test_backfill_does_not_add_phantom_nested_column() -> None:
    # The exact bug: a feed-path query with `comments{...}` expansion, on a post that
    # FB returned without `comments` (zero comments). Pre-fix, the parent row gained a
    # phantom `comments: ""` column. Post-fix, the parent row stays clean.
    row_config = _row_config_for("comments{message,created_time,from,comments{message,created_time,from}}")
    parser = OutputParser(page_loader=None, page_id="page1", row_config=row_config)
    row = {"id": "post1", "message": "hi", "created_time": "2026-01-01"}
    out = parser._backfill_declared_fields(row)
    assert "comments" not in out
    assert out == row


def test_backfill_mixed_fields_only_backfills_flat() -> None:
    # GRPN-shape mixed case: nested `attachments{...}` and `comments{...}` alongside
    # flat `message`, `created_time`, `shares`. Only `shares` is missing from the row.
    row_config = _row_config_for("attachments{type,url},message,created_time,shares,comments{message}")
    parser = OutputParser(page_loader=None, page_id="page1", row_config=row_config)
    row = {"id": "post1", "message": "hi", "created_time": "2026-01-01"}
    out = parser._backfill_declared_fields(row)
    assert "attachments" not in out
    assert "comments" not in out
    assert out["shares"] == ""
