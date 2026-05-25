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


# ---------- PARSER_SYNTHESIZED_FIELDS allowlist (CFTL-656 / SUPPORT-16397) ----------


@pytest.mark.parametrize(
    "synthesized",
    ["from_id", "from_name", "from_full_name", "from_username"],
)
def test_backfill_drops_parser_synthesized_names(synthesized: str) -> None:
    """The names listed in ``PARSER_SYNTHESIZED_FIELDS`` are produced by the parser's
    own flattening of nested ``from{...}`` expansions. The FB Graph API never returns
    them as flat scalar fields — runtime detection therefore can't catch them. Users
    who declared them as bare tokens in their config field list were copying parser
    output columns back into request input. The narrow static exclusion list stops
    backfill from injecting them as phantom empty columns on parent rows."""
    row_config = _row_config_for(f"id,message,{synthesized}")
    parser = OutputParser(page_loader=None, page_id="page1", row_config=row_config)
    out = parser._backfill_declared_fields({"id": "post1", "message": "hi"})
    assert synthesized not in out
    assert out == {"id": "post1", "message": "hi"}


def test_backfill_drops_support_16397_instagram_q16_shape() -> None:
    """Exact field-list shape from the SUPPORT-16397 Instagram failure
    (``Extra columns found: "comments, from_full_name, from_id"`` on the
    ``...media`` table). With the combined runtime + synthesized-name fix the
    parent row must NOT carry any of those three columns:

    * ``comments`` — caught dynamically when any media row returns it as
      ``{"data": [...]}`` (``_observed_connections``);
    * ``from_id`` / ``from_full_name`` — caught by the synthesized-name allowlist.
    """
    row_config = _row_config_for(
        "id,caption,media_type,like_count,ig_id,comments_count,is_comment_enabled,"
        "media_url,owner,permalink,shortcode,timestamp,thumbnail_url,"
        "comments,from_id,from_full_name"
    )
    parser = OutputParser(page_loader=None, page_id="page1", row_config=row_config)
    # Simulate the API revealing ``comments`` as a connection on at least one row.
    parser._observe_connection_keys({"id": "m0", "comments": {"data": [{"id": "c1"}]}})
    out = parser._backfill_declared_fields({"id": "media1", "caption": "x", "media_type": "IMAGE", "like_count": 5})
    for phantom in ("comments", "from_id", "from_full_name"):
        assert phantom not in out, f"phantom column {phantom} re-injected"
    # Legitimate flat fields the API omitted still get backfilled with "".
    assert out["ig_id"] == ""
    assert out["permalink"] == ""


# ---------- _observed_connections runtime detection ----------


def test_observed_connections_drops_after_seen_as_dict_with_data() -> None:
    """If a key appears as ``{"data": [...]}`` in any earlier row, subsequent rows
    that omit the key must NOT receive it as a backfilled empty column. Catches
    account-specific or future connection edges not in the static allowlist."""
    row_config = _row_config_for("id,message,custom_edge")
    parser = OutputParser(page_loader=None, page_id="page1", row_config=row_config)
    # First row reveals `custom_edge` as a connection.
    parser._process_fields({"id": "1", "message": "a", "custom_edge": {"data": [{"x": 1}]}})
    assert "custom_edge" in parser._observed_connections
    # Second row omits `custom_edge` — must NOT be backfilled.
    out = parser._backfill_declared_fields({"id": "2", "message": "b"})
    assert "custom_edge" not in out


def test_observed_connections_drops_after_seen_as_summary_only() -> None:
    """Same dynamic detection when a key appears with only ``summary`` (no ``data``),
    as ``reactions.summary(total_count)`` does."""
    row_config = _row_config_for("id,reactions_count,my_reactions")
    parser = OutputParser(page_loader=None, page_id="page1", row_config=row_config)
    parser._process_fields({"id": "1", "my_reactions": {"summary": {"total_count": 3}}})
    assert "my_reactions" in parser._observed_connections
    out = parser._backfill_declared_fields({"id": "2"})
    assert "my_reactions" not in out


def test_observed_connections_does_not_drop_flat_scalar_fields() -> None:
    """A regular scalar key seen in a previous row must NOT end up in observed
    connections — otherwise CFTL-630 regresses for genuine flat fields."""
    row_config = _row_config_for("id,impressions,reach")
    parser = OutputParser(page_loader=None, page_id="page1", row_config=row_config)
    parser._process_fields({"id": "1", "impressions": 100, "reach": 80})
    assert parser._observed_connections == set()
    # Row with API omitting `impressions` must still get backfilled.
    out = parser._backfill_declared_fields({"id": "2", "reach": 0})
    assert out["impressions"] == ""


def test_observe_connection_keys_picks_up_list_of_dicts() -> None:
    """FB Ads ``actions`` style fields come back as a list of dicts. Without
    catching them in ``_observe_connection_keys``, they'd be backfilled as empty
    strings on parent rows (then leak into action-stats rows via
    ``_add_action_stats_to_main_table``)."""
    row_config = _row_config_for("id,actions,action_values")
    parser = OutputParser(page_loader=None, page_id="page1", row_config=row_config)
    parser._observe_connection_keys({"id": "1", "actions": [{"action_type": "view", "value": 1}]})
    assert "actions" in parser._observed_connections
    out = parser._backfill_declared_fields({"id": "2"})
    assert "actions" not in out


def test_pre_scan_catches_connection_revealed_in_later_row() -> None:
    """End-to-end: a query whose FIRST row omits ``comments`` but LATER rows have it
    as a connection. The within-page pre-scan in ``iter_parsed_data`` must register
    the connection before backfill runs on row 1 — otherwise row 1 gets a phantom
    ``comments`` column while later rows do not, producing a mixed schema (CFTL-656)."""
    row_config = _row_config_for("id,message,comments")
    parser = OutputParser(page_loader=None, page_id="page1", row_config=row_config)
    response = {
        "data": [
            # row 0 omits `comments` entirely (the bug-trigger case)
            {"id": "p1", "message": "first"},
            # row 1 reveals it as a connection edge
            {"id": "p2", "message": "second", "comments": {"data": [{"id": "c1", "text": "hi"}]}},
        ]
    }
    result = parser.parse_data(response, "page", "page1", "feed")
    main_table = next(v for k, v in result.items() if not k.endswith("comments"))
    for row in main_table:
        assert "comments" not in row, f"row {row!r} still has phantom 'comments' column"


# ---------- apply_backfill flag: nested-row no-backfill ----------


def test_process_row_apply_backfill_false_skips_backfill() -> None:
    """``_process_row`` must not backfill declared fields when called from the nested
    recursion path. Otherwise the parent query's field list bleeds into child-table
    rows (the ``feed_attachments`` / ``media_comments`` extra-columns symptom in
    SUPPORT-16397)."""
    row_config = _row_config_for("id,message,created_time,shares")
    parser = OutputParser(page_loader=None, page_id="page1", row_config=row_config)
    result: dict = {}
    # Simulate a child-row payload that lacks parent-only fields (`message`, `shares`).
    child_row = {"id": "comment1", "from_name": "Alice", "comment_text": "hi"}
    parser._process_row(child_row, "page_feed_comments", "post1", "comments", result, apply_backfill=False)
    rows = next(iter(result.values()))
    assert len(rows) == 1
    emitted = rows[0]
    for parent_only in ("message", "shares", "created_time"):
        assert parent_only not in emitted, (
            f"child row received parent-only field {parent_only} despite apply_backfill=False"
        )


def test_process_row_apply_backfill_true_still_backfills() -> None:
    """Sanity: the outer-query path keeps the CFTL-630 backfill behavior."""
    row_config = _row_config_for("id,message,created_time,shares")
    parser = OutputParser(page_loader=None, page_id="page1", row_config=row_config)
    result: dict = {}
    outer_row = {"id": "post1", "message": "hi", "created_time": "2026-01-01"}
    parser._process_row(outer_row, "page_feed", "page1", None, result, apply_backfill=True)
    rows = next(iter(result.values()))
    assert len(rows) == 1
    assert rows[0]["shares"] == ""


def test_nested_data_recursion_passes_apply_backfill_false() -> None:
    """End-to-end: a feed-style response with a nested ``comments`` connection must
    NOT inject the parent query's declared fields into the child ``feed_comments``
    rows."""
    row_config = _row_config_for("id,message,created_time,shares,permalink_url,is_published")
    parser = OutputParser(page_loader=None, page_id="page1", row_config=row_config)
    response = {
        "data": [
            {
                "id": "post1",
                "message": "outer",
                "comments": {
                    "data": [
                        {"id": "c1", "from": {"id": "u1", "name": "Alice"}},
                        {"id": "c2", "from": {"id": "u2", "name": "Bob"}},
                    ]
                },
            }
        ]
    }
    result = parser.parse_data(response, "page", "page1", "feed")
    # The recursion produces a child table whose name carries the nested edge identifier.
    comments_table = next((v for k, v in result.items() if k.endswith("comments") or "comments" in k), None)
    assert comments_table is not None, f"child table missing; got tables: {list(result)}"
    assert len(comments_table) == 2, f"expected 2 child rows, got {len(comments_table)}"
    parent_only_fields = {"message", "created_time", "shares", "permalink_url", "is_published"}
    for child_row in comments_table:
        leaking = parent_only_fields & set(child_row)
        assert not leaking, f"child row leaked parent fields {leaking}: {child_row}"
