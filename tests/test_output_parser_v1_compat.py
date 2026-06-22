"""Unit tests for the opt-in v1_compatibility behavior of OutputParser (CFTL-630)."""

from unittest.mock import MagicMock

from output_parser import OutputParser


def _make_row_config(fields: str = "insights", path: str = "", parameters=None, name: str = "my_query"):
    row_config = MagicMock()
    row_config.name = name
    row_config.type = "regular"
    row_config.query.path = path
    row_config.query.fields = fields
    row_config.query.parameters = parameters
    return row_config


def _parser(v1_compatibility: bool, **rc_kwargs) -> OutputParser:
    return OutputParser(
        page_loader=None,
        page_id="act_1",
        row_config=_make_row_config(**rc_kwargs),
        v1_compatibility=v1_compatibility,
    )


# --- _copy_common_fields -------------------------------------------------

ORIGINAL_ROW = {
    "account_id": "act_1",
    "ad_id": "ad_9",
    "ad_name": "Spring Sale",
    "impressions": "1234",
    "clicks": "56",
    "spend": "7.89",
    "reach": "1000",
    "actions": [{"action_type": "link_click", "value": "10"}],  # list -> skipped
}


def test_copy_common_fields_off_keeps_narrow_list():
    parser = _parser(v1_compatibility=False)
    base: dict = {}
    parser._copy_common_fields(base, ORIGINAL_ROW, extended=True)
    assert base["account_id"] == "act_1"
    assert base["ad_id"] == "ad_9"
    assert "ad_name" not in base
    assert "impressions" not in base


def test_copy_common_fields_on_copies_all_scalar_fields():
    parser = _parser(v1_compatibility=True)
    base: dict = {}
    parser._copy_common_fields(base, ORIGINAL_ROW, extended=True)
    # narrow-list fields are still present
    assert base["account_id"] == "act_1"
    assert base["ad_id"] == "ad_9"
    # extra scalar fields flow through
    assert base["ad_name"] == "Spring Sale"
    assert base["impressions"] == "1234"
    assert base["clicks"] == "56"
    assert base["spend"] == "7.89"
    assert base["reach"] == "1000"
    # nested action-stat arrays are never copied as a scalar column
    assert "actions" not in base


def test_copy_common_fields_off_extended_false_omits_extended_only_fields():
    parser = _parser(v1_compatibility=False)
    base: dict = {}
    parser._copy_common_fields(base, ORIGINAL_ROW, extended=False)
    assert base["account_id"] == "act_1"
    assert base["ad_id"] == "ad_9"
    # account_name / campaign_name are only appended when extended=True
    assert "account_name" not in base
    assert "campaign_name" not in base


# --- declared-field parsing ---------------------------------------------


def test_parse_declared_fields_from_dsl():
    parser = _parser(v1_compatibility=True, fields="insights.date_preset(last_30d){impressions,ad_name,actions}")
    assert parser._declared_fields == ["impressions", "ad_name", "actions"]


def test_parse_declared_fields_strips_expansion_and_modifiers():
    parser = _parser(
        v1_compatibility=True,
        fields="insights{impressions,comments{message,from{name}},comments.limit(0).summary(true)}",
    )
    assert parser._declared_fields == ["impressions", "comments", "comments"]


def test_parse_declared_fields_from_plain_csv():
    parser = _parser(v1_compatibility=True, fields="id,name,impressions")
    assert parser._declared_fields == ["id", "name", "impressions"]


def test_parse_declared_fields_from_parameters_string():
    parser = _parser(v1_compatibility=True, fields="", parameters="level=ad&fields=impressions,ad_name")
    assert parser._declared_fields == ["impressions", "ad_name"]


def test_parse_declared_fields_from_parameters_dict():
    parser = _parser(v1_compatibility=True, fields="", parameters={"fields": "impressions,spend"})
    assert parser._declared_fields == ["impressions", "spend"]


# --- _backfill_declared_fields ------------------------------------------


def test_backfill_fills_missing_declared_field_with_empty_string():
    parser = _parser(v1_compatibility=True, fields="id,impressions,spend")
    filled = parser._backfill_declared_fields({"id": "1", "spend": "5.00"})
    assert filled["impressions"] == ""
    assert filled["spend"] == "5.00"  # present values are never overwritten


def test_backfill_noop_when_nothing_missing():
    parser = _parser(v1_compatibility=True, fields="id,impressions")
    row = {"id": "1", "impressions": "10"}
    assert parser._backfill_declared_fields(row) == row


def test_off_path_does_not_backfill_end_to_end():
    """With the flag off, an omitted declared field must NOT appear in output."""
    off = _parser(v1_compatibility=False, fields="id,spend,impressions", name="q")
    on = _parser(v1_compatibility=True, fields="id,spend,impressions", name="q")
    # spend is a meaningful (non-identifier) field so the row is emitted; impressions
    # is declared but omitted by FB. _has_meaningful_data drops rows that carry only
    # basic identifiers (id/parent_id/ex_account_id/fb_graph_node), so spend is required.
    response = {"data": [{"id": "1", "spend": "5.00"}]}
    off_rows = off.parse_data(response, fb_node="page", parent_id="act_1")["q"]
    on_rows = on.parse_data(response, fb_node="page", parent_id="act_1")["q"]
    assert "impressions" not in off_rows[0]
    assert off_rows[0]["spend"] == "5.00"
    assert on_rows[0]["impressions"] == ""
    assert on_rows[0]["spend"] == "5.00"


# --- bare insights DSL in parameters (CFTL-630 regression) ---------------


_BARE_DSL = (
    "insights.level(campaign).action_attribution_windows(1d_click)"
    ".action_breakdowns(action_type).date_preset(last_7d).time_increment(1)"
    "{campaign_name,campaign_id,actions,action_values}"
)


def test_parse_declared_fields_bare_dsl_in_parameters():
    """Bare DSL string in parameters (no fields= prefix) must yield the brace contents."""
    parser = _parser(v1_compatibility=True, fields="", parameters=_BARE_DSL)
    assert parser._declared_fields == ["campaign_name", "campaign_id", "actions", "action_values"]


def test_parse_declared_fields_bare_dsl_with_none_fields():
    """Same as above but with fields=None (async-insights-query shape)."""
    parser = _parser(v1_compatibility=True, fields=None, parameters=_BARE_DSL)
    assert parser._declared_fields == ["campaign_name", "campaign_id", "actions", "action_values"]


def test_parse_declared_fields_ampersand_fields_still_works():
    """Regression: level=campaign&fields=... shape must still parse correctly."""
    parser = _parser(v1_compatibility=True, fields="", parameters="level=campaign&fields=campaign_id,impressions")
    assert parser._declared_fields == ["campaign_id", "impressions"]


# Bare DSL whose .time_range({...}) / .filtering([{...}]) modifiers carry their own
# braces BEFORE the field-selection braces. The field list is the {...} at paren-depth 0,
# NOT the first "{" (which sits inside .time_range(...)). Regression for the platform bug
# where the modifier JSON was parsed as fields and backfilled as junk columns (CFTL-630).
_BARE_DSL_WITH_MODIFIER_BRACES = (
    "insights.level(ad)"
    ".time_range({'since':'2024-07-14','until':'2024-07-30'})"
    ".time_increment(1)"
    ".filtering([{'field':'impressions','operator':'GREATER_THAN','value':0}])"
    "{ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,impressions,clicks,inline_link_clicks,spend}"
)


def test_parse_declared_fields_bare_dsl_with_modifier_braces():
    """The field list is the paren-depth-0 {...}, not braces inside .time_range/.filtering modifiers."""
    parser = _parser(v1_compatibility=True, fields=None, parameters=_BARE_DSL_WITH_MODIFIER_BRACES)
    assert parser._declared_fields == [
        "ad_id",
        "ad_name",
        "adset_id",
        "adset_name",
        "campaign_id",
        "campaign_name",
        "impressions",
        "clicks",
        "inline_link_clicks",
        "spend",
    ]


def test_parse_declared_fields_fields_dsl_with_modifier_braces():
    """Same brace-skipping when the DSL is in query.fields (not parameters)."""
    parser = _parser(v1_compatibility=True, fields=_BARE_DSL_WITH_MODIFIER_BRACES)
    assert parser._declared_fields == [
        "ad_id",
        "ad_name",
        "adset_id",
        "adset_name",
        "campaign_id",
        "campaign_name",
        "impressions",
        "clicks",
        "inline_link_clicks",
        "spend",
    ]


def test_backfill_bare_dsl_end_to_end():
    """End-to-end: v1_compatibility=True + bare DSL, FB omits a declared field → backfilled."""
    parser = _parser(v1_compatibility=True, fields=None, parameters=_BARE_DSL, name="camp")
    # response omits action_values
    response = {
        "data": [
            {
                "campaign_name": "Summer",
                "campaign_id": "123",
                "spend": "5.00",
                "actions": [{"action_type": "link_click", "value": "10"}],
            }
        ]
    }
    rows = parser.parse_data(response, fb_node="campaign", parent_id="act_1")
    all_rows = [r for rows_list in rows.values() for r in rows_list]
    assert any("action_values" in r and r["action_values"] == "" for r in all_rows), (
        "action_values should be backfilled to '' when omitted by FB"
    )


# --- FacebookClient propagation ------------------------------------------


def test_facebook_client_stores_v1_compatibility():
    from client import FacebookClient

    oauth = MagicMock()
    oauth.data = {"access_token": "tok"}
    client = FacebookClient(oauth, "v23.0", True)
    assert client.v1_compatibility is True

    default_client = FacebookClient(oauth, "v23.0")
    assert default_client.v1_compatibility is False
