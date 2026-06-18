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
