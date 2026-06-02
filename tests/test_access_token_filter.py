import logging

import pytest

from client import _REDACTED, _TOKEN_KEY, AccessTokenFilter


@pytest.fixture
def filt() -> AccessTokenFilter:
    return AccessTokenFilter()


def make_record(msg: str, args: tuple = ()) -> logging.LogRecord:
    record = logging.LogRecord(
        name="test", level=logging.DEBUG, pathname="", lineno=0, msg=msg, args=args, exc_info=None
    )
    return record


# --- string branch ---


def test_masks_url_form(filt: AccessTokenFilter) -> None:
    record = make_record("GET /v23.0/123?access_token=EAAxyz&fields=id")
    filt.filter(record)
    assert "EAAxyz" not in record.msg
    assert f"access_token={_REDACTED}" in record.msg


def test_masks_json_form(filt: AccessTokenFilter) -> None:
    record = make_record('{"error": {"access_token": "EAAxyz", "code": 190}}')
    filt.filter(record)
    assert "EAAxyz" not in record.msg
    assert _REDACTED in record.msg


def test_masks_python_repr_form(filt: AccessTokenFilter) -> None:
    record = make_record("Request params: {'access_token': 'EAAxyz', 'limit': 100}")
    filt.filter(record)
    assert "EAAxyz" not in record.msg
    assert _REDACTED in record.msg


def test_token_free_string_unchanged(filt: AccessTokenFilter) -> None:
    msg = "Processing account 123, no secrets here"
    record = make_record(msg)
    filt.filter(record)
    assert record.msg == msg


# --- dict branch (via record.args) ---
# LogRecord.__init__ unwraps a single-element tuple when that element is a Mapping,
# so record.args ends up as the dict itself (not a one-element tuple).


def test_masks_dict_arg(filt: AccessTokenFilter) -> None:
    record = make_record("params: %s", ({"access_token": "EAAxyz", "limit": 100},))
    filt.filter(record)
    assert record.args[_TOKEN_KEY] == _REDACTED
    assert record.args["limit"] == 100


def test_masks_nested_dict(filt: AccessTokenFilter) -> None:
    record = make_record("data: %s", ({"outer": {"access_token": "EAAxyz"}},))
    filt.filter(record)
    assert record.args["outer"][_TOKEN_KEY] == _REDACTED


def test_non_token_dict_keys_unchanged(filt: AccessTokenFilter) -> None:
    record = make_record("data: %s", ({"user_id": "123", "name": "Alice"},))
    filt.filter(record)
    assert record.args == {"user_id": "123", "name": "Alice"}


# --- list / tuple branches ---


def test_masks_list_of_dicts(filt: AccessTokenFilter) -> None:
    record = make_record("data: %s", ([{"access_token": "EAAxyz"}, {"a": 1}],))
    filt.filter(record)
    assert record.args[0][0][_TOKEN_KEY] == _REDACTED
    assert record.args[0][1] == {"a": 1}


def test_masks_tuple_of_dicts(filt: AccessTokenFilter) -> None:
    record = make_record("data: %s", (({"access_token": "EAAxyz"},),))
    filt.filter(record)
    assert record.args[0][0][_TOKEN_KEY] == _REDACTED


# --- exc_text ---


def test_masks_exc_text(filt: AccessTokenFilter) -> None:
    record = make_record("error")
    record.exc_text = "Request failed: GET ...?access_token=EAAxyz&id=1"
    filt.filter(record)
    assert "EAAxyz" not in record.exc_text
    assert f"access_token={_REDACTED}" in record.exc_text


# --- non-string scalars pass through ---


def test_non_string_scalars_unchanged(filt: AccessTokenFilter) -> None:
    record = make_record("count: %s flag: %s none: %s", (42, True, None))
    filt.filter(record)
    assert record.args == (42, True, None)


# --- round-trip through filter() ---


def test_round_trip_url_in_msg(filt: AccessTokenFilter) -> None:
    record = make_record("paging.next: https://graph.facebook.com/?access_token=EAAxyz&after=abc")
    filt.filter(record)
    assert "EAAxyz" not in record.getMessage()


def test_round_trip_dict_in_args(filt: AccessTokenFilter) -> None:
    record = make_record("request params: %s", ({"access_token": "EAAxyz", "fields": "id,name"},))
    filt.filter(record)
    rendered = record.getMessage()
    assert "EAAxyz" not in rendered
    assert _REDACTED in rendered
