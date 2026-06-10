"""
Unit tests for the 'fail-on-missing-permissions' option (CFTL-489 / SUPPORT-15700).

Covers:
- FacebookErrorHandler authorization-error detection / escalation
- PageLoader HTTP boundary: flag on -> UserException; flag off -> current behavior
- FacebookClient per-account loop: a deliberate UserException propagates (not swallowed)
"""

import json
import unittest
from unittest.mock import MagicMock, patch

from keboola.component.exceptions import UserException
from requests import HTTPError

from src.client import FacebookClient
from src.page_loader import FacebookErrorHandler, PageLoader


def _make_http_error(code=None, subcode=None, message="", status_code=400):
    """Build a requests.HTTPError carrying a Facebook-style JSON error body."""
    error = {"message": message}
    if code is not None:
        error["code"] = code
    if subcode is not None:
        error["error_subcode"] = subcode
    body = {"error": error}

    response = MagicMock()
    response.json.return_value = body
    response.text = json.dumps(body)
    response.status_code = status_code

    http_error = HTTPError(message)
    http_error.response = response
    return http_error


# Facebook message used for the code=100 / subcode=33 permission case
MISSING_PERMS_MSG = (
    "Object does not exist, cannot be loaded due to missing permissions, or does not support this operation"
)
# Facebook message for the reported code=200 case
NO_GRANT_MSG = "Ad account owner has NOT grant ads_management or ads_read permission"


class TestAuthorizationErrorDetection(unittest.TestCase):
    def test_oauth_codes_are_authorization_errors(self):
        for code in (10, 190, 200):
            err = _make_http_error(code=code, message="auth problem")
            self.assertTrue(
                FacebookErrorHandler.is_authorization_error(err),
                f"code {code} should be an authorization error",
            )

    def test_missing_permissions_subcode_is_authorization_error(self):
        err = _make_http_error(code=100, subcode=33, message=MISSING_PERMS_MSG)
        self.assertTrue(FacebookErrorHandler.is_authorization_error(err))

    def test_unrelated_codes_are_not_authorization_errors(self):
        # transient rate-limit code
        self.assertFalse(FacebookErrorHandler.is_authorization_error(_make_http_error(code=4, message="rate limit")))
        # generic code=100 WITHOUT the missing-permissions fragment
        self.assertFalse(
            FacebookErrorHandler.is_authorization_error(_make_http_error(code=100, message="some other 100 error"))
        )

    def test_no_response_is_not_authorization_error(self):
        err = HTTPError("boom")
        err.response = None
        self.assertFalse(FacebookErrorHandler.is_authorization_error(err))

    def test_raise_if_authorization_error_raises_for_auth(self):
        err = _make_http_error(code=200, message=NO_GRANT_MSG)
        with self.assertRaises(UserException) as ctx:
            FacebookErrorHandler.raise_if_authorization_error(err, context="loading 'ads'")
        # message should carry the FB detail and the remediation hint
        self.assertIn("code 200", str(ctx.exception))
        self.assertIn("loading 'ads'", str(ctx.exception))

    def test_raise_if_authorization_error_noop_for_non_auth(self):
        err = _make_http_error(code=4, message="rate limit")
        # Should NOT raise
        FacebookErrorHandler.raise_if_authorization_error(err, context="loading 'ads'")


class TestPageLoaderAuthorizationBoundary(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.query_config = MagicMock()
        self.query_config.limit = "25"
        self.query_config.path = ""
        self.query_config.fields = "id,name"
        self.query_config.since = ""
        self.query_config.until = ""
        self.query_config.parameters = None

    def _loader(self, fail):
        return PageLoader(
            client=self.client,
            query_type="regular",
            api_version="v23.0",
            fail_on_missing_permissions=fail,
        )

    def test_flag_on_code_200_raises_user_exception(self):
        self.client.get.side_effect = _make_http_error(code=200, message=NO_GRANT_MSG)
        loader = self._loader(fail=True)
        with self.assertRaises(UserException):
            loader._load_regular_page(self.query_config, "123")

    def test_flag_off_missing_permissions_returns_empty(self):
        # code 100 / subcode 33 is "recoverable" today -> empty data, job continues
        self.client.get.side_effect = _make_http_error(code=100, subcode=33, message=MISSING_PERMS_MSG)
        loader = self._loader(fail=False)
        result = loader._load_regular_page(self.query_config, "123")
        self.assertEqual(result, {"data": []})

    def test_flag_on_missing_permissions_raises_user_exception(self):
        # with the flag on, the same "recoverable" permission error fails the job
        self.client.get.side_effect = _make_http_error(code=100, subcode=33, message=MISSING_PERMS_MSG)
        loader = self._loader(fail=True)
        with self.assertRaises(UserException):
            loader._load_regular_page(self.query_config, "123")


class TestClientPropagation(unittest.TestCase):
    def _make_client(self, fail):
        oauth = MagicMock()
        oauth.data = {"access_token": "tok"}
        return FacebookClient(oauth, "v23.0", fail_on_missing_permissions=fail)

    def _make_row(self):
        row = MagicMock()
        row.type = "ads-query"
        row.name = "Ads"
        row.query.path = "ads"  # truthy -> not batchable, no page token required
        row.query.fields = "id,name"
        row.query.ids = ""
        return row

    def _account(self):
        account = MagicMock()
        account.id = "123"
        return account

    @patch("src.client.PageLoader")
    def test_user_exception_propagates_when_flag_on(self, mock_page_loader):
        mock_page_loader.return_value.load_page.side_effect = UserException("missing permissions")
        client = self._make_client(fail=True)
        with self.assertRaises(UserException):
            list(client._process_single_sync_query([self._account()], self._make_row()))

    @patch("src.client.PageLoader")
    def test_generic_error_is_swallowed(self, mock_page_loader):
        mock_page_loader.return_value.load_page.side_effect = Exception("some non-auth failure")
        client = self._make_client(fail=False)
        result = list(client._process_single_sync_query([self._account()], self._make_row()))
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
