"""
Unit tests for the 'fail-on-missing-permissions' option (CFTL-489 / SUPPORT-15700).

Behavior: when enabled, per-account Facebook authorization errors are collected during
extraction and the job fails once at the end (listing every affected account) instead of
silently producing empty/partial output. Default off preserves current behavior.

Covers:
- FacebookErrorHandler authorization-error detection + detail extraction
- PageLoader HTTP boundary: flag on -> AuthorizationError; flag off -> current behavior
- FacebookClient: collects AuthorizationError per account and raises one UserException at the end
"""

import json
import unittest
from unittest.mock import MagicMock, patch

from keboola.component.exceptions import UserException
from requests import HTTPError

# Import from the top-level module names that the production code uses internally
# (component.py -> `from client import ...`, client.py -> `from page_loader import ...`).
# This keeps the AuthorizationError class identity consistent with the isinstance() check in
# client.py — importing it via `src.page_loader` would be a different module object.
from client import FacebookClient
from page_loader import AuthorizationError, FacebookErrorHandler, PageLoader


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

    def test_authorization_error_details_extracts_code_and_message(self):
        err = _make_http_error(code=200, message=NO_GRANT_MSG)
        code, message = FacebookErrorHandler.authorization_error_details(err)
        self.assertEqual(code, 200)
        self.assertEqual(message, NO_GRANT_MSG)


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

    def test_flag_on_code_200_raises_authorization_error(self):
        self.client.get.side_effect = _make_http_error(code=200, message=NO_GRANT_MSG)
        loader = self._loader(fail=True)
        with self.assertRaises(AuthorizationError) as ctx:
            loader._load_regular_page(self.query_config, "123")
        self.assertEqual(ctx.exception.account_id, "123")
        self.assertEqual(ctx.exception.code, 200)
        self.assertEqual(ctx.exception.message, NO_GRANT_MSG)

    def test_flag_on_missing_permissions_raises_authorization_error(self):
        # code 100 / subcode 33 is "recoverable" today; with the flag it is surfaced for collection
        self.client.get.side_effect = _make_http_error(code=100, subcode=33, message=MISSING_PERMS_MSG)
        loader = self._loader(fail=True)
        with self.assertRaises(AuthorizationError):
            loader._load_regular_page(self.query_config, "123")

    def test_flag_off_missing_permissions_returns_empty(self):
        # code 100 / subcode 33 stays recoverable -> empty data, job continues (current behavior)
        self.client.get.side_effect = _make_http_error(code=100, subcode=33, message=MISSING_PERMS_MSG)
        loader = self._loader(fail=False)
        result = loader._load_regular_page(self.query_config, "123")
        self.assertEqual(result, {"data": []})

    def test_flag_off_code_200_does_not_raise_authorization_error(self):
        # non-recoverable -> re-raises the HTTPError (client swallows it), never AuthorizationError
        self.client.get.side_effect = _make_http_error(code=200, message=NO_GRANT_MSG)
        loader = self._loader(fail=False)
        with self.assertRaises(HTTPError):
            loader._load_regular_page(self.query_config, "123")


class TestClientCollectAndFail(unittest.TestCase):
    def _make_client(self, fail=True):
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

    def _account(self, account_id):
        account = MagicMock()
        account.id = account_id
        return account

    @patch("client.PageLoader")
    def test_collects_per_account_and_does_not_raise_mid_iteration(self, mock_page_loader):
        def _raise_auth(query, page_id, **kwargs):
            raise AuthorizationError(account_id=page_id, code=200, message=NO_GRANT_MSG)

        mock_page_loader.return_value.load_page.side_effect = _raise_auth
        client = self._make_client(fail=True)

        # iterating does not raise — errors are collected, not fatal on first hit
        result = list(client._process_single_sync_query([self._account("123"), self._account("456")], self._make_row()))
        self.assertEqual(result, [])
        self.assertEqual(len(client.permission_errors), 2)
        self.assertEqual({e["account_id"] for e in client.permission_errors}, {"123", "456"})

        # raising at the end reports every affected account in one UserException
        with self.assertRaises(UserException) as ctx:
            client.raise_for_permission_errors()
        msg = str(ctx.exception)
        self.assertIn("2 account(s)", msg)
        self.assertIn("123", msg)
        self.assertIn("456", msg)

    def test_raise_for_permission_errors_noop_when_empty(self):
        client = self._make_client(fail=True)
        # should not raise
        client.raise_for_permission_errors()

    def test_raise_for_permission_errors_dedupes_by_account(self):
        client = self._make_client(fail=True)
        # same account failing on multiple queries -> reported once
        client._record_permission_error(AuthorizationError("123", 200, NO_GRANT_MSG), "Ads")
        client._record_permission_error(AuthorizationError("123", 200, NO_GRANT_MSG), "Adsets")
        with self.assertRaises(UserException) as ctx:
            client.raise_for_permission_errors()
        self.assertIn("1 account(s)", str(ctx.exception))

    @patch("client.PageLoader")
    def test_generic_error_is_swallowed_and_not_collected(self, mock_page_loader):
        mock_page_loader.return_value.load_page.side_effect = Exception("some non-auth failure")
        client = self._make_client(fail=False)
        result = list(client._process_single_sync_query([self._account("123")], self._make_row()))
        self.assertEqual(result, [])
        self.assertEqual(client.permission_errors, [])


if __name__ == "__main__":
    unittest.main()
