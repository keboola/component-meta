import logging
import re
import time
from collections.abc import Iterator
from typing import Any

from keboola.component.dao import OauthCredentials
from keboola.component.exceptions import UserException
from keboola.http_client import HttpClient
from requests import HTTPError, RequestException
from requests.exceptions import RetryError

from configuration import Account, QueryRow
from output_parser import OutputParser
from page_loader import (
    _FB_TRANSIENT_ERROR_BACKOFF_BASE,
    _FB_TRANSIENT_ERROR_MAX_RETRIES,
    AsyncInsightsJobTransientError,
    AuthorizationError,
    FacebookErrorHandler,
    PageLoader,
)

# Errors that mean "this one object failed transiently / at the API" — contain them
# (skip the object, count it, warn at end of run) instead of killing the whole extraction.
# UserException (user-actionable misconfig) and programming errors (KeyError, ...) are
# intentionally NOT here, so they still propagate.
_CONTAINED_OBJECT_ERRORS = (HTTPError, RetryError, RequestException, AsyncInsightsJobTransientError)


class AccessTokenFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = self._mask(record.msg)
        record.args = self._mask(record.args)
        record.exc_text = self._mask(record.exc_text)
        record.exc_info = self._mask(record.exc_info)
        return True

    def _mask(self, obj):
        if isinstance(obj, str):
            obj = re.sub(r"access_token=[^&\s]+", "access_token=---ACCESS-TOKEN---", obj)
            return re.sub(r"'access_token': '[^']+'", "'access_token': '---ACCESS-TOKEN---'", obj)

        if isinstance(obj, Exception):
            # Convert exception to string and mask it
            masked_str = self._mask(str(obj))
            # Try to create new exception with masked message
            try:
                return type(obj)(masked_str)
            except Exception:
                # If that fails, return the masked string
                return masked_str

        if isinstance(obj, tuple):
            return type(obj)(self._mask(v) for v in obj)

        return obj


access_token_filter = AccessTokenFilter()

# Create module logger after filter is set up
logger = logging.getLogger(__name__)

# Add filter to root logger and all existing loggers
logging.getLogger().addFilter(access_token_filter)
for name in logging.root.manager.loggerDict:
    logging.getLogger(name).addFilter(access_token_filter)


class PageTokenResolver:
    """Resolves the appropriate access token for Facebook API requests."""

    @staticmethod
    def get_page_tokens(
        client: HttpClient, api_version: str, accounts: list[Account], user_token: str
    ) -> dict[str, str]:
        """
        Get page tokens for accounts.
        For accounts with fb_page_id, look up the page token.
        For accounts without fb_page_id, use the user token directly.
        """
        page_tokens = {}

        try:
            # Fetch page tokens from API with pagination
            page_token_map = {}
            endpoint_path = f"/{api_version}/me/accounts"
            params = {"access_token": user_token, "fields": "id,access_token"}

            while endpoint_path:
                response = client.get(endpoint_path=endpoint_path, params=params)
                if not response:
                    break

                # Add tokens from this page to the map
                for page in response.get("data", []):
                    if "id" in page and "access_token" in page:
                        page_token_map[page["id"]] = page["access_token"]

                # Get next page URL (params included in URL for subsequent requests)
                endpoint_path = response.get("paging", {}).get("next")
                params = {}  # Clear params as they're in the next URL

            # Assign tokens to accounts
            for account in accounts:
                if account.fb_page_id:
                    # Instagram account - account.id is IG Business Account ID
                    # Look up page token using the linked Facebook Page ID
                    page_tokens[account.id] = page_token_map.get(account.fb_page_id, user_token)
                else:
                    # Facebook Page account - account.id IS the Facebook Page ID
                    # Look up page token using account.id
                    page_tokens[account.id] = page_token_map.get(account.id, user_token)

        except Exception as e:
            logger.warning(f"Unable to get page tokens: {e}")
            # Fallback to user token for all accounts
            for account in accounts:
                page_tokens[account.id] = user_token

        return page_tokens


class FacebookClient:
    def __init__(self, oauth: OauthCredentials, api_version: str, fail_on_missing_permissions: bool = False):
        self.oauth = oauth
        self.api_version = api_version
        self.fail_on_missing_permissions = fail_on_missing_permissions
        # Authorization errors collected across all accounts/queries when the option is enabled.
        # The run fails at the end (raise_for_permission_errors) so every affected account is reported.
        self.permission_errors: list[dict[str, Any]] = []
        self.page_tokens = None  # Cache for page tokens
        # Count of objects skipped due to contained API errors; surfaced as an
        # end-of-run warning so partial output is not silently read as complete.
        self.skipped_objects = 0

        if self.oauth.data and self.oauth.data.get("token", None) and not self.oauth.data.get("access_token", None):
            logger.info("Direct insert token is used for authentication.")
            self.oauth.data["access_token"] = self.oauth.data["token"]

        self.client = HttpClient(
            base_url="https://graph.facebook.com",
            default_http_header={"Content-Type": "application/json"},
            status_forcelist=(500, 502, 503, 504),
        )

    def _with_token(self, params: dict[str, Any] | None, token: str | None = None) -> dict[str, Any]:
        """
        Return a copy of params with the access_token added.
        If token is not provided, use the main user access token.
        """
        params = dict(params) if params else {}
        params["access_token"] = token or self.oauth.data.get("access_token")
        return params

    def _record_permission_error(self, error: AuthorizationError, query_name: str | None = None) -> None:
        """Collect a per-account authorization error to report (and fail) at the end of the run."""
        self.permission_errors.append(
            {
                "account_id": error.account_id,
                "query": query_name,
                "code": error.code,
                "message": error.message,
            }
        )

    def raise_for_permission_errors(self) -> None:
        """Raise a single UserException summarizing every account that hit an authorization error.

        Called after all queries have been processed so the user sees the full list at once.
        A non-zero exit means the platform discards all output, so no partial data is committed.
        """
        if not self.permission_errors:
            return

        # One line per affected account (dedupe by account id; fall back to message when unknown)
        seen: dict[Any, dict[str, Any]] = {}
        for err in self.permission_errors:
            key = err.get("account_id") or err.get("message")
            seen.setdefault(key, err)

        lines = []
        for err in seen.values():
            account = err.get("account_id") or "unknown account"
            message = err.get("message") or ""
            code = err.get("code")
            detail = f"{message} (code {code})" if message else f"code {code}"
            lines.append(f"  - account {account}: {detail}")

        raise UserException(
            f"Facebook authorization errors prevented data extraction for {len(seen)} account(s). "
            "The access token is missing required permissions — re-authorize the extractor or grant "
            "the necessary access (e.g. ads_read / ads_management) on the affected accounts:\n"
            + "\n".join(lines)
            + "\nDisable 'Fail the job on authorization errors' to skip inaccessible accounts and continue."
        )

    def _extract_page_content(self, query_path: str | None, page_data: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Extract content from page data response.
        For page queries without path, the response is the page object itself, not wrapped in "data".
        """
        if not query_path and "data" not in page_data:
            return [page_data] if page_data and "id" in page_data else []
        return page_data.get("data", [])

    def process_queries(self, accounts: list, queries: list) -> Iterator[dict]:
        """
        Processes a list of queries, handling sync and async execution.
        Async queries are started in parallel, then all results are polled.
        Sync queries are processed sequentially after all async jobs are handled.
        """
        async_queries = []
        sync_queries = []
        for query in queries:
            if hasattr(query, "type") and query.type == "async-insights-query":
                async_queries.append(query)
            else:
                sync_queries.append(query)

        # Start all async jobs
        all_job_details = {}
        if async_queries:
            logger.info(f"Starting {len(async_queries)} async queries in parallel.")
            for query in async_queries:
                logger.info(f"Starting async query: {query.name}")
                job_details = self._start_async_jobs_for_query(accounts, query)
                all_job_details.update(job_details)

        # Poll and process async results
        if all_job_details:
            logger.info(f"Polling and processing {len(all_job_details)} async jobs.")
            yield from self._poll_and_process_async_jobs(all_job_details)

        # Process sync queries
        for query in sync_queries:
            logger.info(f"Processing sync query: {query.name}")
            yield from self._process_single_sync_query(accounts, query)

    def _start_async_jobs_for_query(self, accounts: list, row_config) -> dict:
        if ids_str := row_config.query.ids:
            selected_ids = {id for id in ids_str.split(",")}
            accounts = [account for account in accounts if account.id in selected_ids]

        # Resolve tokens
        user_token = self.oauth.data.get("access_token")
        if self._request_require_page_token(row_config):
            is_page_token = True
            if self.page_tokens is None:
                self.page_tokens = PageTokenResolver.get_page_tokens(
                    self.client, self.api_version, accounts, user_token
                )
            page_tokens = self.page_tokens
        else:
            is_page_token = False
            page_tokens = {account.id: user_token for account in accounts}
        job_details = {}
        for page_id, token in page_tokens.items():
            page_id = str(page_id)
            try:
                # Use the shared client and pass token in params
                page_loader = PageLoader(
                    self.client,
                    row_config.type,
                    self.api_version,
                    fail_on_missing_permissions=self.fail_on_missing_permissions,
                )
                report_id = page_loader.start_async_insights_job(
                    row_config.query, page_id, params=self._with_token({}, token)
                )
                if report_id:
                    job_details[report_id] = {
                        "page_id": page_id,
                        "page_loader": page_loader,
                        "output_parser": OutputParser(page_loader, page_id, row_config),
                        "fb_graph_node": self._get_fb_graph_node(is_page_token, row_config),
                        "access_token": token,
                        # Needed to re-submit the report on a transient poll failure.
                        "row_config": row_config,
                        "start_params": self._with_token({}, token),
                    }
            except UserException:
                raise
            except AuthorizationError as e:
                self._record_permission_error(e, row_config.name)
            except Exception as e:
                logger.error(f"Failed to start async job for {page_id}: {e}")
        return job_details

    def _poll_and_process_async_jobs(self, all_job_details: dict) -> Iterator[dict]:
        for report_id, details in all_job_details.items():
            page_id = details["page_id"]
            try:
                page_data = self._poll_async_with_resubmit(report_id, details)
                if not page_data.get("data"):
                    continue
                yield from details["output_parser"].iter_parsed_data(page_data, details["fb_graph_node"], page_id)
            except UserException:
                raise
            except AuthorizationError as e:
                if e.account_id is None:
                    e.account_id = page_id
                self._record_permission_error(e, details["row_config"].name)
            except _CONTAINED_OBJECT_ERRORS as e:
                # Transient/API failure for this one report — contain it so the rest of the
                # run completes; UserException and programming errors still propagate.
                logger.error(
                    f"Skipping async report {report_id} (object {page_id}) after errors: {type(e).__name__}: {e}"
                )
                self.skipped_objects += 1

    def _poll_async_with_resubmit(self, report_id: str, details: dict) -> dict[str, Any]:
        """Poll an async report, re-submitting it with exponential backoff on transient failures.

        Facebook fails report jobs transiently under load ("Job Failed"/"Job Skipped"/timeout);
        ``poll_async_job`` raises ``AsyncInsightsJobTransientError`` for these. We re-submit the
        report and re-poll. Once the retry budget is exhausted the last error propagates and the
        caller contains it (skip + count). This is the *production* retry path — the previous
        loop in ``PageLoader._load_async_insights`` was never reached by a real job.
        """
        page_loader = details["page_loader"]
        access_token = details.get("access_token", self.oauth.data.get("access_token"))
        for attempt in range(_FB_TRANSIENT_ERROR_MAX_RETRIES + 1):
            try:
                return page_loader.poll_async_job(report_id, access_token)
            except AsyncInsightsJobTransientError as e:
                if attempt >= _FB_TRANSIENT_ERROR_MAX_RETRIES:
                    raise
                wait = _FB_TRANSIENT_ERROR_BACKOFF_BASE * (2**attempt)
                logger.warning(
                    f"Async report {report_id} transiently failed ({e}); resubmitting, "
                    f"attempt {attempt + 2}/{_FB_TRANSIENT_ERROR_MAX_RETRIES + 1}, retrying in {wait}s"
                )
                time.sleep(wait)
                report_id = page_loader.start_async_insights_job(
                    details["row_config"].query, details["page_id"], params=details["start_params"]
                )
                if not report_id:
                    return {"data": []}
        return {"data": []}  # unreachable — loop always returns or raises

    def _handle_batch_request(self, account_ids: list[str], row_config) -> Iterator[dict]:
        """
        Executes and parses a batch request for a list of account IDs.
        Yields parsed data for each item in the response.
        Raises HTTPError on failure so the caller can handle fallbacks.
        """
        logger.info(f"Batch fetching object details for IDs: {','.join(account_ids)}")
        params = {"ids": ",".join(account_ids), "fields": row_config.query.fields}

        # Raises HTTPError on failure
        response = self.client.get(f"/{self.api_version}/", params=self._with_token(params))

        if not response or not isinstance(response, dict):
            logger.warning("Empty or invalid response for batch ID fetch.")
            return

        fb_graph_node = self._get_fb_graph_node(False, row_config)
        for item_id, item_data in response.items():
            if isinstance(item_data, dict) and "error" in item_data:
                logger.warning(f"Error fetching data for ID {item_id}: {item_data['error']}")
                continue

            output_parser = OutputParser(page_loader=None, page_id=item_id, row_config=row_config)
            yield from output_parser.iter_parsed_data(response=item_data, fb_node=fb_graph_node, parent_id=item_id)

    def _process_single_sync_query(self, accounts: list[Account], row_config: QueryRow) -> Iterator[dict[str, Any]]:
        # Determine if a query is eligible for batch processing.
        is_batchable_query = not row_config.query.path and getattr(row_config, "type", "") != "nested-query"
        is_insights_query = str(row_config.query.fields or "").startswith("insights")

        if is_batchable_query and not is_insights_query:
            account_ids = (
                [id.strip() for id in row_config.query.ids.split(",")]
                if row_config.query.ids
                else [acc.id for acc in accounts]
            )

            if account_ids:
                try:
                    logger.info(f"Attempting to batch fetch data for {len(account_ids)} IDs.")
                    params = {
                        "ids": ",".join(account_ids),
                        "fields": row_config.query.fields,
                    }
                    response = self.client.get(f"/{self.api_version}/", params=self._with_token(params))

                    if not response or not isinstance(response, dict):
                        logger.warning("Empty or invalid response for batch ID fetch.")
                    else:
                        fb_graph_node = self._get_fb_graph_node(False, row_config)
                        for item_id, item_data in response.items():
                            if isinstance(item_data, dict) and "error" in item_data:
                                logger.warning(f"Error fetching data for ID {item_id}: {item_data['error']}")
                                continue
                            output_parser = OutputParser(page_loader=None, page_id=item_id, row_config=row_config)
                            yield from output_parser.iter_parsed_data(
                                response=item_data,
                                fb_node=fb_graph_node,
                                parent_id=item_id,
                            )
                        return  # Batch processing successful, exit the function.

                except HTTPError as e:
                    error_text = str(e.response.text) if hasattr(e, "response") else str(e)
                    if "Page Access Token" in error_text:
                        logger.info("Batch request requires page token, falling back to individual requests.")
                        # Let the code fall through to individual processing below.
                    else:
                        # Collect authorization errors (batch covers all accounts at once)
                        if self.fail_on_missing_permissions and FacebookErrorHandler.is_authorization_error(e):
                            code, message = FacebookErrorHandler.authorization_error_details(e)
                            self._record_permission_error(
                                AuthorizationError(account_id=None, code=code, message=message),
                                row_config.name,
                            )
                            return
                        logger.error(f"Batch request failed with a non-token error: {error_text}")
                        return  # A definitive failure, stop processing.

        # If batch processing was not attempted, was skipped (insights), or failed with a token error,
        # proceed with individual requests for all accounts.
        if ids_str := row_config.query.ids:
            selected_ids = {id for id in ids_str.split(",")}
            accounts = [account for account in accounts if account.id in selected_ids]

        # Resolve tokens
        user_token = self.oauth.data.get("access_token")
        if self._request_require_page_token(row_config):
            logger.debug("Require page token")
            is_page_token = True
            if self.page_tokens is None:
                self.page_tokens = PageTokenResolver.get_page_tokens(
                    self.client, self.api_version, accounts, user_token
                )
            page_tokens = self.page_tokens
        else:
            logger.debug("Don't need page token")
            is_page_token = False
            page_tokens = {account.id: user_token for account in accounts}

        for page_id, token in page_tokens.items():
            page_id = str(page_id)

            try:
                # Create new client with page token
                # Use the shared client and pass token in params
                page_loader = PageLoader(
                    self.client,
                    row_config.type,
                    self.api_version,
                    fail_on_missing_permissions=self.fail_on_missing_permissions,
                )
                output_parser = OutputParser(page_loader, page_id, row_config)

                # Construct Facebook Graph node path
                fb_graph_node = self._get_fb_graph_node(is_page_token, row_config)

                # Load data from Facebook API
                page_data = page_loader.load_page(row_config.query, page_id, params={"access_token": token})
                page_content = self._extract_page_content(row_config.query.path, page_data)

            except UserException:
                raise
            except Exception as e:
                is_auth_error = isinstance(e, AuthorizationError)
                # For page-token queries, the page token itself may lack access — retry with the
                # user token (also the original recovery for "400 Page Access Token" errors).
                if is_page_token and (is_auth_error or str(e).startswith("400")):
                    logger.debug(f"Primary token failed for {page_id}, trying user token")
                    try:
                        # Fallback to user token
                        page_loader = PageLoader(
                            self.client,
                            row_config.type,
                            self.api_version,
                            fail_on_missing_permissions=self.fail_on_missing_permissions,
                        )
                        output_parser = OutputParser(page_loader, page_id, row_config)
                        fb_graph_node = self._get_fb_graph_node(False, row_config)
                        page_data = page_loader.load_page(row_config.query, page_id, params=self._with_token({}))
                        page_content = self._extract_page_content(row_config.query.path, page_data)
                    except UserException:
                        raise
                    except AuthorizationError as fallback_error:
                        self._record_permission_error(fallback_error, row_config.name)
                        continue
                    except Exception as user_token_error:
                        logger.debug(f"User token also failed for {page_id}: {str(user_token_error)}")
                        continue
                elif is_auth_error:
                    self._record_permission_error(e, row_config.name)
                    continue
                else:
                    logger.error(f"Failed to load data for {page_id}: {str(e)}")
                    continue

            if not page_content:
                continue

            # Pagination/parsing happens lazily here. A failure on one object's nested
            # pagination (e.g. Facebook code=2 on a deep insights paging.next that outlives
            # the transient-retry budget) must not kill the whole extraction with an opaque
            # "Internal Server Error" — log it with full context and move on to the next object.
            query_name = getattr(row_config, "name", None) or getattr(getattr(row_config, "query", None), "path", "?")
            try:
                yield from output_parser.iter_parsed_data(page_data, fb_graph_node, page_id)
            except AuthorizationError as e:
                # A permission error during lazy pagination is collected like any other.
                if e.account_id is None:
                    e.account_id = page_id
                self._record_permission_error(e, row_config.name)
                continue
            except _CONTAINED_OBJECT_ERRORS as e:
                # Contain transient/API failures for this one object; UserException
                # (user-actionable) and programming errors deliberately propagate.
                logger.error(
                    f"Skipping query '{query_name}' for object {page_id} after errors: {type(e).__name__}: {e}"
                )
                self.skipped_objects += 1
                continue

    def get_accounts(self, url_path: str, fields: str | None) -> list[dict[str, Any]]:
        params = {}
        if fields:
            params["fields"] = fields

        try:
            all_accounts = []
            endpoint_path = f"/{self.api_version}/{url_path}"

            while endpoint_path:
                response = self.client.get(
                    endpoint_path=endpoint_path,
                    params=self._with_token(params),
                )

                if not response:
                    break

                if isinstance(response, dict) and "data" in response:
                    all_accounts.extend(response["data"])
                    endpoint_path = response.get("paging", {}).get("next")
                else:
                    break

            return all_accounts

        except Exception as e:
            raise UserException(f"Failed to list accounts: {str(e)}")

    def get_account_data(self, account_id: str, fields: str) -> dict[str, Any] | None:
        """
        Get account data using proper token logic.
        """
        try:
            response = self.client.get(
                endpoint_path=f"/{self.api_version}/{account_id}",
                params=self._with_token({"fields": fields}),
            )
            return response
        except Exception as e:
            logger.error(f"Failed to fetch account data for {account_id}: {str(e)}")
            return None

    def debug_token(self, token: str) -> dict[str, Any]:
        #  TODO mute the logging for this method
        response = self.client.get(
            endpoint_path=f"/{self.api_version}/debug_token",
            params={
                "input_token": token,
                "access_token": f"{self.oauth.appKey}|{self.oauth.appSecret}",
            },
        )
        return response

    def _request_require_page_token(self, row_config) -> bool:
        """
        Determine if the request requires a page token.
        """
        # Facebook Ads API (async-insights-query) doesn't require page tokens
        if hasattr(row_config, "type") and row_config.type == "async-insights-query":
            return False

        query_config = row_config.query if hasattr(row_config, "query") else row_config
        check_path = query_config.path in [
            "insights",
            "feed",
            "posts",
            "ratings",
            "likes",
            "stories",
        ]

        fields = str(query_config.fields or "")

        return check_path or "insights" in fields or "likes" in fields or "from" in fields or "username" in fields

    def _get_fb_graph_node(self, is_page_token: bool, row_config) -> str:
        """
        Get the Facebook Graph node path.
        """
        # Always start with 'page' as base node
        base_node = "page"

        # Handle async insights queries specifically - these are always insights
        if hasattr(row_config, "type") and row_config.type == "async-insights-query":
            return f"{base_node}_insights"

        # Get query config
        query_config = row_config.query if hasattr(row_config, "query") else row_config
        fields = str(query_config.fields or "")

        # For page token requests without path, default to insights only if 'insights' is in fields
        if is_page_token and not query_config.path:
            return f"{base_node}_insights" if "insights" in fields else base_node

        # If no path specified, return base node
        if not query_config.path:
            return base_node

        # Add path as first level nesting
        node_path = f"{base_node}_{query_config.path}"

        # Handle additional nesting based on fields
        field_parts = []

        # Add any nested fields to the path
        for field in field_parts:
            node_path = f"{node_path}_{field}"

        return node_path
