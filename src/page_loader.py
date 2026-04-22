import logging
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, urlparse

from keboola.component.exceptions import UserException
from keboola.http_client import HttpClient
from keboola.utils.date import get_past_date
from requests import HTTPError
from requests.exceptions import RetryError

logger = logging.getLogger(__name__)


# Facebook API error codes
@dataclass(frozen=True)
class FacebookErrorCode:
    """Facebook API error code constants."""

    code: int
    subcode: int | None = None
    message_fragment: str | None = None


# Known recoverable errors
BUSINESS_CONVERSION_ERROR = FacebookErrorCode(
    code=100,
    subcode=2108006,
    message_fragment="media posted before business account conversion",
)

OBJECT_NOT_FOUND_ERROR = FacebookErrorCode(
    code=100,
    subcode=33,
    message_fragment="does not exist, cannot be loaded due to missing permissions",
)

DATE_RANGE_LIMIT_ERROR = FacebookErrorCode(code=None, message_fragment="there cannot be more than 30 days")

# DSL parameters to extract from insights fields string (e.g., ".param_name(value)")
# These are simple parameters that only need extraction and stripping
DSL_SIMPLE_PARAMS = [
    "period",
    "level",
    "metric_type",
    "action_breakdowns",
    "date_preset",
    "time_increment",
    "breakdown",
    "breakdowns",
    "action_attribution_windows",
    "action_report_time",
    "use_account_attribution_setting",
    "use_unified_attribution_setting",
    "filtering",
    "summary_action_breakdowns",
    "product_id_limit",
    "sort",
    "summary",
    "default_summary",
    "time_range",
    "time_ranges",
]

INVALID_METRIC_ERROR = FacebookErrorCode(code=100, message_fragment="should be specified with parameter metric_type")

# Facebook API error codes that are transient and safe to retry.
# See https://developers.facebook.com/docs/graph-api/guides/error-handling/#errorcodes
# Rate-limit codes (4, 17, 32, 341, 613) would ideally use X-Business-Use-Case-Usage /
# Retry-After for accurate backoff — a simple exponential retry is still better than failing.
_FB_TRANSIENT_ERROR_CODES = frozenset(
    {
        1,  # API Unknown — possibly temporary, retry
        2,  # API Service — temporary downtime, retry
        4,  # App-level rate limit
        17,  # User-level rate limit
        32,  # Page-level rate limit
        341,  # Application limit reached
        613,  # Rate limit exceeded
    }
)
_FB_TRANSIENT_ERROR_MAX_RETRIES = 3
_FB_TRANSIENT_ERROR_BACKOFF_BASE = 5  # seconds; delays are 5s, 10s, 20s


def resolve_query_window(query_config) -> tuple[str | None, str | None]:
    """Return the effective (since, until) YYYY-MM-DD strings that will be sent to the API.

    DSL-level .since()/.until() override the config-level values only when the query has no
    explicit path — this mirrors _build_params(): for nested queries (e.g. path='feed' with
    fields='insights.since(now)...'), the DSL string is sent as-is in 'fields' and the dates
    are interpreted by Facebook server-side, so we must not extract them into query params.
    Returns (None, None) for queries without a time window.
    """
    since = None
    until = None

    if getattr(query_config, "since", "").strip():
        since = get_past_date(query_config.since).strftime("%Y-%m-%d")
    if getattr(query_config, "until", "").strip():
        until = get_past_date(query_config.until).strftime("%Y-%m-%d")

    fields = str(getattr(query_config, "fields", ""))
    path = getattr(query_config, "path", None) or ""
    if not path and fields.startswith("insights"):
        for date_param in ("since", "until"):
            match = re.search(rf"\.{date_param}\(([^)]*)\)", fields)
            if match:
                resolved = get_past_date(match.group(1).strip()).strftime("%Y-%m-%d")
                if date_param == "since":
                    since = resolved
                else:
                    until = resolved

    return since, until


class FacebookErrorHandler:
    """Handles Facebook API error detection and categorization."""

    @staticmethod
    def is_recoverable_error(http_error: HTTPError) -> tuple[bool, str]:
        """
        Check if an HTTP error is recoverable (should return empty data instead of failing).
        Returns (is_recoverable, error_description).
        """
        if FacebookErrorHandler._matches_error(http_error, BUSINESS_CONVERSION_ERROR):
            return True, "Media Posted Before Business Account Conversion"

        if FacebookErrorHandler._matches_error(http_error, DATE_RANGE_LIMIT_ERROR):
            return (
                True,
                "30-day limit exceeded. Change 'since(30 days ago)' to '29 days ago' in config.",
            )

        if FacebookErrorHandler._matches_error(http_error, OBJECT_NOT_FOUND_ERROR):
            return (
                True,
                "Account no longer exists or is inaccessible. Remove it or re-run Add Account.",
            )

        return False, ""

    @staticmethod
    def raise_if_user_actionable(http_error: HTTPError) -> None:
        """Raise UserException for errors that indicate a misconfiguration in the query."""
        if FacebookErrorHandler._matches_error(http_error, INVALID_METRIC_ERROR):
            response = getattr(http_error, "response", None)
            api_msg = ""
            if response is not None:
                try:
                    api_msg = response.json().get("error", {}).get("message", "").strip()
                except Exception:
                    pass
            detail = f"Invalid metric configuration: {api_msg}." if api_msg else "Invalid metric configuration."
            raise UserException(
                f"{detail} "
                "Add 'metric_type(total_value)' to your Fields DSL, e.g.: "
                "insights.period(day).metric_type(total_value).metric(reach,...)"
            ) from http_error

    @staticmethod
    def is_transient_error(http_error: HTTPError) -> bool:
        """Return True for Facebook error codes documented as transient/retryable."""
        try:
            code = http_error.response.json().get("error", {}).get("code")
        except Exception:
            return False
        return code in _FB_TRANSIENT_ERROR_CODES

    @staticmethod
    def _matches_error(http_error: HTTPError, error_code: FacebookErrorCode) -> bool:
        """Check if HTTP error matches the given error code definition.

        When a message_fragment is provided it is REQUIRED to match, even if the code/subcode
        already do. This avoids catching unrelated Facebook errors that happen to share a code
        (e.g. generic code=100 permission errors vs our specific 'metric_type' hint).
        """
        response = getattr(http_error, "response", None)
        fragment = (error_code.message_fragment or "").lower()

        # Check code/subcode match from structured JSON error
        code_matches = False
        if error_code.code is not None and response is not None:
            try:
                error_info = response.json().get("error", {})
                if error_info.get("code") == error_code.code:
                    if error_code.subcode is None or error_info.get("error_subcode") == error_code.subcode:
                        code_matches = True
            except Exception:
                pass

        # Check message_fragment match in response text or exception string
        fragment_matches = False
        if fragment:
            if response is not None:
                try:
                    if fragment in response.text.lower():
                        fragment_matches = True
                except Exception:
                    pass
            if not fragment_matches:
                try:
                    if fragment in str(http_error).lower():
                        fragment_matches = True
                except Exception:
                    pass

        if fragment:
            # When fragment is defined it is REQUIRED. Code-only match is not enough.
            return fragment_matches
        # No fragment defined → fall back to code-based match
        return code_matches


class PaginationHandler:
    """Handles Facebook API pagination timestamp edge cases."""

    @staticmethod
    def should_skip_pagination(params: dict[str, Any]) -> tuple[bool, str]:
        """
        Check if pagination request should be skipped due to invalid timestamps.
        Returns (should_skip, reason).
        """
        now_ts = int(datetime.now(UTC).timestamp())
        since_ts = PaginationHandler._parse_unix_ts(params.get("since"))
        until_ts = PaginationHandler._parse_unix_ts(params.get("until"))
        one_hour_ago = now_ts - 3600

        # Case 1: Very recent 'since' without 'until' - reached end of historical data
        if since_ts is not None and until_ts is None and since_ts > one_hour_ago:
            return True, "reached end of historical data"

        # Case 2: Both timestamps in future - no historical data
        if since_ts is not None and until_ts is not None:
            if since_ts > now_ts and until_ts > now_ts:
                return True, "both timestamps in future"

            # Future 'until' with very recent 'since' - no meaningful data
            if until_ts > now_ts and since_ts > one_hour_ago:
                return True, "reached end of historical data"

        return False, ""

    @staticmethod
    def adjust_params(params: dict[str, Any]) -> dict[str, Any]:
        """Remove future 'until' timestamp if present, return adjusted params."""
        until_ts = PaginationHandler._parse_unix_ts(params.get("until"))
        if until_ts is not None:
            now_ts = int(datetime.now(UTC).timestamp())
            if until_ts > now_ts:
                logger.debug("Adjusting pagination window to end at current time")
                params = params.copy()
                params.pop("until", None)
        return params

    @staticmethod
    def _parse_unix_ts(value: Any) -> int | None:
        """Parse a 10-digit Unix timestamp, return None if not valid."""
        if value is None:
            return None
        s = str(value)
        return int(s) if s.isdigit() and len(s) == 10 else None


class PageLoader:
    def __init__(self, client: HttpClient, query_type: str, api_version: str = "v20.0"):
        self.client = client
        self.query_type = query_type
        self.api_version = api_version

    def _get_with_transient_retry(self, path: str, params: dict[str, Any]) -> dict[str, Any] | None:
        """GET request with exponential-backoff retry on documented FB transient errors.

        All other errors are re-raised immediately so the caller's existing HTTPError
        handlers remain in control of non-transient failures.
        """
        for attempt in range(_FB_TRANSIENT_ERROR_MAX_RETRIES + 1):
            try:
                return self.client.get(endpoint_path=path, params=params)
            except HTTPError as e:
                if attempt < _FB_TRANSIENT_ERROR_MAX_RETRIES and FacebookErrorHandler.is_transient_error(e):
                    wait = _FB_TRANSIENT_ERROR_BACKOFF_BASE * (2**attempt)
                    logger.warning(
                        f"Facebook transient error, attempt {attempt + 1}/{_FB_TRANSIENT_ERROR_MAX_RETRIES + 1}, "
                        f"retrying in {wait}s"
                    )
                    time.sleep(wait)
                else:
                    raise
        return None  # unreachable — loop always returns or raises

    def load_page(self, query_config, page_id: str, params: dict[str, Any] = None) -> dict[str, Any]:
        if self.query_type == "async-insights-query":
            return self._load_async_insights(query_config, page_id, params)
        else:
            return self._load_regular_page(query_config, page_id, params)

    def _load_async_insights(self, query_config, page_id: str, params: dict[str, Any] = None) -> dict[str, Any]:
        report_id = self.start_async_insights_job(query_config, page_id, params)
        if not report_id:
            return {"data": []}

        logger.info(f"Started polling for insights job report: {report_id}")

        # Poll for completion
        return self.poll_async_job(report_id)

    def start_async_insights_job(self, query_config, page_id: str, params: dict = {}) -> str | None:
        page_id = page_id if page_id.startswith("act_") else f"act_{page_id}"
        endpoint_path = f"/{self.api_version}/{page_id}/insights"

        # Build parameters using the same logic as regular page loading
        # This ensures all DSL parameters are properly parsed
        base_params = self._build_params(query_config)
        base_params.update(params)
        params = base_params

        logger.info(f"Starting async insights request: {endpoint_path}")
        logger.debug(f"Async insights params: {params}")

        try:
            response = self.client.post(endpoint_path=endpoint_path, json=params)
            report_id = response.get("report_run_id")
            if not report_id:
                logger.warning("No 'report_run_id' found in the async insights response.")
                return None

            logger.info(f"Async job started successfully with report ID: {report_id}")
            return report_id

        except Exception as e:
            logger.error(f"Error starting async insights job: {e}")
            return None

    def poll_async_job(self, report_id: str, access_token: str = None) -> dict[str, Any]:
        is_finished = False
        max_attempts = 60  # 5 minutes max wait time
        attempt = 0
        async_status = ""

        while (not is_finished or async_status != "Job Completed") and attempt < max_attempts:
            try:
                # Include access token in polling request
                params = {"access_token": access_token} if access_token else {}
                response = self.client.get(endpoint_path=f"/{self.api_version}/{report_id}", params=params)

                if not response:
                    logger.error("Empty response from async job status check")
                    break

                async_percent = response.get("async_percent_completion", 0)
                async_status = response.get("async_status", "Unknown")

                logger.info(f"Async job {report_id}: {async_percent}% complete, status: {async_status}")

                is_finished = async_percent == 100

                if async_status in ["Job Failed", "Job Skipped"]:
                    raise UserException(f"Async insights job failed: {async_status}")

                if not is_finished or async_status != "Job Completed":
                    time.sleep(5)
                    attempt += 1

            except Exception as e:
                logger.error(f"Error polling async job {report_id}: {str(e)}")
                raise e

        if not is_finished or async_status != "Job Completed":
            raise UserException(f"Async insights job {report_id} did not complete within timeout")

        # Get final results with access token
        try:
            params = {"access_token": access_token} if access_token else {}
            final_response = self.client.get(endpoint_path=f"/{self.api_version}/{report_id}/insights", params=params)
            return final_response if final_response else {"data": []}
        except Exception as e:
            logger.error(f"Failed to get final results for job {report_id}: {str(e)}")
            return {"data": []}

    def _load_regular_page(self, query_config, page_id: str, params: dict[str, Any] = None) -> dict[str, Any]:
        base_params = self._build_params(query_config)
        base_params.update(params or {})

        endpoint_path = self._build_endpoint_path(query_config, page_id)

        logger.debug(f"Loading page data from: {endpoint_path}")
        logger.debug(f"Request params: {base_params}")

        try:
            response = self._get_with_transient_retry(endpoint_path, base_params)
            return response or {"data": []}

        except HTTPError as e:
            # Raise UserException for misconfigured queries before checking recoverable errors
            FacebookErrorHandler.raise_if_user_actionable(e)

            # Check for recoverable errors
            is_recoverable, error_desc = FacebookErrorHandler.is_recoverable_error(e)
            if is_recoverable:
                logger.warning(f"Skipping account: {error_desc}")
                return {"data": []}

            # Non-recoverable error
            logger.error(f"HTTP error while loading page data: {e}")
            response = getattr(e, "response", None)
            if response is not None:
                logger.error(f"Facebook API error response: {response.text}")
            raise

        except RetryError as e:
            raise UserException(
                "Facebook API is temporarily unavailable (too many server errors). "
                "Try again later or reduce the number of simultaneous configurations."
            ) from e

        except Exception as e:
            logger.error(f"Failed to load page data: {e}")
            raise

    def _build_params(self, query_config) -> dict[str, Any]:
        params = {
            "limit": query_config.limit,
        }

        # Resolve since/until once from config + DSL overrides (single source of truth).
        since, until = resolve_query_window(query_config)
        if since is not None:
            params["since"] = since
        if until is not None:
            params["until"] = until

        fields = str(getattr(query_config, "fields", ""))
        # Insights queries have special parameter handling
        if not query_config.path and fields.startswith("insights"):
            # TODO: these regexes could also be used to validate DSL fields in sync actions
            #       (accounts, adaccounts, igaccounts) before a job is run.
            # Extract simple parameters (just strip the value)
            for param_name in DSL_SIMPLE_PARAMS:
                match = re.search(rf"\.{param_name}\(([^)]*)\)", fields)
                if match:
                    params[param_name] = match.group(1).strip()

            # Extract 'metric' - special handling: split by comma and join
            match = re.search(r"\.metric\(([^)]*)\)", fields)
            if match:
                metrics = [m.strip() for m in match.group(1).replace("\n", "").split(",") if m.strip()]
                if metrics:
                    params["metric"] = ",".join(metrics)

            # Warn about unrecognized DSL parameters
            known_params = set(DSL_SIMPLE_PARAMS) | {"metric", "since", "until"}
            for unrecognized in re.findall(r"\.([a-zA-Z_]+)\(", fields):
                if unrecognized not in known_params:
                    logger.warning(
                        f"Unrecognized DSL parameter '.{unrecognized}(...)' in query fields — "
                        f"it will be ignored. Known parameters: {sorted(known_params)}"
                    )

            # Extract fields from curly braces (e.g., "insights.level(ad){ad_id,ad_name,spend}")
            if "{" in fields and "}" in fields:
                fields_part = fields.split("{")[1].split("}")[0]
                field_list = [f.strip() for f in fields_part.replace("\n", "").split(",") if f.strip()]

                # Ensure account_id is always included for backwards compatibility
                if field_list and "account_id" not in field_list:
                    field_list.append("account_id")

                if field_list:
                    params["fields"] = ",".join(field_list)

        else:
            # Regular queries use the 'fields' parameter directly
            if query_config.fields:
                params["fields"] = query_config.fields

        # Remove keys with None values
        params = {k: v for k, v in params.items() if v is not None}

        # Add additional parameters
        extras = getattr(query_config, "parameters", None)
        if extras:
            if isinstance(extras, str):
                pairs = (p.split("=", 1) for p in extras.split("&") if "=" in p)
                params.update({k.strip(): v.strip() for k, v in pairs})
            elif isinstance(extras, dict):
                params.update(extras)
        return params

    def _build_endpoint_path(self, query_config, page_id: str) -> str:
        # Start with the API version
        path_parts = [self.api_version, page_id]

        # Check if this is an insights query
        fields = str(getattr(query_config, "fields", ""))
        if not query_config.path and fields.startswith("insights"):
            path_parts.append("insights")
        elif query_config.path:
            path_parts.append(query_config.path)

        return "/" + "/".join(path_parts)

    def load_page_from_url(self, url: str) -> dict[str, Any]:
        """
        Load page data from a full Facebook API URL (used for pagination).
        Respects the paging.next URL as returned by the Facebook API.
        """
        try:
            parsed_url = urlparse(url)
            path = parsed_url.path

            # Parse query parameters
            query_params = parse_qs(parsed_url.query)
            # Convert lists to single values (parse_qs returns lists)
            params = {k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in query_params.items()}

            logger.debug(f"Loading paginated data from path: {path}")
            logger.debug(f"Pagination params: {params}")

            # Check if pagination should be skipped (e.g., future timestamps)
            should_skip, reason = PaginationHandler.should_skip_pagination(params)
            if should_skip:
                logger.info(f"Skipping pagination: {reason} for URL: {url}")
                return {"data": []}

            # Adjust params if needed (e.g., remove future 'until')
            params = PaginationHandler.adjust_params(params)

            response = self._get_with_transient_retry(path, params)
            return response if response else {"data": []}

        except HTTPError as e:
            # Raise UserException for misconfigured queries before checking recoverable errors
            FacebookErrorHandler.raise_if_user_actionable(e)

            # Check for recoverable errors
            is_recoverable, error_desc = FacebookErrorHandler.is_recoverable_error(e)
            if is_recoverable:
                logger.warning(f"Skipping account: {error_desc}")
                return {"data": []}

            # Non-recoverable error
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            logger.error(f"HTTP error while loading paginated data (status={status_code}): {e}")
            response = getattr(e, "response", None)
            if response is not None:
                logger.error(f"Facebook API error response: {response.text}")
            raise

        except RetryError as e:
            raise UserException(
                "Facebook API is temporarily unavailable (too many server errors). "
                "Try again later or reduce the number of simultaneous configurations."
            ) from e

        except Exception as e:
            logger.error(f"Failed to load paginated data from URL {url}: {str(e)}")
            raise
