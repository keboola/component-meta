import logging
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse

from keboola.component.exceptions import UserException
from keboola.http_client import HttpClient
from keboola.utils.date import get_past_date
from requests import HTTPError

logger = logging.getLogger(__name__)

# Facebook error code and subcode for "Media Posted Before Business Account Conversion"
# This error occurs when requesting insights for data that existed before the account
# was converted from personal to business. V1 handles this gracefully by returning empty data.
BUSINESS_CONVERSION_ERROR_CODE = 100
BUSINESS_CONVERSION_ERROR_SUBCODE = 2108006

# Error subcode for "Object does not exist / missing permissions"
OBJECT_NOT_FOUND_ERROR_SUBCODE = 33

# Maximum allowed days for Instagram insights queries (Facebook API constraint)
MAX_INSTAGRAM_INSIGHTS_DAYS = 30

# Instagram-specific metrics that indicate an IG insights query
IG_INSIGHTS_METRICS = ["follower_count", "reach", "impressions", "profile_views"]

# Time threshold in seconds for detecting recent timestamps (1 hour)
ONE_HOUR_SECONDS = 3600


class PageLoader:
    def __init__(self, client: HttpClient, query_type: str, api_version: str = "v20.0"):
        self.client = client
        self.query_type = query_type
        self.api_version = api_version

    def load_page(self, query_config, page_id: str, params: dict[str, Any] = None) -> dict[str, Any]:
        if self.query_type == "async-insights-query":
            return self._load_async_insights(query_config, page_id, params)
        else:
            return self._load_regular_page(query_config, page_id, params)

    def _load_async_insights(self, query_config, page_id: str, params: dict[str, Any] = None) -> dict[str, Any]:
        report_id = self.start_async_insights_job(query_config, page_id, params)
        if not report_id:
            return {"data": []}

        logging.info(f"Started polling for insights job report: {report_id}")

        # Poll for completion
        return self.poll_async_job(report_id)

    def start_async_insights_job(self, query_config, page_id: str, params: dict = {}) -> str | None:
        page_id = page_id if page_id.startswith("act_") else f"act_{page_id}"
        endpoint_path = f"/{self.api_version}/{page_id}/insights"

        # Extract query parameters if present
        if getattr(query_config, "parameters", None):
            param_pairs = (p.split("=", 1) for p in query_config.parameters.split("&") if "=" in p)
            params.update({k.strip(): v.strip() for k, v in param_pairs})

        logging.info(f"Starting async insights request: {endpoint_path}")

        try:
            response = self.client.post(endpoint_path=endpoint_path, json=params)
            report_id = response.get("report_run_id")
            if not report_id:
                logging.warning("No 'report_run_id' found in the async insights response.")
                return None

            logging.info(f"Async job started successfully with report ID: {report_id}")
            return report_id

        except Exception as e:
            logging.error(f"Error starting async insights job: {e}")
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
                    logging.error("Empty response from async job status check")
                    break

                async_percent = response.get("async_percent_completion", 0)
                async_status = response.get("async_status", "Unknown")

                logging.info(f"Async job {report_id}: {async_percent}% complete, status: {async_status}")

                is_finished = async_percent == 100

                if async_status in ["Job Failed", "Job Skipped"]:
                    raise UserException(f"Async insights job failed: {async_status}")

                if not is_finished or async_status != "Job Completed":
                    time.sleep(5)
                    attempt += 1

            except Exception as e:
                logging.error(f"Error polling async job {report_id}: {str(e)}")
                raise e

        if not is_finished or async_status != "Job Completed":
            raise UserException(f"Async insights job {report_id} did not complete within timeout")

        # Get final results with access token
        try:
            params = {"access_token": access_token} if access_token else {}
            final_response = self.client.get(endpoint_path=f"/{self.api_version}/{report_id}/insights", params=params)
            return final_response if final_response else {"data": []}
        except Exception as e:
            logging.error(f"Failed to get final results for job {report_id}: {str(e)}")
            return {"data": []}

    def _load_regular_page(self, query_config, page_id: str, params: dict[str, Any] = None) -> dict[str, Any]:
        base_params = self._build_params(query_config)
        base_params.update(params or {})

        endpoint_path = self._build_endpoint_path(query_config, page_id)

        logging.debug(f"Loading page data from: {endpoint_path}")
        logging.debug(f"Request params: {base_params}")

        try:
            response = self.client.get(endpoint_path=endpoint_path, params=base_params)
            return response or {"data": []}

        except HTTPError as e:
            # Check for recoverable errors - return empty data instead of failing
            is_recoverable, error_type = self._is_recoverable_error(e)
            if is_recoverable:
                logging.warning(f"Skipping account: {error_type}")
                return {"data": []}

            logging.error(f"HTTP error while loading page data: {e}")
            # Log the full error response for debugging
            response = getattr(e, "response", None)
            if response is not None:
                logging.error(f"Facebook API error response: {response.text}")
            raise

        except Exception as e:
            logging.error(f"Failed to load page data: {e}")
            raise

    def _build_params(self, query_config) -> dict[str, Any]:
        params = {
            "limit": query_config.limit,
        }

        if getattr(query_config, "since", "").strip():
            params["since"] = get_past_date(query_config.since).strftime("%Y-%m-%d")

        if getattr(query_config, "until", "").strip():
            params["until"] = get_past_date(query_config.until).strftime("%Y-%m-%d")

        fields = str(getattr(query_config, "fields", ""))
        # Insights queries have special parameter handling
        if not query_config.path and fields.startswith("insights"):
            # Extract 'metric' from the 'fields' string (e.g., "insights.metric(page_fans)")
            if ".metric(" in fields:
                metric_part = fields.split(".metric(")[1].split(")")[0]
                metrics = [m.strip() for m in metric_part.replace("\n", "").split(",") if m.strip()]
                if metrics:
                    params["metric"] = ",".join(metrics)

            # Extract 'period' from the 'fields' string (e.g., "insights.period(day)")
            if ".period(" in fields:
                period_part = fields.split(".period(")[1].split(")")[0]
                params["period"] = period_part.strip()

            # Extract and convert 'since' from the 'fields' string (e.g., "insights.since(90 days ago)")
            if ".since(" in fields:
                since_part = fields.split(".since(")[1].split(")")[0]
                params["since"] = get_past_date(since_part.strip()).strftime("%Y-%m-%d")

            # Extract and convert 'until' from the 'fields' string (e.g., "insights.until(2 days ago)")
            if ".until(" in fields:
                until_part = fields.split(".until(")[1].split(")")[0]
                params["until"] = get_past_date(until_part.strip()).strftime("%Y-%m-%d")

            # Validate 30-day limit for Instagram insights queries
            self._validate_ig_insights_date_range(params, fields)

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

    def _validate_ig_insights_date_range(self, params: dict[str, Any], fields: str) -> None:
        """
        Validate that the date range for Instagram insights queries does not exceed 30 days.

        Facebook API enforces a strict 30-day maximum window for Instagram insights queries.
        This validation catches the issue early with a clear error message instead of
        letting the API return a cryptic 400 error.

        Args:
            params: The query parameters containing 'since' and optionally 'until'
            fields: The fields string to check for Instagram-specific metrics

        Raises:
            UserException: If the date range exceeds 30 days for an IG insights query
        """
        # Only validate if this is an Instagram insights query (contains IG-specific metrics)
        if not any(metric in fields for metric in IG_INSIGHTS_METRICS):
            return

        since_str = params.get("since")
        if not since_str:
            return

        # Parse since date
        try:
            since_date = datetime.strptime(since_str, "%Y-%m-%d")
        except ValueError:
            return  # Can't parse, let the API handle it

        # Parse until date (defaults to today if not specified)
        until_str = params.get("until")
        if until_str:
            try:
                until_date = datetime.strptime(until_str, "%Y-%m-%d")
            except ValueError:
                return  # Can't parse, let the API handle it
        else:
            until_date = datetime.now()

        # Calculate the difference in days
        delta_days = (until_date - since_date).days

        if delta_days > MAX_INSTAGRAM_INSIGHTS_DAYS:
            raise UserException(
                f"Instagram insights queries cannot exceed {MAX_INSTAGRAM_INSIGHTS_DAYS} days. "
                f"Your query spans {delta_days} days (from {since_str} to "
                f"{until_str or 'today'}). Please reduce the date range to {MAX_INSTAGRAM_INSIGHTS_DAYS} days or less."
            )

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

            logging.debug(f"Loading paginated data from path: {path}")
            logging.debug(f"Pagination params: {params}")

            # Handle Meta bug: pagination URLs sometimes have invalid timestamps
            now_ts = int(datetime.now(timezone.utc).timestamp())
            since_ts = self._parse_unix_ts(params.get("since"))
            until_ts = self._parse_unix_ts(params.get("until"))

            # Case 1: since is very recent (within last hour) and no until
            # This happens when Facebook returns pagination URLs pointing to "now"
            # which is invalid for insights queries. Skip these pagination requests.
            if since_ts is not None and until_ts is None:
                one_hour_ago = now_ts - ONE_HOUR_SECONDS
                if since_ts > one_hour_ago:
                    logging.debug(
                        f"Skipping pagination: reached end of historical data (since={since_ts})"
                    )
                    return {"data": []}

            # Case 2: Handle future timestamps
            if until_ts is not None and until_ts > now_ts:
                # Both since and until in future -> no historical data to fetch
                if since_ts is not None and since_ts > now_ts:
                    logging.debug("Skipping pagination: reached end of historical data")
                    return {"data": []}

                # Only until in future -> check if since is also too recent
                # If since is within the last hour, there's no meaningful data to fetch
                one_hour_ago = now_ts - ONE_HOUR_SECONDS
                if since_ts is not None and since_ts > one_hour_ago:
                    logging.debug(
                        f"Skipping pagination: reached end of historical data (since={since_ts})"
                    )
                    return {"data": []}

                # since is old enough, just remove the future until
                logging.debug("Adjusting pagination window to end at current time")
                params.pop("until", None)

            response = self.client.get(endpoint_path=path, params=params)

            return response if response else {"data": []}

        except HTTPError as e:
            # Check for recoverable errors - return empty data instead of failing
            is_recoverable, error_type = self._is_recoverable_error(e)
            if is_recoverable:
                logging.warning(f"Skipping account: {error_type}")
                return {"data": []}

            status_code = getattr(getattr(e, "response", None), "status_code", None)
            logging.error(f"HTTP error while loading paginated data (status={status_code}): {e}")
            if hasattr(e, "response") and e.response is not None:
                logging.error(f"Facebook API error response: {e.response.text}")
            raise

        except Exception as e:
            logging.error(f"Failed to load paginated data from URL {url}: {str(e)}")
            raise

    def _parse_unix_ts(self, value: Any) -> int | None:
        """Parse a 10-digit Unix timestamp, return None if not valid."""
        if value is None:
            return None
        s = str(value)
        if s.isdigit() and len(s) == 10:
            return int(s)
        return None

    def _check_error_matches(
        self, http_error: HTTPError, error_phrase: str, error_code: int | None = None, error_subcode: int | None = None
    ) -> bool:
        """
        Generic helper to check if an HTTP error matches specific criteria.

        Args:
            http_error: The HTTPError to check
            error_phrase: The phrase to search for in error messages (case-insensitive)
            error_code: Optional error code to match
            error_subcode: Optional error subcode to match

        Returns:
            True if the error matches the criteria, False otherwise
        """
        response = getattr(http_error, "response", None)
        if response is not None:
            # Try JSON parsing first for structured error info
            if error_code is not None and error_subcode is not None:
                try:
                    error_data = response.json()
                    error_info = error_data.get("error", {})
                    if error_info.get("code") == error_code and error_info.get("error_subcode") == error_subcode:
                        return True
                    # Also check error message text in JSON response
                    error_msg = str(error_info.get("error_user_title", "")).lower()
                    error_msg += " " + str(error_info.get("error_user_msg", "")).lower()
                    error_msg += " " + str(error_info.get("message", "")).lower()
                    if error_phrase in error_msg:
                        return True
                except Exception:
                    pass

            # Try raw response text
            try:
                response_text = (response.text or "").lower()
                if error_phrase in response_text:
                    return True
            except Exception:
                pass

        # Fallback: check the exception message itself
        try:
            exception_msg = str(http_error).lower()
            if error_phrase in exception_msg:
                return True
        except Exception:
            pass

        return False

    def _is_business_conversion_error(self, http_error: HTTPError) -> bool:
        """
        Check if the HTTP error is the "Media Posted Before Business Account Conversion" error.

        This error (code 100, subcode 2108006) occurs when requesting insights for data
        that existed before the Instagram account was converted from personal to business.
        V1 handles this gracefully by returning empty data instead of failing.
        """
        return self._check_error_matches(
            http_error,
            "media posted before business account conversion",
            BUSINESS_CONVERSION_ERROR_CODE,
            BUSINESS_CONVERSION_ERROR_SUBCODE,
        )

    def _is_30day_limit_error(self, http_error: HTTPError) -> bool:
        """
        Check if the HTTP error is the "30 day limit exceeded" error.

        This error occurs when the date range between since and until exceeds 30 days
        (2592000 seconds). For backwards compatibility, we handle this gracefully.
        """
        return self._check_error_matches(http_error, "there cannot be more than 30 days")

    def _is_object_not_found_error(self, http_error: HTTPError) -> bool:
        """
        Check if the HTTP error is the "Object does not exist / missing permissions" error.

        This error (code 100, subcode 33) occurs when the account no longer exists,
        has been deleted, or the token doesn't have permission to access it.
        For backwards compatibility with old configs, we handle this gracefully.
        """
        return self._check_error_matches(
            http_error,
            "does not exist, cannot be loaded due to missing permissions",
            BUSINESS_CONVERSION_ERROR_CODE,
            OBJECT_NOT_FOUND_ERROR_SUBCODE,
        )

    def _is_recoverable_error(self, http_error: HTTPError) -> tuple[bool, str]:
        """
        Check if the HTTP error is a recoverable error that should return empty data
        instead of failing the job. Returns (is_recoverable, error_type).
        """
        if self._is_business_conversion_error(http_error):
            return True, "Media Posted Before Business Account Conversion"
        if self._is_30day_limit_error(http_error):
            return True, "30-day limit exceeded. The date range must be 30 days or less."
        if self._is_object_not_found_error(http_error):
            return True, "Account no longer exists or is inaccessible. Remove it or re-run Add Account."
        return False, ""
