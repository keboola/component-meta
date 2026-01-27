import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from keboola.component.exceptions import UserException
from keboola.http_client import HttpClient
from keboola.utils.date import get_past_date
from requests import HTTPError

logger = logging.getLogger(__name__)


# Facebook API error codes
@dataclass(frozen=True)
class FacebookErrorCode:
    """Facebook API error code constants."""

    code: int
    subcode: Optional[int] = None
    message_fragment: Optional[str] = None


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

INVALID_METRIC_ERROR = FacebookErrorCode(code=100, message_fragment="should be specified with parameter metric_type")


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

        if FacebookErrorHandler._matches_error(http_error, INVALID_METRIC_ERROR):
            # Extract detailed error message from response
            response = getattr(http_error, "response", None)
            error_msg = "Invalid metric configuration"
            if response is not None:
                try:
                    error_data = response.json()
                    error_msg = error_data.get("error", {}).get("message", error_msg)
                except Exception:
                    pass
            return True, f"Query configuration error: {error_msg}"

        return False, ""

    @staticmethod
    def _matches_error(http_error: HTTPError, error_code: FacebookErrorCode) -> bool:
        """Check if HTTP error matches the given error code definition."""
        response = getattr(http_error, "response", None)

        # Try structured JSON error first
        if response is not None:
            try:
                error_data = response.json()
                error_info = error_data.get("error", {})

                # Match by code and subcode if both are defined
                if error_code.code is not None:
                    if error_info.get("code") == error_code.code:
                        if error_code.subcode is None or error_info.get("error_subcode") == error_code.subcode:
                            return True
            except Exception:
                pass

        # Fall back to message fragment matching
        if error_code.message_fragment:
            # Check response text
            if response is not None:
                try:
                    if error_code.message_fragment in response.text.lower():
                        return True
                except Exception:
                    pass

            # Check exception message
            try:
                if error_code.message_fragment in str(http_error).lower():
                    return True
            except Exception:
                pass

        return False


class PaginationHandler:
    """Handles Facebook API pagination timestamp edge cases."""

    @staticmethod
    def should_skip_pagination(params: dict[str, Any]) -> tuple[bool, str]:
        """
        Check if pagination request should be skipped due to invalid timestamps.
        Returns (should_skip, reason).
        """
        now_ts = int(datetime.now(timezone.utc).timestamp())
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
            now_ts = int(datetime.now(timezone.utc).timestamp())
            if until_ts > now_ts:
                logger.debug("Adjusting pagination window to end at current time")
                params = params.copy()
                params.pop("until", None)
        return params

    @staticmethod
    def _parse_unix_ts(value: Any) -> Optional[int]:
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

    def start_async_insights_job(self, query_config, page_id: str, params: dict = {}) -> Optional[str]:
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
            response = self.client.get(endpoint_path=endpoint_path, params=base_params)
            return response or {"data": []}

        except HTTPError as e:
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

        except Exception as e:
            logger.error(f"Failed to load page data: {e}")
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

            # Extract 'level' from the 'fields' string (e.g., "insights.level(ad)")
            if ".level(" in fields:
                level_part = fields.split(".level(")[1].split(")")[0]
                params["level"] = level_part.strip()

            # Extract 'action_breakdowns' from the 'fields' string (e.g., "insights.action_breakdowns(action_type)")
            if ".action_breakdowns(" in fields:
                action_breakdowns_part = fields.split(".action_breakdowns(")[1].split(")")[0]
                params["action_breakdowns"] = action_breakdowns_part.strip()

            # Extract 'date_preset' from the 'fields' string (e.g., "insights.date_preset(last_3d)")
            if ".date_preset(" in fields:
                date_preset_part = fields.split(".date_preset(")[1].split(")")[0]
                params["date_preset"] = date_preset_part.strip()

            # Extract 'time_increment' from the 'fields' string (e.g., "insights.time_increment(1)")
            if ".time_increment(" in fields:
                time_increment_part = fields.split(".time_increment(")[1].split(")")[0]
                params["time_increment"] = time_increment_part.strip()

            # Extract 'breakdowns' from the 'fields' string (e.g., "insights.breakdowns(age,gender)")
            if ".breakdowns(" in fields:
                breakdowns_part = fields.split(".breakdowns(")[1].split(")")[0]
                params["breakdowns"] = breakdowns_part.strip()

            # Extract fields from curly braces (e.g., "insights.level(ad){ad_id,ad_name,spend}")
            if "{" in fields and "}" in fields:
                # Extract content between curly braces
                fields_part = fields.split("{")[1].split("}")[0]
                # Parse comma-separated field names
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

            response = self.client.get(endpoint_path=path, params=params)
            return response if response else {"data": []}

        except HTTPError as e:
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

        except Exception as e:
            logger.error(f"Failed to load paginated data from URL {url}: {str(e)}")
            raise
