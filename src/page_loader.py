import logging
import time
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from keboola.component.exceptions import UserException
from keboola.http_client import HttpClient
from keboola.utils.date import get_past_date
from requests import HTTPError

logger = logging.getLogger(__name__)


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

    def start_async_insights_job(self, query_config, page_id: str, params: dict = {}) -> Optional[str]:
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

        while not is_finished and attempt < max_attempts:
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

                if not is_finished:
                    time.sleep(5)
                    attempt += 1

            except Exception as e:
                logging.error(f"Error polling async job {report_id}: {str(e)}")
                break

        if not is_finished:
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
                logging.error(f"HTTP error while loading page data: {e.response.text}")

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
        """
        try:
            parsed_url = urlparse(url)

            # Extract the path (remove the base URL part)
            # URL format: https://graph.facebook.com/v19.0/path?params
            path = parsed_url.path

            # Remove the version prefix if present (e.g., /v19.0/)
            if path.startswith("/v"):
                path_parts = path.split("/", 3)
                if len(path_parts) > 2:
                    path = "/" + "/".join(path_parts[2:])

            # Parse query parameters
            query_params = parse_qs(parsed_url.query)
            # Convert lists to single values (parse_qs returns lists)
            params = {k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in query_params.items()}

            logging.debug(f"Loading paginated data from path: {path}")
            logging.debug(f"Pagination params: {params}")

            response = self.client.get(endpoint_path=path, params=params)

            return response if response else {"data": []}

        except HTTPError as e:
            logging.error(f"HTTP error while loading paginated data from URL {url}: {e.response.text}")
            return {"data": []}

        except Exception as e:
            logging.error(f"Failed to load paginated data from URL {url}: {str(e)}")
            return {"data": []}
