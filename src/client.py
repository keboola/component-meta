import logging
import re
from collections.abc import Iterator
from typing import Any

from keboola.component.dao import OauthCredentials
from keboola.component.exceptions import UserException
from keboola.http_client import HttpClient
from requests import HTTPError

from output_parser import OutputParser
from page_loader import PageLoader


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

# Add filter to root logger and all existing loggers
logging.getLogger().addFilter(access_token_filter)
for name in logging.root.manager.loggerDict:
    logging.getLogger(name).addFilter(access_token_filter)


class FacebookClient:
    def __init__(self, oauth: OauthCredentials, api_version: str):
        self.oauth = oauth
        self.api_version = api_version
        self.page_tokens = None  # Cache for page tokens

        if self.oauth.data and self.oauth.data.get("token", None) and not self.oauth.data.get("access_token", None):
            logging.info("Direct insert token is used for authentication.")
            self.oauth.data["access_token"] = self.oauth.data["token"]

        self.client = HttpClient(
            base_url="https://graph.facebook.com",
            default_http_header={"Content-Type": "application/json"},
            status_forcelist=(500, 502, 503, 504),
        )

    def _with_token(self, params: dict, token: str = None) -> dict:
        """
        Return a copy of params with the access_token added.
        If token is not provided, use the main user access token.
        """
        params = dict(params) if params else {}
        params["access_token"] = token or self.oauth.data.get("access_token")
        return params

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
            logging.info(f"Starting {len(async_queries)} async queries in parallel.")
            for query in async_queries:
                logging.info(f"Starting async query: {query.name}")
                job_details = self._start_async_jobs_for_query(accounts, query)
                all_job_details.update(job_details)

        # Poll and process async results
        if all_job_details:
            logging.info(f"Polling and processing {len(all_job_details)} async jobs.")
            yield from self._poll_and_process_async_jobs(all_job_details)

        # Process sync queries
        for query in sync_queries:
            logging.info(f"Processing sync query: {query.name}")
            yield from self._process_single_sync_query(accounts, query)

    def _start_async_jobs_for_query(self, accounts: list, row_config) -> dict:
        if ids_str := row_config.query.ids:
            selected_ids = {id for id in ids_str.split(",")}
            accounts = [account for account in accounts if account.id in selected_ids]

        if self._request_require_page_token(row_config):
            is_page_token = True
            page_tokens = self._get_pages_token(accounts)
        else:
            is_page_token = False
            page_tokens = {account.id: self.oauth.data.get("access_token") for account in accounts}
        job_details = {}
        for page_id, token in page_tokens.items():
            page_id = str(page_id)
            try:
                # Use the shared client and pass token in params
                page_loader = PageLoader(self.client, row_config.type, self.api_version)
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
                    }
            except Exception as e:
                logging.error(f"Failed to start async job for {page_id}: {e}")
        return job_details

    def _poll_and_process_async_jobs(self, all_job_details: dict) -> Iterator[dict]:
        for report_id, details in all_job_details.items():
            try:
                page_loader = details["page_loader"]
                # Get the access token from the job details
                access_token = details.get("access_token", self.oauth.data.get("access_token"))
                page_data = page_loader.poll_async_job(report_id, access_token)
                if not page_data.get("data"):
                    continue
                output_parser = details["output_parser"]
                fb_graph_node = details["fb_graph_node"]
                page_id = details["page_id"]
                res = output_parser.parse_data(page_data, fb_graph_node, page_id)
                if res:
                    yield res
            except Exception as e:
                logging.error(f"Failed to process async job result for report_id: {report_id}: {e}")

    def _handle_batch_request(self, account_ids: list[str], row_config) -> Iterator[dict]:
        """
        Executes and parses a batch request for a list of account IDs.
        Yields parsed data for each item in the response.
        Raises HTTPError on failure so the caller can handle fallbacks.
        """
        logging.info(f"Batch fetching object details for IDs: {','.join(account_ids)}")
        params = {"ids": ",".join(account_ids), "fields": row_config.query.fields}

        # Raises HTTPError on failure
        response = self.client.get(f"/{self.api_version}/", params=self._with_token(params))

        if not response or not isinstance(response, dict):
            logging.warning("Empty or invalid response for batch ID fetch.")
            return

        fb_graph_node = self._get_fb_graph_node(False, row_config)
        for item_id, item_data in response.items():
            if isinstance(item_data, dict) and "error" in item_data:
                logging.warning(f"Error fetching data for ID {item_id}: {item_data['error']}")
                continue

            output_parser = OutputParser(page_loader=None, page_id=item_id, row_config=row_config)
            parsed_result = output_parser.parse_data(response=item_data, fb_node=fb_graph_node, parent_id=item_id)
            if parsed_result:
                yield parsed_result

    def _process_single_sync_query(self, accounts: list, row_config) -> Iterator[dict]:
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
                    logging.info(f"Attempting to batch fetch data for {len(account_ids)} IDs.")
                    params = {
                        "ids": ",".join(account_ids),
                        "fields": row_config.query.fields,
                    }
                    response = self.client.get(f"/{self.api_version}/", params=self._with_token(params))

                    if not response or not isinstance(response, dict):
                        logging.warning("Empty or invalid response for batch ID fetch.")
                    else:
                        fb_graph_node = self._get_fb_graph_node(False, row_config)
                        for item_id, item_data in response.items():
                            if isinstance(item_data, dict) and "error" in item_data:
                                logging.warning(f"Error fetching data for ID {item_id}: {item_data['error']}")
                                continue
                            output_parser = OutputParser(page_loader=None, page_id=item_id, row_config=row_config)
                            parsed_result = output_parser.parse_data(
                                response=item_data,
                                fb_node=fb_graph_node,
                                parent_id=item_id,
                            )
                            if parsed_result:
                                yield parsed_result
                        return  # Batch processing successful, exit the function.

                except HTTPError as e:
                    error_text = str(e.response.text) if hasattr(e, "response") else str(e)
                    if "Page Access Token" in error_text:
                        logging.info("Batch request requires page token, falling back to individual requests.")
                        # Let the code fall through to individual processing below.
                    else:
                        logging.error(f"Batch request failed with a non-token error: {error_text}")
                        return  # A definitive failure, stop processing.

        # If batch processing was not attempted, was skipped (insights), or failed with a token error,
        # proceed with individual requests for all accounts.
        if ids_str := row_config.query.ids:
            selected_ids = {id for id in ids_str.split(",")}
            accounts = [account for account in accounts if account.id in selected_ids]

        if self._request_require_page_token(row_config):
            logging.info("Require page token")
            is_page_token = True
            page_tokens = self._get_pages_token(accounts)
        else:
            logging.info("Don't need page token")
            is_page_token = False
            page_tokens = {account.id: self.oauth.data.get("access_token") for account in accounts}

        for page_id, token in page_tokens.items():
            page_id = str(page_id)

            try:
                # Create new client with page token
                # Use the shared client and pass token in params
                page_loader = PageLoader(self.client, row_config.type, self.api_version)
                output_parser = OutputParser(page_loader, page_id, row_config)

                # Construct Facebook Graph node path
                fb_graph_node = self._get_fb_graph_node(is_page_token, row_config)

                # Load data from Facebook API
                page_data = page_loader.load_page(row_config.query, page_id, params={"access_token": token})
                # For page queries without path, the response is the page object itself, not wrapped in "data"
                if not row_config.query.path and "data" not in page_data:
                    page_content = [page_data] if page_data and "id" in page_data else []
                else:
                    page_content = page_data.get("data", [])

            except Exception as e:
                if is_page_token and str(e).startswith("400"):
                    logging.warning(f"Page token failed with 400 error for {page_id}, falling back to user token")
                    try:
                        # Fallback to user token
                        page_loader = PageLoader(self.client, row_config.type, self.api_version)
                        output_parser = OutputParser(page_loader, page_id, row_config)
                        fb_graph_node = self._get_fb_graph_node(False, row_config)
                        page_data = page_loader.load_page(row_config.query, page_id, params=self._with_token({}))
                        # For page queries without path, the response is the page object itself, not wrapped in "data"
                        if not row_config.query.path and "data" not in page_data:
                            page_content = [page_data] if page_data and "id" in page_data else []
                        else:
                            page_content = page_data.get("data", [])
                    except Exception as user_token_error:
                        logging.error(f"User token also failed for {page_id}: {str(user_token_error)}")
                        continue

                else:
                    logging.error(f"Failed to load data for {page_id}: {str(e)}")
                    continue

            if not page_content:
                continue

            res = output_parser.parse_data(page_data, fb_graph_node, page_id)
            if res:
                yield res

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
            logging.error(f"Failed to fetch account data for {account_id}: {str(e)}")
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

    def _get_pages_token(self, accounts: list) -> dict[str, str]:
        # Return cached tokens if already fetched
        if self.page_tokens is not None:
            return self.page_tokens

        page_tokens = {}

        try:
            # Request page tokens from the API
            response = self.client.get(
                endpoint_path=f"/{self.api_version}/me/accounts",
                params=self._with_token({"fields": "id,access_token"}),
            )

            # Build a map of page_id to access_token if data is available
            page_token_map = {
                page["id"]: page["access_token"]
                for page in response.get("data", [])
                if "id" in page and "access_token" in page
            }

            # Assign the correct token to each account
            for account in accounts:
                if account.fb_page_id:
                    # Account has fb_page_id - look up page token using the Facebook Page ID
                    page_tokens[account.id] = page_token_map.get(
                        account.fb_page_id, self.oauth.data.get("access_token")
                    )
                else:
                    # Account without fb_page_id (e.g., Instagram Business Accounts)
                    # Use user token directly - no page token lookup needed
                    page_tokens[account.id] = self.oauth.data.get("access_token")

        except Exception as e:
            logging.warning(f"Unable to get page tokens: {e}")
            # Use the fallback user token for all accounts
            for account in accounts:
                page_tokens[account.id] = self.oauth.data.get("access_token")

        # Cache the tokens for future use
        self.page_tokens = page_tokens
        return page_tokens

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
