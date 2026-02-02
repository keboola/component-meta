"""VCR recording utilities for debug mode execution.

This module provides VCR recording capabilities when the component runs in debug mode
(KBC_COMPONENT_RUN_MODE=debug). It records all HTTP interactions and saves them as
cassettes to the output files directory for debugging purposes.
"""

import json
import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Generator

import vcr

logger = logging.getLogger(__name__)

# Suppress VCR and related library logging to prevent log pollution
# VCR logs warnings for every request that doesn't match a cassette entry
logging.getLogger("vcr").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("requests").setLevel(logging.CRITICAL)


def is_debug_mode() -> bool:
    """Check if the component is running in debug mode."""
    return os.environ.get("KBC_COMPONENT_RUN_MODE", "").lower() == "debug"


def get_output_files_dir() -> Path:
    """Get the output files directory path."""
    data_dir = os.environ.get("KBC_DATADIR", "/data")
    return Path(data_dir) / "out" / "files"


def scrub_string(string: str, replacements: dict[str, str]) -> str:
    """Apply string replacements for sanitization."""
    if not string:
        return string
    for target, replacement in replacements.items():
        if target and target in string:
            string = string.replace(target, replacement)
    return string


def sanitize_url(url: str) -> str:
    """
    Sanitize URLs by removing dynamic Facebook/Meta parameters.

    Facebook CDN URLs contain session-specific parameters that change
    between requests but don't affect the actual resource.
    """
    if not url or not isinstance(url, str):
        return url

    if not any(domain in url for domain in ["fbcdn.net", "facebook.com"]):
        return url

    dynamic_params = ["_nc_gid", "_nc_tpa", "_nc_oc", "oh", "oe"]

    for param in dynamic_params:
        url = re.sub(f"[&?]{param}=[^&]*", "", url)

    url = re.sub(r"[?&]+$", "", url)
    url = re.sub(r"&{2,}", "&", url)
    url = re.sub(r"\?&", "?", url)

    return url


def scrub_headers(headers: dict, is_response: bool = False) -> dict:
    """Remove sensitive and dynamic headers from requests/responses."""
    whitelist = ["content-type", "content-length", "facebook-api-version"]
    new_headers = {}

    try:
        source = headers.items() if hasattr(headers, "items") else headers
        for k, v in source:
            if k.lower() in whitelist:
                new_headers[k] = v if isinstance(v, list) else [v]
    except Exception:
        return headers

    if is_response and "Content-Length" in new_headers:
        del new_headers["Content-Length"]

    return new_headers


def recursive_scrub(obj: Any, replacements: dict[str, str]) -> Any:
    """Recursively scrub sensitive data from objects."""
    if isinstance(obj, dict):
        new_obj = {}
        for k, v in obj.items():
            if k in ["access_token", "token"]:
                new_obj[k] = "token"
            else:
                new_obj[k] = recursive_scrub(v, replacements)
        return new_obj
    elif isinstance(obj, list):
        return [recursive_scrub(i, replacements) for i in obj]
    elif isinstance(obj, str):
        scrubbed = scrub_string(obj, replacements)
        scrubbed = sanitize_url(scrubbed)
        return scrubbed
    return obj


def before_record_response(response: dict, replacements: dict[str, str]) -> dict:
    """Sanitize response before recording."""
    response["headers"] = scrub_headers(response.get("headers", {}), is_response=True)

    if "body" in response and "string" in response["body"]:
        try:
            body_bytes = response["body"]["string"]
            if not body_bytes:
                return response

            body_str = body_bytes.decode("utf-8")

            try:
                body_json = json.loads(body_str)
                scrubbed_json = recursive_scrub(body_json, replacements)
                response["body"]["string"] = json.dumps(scrubbed_json, sort_keys=True).encode("utf-8")
            except json.JSONDecodeError:
                scrubbed_str = scrub_string(body_str, replacements)
                response["body"]["string"] = scrubbed_str.encode("utf-8")

        except Exception as e:
            logger.warning(f"Failed to scrub response body: {e}")

    return response


def before_record_request(request: Any, replacements: dict[str, str]) -> Any:
    """Sanitize request before recording."""
    request.uri = scrub_string(request.uri, replacements)
    request.headers = scrub_headers(request.headers, is_response=False)

    if request.body:
        try:
            body_str = request.body.decode("utf-8")
            try:
                body_json = json.loads(body_str)
                scrubbed_json = recursive_scrub(body_json, replacements)
                request.body = json.dumps(scrubbed_json, sort_keys=True).encode("utf-8")
            except json.JSONDecodeError:
                request.body = scrub_string(body_str, replacements).encode("utf-8")
        except Exception:
            pass

    return request


class IncrementalJSONPersister:
    """
    Custom VCR persister that writes interactions incrementally to disk.

    Instead of buffering all interactions in memory and writing at the end,
    this persister appends each interaction to the JSON file as it's recorded.
    This prevents OOM errors on large runs with many HTTP requests.
    """

    def __init__(self, cassette_library_dir: str):
        self.cassette_library_dir = cassette_library_dir
        self._file_handle = None
        self._is_first_interaction = True
        self._written_count = 0  # Track how many interactions we've written

    def _serialize_body(self, body: Any) -> Any:
        """Convert bytes in body to JSON-serializable format."""
        if isinstance(body, bytes):
            try:
                return body.decode("utf-8")
            except UnicodeDecodeError:
                # If it's binary data, encode as base64
                import base64

                return {"encoding": "base64", "string": base64.b64encode(body).decode("ascii")}
        elif isinstance(body, dict):
            # Recursively handle nested structures
            return {k: self._serialize_body(v) for k, v in body.items()}
        elif isinstance(body, list):
            return [self._serialize_body(item) for item in body]
        return body

    def _serialize_request(self, request: Any) -> dict:
        """Convert VCR Request object to dictionary."""
        if isinstance(request, dict):
            return {k: self._serialize_body(v) for k, v in request.items()}

        return {
            "uri": request.uri,
            "method": request.method,
            "body": self._serialize_body(request.body),
            "headers": dict(request.headers) if hasattr(request.headers, "items") else request.headers,
        }

    def _serialize_response(self, response: Any) -> dict:
        """Convert response to JSON-serializable format, handling bytes."""
        if not isinstance(response, dict):
            response = dict(response)

        # Recursively serialize all values to handle bytes
        return {k: self._serialize_body(v) for k, v in response.items()}

    def save_cassette(self, cassette_path: str, cassette_dict: dict, serializer: Any = None) -> None:
        """
        Save cassette data incrementally.

        VCR calls this method with ALL interactions accumulated so far,
        so we track how many we've already written and only write new ones.

        Args:
            cassette_path: Path to the cassette file
            cassette_dict: Dictionary containing 'requests' and 'responses'
            serializer: VCR serializer (not used, we handle serialization directly)
        """
        cassette_file = Path(cassette_path)

        # Initialize file on first call
        if self._file_handle is None:
            cassette_file.parent.mkdir(parents=True, exist_ok=True)
            self._file_handle = open(cassette_file, "w", encoding="utf-8")
            # Write JSON header
            self._file_handle.write('{"version": 1, "interactions": [\n')
            self._file_handle.flush()
            self._is_first_interaction = True
            self._written_count = 0

        # VCR passes data as {'requests': [...], 'responses': [...]}
        # Requests are VCR Request objects, responses are already dicts
        requests = cassette_dict.get("requests", [])
        responses = cassette_dict.get("responses", [])

        # Serialize and combine requests and responses into interactions
        interactions = []
        for req, resp in zip(requests, responses):
            # Convert Request and Response objects to JSON-serializable dicts
            req_dict = self._serialize_request(req)
            resp_dict = self._serialize_response(resp)

            interactions.append({"request": req_dict, "response": resp_dict})

        # Write only new interactions (those we haven't written yet)
        new_interactions = interactions[self._written_count:]

        for interaction in new_interactions:
            # Add comma before all but the first interaction
            if not self._is_first_interaction:
                self._file_handle.write(",\n")
            else:
                self._is_first_interaction = False

            # Write the interaction as JSON
            json.dump(interaction, self._file_handle, indent=2)
            self._file_handle.flush()  # Ensure it's written to disk immediately
            self._written_count += 1

    def load_cassette(self, cassette_path: str, serializer: Any = None) -> tuple[list, list]:
        """
        Load existing cassette file.

        VCR calls this method to check for existing cassettes.
        For incremental writing, we return empty lists since we're always recording new.
        """
        cassette_file = Path(cassette_path)

        # If file doesn't exist, return empty
        if not cassette_file.exists():
            return [], []

        # If file exists and we're in incremental mode, we should read it
        # to get existing interactions (in case of append mode)
        try:
            with open(cassette_file, encoding="utf-8") as f:
                data = json.load(f)
                interactions = data.get("interactions", [])

                # VCR expects tuple of (requests, responses) but we return interactions
                # The serializer will handle converting them
                return interactions, []
        except (json.JSONDecodeError, FileNotFoundError):
            return [], []

    def close(self, cassette_path: str) -> None:
        """Close the cassette file properly."""
        if self._file_handle is not None:
            # Close the JSON array and object
            self._file_handle.write("\n]}")
            self._file_handle.close()
            self._file_handle = None
            self._is_first_interaction = True
            self._written_count = 0


class VCRDebugRecorder:
    """VCR recorder for debug mode execution."""

    def __init__(self, access_token: str | None = None):
        """
        Initialize the VCR debug recorder.

        Args:
            access_token: The OAuth access token to sanitize from recordings
        """
        self.access_token = access_token or "token"
        self.replacements = {self.access_token: "token"} if access_token else {}
        self.cassette_path: Path | None = None
        self.output_dir = get_output_files_dir()
        self.persister: IncrementalJSONPersister | None = None

    def _generate_cassette_name(self) -> str:
        """Generate a unique cassette filename."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        config_id = os.environ.get("KBC_CONFIGID", "unknown")
        component_id = os.environ.get("KBC_COMPONENTID", "component")
        component_name = component_id.replace(".", "-")
        return f"vcr_debug_{component_name}_{config_id}_{timestamp}.json"

    def _create_vcr(self) -> vcr.VCR:
        """Create a configured VCR instance with incremental persister."""
        self.persister = IncrementalJSONPersister(str(self.output_dir))

        # Create a custom VCR config
        vcr_config = vcr.VCR(
            cassette_library_dir=str(self.output_dir),
            record_mode="new_episodes",
            match_on=["method", "scheme", "host", "port", "path", "query", "body"],
            filter_headers=[("Authorization", "Bearer token")],
            filter_query_parameters=[("access_token", "token")],
            before_record_response=lambda r: before_record_response(r, self.replacements),
            before_record_request=lambda r: before_record_request(r, self.replacements),
            decode_compressed_response=True,
            serializer="json",
        )

        # Override the persister
        vcr_config.persister = self.persister
        return vcr_config

    @contextmanager
    def record(self) -> Generator[Path, None, None]:
        """
        Context manager for VCR recording with incremental disk writes.

        Yields:
            Path to the cassette file being recorded

        Example:
            recorder = VCRDebugRecorder(access_token="...")
            with recorder.record() as cassette_path:
                # Execute component code
                pass
            # Cassette is saved incrementally to output_dir
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)

        cassette_name = self._generate_cassette_name()
        self.cassette_path = self.output_dir / cassette_name

        logger.info(f"VCR Debug Recording enabled - cassette will be saved incrementally to: {self.cassette_path}")

        my_vcr = self._create_vcr()

        try:
            with my_vcr.use_cassette(cassette_name):
                yield self.cassette_path
        finally:
            # Ensure the persister properly closes the JSON file
            if self.persister:
                self.persister.close(str(self.cassette_path))

        logger.info(f"VCR Debug Recording complete - cassette saved to: {self.cassette_path}")


@contextmanager
def vcr_debug_context(access_token: str | None = None) -> Generator[Path | None, None, None]:
    """
    Context manager that enables VCR recording only in debug mode.

    If not in debug mode, yields None and does nothing.

    Args:
        access_token: The OAuth access token to sanitize from recordings

    Yields:
        Path to cassette file if in debug mode, None otherwise

    Example:
        with vcr_debug_context(access_token="...") as cassette_path:
            if cassette_path:
                print(f"Recording to {cassette_path}")
            # Execute component code
    """
    if not is_debug_mode():
        logger.debug("Not in debug mode - VCR recording disabled")
        yield None
        return

    recorder = VCRDebugRecorder(access_token=access_token)
    with recorder.record() as cassette_path:
        yield cassette_path
