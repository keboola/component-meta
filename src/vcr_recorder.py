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

    def _generate_cassette_name(self) -> str:
        """Generate a unique cassette filename."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        config_id = os.environ.get("KBC_CONFIGID", "unknown")
        component_id = os.environ.get("KBC_COMPONENTID", "component")
        component_name = component_id.replace(".", "-")
        return f"vcr_debug_{component_name}_{config_id}_{timestamp}.json"

    def _create_vcr(self) -> vcr.VCR:
        """Create a configured VCR instance."""
        return vcr.VCR(
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

    @contextmanager
    def record(self) -> Generator[Path, None, None]:
        """
        Context manager for VCR recording.

        Yields:
            Path to the cassette file being recorded

        Example:
            recorder = VCRDebugRecorder(access_token="...")
            with recorder.record() as cassette_path:
                # Execute component code
                pass
            # Cassette is saved to output_dir
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)

        cassette_name = self._generate_cassette_name()
        self.cassette_path = self.output_dir / cassette_name

        logger.info(f"VCR Debug Recording enabled - cassette will be saved to: {self.cassette_path}")

        my_vcr = self._create_vcr()

        with my_vcr.use_cassette(cassette_name):
            yield self.cassette_path

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
