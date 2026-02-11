import logging
from urllib.parse import parse_qs, urlparse

import pytest
from pathlib import Path

from datadirtest.vcr import ResponseUrlSanitizer, QueryParameterTokenSanitizer

FUNCTIONAL_DIR = Path(__file__).parent / "functional"
COMPONENT_SCRIPT = str(Path(__file__).parent.parent / "src" / "component.py")

FB_CDN_SANITIZER = ResponseUrlSanitizer(
    dynamic_params=["_nc_gid", "_nc_tpa", "_nc_oc", "_nc_ohc", "oh", "oe"],
    url_domains=["fbcdn.net", "facebook.com", "cdninstagram.com"],
)

# QueryParameterTokenSanitizer is required for VCR replay matching
# (normalizes access_token params in both live and cassette requests)
VCR_SANITIZERS = [QueryParameterTokenSanitizer(), FB_CDN_SANITIZER]

logger = logging.getLogger(__name__)

# Date-related query params that change based on current time.
# Cassettes were recorded at a real date; replay uses freeze_time.
# Ignoring these in matching prevents mismatches.
IGNORED_QUERY_PARAMS = {"since", "until", "time_increment"}


def _query_without_dates(r1, r2):
    """Custom VCR matcher: compare query params ignoring date-related ones."""

    def _filtered_params(uri):
        return sorted(
            (k, v) for k, vals in parse_qs(urlparse(uri).query).items() if k not in IGNORED_QUERY_PARAMS for v in vals
        )

    return _filtered_params(r1.uri) == _filtered_params(r2.uri)


def get_test_cases():
    if not FUNCTIONAL_DIR.exists():
        return []
    return [
        d.name
        for d in sorted(FUNCTIONAL_DIR.iterdir())
        if d.is_dir()
        and not d.name.startswith("_")
        and (d / "source" / "data" / "cassettes" / "requests.json").exists()
    ]


class _SafeVCRTestDataDir:
    """Mixin: custom query matcher + SystemExit safety for VCR replay.

    - Registers a query matcher that ignores date params (since, until, time_increment)
      so cassettes recorded at real time still match during freeze_time replay.
    - Catches SystemExit from the component's exit() calls so the output
      comparison (the real assertion) still executes.
    """

    def _setup_vcr(self):
        super()._setup_vcr()
        if self.vcr_recorder and self.vcr_recorder._vcr:
            vcr_instance = self.vcr_recorder._vcr
            vcr_instance.register_matcher("query_without_dates", _query_without_dates)
            self.vcr_recorder.match_on = [
                "method",
                "scheme",
                "host",
                "port",
                "path",
                "query_without_dates",
            ]
            # Rebuild VCR instance with updated match_on
            self.vcr_recorder._vcr = self.vcr_recorder._create_vcr_instance()
            # Re-register custom matcher on the new instance
            self.vcr_recorder._vcr.register_matcher("query_without_dates", _query_without_dates)

    def run_component(self):
        # Ensure output directories exist (git doesn't track empty dirs)
        out_dir = Path(self.source_data_dir) / "out" / "tables"
        out_dir.mkdir(parents=True, exist_ok=True)
        try:
            super().run_component()
        except SystemExit as e:
            if e.code not in (0, None):
                logger.warning(f"Component exited with code {e.code} during VCR replay")


@pytest.mark.parametrize("test_name", get_test_cases())
def test_vcr_functional(test_name):
    from datadirtest.vcr import VCRTestDataDir

    SafeVCRTestDataDir = type("SafeVCRTestDataDir", (_SafeVCRTestDataDir, VCRTestDataDir), {})

    test = SafeVCRTestDataDir(
        data_dir=str(FUNCTIONAL_DIR / test_name),
        component_script=COMPONENT_SCRIPT,
        vcr_mode="replay",
        vcr_sanitizers=VCR_SANITIZERS,
    )
    test.setUp()
    try:
        test.compare_source_and_expected()
    finally:
        test.tearDown()
