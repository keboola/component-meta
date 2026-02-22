from pathlib import Path

import pytest
from datadirtest.vcr import get_test_cases
from keboola.vcr import DefaultSanitizer, ResponseUrlSanitizer

FUNCTIONAL_DIR = Path(__file__).parent / "functional"
COMPONENT_SCRIPT = str(Path(__file__).parent.parent / "src" / "component.py")

VCR_SANITIZERS = [
    DefaultSanitizer(additional_sensitive_fields=["page_token"]),
    ResponseUrlSanitizer(
        dynamic_params=["_nc_gid", "_nc_tpa", "_nc_oc", "_nc_ohc", "oh", "oe"],
        url_domains=["fbcdn.net", "facebook.com", "cdninstagram.com"],
    ),
]


@pytest.mark.parametrize("test_name", get_test_cases(str(FUNCTIONAL_DIR)))
def test_functional(test_name):
    from datadirtest.vcr import VCRTestDataDir

    test = VCRTestDataDir(
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
