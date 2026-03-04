"""Functional tests for component using VCR cassettes."""

from pathlib import Path

import pytest
from keboola.datadirtest.vcr import VCRDataDirTester, get_test_cases
from keboola.vcr import DefaultSanitizer, ResponseUrlSanitizer

FUNCTIONAL_DIR = str(Path(__file__).parent / "functional")
COMPONENT_SCRIPT = str(Path(__file__).parent.parent / "src" / "component.py")

VCR_SANITIZERS = [
    DefaultSanitizer(additional_sensitive_fields=["page_token"]),
    ResponseUrlSanitizer(
        dynamic_params=["_nc_gid", "_nc_tpa", "_nc_oc", "_nc_ohc", "oh", "oe"],
        url_domains=["fbcdn.net", "facebook.com", "cdninstagram.com"],
    ),
]


@pytest.mark.parametrize("test_name", get_test_cases(FUNCTIONAL_DIR))
def test_functional(test_name):
    """Run a single VCR functional test case."""
    tester = VCRDataDirTester(
        data_dir=FUNCTIONAL_DIR,
        component_script=COMPONENT_SCRIPT,
        vcr_sanitizers=VCR_SANITIZERS,
        selected_tests=[test_name],
    )
    tester.run()
