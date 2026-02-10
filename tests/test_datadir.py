import logging
import pytest
from pathlib import Path

# Suppress verbose VCR cassette replay logging
logging.getLogger("vcr").setLevel(logging.WARNING)

FUNCTIONAL_DIR = Path(__file__).parent / "functional"
COMPONENT_SCRIPT = str(Path(__file__).parent.parent / "src" / "component.py")


def get_test_cases():
    if not FUNCTIONAL_DIR.exists():
        return []
    return [d.name for d in sorted(FUNCTIONAL_DIR.iterdir())
            if d.is_dir() and not d.name.startswith("_")
            and (d / "source" / "data" / "cassettes" / "requests.json").exists()]


@pytest.mark.parametrize("test_name", get_test_cases())
def test_vcr_functional(test_name):
    from datadirtest.vcr import VCRTestDataDir

    test = VCRTestDataDir(
        data_dir=str(FUNCTIONAL_DIR / test_name),
        component_script=COMPONENT_SCRIPT,
        vcr_mode="replay",
    )
    test.setUp()
    try:
        test.compare_source_and_expected()
    finally:
        test.tearDown()
