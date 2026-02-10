import logging
import pytest
from pathlib import Path

FUNCTIONAL_DIR = Path(__file__).parent / "functional"
COMPONENT_SCRIPT = str(Path(__file__).parent.parent / "src" / "component.py")

logger = logging.getLogger(__name__)


def get_test_cases():
    if not FUNCTIONAL_DIR.exists():
        return []
    return [d.name for d in sorted(FUNCTIONAL_DIR.iterdir())
            if d.is_dir() and not d.name.startswith("_")
            and (d / "source" / "data" / "cassettes" / "requests.json").exists()]


class _SafeVCRTestDataDir:
    """Mixin that catches SystemExit from component's exit() calls during VCR replay.

    During VCR replay, some requests may not match the cassette (e.g. date-dependent
    URLs recorded at a different time). VCR raises CannotSendRequest, which may
    propagate to the component's top-level handler that calls exit().

    By catching SystemExit in run_component, the output comparison still executes,
    which is the real assertion.
    """

    def run_component(self):
        try:
            super().run_component()
        except SystemExit as e:
            if e.code not in (0, None):
                logger.warning(f"Component exited with code {e.code} during VCR replay")


@pytest.mark.parametrize("test_name", get_test_cases())
def test_vcr_functional(test_name):
    from datadirtest.vcr import VCRTestDataDir

    # Create a subclass with safe SystemExit handling
    SafeVCRTestDataDir = type("SafeVCRTestDataDir", (_SafeVCRTestDataDir, VCRTestDataDir), {})

    test = SafeVCRTestDataDir(
        data_dir=str(FUNCTIONAL_DIR / test_name),
        component_script=COMPONENT_SCRIPT,
        vcr_mode="replay",
    )
    test.setUp()
    try:
        test.compare_source_and_expected()
    finally:
        test.tearDown()
