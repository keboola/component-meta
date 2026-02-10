import sys
from pathlib import Path

# Add src and tests directories to python path for tests
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root / "tests"))
