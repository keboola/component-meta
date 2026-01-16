import sys
import os
from pathlib import Path

# Add src and scripts directories to python path for tests
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root / "src"))
sys.path.append(str(project_root / "scripts"))
