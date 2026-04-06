import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("INTRINIO_SECRET_MANAGER_ENABLED", "false")
os.environ.setdefault("UPLOAD_ENABLED", "false")
os.environ.setdefault("TEMP_DIR", str(Path("tmp-tests")))
