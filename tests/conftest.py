import os
from pathlib import Path


os.environ.setdefault("INTRINIO_SECRET_MANAGER_ENABLED", "false")
os.environ.setdefault("UPLOAD_ENABLED", "false")
os.environ.setdefault("TEMP_DIR", str(Path("tmp-tests")))
