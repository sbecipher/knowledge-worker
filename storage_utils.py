import json
import logging
import re
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Optional

from google.cloud.storage import Client as GCSClient # type: ignore

logger = logging.getLogger(__name__)

INTRADAY_DIR_MAP = {
    "daily": "intraday",
    "eod": "intraday",
    "weekly": "week",
    "wk": "week",
    "monthly": "month",
    "mth": "month",
    "quarterly": "quarter",
    "quarter": "quarter",
    "qtr": "quarter",
}

INTRADAY_FREQ_SLUGS = {
    "daily": "eod",
    "eod": "eod",
    "weekly": "wk",
    "wk": "wk",
    "monthly": "mth",
    "mth": "mth",
    "quarterly": "qtr",
    "quarter": "qtr",
    "qtr": "qtr",
}


_SAFE_SEGMENT_PATTERN = re.compile(r"[^A-Za-z0-9._-]")


def sanitize_path_segment(value: str, fallback: str = "unknown") -> str:
    """
    Restrict user/API-provided values to a safe path-segment character set.

    Why:
    - Ticker/universe/frequency values can originate from external systems.
    - Allowing path separators and special characters can corrupt object layout.
    - Normalizing here makes path behavior deterministic across all callers.
    """
    cleaned = _SAFE_SEGMENT_PATTERN.sub("_", value).strip("._-")
    return cleaned or fallback


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def format_date(value: Any) -> str:
    """
    Normalize date-like values to YYYYMMDD strings for deterministic paths.
    """
    if hasattr(value, "strftime"):
        return value.strftime("%Y%m%d")  # type: ignore[attr-defined]
    if isinstance(value, str):
        # Accept YYYY-MM-DD or YYYYMMDD
        parts = value.replace("-", "")
        if len(parts) == 8:
            return parts
    raise ValueError(f"Unsupported date format: {value}")


def build_object_path(
    layer: str,
    dataset: str,
    universe_key: Optional[str] = None,
    ticker: Optional[str] = None,
    freq: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    suffix: Optional[str] = None,
    prefix: str = "",
) -> str:
    """
    Build a hierarchical GCS path using normalized components.
    """
    parts = [p for p in [prefix, layer] if p]

    if dataset == "models":
        if not universe_key:
            raise ValueError("universe_key required for models path")
        if not suffix:
            raise ValueError("suffix required for models metadata path")
        parts.append(dataset)
        parts.append(sanitize_path_segment(universe_key.lower()))
        parts.append("metadata")
        parts.append(f"{sanitize_path_segment(suffix)}.json")
        return str(PurePosixPath(*parts))

    freq_normalized = freq.lower() if isinstance(freq, str) else None
    if dataset == "intraday" and freq_normalized:
        dataset_dir = INTRADAY_DIR_MAP.get(freq_normalized, "intraday")
        raw_freq = INTRADAY_FREQ_SLUGS.get(freq_normalized, freq_normalized)
        filename_freq_slug = sanitize_path_segment(raw_freq)
    else:
        dataset_dir = dataset
        filename_freq_slug = sanitize_path_segment(freq_normalized) if freq_normalized else ""

    parts.append(dataset_dir)
    if ticker:
        safe_ticker = sanitize_path_segment(ticker.upper())
        parts.append(safe_ticker)
    else:
        safe_ticker = ""

    if dataset == "edgar":
        filename_parts = []
        if safe_ticker:
            filename_parts.append(safe_ticker)
        if suffix:
            filename_parts.append(sanitize_path_segment(suffix))
    elif dataset == "fundamentals":
        filename_parts = []
        if safe_ticker:
            filename_parts.append(safe_ticker)
        filename_parts.append("fundamentals")
        if start_date:
            filename_parts.append(format_date(start_date))
        if end_date:
            filename_parts.append(format_date(end_date))
        if suffix:
            filename_parts.append(sanitize_path_segment(suffix))
    else:
        filename_parts = []
        if ticker and dataset != "models":
            filename_parts.append(safe_ticker)
        if filename_freq_slug:
            filename_parts.append(filename_freq_slug)
        if start_date:
            filename_parts.append(format_date(start_date))
        if end_date:
            filename_parts.append(format_date(end_date))
        if suffix:
            filename_parts.append(sanitize_path_segment(suffix))
    if not filename_parts:
        # Avoid ambiguous ".json" object names when metadata is incomplete.
        filename_parts.append("artifact")
    filename = "_".join(filename_parts) + ".json"
    parts.append(filename)
    return str(PurePosixPath(*parts))


def build_active_universe_object_path(universe_key: str, prefix: str = "") -> str:
    if not universe_key:
        raise ValueError("universe_key required for active universe path")
    parts = [p for p in [prefix, "prod", "models", sanitize_path_segment(universe_key.lower())] if p]
    parts.append("active.json")
    return str(PurePosixPath(*parts))


def build_metadata_manifest_object_path(workflow_id: str, prefix: str = "") -> str:
    if not workflow_id:
        raise ValueError("workflow_id required for metadata manifest path")
    parts = [p for p in [prefix, "source", "metadata", "manifests"] if p]
    parts.append(f"{sanitize_path_segment(workflow_id)}.json")
    return str(PurePosixPath(*parts))


class GCSUploader:
    """
    Thin wrapper around google-cloud-storage with optional dry-run.
    """

    def __init__(
        self,
        bucket: Optional[str],
        service_account_key_json: Optional[str],
        enabled: bool = True,
    ):
        self.bucket_name = bucket
        self.enabled = enabled and bool(bucket)
        self._service_account_key_json = service_account_key_json
        self._client = None
        self._bucket = None

    def _ensure_bucket(self):
        if self._bucket is not None:
            return self._bucket
        if not self.bucket_name:
            raise RuntimeError("GCS bucket is not configured")
        if GCSClient is None:
            raise RuntimeError("google-cloud-storage is required for GCS access")
        if self._service_account_key_json:
            try:
                key_info = json.loads(self._service_account_key_json)
                self._client = GCSClient.from_service_account_info(key_info)
            except (json.JSONDecodeError, TypeError) as e:
                raise ValueError("Failed to parse GCS service account JSON") from e
        else:
            self._client = GCSClient()
        self._bucket = self._client.bucket(self.bucket_name)
        return self._bucket

    def upload_file(self, local_path: Path, object_path: str, metadata: Optional[Dict[str, str]] = None) -> str:
        """
        Upload a JSON file to GCS, returning the gs:// URI. If disabled, returns a file URI.
        """
        if not self.enabled:
            logger.info("Uploads disabled; skipping upload for %s", local_path)
            return f"file://{local_path}"

        blob = self._ensure_bucket().blob(object_path)
        if metadata:
            blob.metadata = metadata
        blob.content_type = "application/json"
        blob.upload_from_filename(str(local_path))
        uri = f"gs://{self.bucket_name}/{object_path}"
        logger.info("Uploaded %s to %s", local_path, uri)
        return uri

    def download_json(self, object_path: str) -> Any:
        blob = self._ensure_bucket().blob(object_path)
        payload = blob.download_as_bytes()
        return json.loads(payload.decode("utf-8"))


def write_json(path: Path, payload: Any) -> Path:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False)
    return path
