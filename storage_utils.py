import json
import logging
import os
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
    instrument: str,
    dataset: str,
    ticker: Optional[str] = None,
    freq: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    model_version: Optional[str] = None,
    suffix: Optional[str] = None,
    prefix: str = "",
) -> str:
    """
    Build a hierarchical GCS path using normalized components.
    """
    parts = [p for p in [prefix, layer] if p]

    if dataset == "models":
        if not model_version:
            raise ValueError("model_version required for models path")
        if not instrument:
            raise ValueError("instrument required for models path")
        parts.append(dataset)
        parts.append(instrument.lower())
        filename = f"{model_version}.json"
        parts.append(filename)
        return str(PurePosixPath(*parts))

    freq_normalized = freq.lower() if isinstance(freq, str) else None
    if dataset == "intraday" and freq_normalized:
        dataset_dir = INTRADAY_DIR_MAP.get(freq_normalized, "intraday")
        filename_freq_slug = INTRADAY_FREQ_SLUGS.get(freq_normalized, freq_normalized)
    else:
        dataset_dir = dataset
        filename_freq_slug = freq_normalized if freq_normalized else ""

    parts.append(dataset_dir)
    if ticker:
        parts.append(ticker.upper())
    filename_parts = []
    if ticker and dataset != "models":
        filename_parts.append(ticker.upper())
    if filename_freq_slug:
        filename_parts.append(filename_freq_slug)
    if start_date:
        filename_parts.append(format_date(start_date))
    if end_date:
        filename_parts.append(format_date(end_date))
    filename = "_".join(filename_parts) + ".json"
    parts.append(filename)
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
        self._client = None
        if self.enabled:
            if GCSClient is None:
                raise RuntimeError("google-cloud-storage is required for uploads")
            if service_account_key_json:
                try:
                    key_info = json.loads(service_account_key_json)
                    self._client = GCSClient.from_service_account_info(key_info)
                except (json.JSONDecodeError, TypeError) as e:
                    raise ValueError("Failed to parse GCS service account JSON") from e
            else:
                self._client = GCSClient()
            self._bucket = self._client.bucket(bucket)
        else:
            self._bucket = None

    def upload_file(self, local_path: Path, object_path: str, metadata: Optional[Dict[str, str]] = None) -> str:
        """
        Upload a JSON file to GCS, returning the gs:// URI. If disabled, returns a file URI.
        """
        if not self.enabled or not self._bucket:
            logger.info("Uploads disabled; skipping upload for %s", local_path)
            return f"file://{local_path}"

        blob = self._bucket.blob(object_path)
        if metadata:
            blob.metadata = metadata
        blob.content_type = "application/json"
        blob.upload_from_filename(str(local_path))
        uri = f"gs://{self.bucket_name}/{object_path}"
        logger.info("Uploaded %s to %s", local_path, uri)
        return uri


def write_json(path: Path, payload: Any) -> Path:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False)
    return path
