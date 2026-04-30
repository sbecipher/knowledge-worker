import json
import logging
import re
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, Optional

from google.cloud.storage import Client as GCSClient # type: ignore

logger = logging.getLogger(__name__)


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


def format_iso_date(value: Any) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")  # type: ignore[attr-defined]
    if isinstance(value, str):
        text = value.strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            return text
        if re.fullmatch(r"\d{8}", text):
            return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    raise ValueError(f"Unsupported ISO date format: {value}")


def build_object_path(
    layer: str,
    dataset: str,
    universe_key: Optional[str] = None,
    ticker: Optional[str] = None,
    start_date: Optional[str] = None,
    date: Optional[str] = None,
    suffix: Optional[str] = None,
    requested_period: Optional[str] = None,
    bar_granularity: Optional[str] = None,
    effective_end_date: Optional[str] = None,
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

    if dataset == "prices":
        if not ticker:
            raise ValueError("ticker required for prices path")
        if not suffix:
            raise ValueError("suffix required for prices path")
        if not bar_granularity:
            raise ValueError("bar_granularity required for prices path")
        if not effective_end_date:
            raise ValueError("effective_end_date required for prices path")
        safe_ticker = sanitize_path_segment(ticker.upper())
        if layer == "prod":
            file_stem = sanitize_path_segment(suffix)
            if not file_stem.startswith("part-"):
                file_stem = f"part-00000-{file_stem}"
            parts.extend(
                [
                    "prices",
                    "eod",
                    "v1",
                    f"date={format_iso_date(effective_end_date)}",
                    f"{file_stem}.snappy.parquet",
                ]
            )
            return str(PurePosixPath(*parts))
        parts.extend(
            [
                "prices",
                f"granularity={sanitize_path_segment(bar_granularity.lower())}",
                f"date={format_iso_date(effective_end_date)}",
                f"ticker={safe_ticker}",
                f"{sanitize_path_segment(suffix)}.ndjson",
            ]
        )
        return str(PurePosixPath(*parts))

    if dataset == "fundamentals":
        if not ticker:
            raise ValueError("ticker required for fundamentals path")
        if not suffix:
            raise ValueError("suffix required for fundamentals path")
        if not requested_period:
            raise ValueError("requested_period required for fundamentals path")
        if not effective_end_date:
            raise ValueError("effective_end_date required for fundamentals path")
        safe_ticker = sanitize_path_segment(ticker.upper())
        safe_frequency = sanitize_path_segment(str(requested_period).upper())
        parts.extend(
            [
                "fundamentals",
                f"frequency={safe_frequency}",
                f"date={format_iso_date(effective_end_date)}",
                f"ticker={safe_ticker}",
                f"{sanitize_path_segment(suffix)}.ndjson",
            ]
        )
        return str(PurePosixPath(*parts))

    if dataset == "metadata" and date:
        if not ticker:
            raise ValueError("ticker required for metadata path")
        if not suffix:
            raise ValueError("suffix required for metadata path")
        safe_ticker = sanitize_path_segment(ticker.upper())
        parts.extend(
            [
                "metadata",
                f"date={format_iso_date(date)}",
                f"ticker={safe_ticker}",
                f"{sanitize_path_segment(suffix)}.json",
            ]
        )
        return str(PurePosixPath(*parts))

    parts.append(dataset)
    if ticker:
        safe_ticker = sanitize_path_segment(ticker.upper())
        parts.append(safe_ticker)
    else:
        safe_ticker = ""

    if dataset == "edgar" and date:
        if not ticker:
            raise ValueError("ticker required for edgar path")
        if not suffix:
            raise ValueError("suffix required for edgar path")
        parts = [p for p in [prefix, layer, "edgar"] if p]
        parts.extend(
            [
                f"date={format_iso_date(date)}",
                f"ticker={safe_ticker}",
                f"{sanitize_path_segment(suffix)}.json",
            ]
        )
        return str(PurePosixPath(*parts))

    if dataset == "edgar":
        filename_parts = []
        if safe_ticker:
            filename_parts.append(safe_ticker)
        if suffix:
            filename_parts.append(sanitize_path_segment(suffix))
    else:
        filename_parts = []
        if ticker and dataset != "models":
            filename_parts.append(safe_ticker)
        if requested_period:
            filename_parts.append(sanitize_path_segment(requested_period.lower()))
        if start_date:
            filename_parts.append(format_date(start_date))
        if date:
            filename_parts.append(format_date(date))
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


def build_manifest_object_path(
    layer: str,
    workflow_id: str,
    prefix: str = "",
    date: Optional[str] = None,
) -> str:
    if not layer:
        raise ValueError("layer required for manifest path")
    if not workflow_id:
        raise ValueError("workflow_id required for manifest path")
    parts = [p for p in [prefix, sanitize_path_segment(layer.lower()), "manifests"] if p]
    if date:
        parts.append(f"date={format_iso_date(date)}")
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
        if object_path.endswith(".ndjson"):
            blob.content_type = "application/x-ndjson"
        elif object_path.endswith(".parquet"):
            blob.content_type = "application/octet-stream"
        else:
            blob.content_type = "application/json"
        blob.upload_from_filename(str(local_path))
        uri = f"gs://{self.bucket_name}/{object_path}"
        logger.info("Uploaded %s to %s", local_path, uri)
        return uri

    def download_json(self, object_path: str) -> Any:
        blob = self._ensure_bucket().blob(object_path)
        payload = blob.download_as_bytes()
        text = payload.decode("utf-8")
        if object_path.endswith(".ndjson"):
            return [json.loads(line) for line in text.splitlines() if line.strip()]
        return json.loads(text)


def write_json(path: Path, payload: Any) -> Path:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False)
    return path


def write_ndjson(path: Path, rows: Iterable[Dict[str, Any]]) -> Path:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False))
            fp.write("\n")
    return path
