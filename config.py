import os
from dataclasses import dataclass
from datetime import date
from typing import Optional


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "y", "on"}


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None else default


@dataclass
class Settings:
    """
    Lightweight settings loader for Marketio Temporal pipeline.
    """

    marketio_api_url: str
    gcs_bucket: Optional[str]
    gcs_prefix: str
    instrument: str
    model_version: str
    temp_dir: str
    upload_enabled: bool
    gcs_service_account_key_path: Optional[str]
    http_timeout: float
    http_stream_timeout: float
    temporal_task_queue: str
    temporal_address: str
    run_id: str


def load_settings() -> Settings:
    marketio_api_url = _env_str("MARKETIO_API_URL", "http://localhost:8000").rstrip("/")
    gcs_bucket = os.getenv("GCS_BUCKET")
    gcs_prefix = _env_str("GCS_PREFIX", "").strip("/")
    instrument = _env_str("INSTRUMENT", "ssga-xme").lower()
    model_version = _env_str("MODEL_VERSION", "1125v")
    temp_dir = _env_str("TEMP_DIR", "tmp")
    upload_enabled = _env_bool("UPLOAD_ENABLED", True)
    gcs_service_account_key_path = os.getenv("GCS_SERVICE_ACCOUNT_KEY_PATH")
    http_timeout = float(_env_str("HTTP_CLIENT_TIMEOUT", "60"))
    http_stream_timeout = float(_env_str("STREAM_CLIENT_TIMEOUT", "600"))
    temporal_task_queue = _env_str("TEMPORAL_TASK_QUEUE", "market-data-task-queue")
    temporal_address = _env_str("TEMPORAL_ADDRESS", "localhost:7233")
    run_id = _env_str("RUN_ID", date.today().isoformat())

    return Settings(
        marketio_api_url=marketio_api_url,
        gcs_bucket=gcs_bucket,
        gcs_prefix=gcs_prefix,
        instrument=instrument,
        model_version=model_version,
        temp_dir=temp_dir,
        upload_enabled=upload_enabled,
        gcs_service_account_key_path=gcs_service_account_key_path,
        http_timeout=http_timeout,
        http_stream_timeout=http_stream_timeout,
        temporal_task_queue=temporal_task_queue,
        temporal_address=temporal_address,
        run_id=run_id,
    )
