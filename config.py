import os
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

try:
    from google.cloud import secretmanager
except ImportError:  # pragma: no cover - optional during tests/local planning
    secretmanager = None

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "y", "on"}


def _env_optional_bool(name: str) -> Optional[bool]:
    value = os.getenv(name)
    if value is None:
        return None
    return value.lower() in {"1", "true", "yes", "y", "on"}


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None else default


def _load_gcs_service_account_json() -> str:
    json_value = _env_str("GCS_SERVICE_ACCOUNT_KEY_JSON", "")
    if json_value:
        return json_value
    path_value = _env_str("GCS_SERVICE_ACCOUNT_KEY_PATH", "")
    if not path_value:
        return ""
    try:
        return Path(path_value).read_text(encoding="utf-8").strip()
    except FileNotFoundError as exc:
        raise ValueError(f"GCS service account key path not found: {path_value}") from exc


def _load_intrinio_api_key() -> str:
    env_key = _env_str("INTRINIO_API_KEY", "")
    if env_key:
        return env_key
    if not _env_bool("INTRINIO_SECRET_MANAGER_ENABLED", True):
        return ""
    if secretmanager is None:
        logger.warning("google-cloud-secret-manager is unavailable; Intrinio API key lookup skipped")
        return ""
    secret_name = "projects/875978034496/secrets/marketio-data-api-intrinio/versions/latest"
    try:
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(request={"name": secret_name})
        return response.payload.data.decode("utf-8").strip()
    except Exception as exc:  # noqa: BLE001
        # Do not fail worker startup for pipelines that don't use Intrinio endpoints.
        logger.warning("Intrinio API key unavailable from Secret Manager: %s", exc)
        return ""


def _infer_marketio_auth(api_url: str) -> bool:
    parsed = urlparse(api_url)
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme != "https":
        return False
    if hostname in {"localhost", "127.0.0.1"}:
        return False
    return True


@dataclass
class Settings:
    """
    Lightweight settings loader for Marketio Temporal pipeline.
    """

    marketio_api_url: str
    marketio_require_auth: bool
    gcs_bucket: Optional[str]
    gcs_prefix: str
    instrument: str
    model_version: str
    temp_dir: str
    upload_enabled: bool
    gcs_service_account_key_json: Optional[str]
    http_timeout: float
    http_stream_timeout: float
    temporal_task_queue: str
    temporal_address: str
    intrinio_api_key: str
    cleanup_local_artifacts: bool
    activity_executor_threads: int
    max_concurrent_activities: int
    max_concurrent_workflow_tasks: int
    max_cached_workflows: int


def load_settings() -> Settings:
    marketio_api_url = _env_str("MARKETIO_API_URL", "https://marketio-875978034496.us-central1.run.app:8000").rstrip("/")
    marketio_require_auth = _env_optional_bool("MARKETIO_REQUIRE_AUTH")
    if marketio_require_auth is None:
        marketio_require_auth = _infer_marketio_auth(marketio_api_url)
    gcs_bucket = _env_str("GCS_BUCKET", "sbecipher-intelligence")
    gcs_prefix = _env_str("GCS_PREFIX", "").strip("/")
    instrument = _env_str("INSTRUMENT", "mm-h5r1").lower()
    model_version = _env_str("MODEL_VERSION", "metadata")
    temp_dir = _env_str("TEMP_DIR", "tmp")
    upload_enabled = _env_bool("UPLOAD_ENABLED", False)
    gcs_service_account_key_json = _load_gcs_service_account_json()
    http_timeout = float(_env_str("HTTP_CLIENT_TIMEOUT", "60"))
    http_stream_timeout = float(_env_str("STREAM_CLIENT_TIMEOUT", "600"))
    temporal_task_queue = _env_str("TEMPORAL_TASK_QUEUE", "marketio-task-queue")
    temporal_address = _env_str("TEMPORAL_ADDRESS", "172.0.0.4:7233")
    intrinio_api_key = _load_intrinio_api_key()
    cleanup_local_artifacts = _env_bool("CLEANUP_LOCAL_ARTIFACTS", True)
    activity_executor_threads = int(_env_str("ACTIVITY_EXECUTOR_THREADS", "16"))
    max_concurrent_activities = int(_env_str("MAX_CONCURRENT_ACTIVITIES", str(activity_executor_threads)))
    max_concurrent_workflow_tasks = int(_env_str("MAX_CONCURRENT_WORKFLOW_TASKS", "100"))
    max_cached_workflows = int(_env_str("MAX_CACHED_WORKFLOWS", "1000"))

    return Settings(
        marketio_api_url=marketio_api_url,
        marketio_require_auth=marketio_require_auth,
        gcs_bucket=gcs_bucket,
        gcs_prefix=gcs_prefix,
        instrument=instrument,
        model_version=model_version,
        temp_dir=temp_dir,
        upload_enabled=upload_enabled,
        gcs_service_account_key_json=gcs_service_account_key_json,
        http_timeout=http_timeout,
        http_stream_timeout=http_stream_timeout,
        temporal_task_queue=temporal_task_queue,
        temporal_address=temporal_address,
        intrinio_api_key=intrinio_api_key,
        cleanup_local_artifacts=cleanup_local_artifacts,
        activity_executor_threads=activity_executor_threads,
        max_concurrent_activities=max_concurrent_activities,
        max_concurrent_workflow_tasks=max_concurrent_workflow_tasks,
        max_cached_workflows=max_cached_workflows,
    )
