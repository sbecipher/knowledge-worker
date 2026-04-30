import base64
from datetime import date, datetime, timedelta, timezone
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd
import pandas_market_calendars as mcal
from google.auth.transport.requests import Request
from google.oauth2 import id_token
from temporalio import activity
from temporalio.exceptions import ApplicationError

from config import load_settings
from models import ArtifactRef, ExecutionMetadata
from price_lake import canonical_price_eod_rows, write_price_eod_parquet
from storage_utils import (
    GCSUploader,
    build_active_universe_object_path,
    build_manifest_object_path,
    build_object_path,
    ensure_dir,
    format_date,
    format_iso_date,
    sanitize_path_segment,
    write_ndjson,
    write_json,
)
from transforms.fundamentals import prod_fundamentals_data
from transforms.prices import prod_prices_data

logger = logging.getLogger(__name__)
SETTINGS = load_settings()
UPLOADER = GCSUploader(
    bucket=SETTINGS.gcs_bucket,
    service_account_key_json=SETTINGS.gcs_service_account_key_json,
    enabled=SETTINGS.upload_enabled,
)

_INTRINIO_HEADERS = (
    {"X-Intrinio-Api-Key": SETTINGS.intrinio_api_key} if SETTINGS.intrinio_api_key else {}
)
_MARKETIO_TOKEN_CACHE: Dict[str, Dict[str, Any]] = {}
_MARKETIO_TOKEN_LOCK = threading.Lock()
_MARKETIO_TOKEN_SKEW_SECONDS = 60
_MARKETIO_TOKEN_FALLBACK_TTL_SECONDS = 300
_NON_RETRYABLE_HTTP_STATUS_CODES = {400, 401, 403, 404, 422}
MARKETIO_ROUTE_COMPANIES = "/api/v2/companies"
MARKETIO_ROUTE_EDGAR_RAW = "/api/v2/edgar/raw"
MARKETIO_ROUTE_FUNDAMENTALS_RAW = "/api/v2/fundamentals/raw"
MARKETIO_ROUTE_MARKET_DAILY_RAW = "/api/v2/market/daily/raw"
MARKETIO_MARKET_SOURCE_LSEG = "lseg"
MARKETIO_MARKET_FREQUENCY_DAILY = "daily"
MARKET_BAR_GRANULARITY_DAY = "day"
MARKETIO_MARKET_EMPTY_RETRY_DELAY_SECONDS = 3.0
MARKETIO_MARKET_EMPTY_RESPONSE_TYPE = "EmptyMarketFieldsResponse"
DEFAULT_MARKET_CALENDAR = "XNYS"
FUNDAMENTALS_DEFAULT_FREQUENCY = "FQ"
FUNDAMENTALS_DEFAULT_REQUEST_PERIOD = "FQ0"
EXCHANGE_CALENDAR_MAP = {
    "ARC": "XNYS",
    "ASE": "XNYS",
    "BATS": "XNYS",
    "NAS": "XNAS",
    "NASD": "XNAS",
    "NMS": "XNAS",
    "NYSE": "XNYS",
    "NYQ": "XNYS",
    "NYS": "XNYS",
}
FUNDAMENTALS_PROD_TRANSFORM_NAME = "fundamentals_prod_transform"
PRICES_PROD_TRANSFORM_NAME = "prices_prod_transform"
PROD_TRANSFORM_VERSION = "v1"


def _non_retryable(message: str, type_name: str = "InvalidRequest") -> ApplicationError:
    return ApplicationError(message, type=type_name, non_retryable=True)


def _activity_is_cancelled() -> bool:
    try:
        return activity.is_cancelled()
    except RuntimeError:
        return False


def _activity_heartbeat(*details: Any) -> None:
    try:
        activity.heartbeat(*details)
    except RuntimeError:
        return


def _make_client(
    stream: bool = False,
    headers: Optional[Dict[str, str]] = None,
    include_intrinio: bool = False,
) -> httpx.Client:
    timeout = SETTINGS.http_stream_timeout if stream else SETTINGS.http_timeout
    merged_headers: Dict[str, str] = {}
    if include_intrinio and _INTRINIO_HEADERS:
        merged_headers.update(_INTRINIO_HEADERS)
    if headers:
        merged_headers.update(headers)
    return httpx.Client(timeout=timeout, headers=merged_headers)


def _temp_path(object_path: str) -> Path:
    path = Path(SETTINGS.temp_dir) / object_path
    ensure_dir(path.parent)
    return path


def _execution_metadata_from_payload(payload: Optional[Dict[str, Any]]) -> ExecutionMetadata:
    if not payload:
        raise _non_retryable("Missing execution metadata", type_name="ExecutionMetadataError")
    request_id = str(payload.get("request_id") or "").strip()
    workflow_id = str(payload.get("workflow_id") or "").strip()
    workflow_run_id = str(payload.get("workflow_run_id") or "").strip()
    if not request_id or not workflow_id or not workflow_run_id:
        raise _non_retryable("Execution metadata must include request_id, workflow_id, and workflow_run_id")
    return ExecutionMetadata(
        request_id=request_id,
        workflow_id=workflow_id,
        workflow_run_id=workflow_run_id,
    )


def _log_prefix(execution: ExecutionMetadata, stage: str, ticker: Optional[str] = None) -> str:
    parts = [
        f"stage={stage}",
        f"request_id={execution.request_id}",
        f"workflow_id={execution.workflow_id}",
        f"workflow_run_id={execution.workflow_run_id}",
    ]
    if ticker:
        parts.append(f"ticker={ticker}")
    return " ".join(parts)


def _normalize_universe_key(universe_key: Optional[str]) -> Optional[str]:
    value = (universe_key or "").strip()
    return value.lower() if value else None


def _resolve_universe_key(universe_key: Optional[str]) -> str:
    return _normalize_universe_key(universe_key) or SETTINGS.universe_key


def _metadata_base(
    layer: str,
    dataset: str,
    execution: ExecutionMetadata,
    ticker: Optional[str],
    start: Optional[str],
    end: Optional[str],
    freq: Optional[str],
    universe_key: Optional[str] = None,
) -> Dict[str, str]:
    meta = {
        "layer": layer,
        "dataset": dataset,
        "request_id": execution.request_id,
        "workflow_id": execution.workflow_id,
        "workflow_run_id": execution.workflow_run_id,
        "source": "marketio-api",
    }
    resolved_universe_key = _normalize_universe_key(universe_key)
    if resolved_universe_key is not None:
        meta["universe_key"] = resolved_universe_key
    if ticker:
        meta["ticker"] = ticker.upper()
    if start:
        meta["start_date"] = format_date(start)
    if end:
        meta["end_date"] = format_date(end)
    if freq:
        meta["frequency"] = freq.lower()
    return meta


def _current_end_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _active_source_lineage(universe_key: Optional[str]) -> Dict[str, str]:
    explicit_universe_key = _optional_str(universe_key)
    if explicit_universe_key is None:
        return {}
    object_path = build_active_universe_object_path(_resolve_universe_key(explicit_universe_key), prefix=SETTINGS.gcs_prefix)
    return {
        "active_source_uri": _object_uri(object_path),
        "active_source_object_path": object_path,
    }


def _object_uri(object_path: str) -> str:
    if UPLOADER.bucket_name:
        return f"gs://{UPLOADER.bucket_name}/{object_path}"
    return object_path


def _manifest_summary_payload(
    *,
    date: str,
    manifest_uri: str,
    manifest_object_path: str,
    artifact_count: int,
    datasets: List[str],
) -> Dict[str, Any]:
    return {
        "date": date,
        "manifest_uri": manifest_uri,
        "manifest_object_path": manifest_object_path,
        "artifact_count": artifact_count,
        "datasets": datasets,
    }


def _validated_manifest_artifact(
    artifact: Dict[str, Any],
    *,
    execution: ExecutionMetadata,
    universe_key: Optional[str],
) -> Dict[str, Any]:
    if not isinstance(artifact, dict):
        raise _non_retryable("Manifest artifact payload must be a dict", type_name="ArtifactValidationError")
    layer = _optional_str(artifact.get("layer"))
    if layer not in {"source", "prod"}:
        raise _non_retryable(
            f"Manifest artifact layer must be source or prod: {artifact}",
            type_name="ArtifactValidationError",
        )
    dataset = _optional_str(artifact.get("dataset"))
    if not dataset:
        raise _non_retryable(
            f"Manifest artifact dataset is required: {artifact}",
            type_name="ArtifactValidationError",
        )
    object_path = _optional_str(artifact.get("object_path"))
    if not object_path:
        raise _non_retryable(
            f"Manifest artifact object_path is required: {artifact}",
            type_name="ArtifactValidationError",
        )
    uri = _optional_str(artifact.get("uri"))
    if not uri:
        raise _non_retryable(
            f"Manifest artifact uri is required: {artifact}",
            type_name="ArtifactValidationError",
        )
    partition_date = _optional_iso_date(artifact.get("date"))
    if not partition_date:
        raise _non_retryable(
            f"Manifest artifact date is required: {artifact}",
            type_name="ArtifactValidationError",
        )
    artifact_request_id = _optional_str(artifact.get("request_id"))
    artifact_workflow_id = _optional_str(artifact.get("workflow_id"))
    artifact_workflow_run_id = _optional_str(artifact.get("workflow_run_id"))
    artifact_universe_key = _optional_str(artifact.get("universe_key"))
    expected_fields = {
        "request_id": execution.request_id,
        "workflow_id": execution.workflow_id,
        "workflow_run_id": execution.workflow_run_id,
    }
    if universe_key is not None:
        expected_fields["universe_key"] = universe_key
    actual_fields = {
        "request_id": artifact_request_id,
        "workflow_id": artifact_workflow_id,
        "workflow_run_id": artifact_workflow_run_id,
        "universe_key": artifact_universe_key,
    }
    for field_name, expected_value in expected_fields.items():
        actual_value = actual_fields[field_name]
        if actual_value and actual_value != expected_value:
            raise _non_retryable(
                f"Manifest artifact {field_name} mismatch: expected {expected_value}, got {actual_value}",
                type_name="ArtifactValidationError",
            )

    normalized = dict(artifact)
    normalized["layer"] = layer
    normalized["dataset"] = dataset
    normalized["uri"] = uri
    normalized["object_path"] = object_path
    normalized["date"] = partition_date
    normalized["request_id"] = execution.request_id
    normalized["workflow_id"] = execution.workflow_id
    normalized["workflow_run_id"] = execution.workflow_run_id
    normalized["universe_key"] = universe_key
    ticker = _optional_str(normalized.get("ticker"))
    if ticker:
        normalized["ticker"] = ticker.upper()
    return normalized


def _normalized_ticker_list(tickers: Optional[List[str]]) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for raw in tickers or []:
        ticker = str(raw).strip().upper()
        if ticker and ticker not in seen:
            normalized.append(ticker)
            seen.add(ticker)
    return normalized


def _load_active_universe_rows(universe_key: str) -> tuple[str, List[Dict[str, Any]]]:
    object_path = build_active_universe_object_path(universe_key, prefix=SETTINGS.gcs_prefix)
    try:
        payload = UPLOADER.download_json(object_path)
    except Exception as exc:
        if exc.__class__.__name__ in {"NotFound", "FileNotFoundError"}:
            raise _non_retryable(
                f"Active universe file not found for universe_key={universe_key} at {object_path}",
                type_name="ArtifactReferenceError",
            ) from exc
        raise
    if not isinstance(payload, list):
        raise _non_retryable(
            f"Active universe payload must be a list for universe_key={universe_key}",
            type_name="ArtifactValidationError",
        )
    rows = [dict(item) for item in payload if isinstance(item, dict)]
    return object_path, rows


def _active_universe_index_payload(universe_key: str) -> Dict[str, Any]:
    object_path, rows = _load_active_universe_rows(universe_key)
    tickers: List[str] = []
    rics: Dict[str, str] = {}
    exchange_codes: Dict[str, str] = {}
    rows_by_ticker: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker or ticker in rows_by_ticker:
            continue
        rows_by_ticker[ticker] = dict(row)
        tickers.append(ticker)
        ric = _preferred_ric(row.get("primary_ric") or row.get("ric"))
        if ric:
            rics[ticker] = ric
        exchange_code = _optional_str(row.get("exchange_code"))
        if exchange_code:
            exchange_codes[ticker] = exchange_code.upper()
    return {
        "active_source_uri": _object_uri(object_path),
        "active_source_object_path": object_path,
        "tickers": tickers,
        "rics": rics,
        "exchange_codes": exchange_codes,
        "rows_by_ticker": rows_by_ticker,
        "record_count": len(tickers),
    }


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    return None


def _optional_bool(value: Any) -> Optional[bool]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off"}:
        return False
    return None


def _optional_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    try:
        return float(text)
    except ValueError:
        return None


def _parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise _non_retryable(f"{field_name} must be YYYY-MM-DD", type_name="InvalidRequest") from exc


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                return value
            continue
        return value
    return None


def _artifact_partition_date_from_execution(payload: Optional[Dict[str, Any]]) -> str:
    partition_date = None
    if isinstance(payload, dict):
        partition_date = _first_non_empty(
            payload.get("artifact_partition_date"),
            payload.get("partition_date"),
        )
    if partition_date is None:
        return _current_end_date()
    try:
        return format_iso_date(partition_date)
    except ValueError as exc:
        raise _non_retryable(
            "artifact_partition_date must be YYYY-MM-DD",
            type_name="ExecutionMetadataError",
        ) from exc


def _clean_dict_none_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in payload.items() if value is not None}


def _normalize_company_metadata_row(
    ticker: str,
    universe_key: str,
    active_row: Optional[Dict[str, Any]],
    provider_row: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    active_payload = dict(active_row or {})
    provider_payload = dict(provider_row or {})
    merged = {**active_payload, **provider_payload}

    canonical = {
        "ticker": ticker,
        "universe_key": universe_key,
        "organization_id": _optional_str(_first_non_empty(merged.get("organization_id"), merged.get("perm_id"))),
        "perm_id": _optional_str(_first_non_empty(merged.get("perm_id"), merged.get("organization_id"))),
        "cik_number": _optional_str(_first_non_empty(merged.get("cik_number"), merged.get("cik"))),
        "lei": _optional_str(merged.get("lei")),
        "issuer_oa_perm_id": _optional_str(merged.get("issuer_oa_perm_id")),
        "isin": _optional_str(_first_non_empty(merged.get("issue_isin"), merged.get("isin"))),
        "sedol": _optional_str(_first_non_empty(merged.get("sedol"), merged.get("sedol_code"))),
        "ric": _optional_str(_preferred_ric(merged.get("ric"))),
        "primary_ric": _optional_str(_preferred_ric(_first_non_empty(merged.get("primary_ric"), merged.get("ric")))),
        "organization_name": _optional_str(merged.get("organization_name")),
        "company_name": _optional_str(_first_non_empty(merged.get("company_name"), merged.get("organization_name"))),
        "common_name": _optional_str(_first_non_empty(merged.get("common_name"), merged.get("company_name"))),
        "document_title": _optional_str(merged.get("document_title")),
        "instrument_type": _optional_str(merged.get("instrument_type")),
        "asset_category": _optional_str(merged.get("asset_category")),
        "asset_category_code": _optional_str(merged.get("asset_category_code")),
        "is_public": _optional_bool(merged.get("is_public")),
        "instrument_is_active": _optional_bool(merged.get("instrument_is_active")),
        "is_primary_quote": _optional_bool(merged.get("is_primary_quote")),
        "exchange": _optional_str(merged.get("exchange")),
        "exchange_code": _optional_str(merged.get("exchange_code")),
        "exchange_country": _optional_str(merged.get("exchange_country")),
        "hq_city": _optional_str(merged.get("hq_city")),
        "hq_state": _optional_str(merged.get("hq_state")),
        "hq_country": _optional_str(merged.get("hq_country")),
        "country_code": _optional_str(merged.get("country_code")),
        "sic_code": _optional_str(_first_non_empty(merged.get("sic_code"), merged.get("sic"))),
        "sic_description": _optional_str(merged.get("sic_description")),
        "sector": _optional_str(merged.get("sector")),
        "industry_group": _optional_str(merged.get("industry_group")),
        "trbc_economic_sector": _optional_str(merged.get("trbc_economic_sector")),
        "trbc_business_sector": _optional_str(merged.get("trbc_business_sector")),
        "trbc_industry_group": _optional_str(merged.get("trbc_industry_group")),
        "trbc_industry": _optional_str(merged.get("trbc_industry")),
        "trbc_activity": _optional_str(merged.get("trbc_activity")),
        "trbc_economic_sector_code": _optional_str(merged.get("trbc_economic_sector_code")),
        "trbc_business_sector_code": _optional_str(merged.get("trbc_business_sector_code")),
        "trbc_industry_group_code": _optional_str(merged.get("trbc_industry_group_code")),
        "trbc_industry_code": _optional_str(merged.get("trbc_industry_code")),
        "trbc_activity_code": _optional_str(merged.get("trbc_activity_code")),
        "has_fundamental_coverage": _optional_bool(merged.get("has_fundamental_coverage")),
        "has_esg_coverage": _optional_bool(merged.get("has_esg_coverage")),
        "primary_listing_fundamentals_exist": _optional_bool(merged.get("primary_listing_fundamentals_exist")),
        "company_report_currency": _optional_str(merged.get("company_report_currency")),
        "website": _optional_str(merged.get("website")),
        "phone_number": _optional_str(merged.get("phone_number")),
        "employees": _optional_int(merged.get("employees")),
        "business_description": _optional_str(merged.get("business_description")),
        "financial_summary": _optional_str(
            _first_non_empty(merged.get("financial_summary"), merged.get("company_financial_summary"))
        ),
        "provider": _optional_str(merged.get("provider"))
        or ("lseg" if isinstance(provider_payload.get("lseg_metadata"), dict) else "marketio"),
        "source": _optional_str(merged.get("source")) or "marketio",
    }

    canonical["raw"] = _clean_dict_none_values(
        {
            "active_universe_row": active_payload or None,
            "provider_row": provider_payload or None,
            "lseg_metadata": provider_payload.get("lseg_metadata")
            if isinstance(provider_payload.get("lseg_metadata"), dict)
            else None,
        }
    )
    return canonical


def _jwt_exp(token: str) -> Optional[int]:
    parts = token.split(".")
    if len(parts) < 2:
        return None
    payload = parts[1]
    padding = "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload + padding)
        payload_obj = json.loads(decoded)
        exp = payload_obj.get("exp")
        if isinstance(exp, (int, float)):
            return int(exp)
        if isinstance(exp, str) and exp.isdigit():
            return int(exp)
    except Exception:
        return None
    return None


def _get_marketio_token(audience: str) -> str:
    now = int(time.time())
    cached = _MARKETIO_TOKEN_CACHE.get(audience)
    if cached:
        exp = cached.get("exp")
        if exp and exp - _MARKETIO_TOKEN_SKEW_SECONDS > now:
            return cached["token"]

    with _MARKETIO_TOKEN_LOCK:
        cached = _MARKETIO_TOKEN_CACHE.get(audience)
        if cached:
            exp = cached.get("exp")
            if exp and exp - _MARKETIO_TOKEN_SKEW_SECONDS > now:
                return cached["token"]

        auth_req = Request()
        token = id_token.fetch_id_token(auth_req, audience)
        exp = _jwt_exp(token)
        if exp is None:
            exp = now + _MARKETIO_TOKEN_FALLBACK_TTL_SECONDS
        _MARKETIO_TOKEN_CACHE[audience] = {"token": token, "exp": int(exp)}
        return token


def _invalidate_marketio_token(audience: str) -> None:
    with _MARKETIO_TOKEN_LOCK:
        _MARKETIO_TOKEN_CACHE.pop(audience, None)


def _marketio_auth_headers(audience: str) -> Dict[str, str]:
    if not SETTINGS.marketio_require_auth:
        return {}
    token = _get_marketio_token(audience)
    return {"Authorization": f"Bearer {token}"}


def _raise_for_status(response: httpx.Response, url: str) -> None:
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code
        body_excerpt = exc.response.text[:200]
        if status_code in _NON_RETRYABLE_HTTP_STATUS_CODES:
            raise _non_retryable(
                f"Marketio request failed with status={status_code} url={url} body={body_excerpt}",
                type_name="RemoteValidationError",
            ) from exc
        raise


def _post_json(endpoint: str, payload: Any) -> Any:
    base_url = SETTINGS.marketio_api_url.rstrip("/")
    url = f"{base_url}{endpoint}"
    headers = _marketio_auth_headers(base_url)
    with _make_client(headers=headers) as client:
        response = client.post(url, json=payload)
        if response.status_code in {401, 403} and SETTINGS.marketio_require_auth:
            response.close()
            _invalidate_marketio_token(base_url)
            headers = _marketio_auth_headers(base_url)
            response = client.post(url, json=payload, headers=headers)
        _raise_for_status(response, url)
        return response.json()


def _load_artifact_payload(artifact_ref: Dict[str, Any], warning_prefix: str) -> Any:
    object_path = str(artifact_ref.get("object_path") or "").strip()
    uri = str(artifact_ref.get("uri") or "").strip()
    local_path = str(artifact_ref.get("local_path") or "").strip()
    try:
        if uri.startswith("gs://"):
            if not object_path:
                raise _non_retryable(f"{warning_prefix}: missing object_path for {uri}", type_name="ArtifactReferenceError")
            loaded = UPLOADER.download_json(object_path)
        elif local_path:
            raw_text = Path(local_path).read_text(encoding="utf-8")
            if local_path.endswith(".ndjson"):
                loaded = [json.loads(line) for line in raw_text.splitlines() if line.strip()]
            else:
                loaded = json.loads(raw_text)
        else:
            raise _non_retryable(
                f"{warning_prefix}: no durable artifact reference available for ticker={artifact_ref.get('ticker')}",
                type_name="ArtifactReferenceError",
            )
        return loaded.get("data", loaded) if isinstance(loaded, dict) else loaded
    except ApplicationError:
        raise
    except FileNotFoundError as exc:
        raise _non_retryable(f"{warning_prefix}: local artifact missing at {local_path}", type_name="ArtifactReferenceError") from exc
    except Exception as exc:
        logger.warning("%s object_path=%s uri=%s error=%s", warning_prefix, object_path, uri, exc)
        raise


def _artifact_list(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def _preferred_ric(value: Optional[Any]) -> Optional[str]:
    text = str(value or "").strip().upper()
    return text or None


def _identifier_payload(*, ticker: str, ric: Optional[str]) -> Dict[str, Any]:
    preferred_ric = _preferred_ric(ric)
    if preferred_ric:
        return {"ric": preferred_ric}
    return {"tickers": [ticker]}


def _artifact_identifier_payload(artifact_ref: Dict[str, Any]) -> Dict[str, Any]:
    ticker = _required_ticker(artifact_ref, "artifact")
    ric = artifact_ref.get("primary_ric") or artifact_ref.get("ric")
    return _identifier_payload(ticker=ticker, ric=_preferred_ric(ric))


def _non_empty_fields_map(value: Any) -> bool:
    return isinstance(value, dict) and bool(value)


def _market_raw_artifact_stats(artifact: Dict[str, Any]) -> Dict[str, int]:
    fields = artifact.get("fields")
    data = artifact.get("data")
    rows = data if isinstance(data, list) else []
    populated_row_fields = sum(
        1
        for row in rows
        if isinstance(row, dict) and _non_empty_fields_map(row.get("fields"))
    )
    return {
        "field_count": int(artifact.get("field_count") or 0),
        "top_level_fields_count": len(fields) if isinstance(fields, list) else 0,
        "row_count": len(rows),
        "populated_row_fields_count": populated_row_fields,
    }


def _market_raw_artifact_usable(artifact: Dict[str, Any]) -> bool:
    stats = _market_raw_artifact_stats(artifact)
    return (
        stats["field_count"] > 0
        and stats["top_level_fields_count"] > 0
        and stats["row_count"] > 0
        and stats["populated_row_fields_count"] > 0
    )


def _fetch_market_daily_raw_with_empty_retry(
    payload: Dict[str, Any],
    execution: ExecutionMetadata,
    *,
    ticker: str,
    frequency: str,
) -> List[Dict[str, Any]]:
    last_artifacts: List[Dict[str, Any]] = []
    for attempt in range(1, 3):
        _activity_heartbeat(
            {"stage": "prices_raw_request", "ticker": ticker, "attempt": attempt}
        )
        response = _post_json(MARKETIO_ROUTE_MARKET_DAILY_RAW, payload)
        artifacts = _artifact_list(response)
        last_artifacts = artifacts
        artifact_stats = _market_raw_artifact_stats(artifacts[0]) if artifacts else {
            "field_count": 0,
            "top_level_fields_count": 0,
            "row_count": 0,
            "populated_row_fields_count": 0,
        }
        if artifacts and any(_market_raw_artifact_usable(artifact) for artifact in artifacts):
            return artifacts
        logger.warning(
            "%s market_raw_empty_fields attempt=%s/%s frequency=%s field_count=%s top_level_fields=%s row_count=%s populated_row_fields=%s",
            _log_prefix(execution, "prices_raw", ticker),
            attempt,
            2,
            frequency,
            artifact_stats["field_count"],
            artifact_stats["top_level_fields_count"],
            artifact_stats["row_count"],
            artifact_stats["populated_row_fields_count"],
        )
        if attempt < 2:
            time.sleep(MARKETIO_MARKET_EMPTY_RETRY_DELAY_SECONDS)

    raise ApplicationError(
        f"Market raw response returned empty fields for ticker={ticker} frequency={frequency} after local retry",
        type=MARKETIO_MARKET_EMPTY_RESPONSE_TYPE,
    )


def _calendar_name_for_exchange_code(exchange_code: Optional[str]) -> str:
    code = str(exchange_code or "").strip().upper()
    return EXCHANGE_CALENDAR_MAP.get(code, DEFAULT_MARKET_CALENDAR)


def _resolve_market_window(
    *,
    period: str,
    as_of_date: str,
    exchange_code: Optional[str] = None,
) -> Dict[str, str]:
    normalized_period = str(period or MARKET_BAR_GRANULARITY_DAY).strip().lower()
    if normalized_period not in {"day", "week", "month", "quarter"}:
        raise _non_retryable(f"Unsupported period: {normalized_period}", type_name="InvalidRequest")
    anchor_date = _parse_iso_date(as_of_date, "as_of_date")
    calendar_name = _calendar_name_for_exchange_code(exchange_code)
    calendar = mcal.get_calendar(calendar_name)
    schedule = calendar.schedule(
        start_date=(anchor_date - timedelta(days=370)).isoformat(),
        end_date=anchor_date.isoformat(),
    )
    if schedule.empty:
        raise _non_retryable(
            f"No market sessions available for calendar={calendar_name} as_of_date={as_of_date}",
            type_name="InvalidRequest",
        )
    sessions = [timestamp.date() for timestamp in schedule.index]
    effective_end = sessions[-1]
    if effective_end == anchor_date and len(sessions) >= 2:
        session_schedule = schedule[schedule.index.date == effective_end]
        if not session_schedule.empty:
            market_close = session_schedule.iloc[-1]["market_close"]
            if isinstance(market_close, pd.Timestamp):
                market_close_utc = market_close.tz_convert("UTC") if market_close.tzinfo else market_close.tz_localize("UTC")
                if datetime.now(timezone.utc) < market_close_utc.to_pydatetime():
                    effective_end = sessions[-2]
    if normalized_period == "day":
        effective_start = effective_end
    else:
        if normalized_period == "week":
            boundary = effective_end - timedelta(days=effective_end.weekday())
        elif normalized_period == "month":
            boundary = effective_end.replace(day=1)
        else:
            quarter_start_month = ((effective_end.month - 1) // 3) * 3 + 1
            boundary = effective_end.replace(month=quarter_start_month, day=1)
        period_schedule = calendar.schedule(start_date=boundary.isoformat(), end_date=effective_end.isoformat())
        if period_schedule.empty:
            effective_start = effective_end
        else:
            effective_start = period_schedule.index[0].date()
    return {
        "requested_period": normalized_period,
        "bar_granularity": MARKET_BAR_GRANULARITY_DAY,
        "as_of_date": anchor_date.isoformat(),
        "effective_start_date": effective_start.isoformat(),
        "effective_end_date": effective_end.isoformat(),
        "calendar": calendar_name,
    }


def _price_artifact_ticker(artifact: Dict[str, Any]) -> str:
    security = artifact.get("security")
    security_ticker = security.get("ticker") if isinstance(security, dict) else None
    ticker = str(artifact.get("ticker") or security_ticker or "").strip().upper()
    if not ticker:
        raise _non_retryable("Missing ticker in prices artifact", type_name="ArtifactValidationError")
    return ticker


def _price_row_base(
    *,
    artifact: Dict[str, Any],
    ticker: str,
    universe_key: Optional[str],
    execution: ExecutionMetadata,
    requested_period: str,
    as_of_date: str,
    effective_start_date: str,
    effective_end_date: str,
) -> Dict[str, Any]:
    row = {
        "ticker": ticker,
        "requested_period": requested_period,
        "as_of_date": as_of_date,
        "effective_start_date": effective_start_date,
        "effective_end_date": effective_end_date,
        "bar_granularity": artifact.get("bar_granularity") or MARKET_BAR_GRANULARITY_DAY,
        "workflow_id": execution.workflow_id,
        "workflow_run_id": execution.workflow_run_id,
        "request_id": execution.request_id,
        "source_system": "marketio",
    }
    if universe_key is not None:
        row["universe_key"] = universe_key
    provider = _optional_str(artifact.get("provider")) or _optional_str(artifact.get("source")) or MARKETIO_MARKET_SOURCE_LSEG
    row["provider"] = provider
    frequency = _optional_str(artifact.get("frequency")) or MARKETIO_MARKET_FREQUENCY_DAILY
    row["frequency"] = frequency
    source = _optional_str(artifact.get("source"))
    if source is not None:
        row["source"] = source
    for key in ("ric", "primary_ric", "cik_number", "organization_id"):
        value = _optional_str(artifact.get(key))
        if value is not None:
            row[key] = value
    return row


def _legacy_stock_price_fields(raw_row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "TR.OPENPRICE": _optional_float(raw_row.get("open")),
        "TR.HIGHPRICE": _optional_float(raw_row.get("high")),
        "TR.LOWPRICE": _optional_float(raw_row.get("low")),
        "TR.CLOSEPRICE": _optional_float(raw_row.get("close")),
        "TR.VOLUME": _optional_float(_first_non_empty(raw_row.get("volume"), raw_row.get("accumulated_volume"))),
        "TR.ADJOPENPRICE": _optional_float(raw_row.get("adj_open")),
        "TR.ADJHIGHPRICE": _optional_float(raw_row.get("adj_high")),
        "TR.ADJLOWPRICE": _optional_float(raw_row.get("adj_low")),
        "TR.ADJCLOSEPRICE": _optional_float(raw_row.get("adj_close")),
        "TR.ADJVOLUME": _optional_float(raw_row.get("adj_volume")),
        "TR.DIVIDENDYIELD": _optional_float(raw_row.get("dividend")),
        "TR.FACTOR": _optional_float(raw_row.get("factor")),
        "TR.SPLITRATIO": _optional_float(raw_row.get("split_ratio")),
        "TR.CHANGEPCT": _optional_float(raw_row.get("percent_change")),
        "TR.PRICEPCTCHG1D": _optional_float(raw_row.get("percent_change")),
        "TR.PRICECHG": _optional_float(raw_row.get("change")),
        "TR.PRICE52WEEKHIGH": _optional_float(raw_row.get("fifty_two_week_high")),
        "TR.PRICE52WEEKLOW": _optional_float(raw_row.get("fifty_two_week_low")),
    }


def _flatten_price_rows(
    artifact: Dict[str, Any],
    *,
    universe_key: Optional[str],
    execution: ExecutionMetadata,
) -> List[Dict[str, Any]]:
    ticker = _price_artifact_ticker(artifact)
    requested_period = str(artifact.get("requested_period") or MARKET_BAR_GRANULARITY_DAY)
    as_of_date = str(artifact.get("as_of_date") or artifact.get("effective_end_date") or "")
    effective_start_date = str(artifact.get("effective_start_date") or artifact.get("start_date") or "")
    effective_end_date = str(artifact.get("effective_end_date") or artifact.get("date") or "")
    base = _price_row_base(
        artifact=artifact,
        ticker=ticker,
        universe_key=universe_key,
        execution=execution,
        requested_period=requested_period,
        as_of_date=as_of_date,
        effective_start_date=effective_start_date,
        effective_end_date=effective_end_date,
    )

    flattened_rows: List[Dict[str, Any]] = []
    stock_prices = artifact.get("stock_prices")
    if isinstance(stock_prices, list) and stock_prices:
        instrument = _optional_str(
            _first_non_empty(
                artifact.get("instrument"),
                artifact.get("primary_ric"),
                artifact.get("ric"),
                artifact.get("ticker"),
            )
        )
        for raw_row in stock_prices:
            if not isinstance(raw_row, dict):
                continue
            row = dict(base)
            row["date"] = _optional_str(raw_row.get("date"))
            row["instrument"] = instrument
            row["fields"] = _legacy_stock_price_fields(raw_row)
            flattened_rows.append(row)
        return flattened_rows

    raw_like = any(
        isinstance(raw_row, dict) and isinstance(raw_row.get("fields"), dict)
        for raw_row in (artifact.get("data") or [])
    )
    for raw_row in artifact.get("data") or []:
        if not isinstance(raw_row, dict):
            continue
        row = dict(base)
        row["date"] = _optional_str(raw_row.get("date"))
        row["instrument"] = _optional_str(raw_row.get("instrument"))
        if raw_like:
            row["fields"] = dict(raw_row.get("fields") or {})
        else:
            for key, value in raw_row.items():
                if key == "fields":
                    continue
                row[key] = value
        flattened_rows.append(row)
    return flattened_rows


def _save_price_artifacts(
    artifacts: List[dict],
    *,
    layer: str,
    execution: ExecutionMetadata,
    universe_key: Optional[str] = None,
) -> List[dict]:
    resolved_universe_key = _normalize_universe_key(universe_key)
    grouped: Dict[str, Dict[str, Any]] = {}
    for artifact in artifacts:
        ticker = _price_artifact_ticker(artifact)
        rows = _flatten_price_rows(artifact, universe_key=resolved_universe_key, execution=execution)
        if not rows:
            continue
        grouped.setdefault(
            ticker,
            {
                "rows": [],
                "requested_period": artifact.get("requested_period") or MARKET_BAR_GRANULARITY_DAY,
                "bar_granularity": artifact.get("bar_granularity") or MARKET_BAR_GRANULARITY_DAY,
                "date": artifact.get("date") or artifact.get("effective_end_date"),
                "as_of_date": artifact.get("as_of_date") or artifact.get("effective_end_date"),
                "effective_start_date": artifact.get("effective_start_date") or artifact.get("start_date"),
                "effective_end_date": artifact.get("effective_end_date") or artifact.get("date"),
                "provider": _optional_str(artifact.get("provider")),
                "source": _optional_str(artifact.get("source")),
                "ric": _preferred_ric(_first_non_empty(artifact.get("primary_ric"), artifact.get("ric"))),
                "primary_ric": _preferred_ric(_first_non_empty(artifact.get("primary_ric"), artifact.get("ric"))),
                "organization_id": _optional_str(artifact.get("organization_id")),
                "cik_number": _optional_str(artifact.get("cik_number")),
                "source_uri": _optional_str(artifact.get("source_uri")),
                "source_object_path": _optional_str(artifact.get("source_object_path")),
                "source_dataset": _optional_str(artifact.get("source_dataset")),
                "transform_name": _optional_str(artifact.get("transform_name")),
                "transform_version": _optional_str(artifact.get("transform_version")),
            },
        )["rows"].extend(rows)

    summaries: List[dict] = []
    for ticker, grouped_payload in grouped.items():
        object_path = build_object_path(
            layer=layer,
            dataset="prices",
            universe_key=resolved_universe_key,
            ticker=ticker,
            suffix=(
                execution.workflow_id
                if layer != "prod"
                else f"part-00000-{execution.workflow_id}-{sanitize_path_segment(ticker)}"
            ),
            bar_granularity=str(grouped_payload["bar_granularity"]),
            effective_end_date=str(grouped_payload["effective_end_date"]),
            prefix=SETTINGS.gcs_prefix,
        )
        local_path = _temp_path(object_path)
        if layer == "prod":
            parquet_rows = canonical_price_eod_rows(
                grouped_payload["rows"],
                context={
                    "ticker": ticker,
                    "date": grouped_payload["date"],
                    "bar_granularity": grouped_payload["bar_granularity"],
                    "requested_period": grouped_payload["requested_period"],
                    "effective_start_date": grouped_payload["effective_start_date"],
                    "effective_end_date": grouped_payload["effective_end_date"],
                    "provider": grouped_payload.get("provider"),
                    "source": grouped_payload.get("source"),
                    "ric": grouped_payload.get("ric"),
                    "primary_ric": grouped_payload.get("primary_ric"),
                    "organization_id": grouped_payload.get("organization_id"),
                    "cik_number": grouped_payload.get("cik_number"),
                    "source_uri": grouped_payload.get("source_uri"),
                    "source_object_uri": grouped_payload.get("source_uri"),
                    "source_object_path": grouped_payload.get("source_object_path"),
                    "source_dataset": grouped_payload.get("source_dataset") or "prices",
                    "transform_name": grouped_payload.get("transform_name"),
                    "transform_version": grouped_payload.get("transform_version"),
                    "run_id": execution.workflow_id,
                    "workflow_id": execution.workflow_id,
                    "workflow_run_id": execution.workflow_run_id,
                    "request_id": execution.request_id,
                    "universe_key": resolved_universe_key,
                    "source_system": "marketio",
                    "frequency": MARKETIO_MARKET_FREQUENCY_DAILY,
                },
            )
            write_price_eod_parquet(local_path, parquet_rows)
        else:
            write_ndjson(local_path, grouped_payload["rows"])
        meta = _metadata_base(
            layer,
            "prices",
            execution,
            ticker,
            str(grouped_payload["effective_start_date"]),
            str(grouped_payload["effective_end_date"]),
            None,
            universe_key=resolved_universe_key,
        )
        meta.update(
            {
                "requested_period": str(grouped_payload["requested_period"]),
                "bar_granularity": str(grouped_payload["bar_granularity"]),
                "as_of_date": format_iso_date(str(grouped_payload["as_of_date"])),
                "effective_start_date": format_iso_date(str(grouped_payload["effective_start_date"])),
                "effective_end_date": format_iso_date(str(grouped_payload["effective_end_date"])),
            }
        )
        for key in ("provider", "source", "ric", "primary_ric", "cik_number", "organization_id"):
            value = grouped_payload.get(key)
            if value is not None:
                meta[key] = str(value)
        for key in ("source_uri", "source_object_path", "source_dataset", "transform_name", "transform_version"):
            value = grouped_payload.get(key)
            if value is not None:
                meta[key] = str(value)
        uri = UPLOADER.upload_file(local_path, object_path, metadata=meta)
        ref = ArtifactRef(
            uri=uri,
            object_path=object_path,
            layer=layer,
            dataset="prices",
            universe_key=resolved_universe_key,
            request_id=execution.request_id,
            workflow_id=execution.workflow_id,
            workflow_run_id=execution.workflow_run_id,
            ticker=ticker,
            start_date=str(grouped_payload["effective_start_date"]),
            date=str(grouped_payload["date"]),
            requested_period=str(grouped_payload["requested_period"]),
            bar_granularity=str(grouped_payload["bar_granularity"]),
            as_of_date=str(grouped_payload["as_of_date"]),
            effective_start_date=str(grouped_payload["effective_start_date"]),
            effective_end_date=str(grouped_payload["effective_end_date"]),
            record_count=len(grouped_payload["rows"]),
            local_path=str(local_path),
            provider=grouped_payload.get("provider"),
            source=grouped_payload.get("source"),
            ric=grouped_payload.get("ric"),
            primary_ric=grouped_payload.get("primary_ric"),
            organization_id=grouped_payload.get("organization_id"),
            cik_number=grouped_payload.get("cik_number"),
            source_uri=grouped_payload.get("source_uri"),
            source_object_path=grouped_payload.get("source_object_path"),
            source_dataset=grouped_payload.get("source_dataset"),
            transform_name=grouped_payload.get("transform_name"),
            transform_version=grouped_payload.get("transform_version"),
        )
        if SETTINGS.cleanup_local_artifacts and UPLOADER.enabled:
            local_path.unlink(missing_ok=True)
            ref = ArtifactRef(**{**ref.to_payload(), "local_path": None})
        _activity_heartbeat(
            {
                "stage": f"prices_{layer}_saved",
                "ticker": ticker,
                "count": len(grouped_payload["rows"]),
            }
        )
        logger.info(
            "%s artifact_saved uri=%s object_path=%s",
            _log_prefix(execution, f"prices_{layer}", ticker),
            uri,
            object_path,
        )
        summaries.append(ref.to_payload())
    return summaries


def _optional_iso_date(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    try:
        return format_iso_date(value)
    except ValueError:
        return _optional_str(value)


def _fundamentals_request_context(artifact: Dict[str, Any]) -> Dict[str, Any]:
    parameter_overrides = artifact.get("parameter_overrides")
    if not isinstance(parameter_overrides, dict):
        parameter_overrides = {}
    requested_period = _optional_str(
        _first_non_empty(
            artifact.get("requested_period"),
            artifact.get("frequency"),
            parameter_overrides.get("Frq"),
        )
    ) or FUNDAMENTALS_DEFAULT_FREQUENCY
    request_start_date = _optional_iso_date(
        _first_non_empty(
            artifact.get("request_start_date"),
            parameter_overrides.get("SDate"),
            artifact.get("start_date"),
        )
    )
    request_end_date = _optional_iso_date(
        _first_non_empty(
            artifact.get("request_end_date"),
            parameter_overrides.get("EDate"),
            artifact.get("end_date"),
        )
    )
    request_period = _optional_str(
        _first_non_empty(
            artifact.get("request_period"),
            parameter_overrides.get("Period"),
        )
    ) or FUNDAMENTALS_DEFAULT_REQUEST_PERIOD
    request_currency = _optional_str(
        _first_non_empty(
            artifact.get("request_currency"),
            parameter_overrides.get("Curn"),
            artifact.get("currency"),
        )
    )
    request_scale = _optional_int(
        _first_non_empty(
            artifact.get("request_scale"),
            parameter_overrides.get("Scale"),
            artifact.get("scale"),
        )
    )
    return {
        "requested_period": requested_period,
        "request_start_date": request_start_date,
        "request_end_date": request_end_date,
        "request_period": request_period,
        "request_currency": request_currency,
        "request_scale": request_scale,
        "provider": _optional_str(artifact.get("provider")),
        "source": _optional_str(artifact.get("source")),
        "ric": _preferred_ric(artifact.get("ric")),
        "primary_ric": _preferred_ric(_first_non_empty(artifact.get("primary_ric"), artifact.get("ric"))),
        "organization_id": _optional_str(artifact.get("organization_id")),
        "cik_number": _optional_str(artifact.get("cik_number")),
        "field_count": _optional_int(artifact.get("field_count")),
        "page_count": _optional_int(artifact.get("page_count")),
        "source_uri": _optional_str(artifact.get("source_uri")),
        "source_object_path": _optional_str(artifact.get("source_object_path")),
        "source_dataset": _optional_str(artifact.get("source_dataset")),
        "transform_name": _optional_str(artifact.get("transform_name")),
        "transform_version": _optional_str(artifact.get("transform_version")),
    }


def _fundamentals_row_base(
    *,
    ticker: str,
    universe_key: Optional[str],
    execution: ExecutionMetadata,
    request_context: Dict[str, Any],
) -> Dict[str, Any]:
    row = {
        "ticker": ticker,
        "workflow_id": execution.workflow_id,
        "workflow_run_id": execution.workflow_run_id,
        "request_id": execution.request_id,
        "source_system": "marketio",
        "frequency": request_context["requested_period"],
        "requested_period": request_context["requested_period"],
        "request_period": request_context["request_period"],
    }
    if universe_key is not None:
        row["universe_key"] = universe_key
    for key in (
        "request_start_date",
        "request_end_date",
        "request_currency",
        "request_scale",
        "provider",
        "source",
        "ric",
        "primary_ric",
        "organization_id",
        "cik_number",
        "source_uri",
        "source_object_path",
        "source_dataset",
        "transform_name",
        "transform_version",
    ):
        value = request_context.get(key)
        if value is not None:
            row[key] = value
    return row


def _flatten_fundamentals_rows(
    artifact: Dict[str, Any],
    *,
    layer: str,
    universe_key: Optional[str],
    execution: ExecutionMetadata,
) -> List[Dict[str, Any]]:
    ticker = _required_ticker(artifact, f"{layer} fundamentals")
    request_context = _fundamentals_request_context(artifact)
    base = _fundamentals_row_base(
        ticker=ticker,
        universe_key=universe_key,
        execution=execution,
        request_context=request_context,
    )
    flattened_rows: List[Dict[str, Any]] = []
    for raw_row in artifact.get("data") or []:
        if not isinstance(raw_row, dict):
            continue
        period_end_date = _optional_iso_date(raw_row.get("period_end_date"))
        if not period_end_date:
            logger.warning(
                "%s fundamentals_row_missing_period_end_date row=%s",
                _log_prefix(execution, f"fundamentals_{layer}", ticker),
                raw_row,
            )
            continue
        row = dict(base)
        row.update(raw_row)
        period_start_date = _optional_iso_date(row.get("period_start_date"))
        if period_start_date is not None:
            row["period_start_date"] = period_start_date
        row["period_end_date"] = period_end_date
        flattened_rows.append(row)
    return flattened_rows


def _save_fundamentals_artifacts(
    artifacts: List[dict],
    *,
    layer: str,
    execution: ExecutionMetadata,
    universe_key: Optional[str] = None,
) -> List[dict]:
    resolved_universe_key = _normalize_universe_key(universe_key)
    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}
    for artifact in artifacts:
        ticker = _required_ticker(artifact, f"{layer} fundamentals")
        request_context = _fundamentals_request_context(artifact)
        rows = _flatten_fundamentals_rows(
            artifact,
            layer=layer,
            universe_key=resolved_universe_key,
            execution=execution,
        )
        for row in rows:
            period_end_date = str(row["period_end_date"])
            period_start_date = _optional_iso_date(row.get("period_start_date"))
            grouped_payload = grouped.setdefault(
                (ticker, period_end_date),
                {
                    "rows": [],
                    "ticker": ticker,
                    "start_date": None,
                    "date": period_end_date,
                    "requested_period": request_context["requested_period"],
                    "request_start_date": request_context["request_start_date"],
                    "request_end_date": request_context["request_end_date"],
                    "request_period": request_context["request_period"],
                    "request_currency": request_context["request_currency"],
                    "request_scale": request_context["request_scale"],
                    "provider": request_context["provider"],
                    "source": request_context["source"],
                    "ric": request_context["ric"],
                    "primary_ric": request_context["primary_ric"],
                    "organization_id": request_context["organization_id"],
                    "cik_number": request_context["cik_number"],
                    "field_count": request_context["field_count"],
                    "page_count": request_context["page_count"],
                    "source_uri": request_context["source_uri"],
                    "source_object_path": request_context["source_object_path"],
                    "source_dataset": request_context["source_dataset"],
                    "transform_name": request_context["transform_name"],
                    "transform_version": request_context["transform_version"],
                },
            )
            if period_start_date is not None and (
                grouped_payload["start_date"] is None or period_start_date < grouped_payload["start_date"]
            ):
                grouped_payload["start_date"] = period_start_date
            grouped_payload["rows"].append(row)

    summaries: List[dict] = []
    for (_, _), grouped_payload in sorted(
        grouped.items(),
        key=lambda item: (
            item[1]["ticker"],
            item[1]["date"],
            item[1]["start_date"] or "",
        ),
    ):
        ticker = str(grouped_payload["ticker"])
        object_path = build_object_path(
            layer=layer,
            dataset="fundamentals",
            universe_key=resolved_universe_key,
            ticker=ticker,
            suffix=execution.workflow_id,
            requested_period=str(grouped_payload["requested_period"]),
            effective_end_date=str(grouped_payload["date"]),
            prefix=SETTINGS.gcs_prefix,
        )
        local_path = _temp_path(object_path)
        write_ndjson(local_path, grouped_payload["rows"])
        meta = _metadata_base(
            layer,
            "fundamentals",
            execution,
            ticker,
            grouped_payload["start_date"],
            str(grouped_payload["date"]),
            str(grouped_payload["requested_period"]),
            universe_key=resolved_universe_key,
        )
        meta["frequency"] = str(grouped_payload["requested_period"])
        for key in ("request_start_date", "request_end_date"):
            value = grouped_payload.get(key)
            if value is not None:
                meta[key] = format_iso_date(str(value))
        for key in ("request_period", "request_currency"):
            value = grouped_payload.get(key)
            if value is not None:
                meta[key] = str(value)
        request_scale = grouped_payload.get("request_scale")
        if request_scale is not None:
            meta["request_scale"] = str(request_scale)
        for key in ("provider", "source", "ric", "primary_ric", "cik_number", "organization_id"):
            value = grouped_payload.get(key)
            if value is not None:
                meta[key] = str(value)
        for key in ("source_uri", "source_object_path", "source_dataset", "transform_name", "transform_version"):
            value = grouped_payload.get(key)
            if value is not None:
                meta[key] = str(value)
        field_count = grouped_payload.get("field_count")
        if field_count is not None:
            meta["field_count"] = str(field_count)
        page_count = grouped_payload.get("page_count")
        if page_count is not None:
            meta["page_count"] = str(page_count)
        uri = UPLOADER.upload_file(local_path, object_path, metadata=meta)
        ref = ArtifactRef(
            uri=uri,
            object_path=object_path,
            layer=layer,
            dataset="fundamentals",
            universe_key=resolved_universe_key,
            request_id=execution.request_id,
            workflow_id=execution.workflow_id,
            workflow_run_id=execution.workflow_run_id,
            ticker=ticker,
            start_date=grouped_payload["start_date"],
            date=str(grouped_payload["date"]),
            requested_period=str(grouped_payload["requested_period"]),
            request_start_date=grouped_payload.get("request_start_date"),
            request_end_date=grouped_payload.get("request_end_date"),
            request_period=grouped_payload.get("request_period"),
            request_currency=grouped_payload.get("request_currency"),
            request_scale=grouped_payload.get("request_scale"),
            record_count=len(grouped_payload["rows"]),
            local_path=str(local_path),
            provider=grouped_payload.get("provider"),
            source=grouped_payload.get("source"),
            ric=grouped_payload.get("ric"),
            primary_ric=grouped_payload.get("primary_ric"),
            organization_id=grouped_payload.get("organization_id"),
            cik_number=grouped_payload.get("cik_number"),
            field_count=grouped_payload.get("field_count"),
            page_count=grouped_payload.get("page_count"),
            source_uri=grouped_payload.get("source_uri"),
            source_object_path=grouped_payload.get("source_object_path"),
            source_dataset=grouped_payload.get("source_dataset"),
            transform_name=grouped_payload.get("transform_name"),
            transform_version=grouped_payload.get("transform_version"),
        )
        if SETTINGS.cleanup_local_artifacts and UPLOADER.enabled:
            local_path.unlink(missing_ok=True)
            ref = ArtifactRef(**{**ref.to_payload(), "local_path": None})
        _activity_heartbeat(
            {
                "stage": f"fundamentals_{layer}_saved",
                "ticker": ticker,
                "date": grouped_payload["date"],
                "count": len(grouped_payload["rows"]),
            }
        )
        logger.info(
            "%s artifact_saved uri=%s object_path=%s",
            _log_prefix(execution, f"fundamentals_{layer}", ticker),
            uri,
            object_path,
        )
        summaries.append(ref.to_payload())
    return summaries


def _required_ticker(artifact_ref: Dict[str, Any], artifact_type: str) -> str:
    ticker = str(artifact_ref.get("ticker") or "").strip()
    if not ticker:
        raise _non_retryable(
            f"Missing ticker in {artifact_type} artifact: {artifact_ref.get('object_path')}",
            type_name="ArtifactValidationError",
        )
    return ticker


def _recent_filings_count(payload: Dict[str, Any]) -> int:
    recent = payload.get("filings", {}).get("recent", {})
    if not isinstance(recent, dict):
        return 0
    accessions = recent.get("accessionNumber") or []
    return len(accessions) if isinstance(accessions, list) else 0


def _save_artifacts(
    artifacts: List[dict],
    layer: str,
    dataset: str,
    execution: ExecutionMetadata,
    extra_meta: Optional[Dict[str, str]] = None,
    freq: Optional[str] = None,
    universe_key: Optional[str] = None,
) -> List[dict]:
    resolved_universe_key = _normalize_universe_key(universe_key)
    summaries: List[dict] = []
    for artifact in artifacts:
        data_items = artifact.get("data")
        record_count = 0
        if isinstance(data_items, list):
            record_count = len(data_items)
        else:
            raw_record_count = artifact.get("record_count")
            if isinstance(raw_record_count, int):
                record_count = raw_record_count
            elif isinstance(raw_record_count, str) and raw_record_count.isdigit():
                record_count = int(raw_record_count)
        ticker = artifact.get("ticker") or ""
        start_date = artifact.get("start_date")
        partition_date = _optional_iso_date(
            _first_non_empty(
                artifact.get("date"),
                artifact.get("_partition_date"),
            )
        )
        filename_suffix: Optional[str] = None
        if dataset == "edgar":
            filename_suffix = execution.workflow_id if partition_date else f"edgar_{date.today().strftime('%Y%m%d')}"
        object_path = build_object_path(
            layer=layer,
            dataset=dataset,
            universe_key=resolved_universe_key,
            ticker=ticker,
            start_date=start_date,
            date=partition_date,
            suffix=filename_suffix,
            requested_period=freq or artifact.get("frequency"),
            prefix=SETTINGS.gcs_prefix,
        )
        local_path = _temp_path(object_path)
        payload_to_write = dict(artifact)
        payload_to_write.pop("_partition_date", None)
        write_json(local_path, payload_to_write)
        meta = _metadata_base(
            layer,
            dataset,
            execution,
            ticker,
            start_date,
            partition_date,
            freq or artifact.get("frequency"),
            universe_key=resolved_universe_key,
        )
        if extra_meta:
            meta.update(extra_meta)
        provider = str(artifact.get("provider") or "").strip()
        if provider:
            meta["provider"] = provider
        source = str(artifact.get("source") or "").strip()
        if source:
            meta["source"] = source
            meta["source_provider"] = source
        ric = _preferred_ric(artifact.get("ric"))
        if ric:
            meta["ric"] = ric
        primary_ric = _preferred_ric(artifact.get("primary_ric"))
        if primary_ric:
            meta["primary_ric"] = primary_ric
        cik_number = str(artifact.get("cik_number") or "").strip()
        if cik_number:
            meta["cik_number"] = cik_number
        if artifact.get("cik"):
            meta["cik"] = str(artifact["cik"])
        organization_id = str(artifact.get("organization_id") or "").strip()
        if organization_id:
            meta["organization_id"] = organization_id
        source_uri = _optional_str(artifact.get("source_uri"))
        if source_uri:
            meta["source_uri"] = source_uri
        source_object_path = _optional_str(artifact.get("source_object_path"))
        if source_object_path:
            meta["source_object_path"] = source_object_path
        source_dataset = _optional_str(artifact.get("source_dataset"))
        if source_dataset:
            meta["source_dataset"] = source_dataset
        transform_name = _optional_str(artifact.get("transform_name"))
        if transform_name:
            meta["transform_name"] = transform_name
        transform_version = _optional_str(artifact.get("transform_version"))
        if transform_version:
            meta["transform_version"] = transform_version
        field_count = artifact.get("field_count")
        if field_count is not None:
            meta["field_count"] = str(field_count)
        page_count = artifact.get("page_count")
        if page_count is not None:
            meta["page_count"] = str(page_count)
        active_source_uri = _optional_str(artifact.get("active_source_uri"))
        if active_source_uri:
            meta["active_source_uri"] = active_source_uri
        active_source_object_path = _optional_str(artifact.get("active_source_object_path"))
        if active_source_object_path:
            meta["active_source_object_path"] = active_source_object_path
        company_id = artifact.get("company_id") or artifact.get("id")
        if company_id:
            meta["company_id"] = str(company_id)
        identifier = artifact.get("identifier")
        if identifier:
            meta["identifier"] = str(identifier)
        uri = UPLOADER.upload_file(local_path, object_path, metadata=meta)

        ref = ArtifactRef(
            uri=uri,
            object_path=object_path,
            layer=layer,
            dataset=dataset,
            universe_key=resolved_universe_key,
            request_id=execution.request_id,
            workflow_id=execution.workflow_id,
            workflow_run_id=execution.workflow_run_id,
            ticker=str(ticker).upper() if ticker else None,
            start_date=start_date,
            date=partition_date,
            requested_period=_optional_str(freq or artifact.get("frequency")),
            record_count=record_count,
            local_path=str(local_path),
            provider=provider or None,
            source=source or None,
            ric=ric,
            primary_ric=primary_ric,
            organization_id=organization_id or None,
            cik_number=cik_number or None,
            field_count=int(field_count) if isinstance(field_count, int) else None,
            page_count=int(page_count) if isinstance(page_count, int) else None,
            active_source_uri=active_source_uri,
            active_source_object_path=active_source_object_path,
            source_uri=source_uri,
            source_object_path=source_object_path,
            source_dataset=source_dataset,
            transform_name=transform_name,
            transform_version=transform_version,
        )

        if SETTINGS.cleanup_local_artifacts and UPLOADER.enabled:
            try:
                local_path.unlink(missing_ok=True)
                ref = ArtifactRef(**{**ref.to_payload(), "local_path": None})
            except Exception as exc:
                logger.warning(
                    "%s cleanup_failed path=%s error=%s",
                    _log_prefix(execution, f"{dataset}_{layer}", ticker or None),
                    local_path,
                    exc,
                )

        _activity_heartbeat(
            {
                "stage": f"{dataset}_{layer}_saved",
                "ticker": str(ticker).upper() if ticker else None,
            }
        )
        logger.info(
            "%s artifact_saved uri=%s object_path=%s",
            _log_prefix(execution, f"{dataset}_{layer}", ticker or None),
            uri,
            object_path,
        )
        summaries.append(ref.to_payload())
    return summaries


@activity.defn(name="check_marketio_health")
def check_marketio_health(execution: Dict[str, Any]) -> None:
    if _activity_is_cancelled():
        raise RuntimeError("check_marketio_health cancelled")
    execution_meta = _execution_metadata_from_payload(execution)
    base_url = SETTINGS.marketio_api_url.rstrip("/")
    url = f"{base_url}/health"
    headers = _marketio_auth_headers(base_url)
    _activity_heartbeat({"stage": "healthcheck_start"})
    with _make_client(headers=headers) as client:
        response = client.get(url)
        if response.status_code in {401, 403} and SETTINGS.marketio_require_auth:
            response.close()
            _invalidate_marketio_token(base_url)
            headers = _marketio_auth_headers(base_url)
            response = client.get(url, headers=headers)
        _raise_for_status(response, url)
    logger.info("%s healthcheck_ok", _log_prefix(execution_meta, "healthcheck"))
    _activity_heartbeat({"status": "healthy"})


@activity.defn(name="load_active_universe_index")
def load_active_universe_index(
    universe_key: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if _activity_is_cancelled():
        raise RuntimeError("load_active_universe_index cancelled")
    execution_meta = _execution_metadata_from_payload(execution)
    resolved_universe_key = _resolve_universe_key(universe_key)
    _activity_heartbeat({"stage": "active_universe_load", "universe_key": resolved_universe_key})
    index = _active_universe_index_payload(resolved_universe_key)
    logger.info(
        "%s active_universe_loaded active_object_path=%s record_count=%s",
        _log_prefix(execution_meta, "active_universe"),
        index["active_source_object_path"],
        index["record_count"],
    )
    _activity_heartbeat({"count": index["record_count"]})
    return {
        "active_source_uri": index["active_source_uri"],
        "active_source_object_path": index["active_source_object_path"],
        "tickers": index["tickers"],
        "rics": index["rics"],
        "exchange_codes": index["exchange_codes"],
        "record_count": index["record_count"],
        "universe_key": resolved_universe_key,
    }


@activity.defn(name="resolve_company_identifiers")
def resolve_company_identifiers(
    tickers: Optional[List[str]] = None,
    universe_key: Optional[str] = None,
    include_metadata_rows: bool = False,
    execution: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if _activity_is_cancelled():
        raise RuntimeError("resolve_company_identifiers cancelled")
    execution_meta = _execution_metadata_from_payload(execution)
    resolved_universe_key = _resolve_universe_key(universe_key)
    _activity_heartbeat(
        {
            "stage": "identifier_active_universe_load",
            "universe_key": resolved_universe_key,
        }
    )
    active_index = _active_universe_index_payload(resolved_universe_key)
    active_rows_by_ticker = active_index["rows_by_ticker"]

    requested_tickers = _normalized_ticker_list(tickers)
    if not requested_tickers:
        requested_tickers = list(active_index["tickers"])
    if not requested_tickers:
        raise _non_retryable(
            f"No tickers available for universe_key={resolved_universe_key}",
            type_name="ArtifactValidationError",
        )

    _activity_heartbeat(
        {"stage": "identifier_provider_request", "count": len(requested_tickers)}
    )
    data = _post_json(MARKETIO_ROUTE_COMPANIES, {"tickers": requested_tickers})
    if isinstance(data, list):
        metadata_rows = [item for item in data if isinstance(item, dict)]
    elif isinstance(data, dict):
        metadata_rows = [data]
    else:
        metadata_rows = []

    metadata_by_ticker = {
        str(item.get("ticker") or "").strip().upper(): dict(item)
        for item in metadata_rows
        if str(item.get("ticker") or "").strip()
    }

    missing_from_active: List[str] = []
    missing_from_provider: List[str] = []
    cik_map: Dict[str, str] = {}
    ric_map: Dict[str, str] = {}
    rows_by_ticker: Dict[str, Dict[str, Any]] = {}

    for ticker in requested_tickers:
        active_row = active_rows_by_ticker.get(ticker)
        provider_row = metadata_by_ticker.get(ticker)
        if active_row is None:
            missing_from_active.append(ticker)
        if provider_row is None:
            missing_from_provider.append(ticker)

        normalized_row = _normalize_company_metadata_row(
            ticker=ticker,
            universe_key=resolved_universe_key,
            active_row=active_row,
            provider_row=provider_row,
        )
        if include_metadata_rows:
            rows_by_ticker[ticker] = normalized_row

        cik_value = normalized_row.get("cik_number")
        if cik_value:
            cik_map[ticker] = str(cik_value).zfill(10)
        ric_value = normalized_row.get("primary_ric") or normalized_row.get("ric")
        if ric_value:
            ric_map[ticker] = str(ric_value).strip().upper()

    if missing_from_active:
        logger.warning(
            "%s identifiers_requested_tickers_not_in_active count=%s tickers=%s",
            _log_prefix(execution_meta, "identifier_resolution"),
            len(missing_from_active),
            missing_from_active,
        )
    if missing_from_provider:
        logger.warning(
            "%s identifiers_missing_provider_rows count=%s tickers=%s",
            _log_prefix(execution_meta, "identifier_resolution"),
            len(missing_from_provider),
            missing_from_provider,
        )

    _activity_heartbeat({"count": len(rows_by_ticker)})
    logger.info(
        "%s identifiers_resolved count=%s active_object_path=%s",
        _log_prefix(execution_meta, "identifier_resolution"),
        len(rows_by_ticker),
        active_index["active_source_object_path"],
    )
    return {
        "active_source_uri": active_index["active_source_uri"],
        "active_source_object_path": active_index["active_source_object_path"],
        "tickers": requested_tickers,
        "ciks": cik_map,
        "rics": ric_map,
        "rows_by_ticker": rows_by_ticker,
        "missing_from_active": missing_from_active,
        "missing_from_provider": missing_from_provider,
        "request_id": execution_meta.request_id,
        "workflow_id": execution_meta.workflow_id,
        "workflow_run_id": execution_meta.workflow_run_id,
        "universe_key": resolved_universe_key,
    }


@activity.defn(name="persist_company_metadata")
def persist_company_metadata(
    identifier_resolution: Dict[str, Any],
    universe_key: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if _activity_is_cancelled():
        raise RuntimeError("persist_company_metadata cancelled")
    execution_meta = _execution_metadata_from_payload(execution)
    if not isinstance(identifier_resolution, dict):
        raise _non_retryable("Identifier resolution payload must be a dict", type_name="ArtifactValidationError")

    resolved_universe_key = _resolve_universe_key(universe_key or identifier_resolution.get("universe_key"))
    metadata_end_date = _artifact_partition_date_from_execution(execution)
    rows_by_ticker = identifier_resolution.get("rows_by_ticker")
    if not isinstance(rows_by_ticker, dict) or not rows_by_ticker:
        raise _non_retryable(
            "No normalized metadata rows provided for persistence",
            type_name="ArtifactValidationError",
        )
    active_source_uri = _optional_str(identifier_resolution.get("active_source_uri"))
    active_source_object_path = _optional_str(identifier_resolution.get("active_source_object_path"))

    artifacts_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
    persisted_tickers: List[str] = []
    for ticker in _normalized_ticker_list(list(rows_by_ticker.keys())):
        _activity_heartbeat({"stage": "metadata_source_save", "ticker": ticker})
        row = rows_by_ticker.get(ticker)
        if not isinstance(row, dict):
            continue
        persisted_row = dict(row)
        if active_source_uri:
            persisted_row["active_source_uri"] = active_source_uri
        if active_source_object_path:
            persisted_row["active_source_object_path"] = active_source_object_path
        object_path = build_object_path(
            layer="source",
            dataset="metadata",
            universe_key=resolved_universe_key,
            ticker=ticker,
            suffix=execution_meta.workflow_id,
            date=metadata_end_date,
            prefix=SETTINGS.gcs_prefix,
        )
        local_path = _temp_path(object_path)
        write_json(local_path, persisted_row)
        metadata = _metadata_base(
            "source",
            "metadata",
            execution_meta,
            ticker,
            None,
            metadata_end_date,
            None,
            universe_key=resolved_universe_key,
        )
        if active_source_uri:
            metadata["active_source_uri"] = active_source_uri
        if active_source_object_path:
            metadata["active_source_object_path"] = active_source_object_path
        for key in ("provider", "source", "ric", "primary_ric", "cik_number", "organization_id"):
            value = row.get(key)
            if value is not None:
                metadata[key] = str(value)
        uri = UPLOADER.upload_file(local_path, object_path, metadata=metadata)
        ref = ArtifactRef(
            uri=uri,
            object_path=object_path,
            layer="source",
            dataset="metadata",
            universe_key=resolved_universe_key,
            request_id=execution_meta.request_id,
            workflow_id=execution_meta.workflow_id,
            workflow_run_id=execution_meta.workflow_run_id,
            ticker=ticker,
            record_count=1,
            local_path=str(local_path),
            provider=_optional_str(row.get("provider")),
            source=_optional_str(row.get("source")),
            ric=_optional_str(row.get("ric")),
            primary_ric=_optional_str(row.get("primary_ric")),
            organization_id=_optional_str(row.get("organization_id")),
            cik_number=_optional_str(row.get("cik_number")),
            date=metadata_end_date,
            active_source_uri=active_source_uri,
            active_source_object_path=active_source_object_path,
        )
        if SETTINGS.cleanup_local_artifacts and UPLOADER.enabled:
            local_path.unlink(missing_ok=True)
            ref = ArtifactRef(**{**ref.to_payload(), "local_path": None})
        artifacts_by_ticker[ticker] = [ref.to_payload()]
        persisted_tickers.append(ticker)
        _activity_heartbeat({"stage": "metadata_source_saved", "ticker": ticker})
        logger.info(
            "%s metadata_source_saved uri=%s object_path=%s",
            _log_prefix(execution_meta, "metadata_source", ticker),
            uri,
            object_path,
        )
    _activity_heartbeat({"count": len(persisted_tickers)})
    return {
        "persisted_tickers": persisted_tickers,
        "artifacts_by_ticker": artifacts_by_ticker,
        "active_source_uri": identifier_resolution.get("active_source_uri"),
        "active_source_object_path": identifier_resolution.get("active_source_object_path"),
        "ciks": identifier_resolution.get("ciks") or {},
        "rics": identifier_resolution.get("rics") or {},
        "missing_from_active": identifier_resolution.get("missing_from_active") or [],
        "missing_from_provider": identifier_resolution.get("missing_from_provider") or [],
        "tickers": identifier_resolution.get("tickers") or persisted_tickers,
        "request_id": execution_meta.request_id,
        "workflow_id": execution_meta.workflow_id,
        "workflow_run_id": execution_meta.workflow_run_id,
        "universe_key": resolved_universe_key,
    }


@activity.defn(name="persist_layer_manifests")
def persist_layer_manifests(
    artifacts: List[Dict[str, Any]],
    universe_key: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    if _activity_is_cancelled():
        raise RuntimeError("persist_layer_manifests cancelled")
    execution_meta = _execution_metadata_from_payload(execution)
    if not isinstance(artifacts, list):
        raise _non_retryable("Manifest artifacts payload must be a list", type_name="ArtifactValidationError")
    resolved_universe_key = _normalize_universe_key(universe_key)
    manifests: Dict[str, List[Dict[str, Any]]] = {"source": [], "prod": []}
    if not artifacts:
        return manifests

    grouped_artifacts: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for artifact in artifacts:
        normalized = _validated_manifest_artifact(
            artifact,
            execution=execution_meta,
            universe_key=resolved_universe_key,
        )
        group_key = (str(normalized["layer"]), str(normalized["date"]))
        grouped_artifacts.setdefault(group_key, []).append(normalized)

    manifest_count = 0
    for (layer, partition_date), grouped_payload in sorted(grouped_artifacts.items()):
        _activity_heartbeat({"stage": "manifest_save", "layer": layer, "date": partition_date})
        ordered_artifacts = sorted(
            grouped_payload,
            key=lambda artifact: (
                str(artifact.get("dataset") or ""),
                str(artifact.get("ticker") or ""),
                str(artifact.get("object_path") or ""),
            ),
        )
        datasets = sorted({str(artifact["dataset"]) for artifact in ordered_artifacts})
        manifest_payload = {
            "request_id": execution_meta.request_id,
            "workflow_id": execution_meta.workflow_id,
            "workflow_run_id": execution_meta.workflow_run_id,
            "universe_key": resolved_universe_key,
            "layer": layer,
            "date": partition_date,
            "artifact_count": len(ordered_artifacts),
            "datasets": datasets,
            "artifacts": ordered_artifacts,
        }
        manifest_object_path = build_manifest_object_path(
            layer=layer,
            workflow_id=execution_meta.workflow_id,
            prefix=SETTINGS.gcs_prefix,
            date=partition_date,
        )
        manifest_local_path = _temp_path(manifest_object_path)
        write_json(manifest_local_path, manifest_payload)
        manifest_metadata = _metadata_base(
            layer,
            "manifest",
            execution_meta,
            None,
            None,
            partition_date,
            None,
            universe_key=resolved_universe_key,
        )
        manifest_metadata["artifact_count"] = str(len(ordered_artifacts))
        manifest_metadata["datasets"] = ",".join(datasets)
        manifest_uri = UPLOADER.upload_file(manifest_local_path, manifest_object_path, metadata=manifest_metadata)
        if SETTINGS.cleanup_local_artifacts and UPLOADER.enabled:
            manifest_local_path.unlink(missing_ok=True)
        manifests[layer].append(
            _manifest_summary_payload(
                date=partition_date,
                manifest_uri=manifest_uri,
                manifest_object_path=manifest_object_path,
                artifact_count=len(ordered_artifacts),
                datasets=datasets,
            )
        )
        manifest_count += 1
        _activity_heartbeat({"stage": "manifest_saved", "layer": layer, "date": partition_date})
        logger.info(
            "%s manifest_saved uri=%s object_path=%s artifact_count=%s",
            _log_prefix(execution_meta, f"{layer}_manifest"),
            manifest_uri,
            manifest_object_path,
            len(ordered_artifacts),
        )

    _activity_heartbeat({"count": manifest_count})
    return manifests


@activity.defn(name="fetch_companies_metadata")
def fetch_companies_metadata(
    tickers: Optional[List[str]] = None,
    universe_key: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Compatibility wrapper around the decoupled metadata activities.
    """
    identifier_resolution = resolve_company_identifiers(
        tickers=tickers,
        universe_key=universe_key,
        include_metadata_rows=True,
        execution=execution,
    )
    partition_date = _artifact_partition_date_from_execution(execution)
    persisted = persist_company_metadata(
        identifier_resolution=identifier_resolution,
        universe_key=universe_key,
        execution={**(execution or {}), "artifact_partition_date": partition_date},
    )
    artifacts: List[Dict[str, Any]] = []
    for artifact_group in persisted.get("artifacts_by_ticker", {}).values():
        if isinstance(artifact_group, list):
            artifacts.extend([artifact for artifact in artifact_group if isinstance(artifact, dict)])
    manifests = persist_layer_manifests(
        artifacts=artifacts,
        universe_key=universe_key,
        execution=execution,
    )
    return {
        "manifests": manifests,
        "active_source_uri": persisted["active_source_uri"],
        "active_source_object_path": persisted["active_source_object_path"],
        "record_count": len(persisted["persisted_tickers"]),
        "ciks": persisted["ciks"],
        "rics": persisted["rics"],
        "tickers": persisted["tickers"],
        "persisted_tickers": persisted["persisted_tickers"],
        "artifacts_by_ticker": persisted["artifacts_by_ticker"],
        "request_id": persisted["request_id"],
        "workflow_id": persisted["workflow_id"],
        "workflow_run_id": persisted["workflow_run_id"],
        "universe_key": persisted["universe_key"],
    }


@activity.defn(name="fetch_edgar_source")
def fetch_edgar_source(
    tickers: Optional[List[str]] = None,
    ciks: Optional[List[str]] = None,
    universe_key: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    if _activity_is_cancelled():
        raise RuntimeError("fetch_edgar_source cancelled")
    execution_meta = _execution_metadata_from_payload(execution)

    requests: List[Dict[str, Any]] = []
    if tickers:
        requests.extend({"ticker": ticker} for ticker in tickers)
    if ciks:
        requests.extend({"cik": cik} for cik in ciks)
    if not requests:
        raise _non_retryable("At least one ticker or CIK must be provided", type_name="ArtifactValidationError")

    edgar_end_date = _artifact_partition_date_from_execution(execution)
    active_source_lineage = _active_source_lineage(universe_key)
    payload: Any = requests[0] if len(requests) == 1 else requests
    _activity_heartbeat({"stage": "edgar_request", "requested": len(requests)})
    raw_response = _post_json(MARKETIO_ROUTE_EDGAR_RAW, payload)
    responses: List[Any] = raw_response if isinstance(raw_response, list) else [raw_response]

    if len(responses) != len(requests):
        logger.warning(
            "%s edgar_response_count_mismatch requests=%s responses=%s",
            _log_prefix(execution_meta, "edgar"),
            len(requests),
            len(responses),
        )

    artifacts: List[dict] = []
    for idx, response in enumerate(responses):
        _activity_heartbeat(
            {
                "stage": "edgar_response_normalize",
                "index": idx + 1,
                "count": len(responses),
            }
        )
        request_meta = requests[idx] if idx < len(requests) else {}
        artifact: Dict[str, Any] = dict(response) if isinstance(response, dict) else {"payload": response}

        ticker_candidate = artifact.get("ticker") or request_meta.get("ticker")
        ticker_list = artifact.get("tickers") if isinstance(artifact, dict) else None
        derived_ticker = (
            (ticker_candidate or "").upper()
            or (str(ticker_list[0]).upper() if isinstance(ticker_list, list) and ticker_list else "")
        )
        cik_value = artifact.get("cik") or request_meta.get("cik")
        if not derived_ticker:
            derived_ticker = f"CIK{str(cik_value).zfill(10)}" if cik_value else f"edgar-{idx + 1}"

        artifact["ticker"] = derived_ticker
        if cik_value:
            artifact["cik"] = str(cik_value).zfill(10)

        artifact["record_count"] = _recent_filings_count(artifact)
        artifact["requested_ticker"] = ticker_candidate
        artifact["_partition_date"] = edgar_end_date
        artifact.update(active_source_lineage)
        artifacts.append(artifact)

    _activity_heartbeat({"requested": len(requests), "received": len(artifacts)})
    return _save_artifacts(
        artifacts,
        layer="source",
        dataset="edgar",
        execution=execution_meta,
        extra_meta={"edgar_source": "true"},
        universe_key=universe_key,
    )


@activity.defn(name="fetch_fundamentals_raw")
def fetch_fundamentals_raw(
    ticker: str,
    ric: Optional[str],
    start_date: str,
    end_date: str,
    universe_key: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    execution_meta = _execution_metadata_from_payload(execution)
    payload = _identifier_payload(ticker=ticker, ric=ric)
    payload.update({"start_date": start_date, "end_date": end_date})
    _activity_heartbeat({"stage": "fundamentals_raw_request", "ticker": ticker})
    data = _post_json(MARKETIO_ROUTE_FUNDAMENTALS_RAW, payload)
    artifacts = _artifact_list(data)
    _activity_heartbeat({"count": len(artifacts)})
    return _save_fundamentals_artifacts(
        artifacts,
        layer="source",
        execution=execution_meta,
        universe_key=universe_key,
    )


@activity.defn(name="fetch_fundamentals_stage")
def fetch_fundamentals_stage(
    raw_artifacts: List[dict],
    universe_key: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    raise _non_retryable(
        "Fundamentals staging is no longer supported by the Marketio API",
        type_name="UnsupportedMode",
    )


@activity.defn(name="fetch_fundamentals_prod")
def fetch_fundamentals_prod(
    raw_artifacts: List[dict],
    universe_key: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    if not raw_artifacts:
        raise _non_retryable("No raw fundamentals provided for production step", type_name="ArtifactValidationError")
    execution_meta = _execution_metadata_from_payload(execution)

    results: List[dict] = []
    for artifact_ref in raw_artifacts:
        ticker = _required_ticker(artifact_ref, "raw fundamentals")
        _activity_heartbeat({"stage": "fundamentals_prod_load_source", "ticker": ticker})
        source_rows = _load_artifact_payload(
            artifact_ref,
            warning_prefix=f"Unable to load raw fundamentals artifact for ticker={ticker}",
        )
        payload = {
            "ticker": ticker,
            "ric": artifact_ref.get("ric"),
            "primary_ric": artifact_ref.get("primary_ric"),
            "cik_number": artifact_ref.get("cik_number"),
            "organization_id": artifact_ref.get("organization_id"),
            "data": source_rows,
        }
        flattened = prod_fundamentals_data(payload)
        artifacts = [
            {
                "ticker": ticker,
                "ric": artifact_ref.get("ric"),
                "primary_ric": artifact_ref.get("primary_ric"),
                "organization_id": artifact_ref.get("organization_id"),
                "cik_number": artifact_ref.get("cik_number"),
                "start_date": artifact_ref.get("request_start_date") or artifact_ref.get("start_date"),
                "record_count": len(flattened),
                "page_count": artifact_ref.get("page_count") or 1,
                "frequency": artifact_ref.get("requested_period") or FUNDAMENTALS_DEFAULT_FREQUENCY,
                "requested_period": artifact_ref.get("requested_period") or FUNDAMENTALS_DEFAULT_FREQUENCY,
                "request_start_date": artifact_ref.get("request_start_date") or artifact_ref.get("start_date"),
                "request_end_date": artifact_ref.get("request_end_date"),
                "request_period": artifact_ref.get("request_period") or FUNDAMENTALS_DEFAULT_REQUEST_PERIOD,
                "request_currency": artifact_ref.get("request_currency"),
                "request_scale": artifact_ref.get("request_scale"),
                "provider": artifact_ref.get("provider") or MARKETIO_MARKET_SOURCE_LSEG,
                "field_count": artifact_ref.get("field_count"),
                "data": flattened,
                "source_uri": artifact_ref.get("uri"),
                "source_object_path": artifact_ref.get("object_path"),
                "source_dataset": artifact_ref.get("dataset"),
                "transform_name": FUNDAMENTALS_PROD_TRANSFORM_NAME,
                "transform_version": PROD_TRANSFORM_VERSION,
            }
        ]
        logger.info(
            "%s fundamentals_prod_transform source_object_path=%s",
            _log_prefix(execution_meta, "fundamentals_prod", ticker),
            artifact_ref.get("object_path"),
        )
        _activity_heartbeat({"ticker": ticker, "count": len(artifacts)})
        results.extend(artifacts)
    return _save_fundamentals_artifacts(
        results,
        layer="prod",
        execution=execution_meta,
        universe_key=universe_key,
    )


@activity.defn(name="fetch_prices_raw")
def fetch_prices_raw(
    ticker: str,
    ric: Optional[str],
    as_of_date: str,
    period: str,
    exchange_code: Optional[str] = None,
    universe_key: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    execution_meta = _execution_metadata_from_payload(execution)
    _activity_heartbeat({"stage": "prices_raw_resolve_window", "ticker": ticker})
    window = _resolve_market_window(period=period, as_of_date=as_of_date, exchange_code=exchange_code)
    payload = _identifier_payload(ticker=ticker, ric=ric)
    payload.update(
        {
            "source": MARKETIO_MARKET_SOURCE_LSEG,
            "start_date": window["effective_start_date"],
            "end_date": window["effective_end_date"],
            "frequency": MARKETIO_MARKET_FREQUENCY_DAILY,
        }
    )
    artifacts = _fetch_market_daily_raw_with_empty_retry(
        payload,
        execution_meta,
        ticker=ticker,
        frequency=window["requested_period"],
    )
    for artifact in artifacts:
        artifact["date"] = window["effective_end_date"]
        artifact["requested_period"] = window["requested_period"]
        artifact["bar_granularity"] = window["bar_granularity"]
        artifact["as_of_date"] = window["as_of_date"]
        artifact["effective_start_date"] = window["effective_start_date"]
        artifact["effective_end_date"] = window["effective_end_date"]
    _activity_heartbeat({"count": len(artifacts)})
    return _save_price_artifacts(
        artifacts,
        layer="source",
        execution=execution_meta,
        universe_key=universe_key,
    )


@activity.defn(name="fetch_prices_prod")
def fetch_prices_prod(
    raw_artifacts: List[dict],
    universe_key: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    if not raw_artifacts:
        raise _non_retryable("No raw prices artifacts provided for production step", type_name="ArtifactValidationError")
    execution_meta = _execution_metadata_from_payload(execution)
    results: List[dict] = []
    for artifact_ref in raw_artifacts:
        ticker = _required_ticker(artifact_ref, "raw prices")
        _activity_heartbeat({"stage": "prices_prod_load_source", "ticker": ticker})
        start_date = artifact_ref.get("effective_start_date") or artifact_ref.get("start_date")
        partition_date = artifact_ref.get("date") or artifact_ref.get("effective_end_date")
        source_rows = _load_artifact_payload(
            artifact_ref,
            warning_prefix=f"Unable to load raw prices artifact for ticker={ticker}",
        )
        rehydrated_rows: List[Dict[str, Any]] = []
        requested_fields: List[str] = []
        seen_fields: set[str] = set()
        for source_row in source_rows or []:
            if not isinstance(source_row, dict):
                continue
            provider_fields = source_row.get("fields")
            if not isinstance(provider_fields, dict):
                provider_fields = {}
            row_payload: Dict[str, Any] = {
                "date": source_row.get("date"),
                "instrument": source_row.get("instrument"),
            }
            for field_name, value in provider_fields.items():
                row_payload[str(field_name)] = value
                if str(field_name) not in seen_fields:
                    requested_fields.append(str(field_name))
                    seen_fields.add(str(field_name))
            rehydrated_rows.append(row_payload)
        payload = {
            "ticker": ticker,
            "ric": artifact_ref.get("ric"),
            "primary_ric": artifact_ref.get("primary_ric"),
            "cik_number": artifact_ref.get("cik_number"),
            "organization_id": artifact_ref.get("organization_id"),
            "frequency": MARKETIO_MARKET_FREQUENCY_DAILY,
            "fields": requested_fields,
            "data": rehydrated_rows,
        }
        flattened = prod_prices_data(payload)
        artifacts = [
            {
                "ticker": ticker,
                "ric": artifact_ref.get("ric"),
                "primary_ric": artifact_ref.get("primary_ric"),
                "organization_id": artifact_ref.get("organization_id"),
                "cik_number": artifact_ref.get("cik_number"),
                "start_date": start_date,
                "date": partition_date,
                "record_count": len(flattened),
                "page_count": artifact_ref.get("page_count") or 1,
                "frequency": MARKETIO_MARKET_FREQUENCY_DAILY,
                "provider": artifact_ref.get("provider") or MARKETIO_MARKET_SOURCE_LSEG,
                "field_count": len(requested_fields),
                "requested_period": artifact_ref.get("requested_period") or MARKET_BAR_GRANULARITY_DAY,
                "bar_granularity": artifact_ref.get("bar_granularity") or MARKET_BAR_GRANULARITY_DAY,
                "as_of_date": artifact_ref.get("as_of_date") or artifact_ref.get("effective_end_date"),
                "effective_start_date": start_date,
                "effective_end_date": partition_date,
                "data": flattened,
                "source_uri": artifact_ref.get("uri"),
                "source_object_path": artifact_ref.get("object_path"),
                "source_dataset": artifact_ref.get("dataset"),
                "transform_name": PRICES_PROD_TRANSFORM_NAME,
                "transform_version": PROD_TRANSFORM_VERSION,
            }
        ]
        logger.info(
            "%s prices_prod_transform source_object_path=%s",
            _log_prefix(execution_meta, "prices_prod", ticker),
            artifact_ref.get("object_path"),
        )
        _activity_heartbeat({"ticker": ticker, "count": len(artifacts)})
        results.extend(artifacts)
    return _save_price_artifacts(
        results,
        layer="prod",
        execution=execution_meta,
        universe_key=universe_key,
    )
