import base64
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from google.auth.transport.requests import Request
from google.oauth2 import id_token
from temporalio import activity
from temporalio.exceptions import ApplicationError

from config import load_settings
from models import ArtifactRef, ExecutionMetadata
from storage_utils import GCSUploader, build_object_path, ensure_dir, format_date, write_json

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
MARKETIO_ROUTE_FUNDAMENTALS_PROD = "/api/v2/fundamentals/production"
MARKETIO_ROUTE_MARKET_DAILY_RAW = "/api/v2/market/daily/raw"
MARKETIO_ROUTE_MARKET_DAILY_PROD = "/api/v2/market/daily/production"
MARKETIO_MARKET_SOURCE_LSEG = "lseg"
MARKETIO_MARKET_FREQUENCY_DAILY = "daily"
MARKETIO_MARKET_EMPTY_RETRY_DELAY_SECONDS = 3.0
MARKETIO_MARKET_EMPTY_RESPONSE_TYPE = "EmptyMarketFieldsResponse"


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


def _resolve_instrument(instrument: Optional[str]) -> str:
    value = (instrument or "").strip()
    return value.lower() if value else SETTINGS.instrument


def _resolve_model_version(model_version: Optional[str]) -> str:
    value = (model_version or "").strip()
    return value if value else SETTINGS.model_version


def _metadata_base(
    layer: str,
    dataset: str,
    execution: ExecutionMetadata,
    ticker: Optional[str],
    start: Optional[str],
    end: Optional[str],
    freq: Optional[str],
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
) -> Dict[str, str]:
    resolved_instrument = _resolve_instrument(instrument)
    resolved_model_version = _resolve_model_version(model_version)
    meta = {
        "layer": layer,
        "dataset": dataset,
        "instrument": resolved_instrument,
        "model_version": resolved_model_version,
        "request_id": execution.request_id,
        "workflow_id": execution.workflow_id,
        "workflow_run_id": execution.workflow_run_id,
        "source": "marketio-api",
    }
    if ticker:
        meta["ticker"] = ticker.upper()
    if start:
        meta["start_date"] = format_date(start)
    if end:
        meta["end_date"] = format_date(end)
    if freq:
        meta["frequency"] = freq.lower()
    return meta


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
            loaded = json.loads(Path(local_path).read_text(encoding="utf-8"))
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
            _log_prefix(execution, "intraday_raw", ticker),
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
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
) -> List[dict]:
    resolved_instrument = _resolve_instrument(instrument)
    resolved_model_version = _resolve_model_version(model_version)
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
        end_date = artifact.get("end_date")
        object_path = build_object_path(
            layer=layer,
            instrument=resolved_instrument,
            dataset=dataset,
            ticker=ticker,
            freq=freq or artifact.get("frequency"),
            start_date=start_date,
            end_date=end_date,
            prefix=SETTINGS.gcs_prefix,
        )
        local_path = _temp_path(object_path)
        write_json(local_path, artifact)
        meta = _metadata_base(
            layer,
            dataset,
            execution,
            ticker,
            start_date,
            end_date,
            freq or artifact.get("frequency"),
            instrument=resolved_instrument,
            model_version=resolved_model_version,
        )
        if extra_meta:
            meta.update(extra_meta)
        provider = str(artifact.get("provider") or "").strip()
        if provider:
            meta["provider"] = provider
        source = str(artifact.get("source") or "").strip()
        if source:
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
        field_count = artifact.get("field_count")
        if field_count is not None:
            meta["field_count"] = str(field_count)
        page_count = artifact.get("page_count")
        if page_count is not None:
            meta["page_count"] = str(page_count)
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
            instrument=resolved_instrument,
            model_version=resolved_model_version,
            request_id=execution.request_id,
            workflow_id=execution.workflow_id,
            workflow_run_id=execution.workflow_run_id,
            ticker=str(ticker).upper() if ticker else None,
            start_date=start_date,
            end_date=end_date,
            frequency=freq or artifact.get("frequency"),
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


@activity.defn(name="fetch_companies_metadata")
def fetch_companies_metadata(
    tickers: Optional[List[str]] = None,
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if _activity_is_cancelled():
        raise RuntimeError("fetch_companies_metadata cancelled")
    execution_meta = _execution_metadata_from_payload(execution)
    payload: Dict[str, Any] = {"tickers": tickers} if tickers else {}

    data = _post_json(MARKETIO_ROUTE_COMPANIES, payload)
    record_count = len(data) if isinstance(data, list) else 1
    _activity_heartbeat({"count": record_count})

    resolved_instrument = _resolve_instrument(instrument)
    resolved_model_version = _resolve_model_version(model_version)
    object_path = build_object_path(
        layer="prod",
        instrument=resolved_instrument,
        dataset="models",
        model_version=resolved_model_version,
        suffix=execution_meta.request_id,
        prefix=SETTINGS.gcs_prefix,
    )
    local_path = _temp_path(object_path)
    write_json(local_path, data)
    metadata = _metadata_base(
        "prod",
        "models",
        execution_meta,
        None,
        None,
        None,
        None,
        instrument=resolved_instrument,
        model_version=resolved_model_version,
    )
    uri = UPLOADER.upload_file(local_path, object_path, metadata=metadata)
    local_path_value = str(local_path)
    if SETTINGS.cleanup_local_artifacts and UPLOADER.enabled:
        local_path.unlink(missing_ok=True)
        local_path_value = None

    cik_map = {
        (item.get("ticker") or "").upper(): str(item.get("cik_number") or item.get("cik")).zfill(10)
        for item in data
        if isinstance(item, dict) and item.get("ticker") and (item.get("cik_number") or item.get("cik"))
    } if isinstance(data, list) else {}
    ric_map = {
        (item.get("ticker") or "").upper(): str(item.get("primary_ric") or item.get("ric")).strip().upper()
        for item in data
        if isinstance(item, dict) and item.get("ticker") and (item.get("primary_ric") or item.get("ric"))
    } if isinstance(data, list) else {}

    logger.info(
        "%s metadata_saved uri=%s object_path=%s record_count=%s",
        _log_prefix(execution_meta, "metadata"),
        uri,
        object_path,
        record_count,
    )
    return {
        "uri": uri,
        "object_path": object_path,
        "record_count": record_count,
        "ciks": cik_map,
        "rics": ric_map,
        "request_id": execution_meta.request_id,
        "workflow_id": execution_meta.workflow_id,
        "workflow_run_id": execution_meta.workflow_run_id,
        "instrument": resolved_instrument,
        "model_version": resolved_model_version,
        "local_path": local_path_value,
    }


@activity.defn(name="fetch_edgar_source")
def fetch_edgar_source(
    tickers: Optional[List[str]] = None,
    ciks: Optional[List[str]] = None,
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
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

    payload: Any = requests[0] if len(requests) == 1 else requests
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
        artifacts.append(artifact)

    _activity_heartbeat({"requested": len(requests), "received": len(artifacts)})
    return _save_artifacts(
        artifacts,
        layer="source",
        dataset="edgar",
        execution=execution_meta,
        extra_meta={"edgar_source": "true"},
        instrument=instrument,
        model_version=model_version,
    )


@activity.defn(name="fetch_fundamentals_raw")
def fetch_fundamentals_raw(
    ticker: str,
    ric: Optional[str],
    start_date: str,
    end_date: str,
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    execution_meta = _execution_metadata_from_payload(execution)
    payload = _identifier_payload(ticker=ticker, ric=ric)
    payload.update({"start_date": start_date, "end_date": end_date})
    data = _post_json(MARKETIO_ROUTE_FUNDAMENTALS_RAW, payload)
    artifacts = _artifact_list(data)
    _activity_heartbeat({"count": len(artifacts)})
    return _save_artifacts(
        artifacts,
        layer="source",
        dataset="fundamentals",
        execution=execution_meta,
        instrument=instrument,
        model_version=model_version,
    )


@activity.defn(name="fetch_fundamentals_stage")
def fetch_fundamentals_stage(
    raw_artifacts: List[dict],
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    raise _non_retryable(
        "Fundamentals staging is no longer supported by the Marketio API",
        type_name="UnsupportedMode",
    )


@activity.defn(name="fetch_fundamentals_prod")
def fetch_fundamentals_prod(
    raw_artifacts: List[dict],
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    if not raw_artifacts:
        raise _non_retryable("No raw fundamentals provided for production step", type_name="ArtifactValidationError")
    execution_meta = _execution_metadata_from_payload(execution)

    results: List[dict] = []
    for artifact_ref in raw_artifacts:
        ticker = _required_ticker(artifact_ref, "raw fundamentals")
        start_date = artifact_ref.get("start_date")
        end_date = artifact_ref.get("end_date")
        payload = _artifact_identifier_payload(artifact_ref)
        payload.update({"start_date": start_date, "end_date": end_date})
        data = _post_json(MARKETIO_ROUTE_FUNDAMENTALS_PROD, payload)
        artifacts = _artifact_list(data)
        logger.info(
            "%s fundamentals_prod_request ric=%s",
            _log_prefix(execution_meta, "fundamentals_prod", ticker),
            payload.get("ric"),
        )
        _activity_heartbeat({"ticker": ticker, "count": len(artifacts)})
        results.extend(artifacts)
    return _save_artifacts(
        results,
        layer="prod",
        dataset="fundamentals",
        execution=execution_meta,
        instrument=instrument,
        model_version=model_version,
    )


@activity.defn(name="fetch_intraday_raw")
def fetch_intraday_raw(
    ticker: str,
    ric: Optional[str],
    start_date: str,
    end_date: str,
    frequency: str,
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    execution_meta = _execution_metadata_from_payload(execution)
    payload = _identifier_payload(ticker=ticker, ric=ric)
    payload.update(
        {
            "source": MARKETIO_MARKET_SOURCE_LSEG,
            "start_date": start_date,
            "end_date": end_date,
            "frequency": MARKETIO_MARKET_FREQUENCY_DAILY,
        }
    )
    artifacts = _fetch_market_daily_raw_with_empty_retry(
        payload,
        execution_meta,
        ticker=ticker,
        frequency=MARKETIO_MARKET_FREQUENCY_DAILY,
    )
    _activity_heartbeat({"count": len(artifacts)})
    return _save_artifacts(
        artifacts,
        layer="source",
        dataset="intraday",
        execution=execution_meta,
        freq=MARKETIO_MARKET_FREQUENCY_DAILY,
        instrument=instrument,
        model_version=model_version,
    )


@activity.defn(name="fetch_intraday_prod")
def fetch_intraday_prod(
    raw_artifacts: List[dict],
    frequency: str,
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
    execution: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    if not raw_artifacts:
        raise _non_retryable("No raw intraday artifacts provided for production step", type_name="ArtifactValidationError")
    execution_meta = _execution_metadata_from_payload(execution)
    results: List[dict] = []
    for artifact_ref in raw_artifacts:
        ticker = _required_ticker(artifact_ref, "raw intraday")
        start_date = artifact_ref.get("start_date")
        end_date = artifact_ref.get("end_date")
        payload = _artifact_identifier_payload(artifact_ref)
        payload.update(
            {
                "start_date": start_date,
                "end_date": end_date,
                "frequency": MARKETIO_MARKET_FREQUENCY_DAILY,
            }
        )
        data = _post_json(MARKETIO_ROUTE_MARKET_DAILY_PROD, payload)
        artifacts = _artifact_list(data)
        logger.info(
            "%s intraday_prod_request ric=%s",
            _log_prefix(execution_meta, "intraday_prod", ticker),
            payload.get("ric"),
        )
        _activity_heartbeat({"ticker": ticker, "count": len(artifacts)})
        results.extend(artifacts)
    return _save_artifacts(
        results,
        layer="prod",
        dataset="intraday",
        execution=execution_meta,
        freq=MARKETIO_MARKET_FREQUENCY_DAILY,
        instrument=instrument,
        model_version=model_version,
    )
