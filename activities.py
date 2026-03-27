import asyncio
import logging
import json
import base64
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from google.auth.transport.requests import Request
from google.oauth2 import id_token
from temporalio import activity

from config import load_settings
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
_MARKETIO_TOKEN_LOCK = asyncio.Lock()
_MARKETIO_TOKEN_SKEW_SECONDS = 60
_MARKETIO_TOKEN_FALLBACK_TTL_SECONDS = 300


def _make_client(
    stream: bool = False,
    headers: Optional[Dict[str, str]] = None,
    include_intrinio: bool = False,
) -> httpx.AsyncClient:
    timeout = SETTINGS.http_stream_timeout if stream else SETTINGS.http_timeout
    merged_headers: Dict[str, str] = {}
    if include_intrinio and _INTRINIO_HEADERS:
        merged_headers.update(_INTRINIO_HEADERS)
    if headers:
        merged_headers.update(headers)
    return httpx.AsyncClient(timeout=timeout, headers=merged_headers)


def _temp_path(object_path: str) -> Path:
    path = Path(SETTINGS.temp_dir) / object_path
    ensure_dir(path.parent)
    return path


def _load_artifact_data_block(artifact: Dict[str, Any], warning_prefix: str) -> Any:
    """
    Resolve the artifact payload used for stage/prod API calls.

    Logic:
    1) Prefer in-memory `artifact["data"]` when present.
    2) If summaries were passed (no embedded `data`), reload from `local_path`.
    3) Return an empty list only when no data can be recovered.
    """
    data_block = artifact.get("data")
    if data_block:
        return data_block
    local_path = artifact.get("local_path")
    if not local_path:
        return []
    try:
        loaded = json.loads(Path(local_path).read_text(encoding="utf-8"))
        return loaded.get("data", loaded) if isinstance(loaded, dict) else loaded
    except Exception as exc:  # noqa: BLE001
        logger.warning("%s from %s: %s", warning_prefix, local_path, exc)
        return []


def _required_ticker(artifact: Dict[str, Any], artifact_type: str) -> str:
    """
    Validate that artifacts contain a ticker before downstream API calls.
    """
    ticker = str(artifact.get("ticker") or "").strip()
    if not ticker:
        raise ValueError(f"Missing ticker in {artifact_type} artifact: {artifact.get('object_path')}")
    return ticker


def _recent_filings_count(payload: Dict[str, Any]) -> int:
    """
    Count the number of recent SEC filings in a submissions payload.
    """
    recent = payload.get("filings", {}).get("recent", {})
    if not isinstance(recent, dict):
        return 0
    accessions = recent.get("accessionNumber") or []
    return len(accessions) if isinstance(accessions, list) else 0


def _resolve_instrument(instrument: Optional[str]) -> str:
    value = (instrument or "").strip()
    return value.lower() if value else SETTINGS.instrument


def _resolve_model_version(model_version: Optional[str]) -> str:
    value = (model_version or "").strip()
    return value if value else SETTINGS.model_version


def _metadata_base(
    layer: str,
    dataset: str,
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
        "run_id": SETTINGS.run_id,
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


async def _post_json(endpoint: str, payload: Any) -> Any:
    base_url = SETTINGS.marketio_api_url.rstrip("/")
    url = f"{base_url}{endpoint}"
    headers = await _marketio_auth_headers(base_url)
    async with _make_client(headers=headers) as client:
        resp = await client.post(url, json=payload)
        if resp.status_code in {401, 403} and SETTINGS.marketio_require_auth:
            await resp.aclose()
            await _invalidate_marketio_token(base_url)
            headers = await _marketio_auth_headers(base_url)
            resp = await client.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        return resp.json()


async def _marketio_auth_headers(audience: str) -> Dict[str, str]:
    if not SETTINGS.marketio_require_auth:
        return {}
    token = await _get_marketio_token(audience)
    return {"Authorization": f"Bearer {token}"}


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
    except Exception:  # noqa: BLE001
        return None
    return None


async def _get_marketio_token(audience: str) -> str:
    now = int(time.time())
    cached = _MARKETIO_TOKEN_CACHE.get(audience)
    if cached:
        exp = cached.get("exp")
        if exp and exp - _MARKETIO_TOKEN_SKEW_SECONDS > now:
            return cached["token"]

    async with _MARKETIO_TOKEN_LOCK:
        cached = _MARKETIO_TOKEN_CACHE.get(audience)
        if cached:
            exp = cached.get("exp")
            if exp and exp - _MARKETIO_TOKEN_SKEW_SECONDS > now:
                return cached["token"]

        auth_req = Request()
        token = await asyncio.to_thread(id_token.fetch_id_token, auth_req, audience)
        exp = _jwt_exp(token)
        if exp is None:
            exp = now + _MARKETIO_TOKEN_FALLBACK_TTL_SECONDS
        _MARKETIO_TOKEN_CACHE[audience] = {"token": token, "exp": int(exp)}
        return token


async def _invalidate_marketio_token(audience: str) -> None:
    async with _MARKETIO_TOKEN_LOCK:
        _MARKETIO_TOKEN_CACHE.pop(audience, None)


@activity.defn(name="check_marketio_health")
async def check_marketio_health() -> None:
    """
    Health check against Marketio API.
    """
    if activity.is_cancelled():
        raise RuntimeError("check_marketio_health cancelled")
    base_url = SETTINGS.marketio_api_url.rstrip("/")
    url = f"{base_url}/health"
    headers = await _marketio_auth_headers(base_url)
    async with _make_client(headers=headers) as client:
        resp = await client.get(url)
        if resp.status_code in {401, 403} and SETTINGS.marketio_require_auth:
            await resp.aclose()
            await _invalidate_marketio_token(base_url)
            headers = await _marketio_auth_headers(base_url)
            resp = await client.get(url, headers=headers)
        resp.raise_for_status()
    activity.heartbeat({"status": "healthy"})


@activity.defn(name="fetch_companies_metadata")
async def fetch_companies_metadata(
    tickers: Optional[List[str]] = None,
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch metadata and upload to prod/models/{instrument}/{model_version}.json.
    """
    if activity.is_cancelled():
        raise RuntimeError("fetch_companies_metadata cancelled")
    payload: Dict[str, Any] = {"tickers": tickers} if tickers else {}

    data = await _post_json("/api/v2/companies/metadata", payload)
    activity.heartbeat({"count": len(data)})

    resolved_instrument = _resolve_instrument(instrument)
    resolved_model_version = _resolve_model_version(model_version)
    object_path = build_object_path(
        layer="prod",
        instrument=resolved_instrument,
        dataset="models",
        model_version=resolved_model_version,
        prefix=SETTINGS.gcs_prefix,
    )
    local_path = _temp_path(object_path)
    write_json(local_path, data)
    metadata = _metadata_base(
        "prod",
        "models",
        None,
        None,
        None,
        None,
        instrument=resolved_instrument,
        model_version=resolved_model_version,
    )
    uri = UPLOADER.upload_file(local_path, object_path, metadata=metadata)

    cik_map = {
        (item.get("ticker") or "").upper(): str(item.get("cik")).zfill(10)
        for item in data
        if isinstance(item, dict) and item.get("ticker") and item.get("cik")
    }

    return {
        "uri": uri,
        "object_path": object_path,
        "record_count": len(data),
        "ciks": cik_map,
    }


@activity.defn(name="fetch_edgar_source")
async def fetch_edgar_source(
    tickers: Optional[List[str]] = None,
    ciks: Optional[List[str]] = None,
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
) -> List[dict]:
    """
    Fetch raw EDGAR submissions (source=True) and upload them to GCS.
    """
    if activity.is_cancelled():
        raise RuntimeError("fetch_edgar_source cancelled")

    requests: List[Dict[str, Any]] = []
    if tickers:
        requests.extend({"ticker": t, "source": True} for t in tickers)
    if ciks:
        requests.extend({"cik": c, "source": True} for c in ciks)
    if not requests:
        raise ValueError("At least one ticker or CIK must be provided")

    payload: Any = requests[0] if len(requests) == 1 else requests
    raw_response = await _post_json("/api/v2/companies/edgar", payload)
    responses: List[Any] = raw_response if isinstance(raw_response, list) else [raw_response]

    if len(responses) != len(requests):
        logger.warning("EDGAR response count mismatch requests=%s responses=%s", len(requests), len(responses))

    artifacts: List[dict] = []
    for idx, response in enumerate(responses):
        request_meta = requests[idx] if idx < len(requests) else {}
        artifact: Dict[str, Any] = dict(response) if isinstance(response, dict) else {"payload": response}

        ticker_candidate = request_meta.get("ticker")
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
            try:
                artifact["cik"] = str(cik_value).zfill(10)
            except Exception:  # noqa: BLE001
                artifact["cik"] = str(cik_value)

        artifact["record_count"] = _recent_filings_count(artifact)
        artifact["requested_ticker"] = ticker_candidate
        artifacts.append(artifact)

    activity.heartbeat({"requested": len(requests), "received": len(artifacts)})
    return _save_artifacts(
        artifacts,
        layer="source",
        dataset="edgar",
        extra_meta={"edgar_source": "true"},
        include_full_artifact=False,
        instrument=instrument,
        model_version=model_version,
    )


def _save_artifacts(
    artifacts: List[dict],
    layer: str,
    dataset: str,
    extra_meta: Optional[Dict[str, str]] = None,
    freq: Optional[str] = None,
    include_full_artifact: bool = False,
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
) -> List[dict]:
    """
    Persist artifacts locally, upload to GCS, and return a lightweight summary list.
    """
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
            ticker,
            start_date,
            end_date,
            freq or artifact.get("frequency"),
            instrument=resolved_instrument,
            model_version=resolved_model_version,
        )
        if extra_meta:
            meta.update(extra_meta)
        if artifact.get("cik"):
            meta["cik"] = artifact["cik"]
        company_id = artifact.get("company_id") or artifact.get("id")
        if company_id:
            meta["company_id"] = company_id
        identifier = artifact.get("identifier")
        if identifier:
            meta["identifier"] = identifier
        uri = UPLOADER.upload_file(local_path, object_path, metadata=meta)
        if SETTINGS.cleanup_local_artifacts and UPLOADER.enabled:
            try:
                local_path.unlink(missing_ok=True)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to cleanup local artifact %s: %s", local_path, exc)
        if include_full_artifact:
            summary = dict(artifact)
        else:
            summary = {
                "ticker": ticker,
                "start_date": start_date,
                "end_date": end_date,
                "frequency": freq or artifact.get("frequency"),
                "record_count": record_count,
            }
        summary["object_path"] = object_path
        summary["uri"] = uri
        summary["local_path"] = str(local_path)
        if "record_count" not in summary:
            summary["record_count"] = record_count
        summaries.append(summary)
    return summaries


@activity.defn(name="fetch_fundamentals_raw")
async def fetch_fundamentals_raw(
    tickers: List[str],
    start_date: str,
    end_date: str,
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
) -> List[dict]:
    payload = {
        "tickers": tickers,
        "start_date": start_date,
        "end_date": end_date,
        "collect": True,
    }
    data = await _post_json("/api/v2/companies/fundamentals", payload)
    activity.heartbeat({"count": len(data)})
    return _save_artifacts(
        data,
        layer="source",
        dataset="fundamentals",
        instrument=instrument,
        model_version=model_version,
    )


@activity.defn(name="fetch_fundamentals_stage")
async def fetch_fundamentals_stage(
    raw_artifacts: List[dict],
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
) -> List[dict]:
    """
    Build staged fundamentals from raw artifacts and upload.
    """
    if not raw_artifacts:
        raise ValueError("No raw fundamentals provided for staging step")

    results: List[dict] = []
    for artifact in raw_artifacts:
        ticker = _required_ticker(artifact, "raw fundamentals")
        start_date = artifact.get("start_date")
        end_date = artifact.get("end_date")
        data_block = _load_artifact_data_block(artifact, "Failed to reload raw fundamentals")
        payload = {
            "tickers": [ticker],
            "start_date": start_date,
            "end_date": end_date,
            "collect": False,
            "data": data_block or [],
        }
        data = await _post_json("/api/v2/companies/fundamentals/processed", payload)
        activity.heartbeat({"ticker": ticker, "count": len(data)})
        results.extend(data)
    return _save_artifacts(
        results,
        layer="stage",
        dataset="fundamentals",
        instrument=instrument,
        model_version=model_version,
    )


@activity.defn(name="fetch_fundamentals_prod")
async def fetch_fundamentals_prod(
    staged_artifacts: List[dict],
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
) -> List[dict]:
    """
    Build production fundamentals from staged artifacts and upload.
    """
    if not staged_artifacts:
        raise ValueError("No staged fundamentals provided for production step")

    results: List[dict] = []
    for artifact in staged_artifacts:
        ticker = _required_ticker(artifact, "staged fundamentals")
        start_date = artifact.get("start_date")
        end_date = artifact.get("end_date")
        data_block = _load_artifact_data_block(artifact, "Failed to reload staged fundamentals")
        payload = {
            "tickers": [ticker],
            "start_date": start_date,
            "end_date": end_date,
            "collect": False,
            "data": data_block or [],
        }
        data = await _post_json("/api/v2/companies/fundamentals/production", payload)
        activity.heartbeat({"ticker": ticker, "count": len(data)})
        results.extend(data)
    return _save_artifacts(
        results,
        layer="prod",
        dataset="fundamentals",
        instrument=instrument,
        model_version=model_version,
    )


@activity.defn(name="fetch_intraday_raw")
async def fetch_intraday_raw(
    tickers: List[str],
    start_date: str,
    end_date: str,
    frequency: str,
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
) -> List[dict]:
    payload = {
        "tickers": tickers,
        "start_date": start_date,
        "end_date": end_date,
        "frequency": frequency,
        "collect": True,
    }
    data = await _post_json("/api/v2/companies/intraday", payload)
    activity.heartbeat({"count": len(data)})
    return _save_artifacts(
        data,
        layer="source",
        dataset="intraday",
        freq=frequency,
        instrument=instrument,
        model_version=model_version,
    )


@activity.defn(name="fetch_intraday_prod")
async def fetch_intraday_prod(
    raw_artifacts: List[dict],
    frequency: str,
    instrument: Optional[str] = None,
    model_version: Optional[str] = None,
) -> List[dict]:
    if not raw_artifacts:
        raise ValueError("No raw intraday artifacts provided for production step")
    results: List[dict] = []
    for artifact in raw_artifacts:
        ticker = _required_ticker(artifact, "raw intraday")
        start_date = artifact.get("start_date")
        end_date = artifact.get("end_date")
        data_block = _load_artifact_data_block(artifact, "Failed to reload raw intraday")
        payload = {
            "tickers": [ticker],
            "start_date": start_date,
            "end_date": end_date,
            "frequency": frequency,
            "collect": False,
            "data": data_block or [],
        }
        data = await _post_json("/api/v2/companies/intraday/production", payload)
        activity.heartbeat({"ticker": ticker, "count": len(data)})
        results.extend(data)
    return _save_artifacts(
        results,
        layer="prod",
        dataset="intraday",
        freq=frequency,
        instrument=instrument,
        model_version=model_version,
    )
