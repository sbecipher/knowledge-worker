import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from temporalio import activity

from config import load_settings
from storage_utils import GCSUploader, build_object_path, ensure_dir, format_date, write_json

logger = logging.getLogger(__name__)
SETTINGS = load_settings()
UPLOADER = GCSUploader(
    bucket=SETTINGS.gcs_bucket,
    service_account_key_path=SETTINGS.gcs_service_account_key_path,
    enabled=SETTINGS.upload_enabled,
)


def _make_client(stream: bool = False) -> httpx.AsyncClient:
    timeout = SETTINGS.http_stream_timeout if stream else SETTINGS.http_timeout
    return httpx.AsyncClient(timeout=timeout)


def _temp_path(object_path: str) -> Path:
    path = Path(SETTINGS.temp_dir) / object_path
    ensure_dir(path.parent)
    return path


def _metadata_base(layer: str, dataset: str, ticker: Optional[str], start: Optional[str], end: Optional[str], freq: Optional[str]) -> Dict[str, str]:
    meta = {
        "layer": layer,
        "dataset": dataset,
        "instrument": SETTINGS.instrument,
        "model_version": SETTINGS.model_version,
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


async def _post_json(endpoint: str, payload: dict) -> Any:
    url = f"{SETTINGS.marketio_api_url}{endpoint}"
    async with _make_client() as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()


@activity.defn(name="check_marketio_health")
async def check_marketio_health() -> None:
    """
    Health check against Marketio API.
    """
    if activity.is_cancelled():
        raise RuntimeError("check_marketio_health cancelled")
    url = f"{SETTINGS.marketio_api_url}/health"
    async with _make_client() as client:
        resp = await client.get(url)
        resp.raise_for_status()
    activity.heartbeat({"status": "healthy"})


@activity.defn(name="fetch_companies_metadata")
async def fetch_companies_metadata(tickers: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Fetch metadata and upload to prod/{instrument}/models/companies_{model_version}.json.
    """
    if activity.is_cancelled():
        raise RuntimeError("fetch_companies_metadata cancelled")
    payload: Dict[str, Any] = {}
    if tickers:
        payload["tickers"] = tickers

    data = await _post_json("/api/v2/companies/metadata", payload)
    activity.heartbeat({"count": len(data)})

    object_path = build_object_path(
        layer="prod",
        instrument=SETTINGS.instrument,
        dataset="models",
        model_version=SETTINGS.model_version,
        prefix=SETTINGS.gcs_prefix,
    )
    local_path = _temp_path(object_path)
    write_json(local_path, data)
    metadata = _metadata_base("prod", "models", None, None, None, None)
    uri = UPLOADER.upload_file(local_path, object_path, metadata=metadata)

    return {"uri": uri, "object_path": object_path, "record_count": len(data)}


def _save_artifacts(
    artifacts: List[dict],
    layer: str,
    dataset: str,
    extra_meta: Optional[Dict[str, str]] = None,
    freq: Optional[str] = None,
) -> List[dict]:
    summaries: List[dict] = []
    for artifact in artifacts:
        ticker = artifact.get("ticker") or ""
        start_date = artifact.get("start_date")
        end_date = artifact.get("end_date")
        object_path = build_object_path(
            layer=layer,
            instrument=SETTINGS.instrument,
            dataset=dataset,
            ticker=ticker,
            freq=freq or artifact.get("frequency"),
            start_date=start_date,
            end_date=end_date,
            prefix=SETTINGS.gcs_prefix,
        )
        local_path = _temp_path(object_path)
        write_json(local_path, artifact)
        meta = _metadata_base(layer, dataset, ticker, start_date, end_date, freq or artifact.get("frequency"))
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
        summaries.append(
            {
                "ticker": ticker,
                "start_date": start_date,
                "end_date": end_date,
                "frequency": freq or artifact.get("frequency"),
                "object_path": object_path,
                "uri": uri,
                "record_count": len(artifact.get("data", [])),
            }
        )
    return summaries


@activity.defn(name="fetch_fundamentals_raw")
async def fetch_fundamentals_raw(tickers: List[str], start_date: str, end_date: str) -> List[dict]:
    payload = {
        "tickers": tickers,
        "start_date": start_date,
        "end_date": end_date,
        "collect": True,
    }
    data = await _post_json("/api/v2/companies/fundamentals", payload)
    activity.heartbeat({"count": len(data)})
    return _save_artifacts(data, layer="source", dataset="fundamentals")


@activity.defn(name="fetch_fundamentals_stage")
async def fetch_fundamentals_stage(tickers: List[str], start_date: str, end_date: str) -> List[dict]:
    payload = {
        "tickers": tickers,
        "start_date": start_date,
        "end_date": end_date,
        "collect": True,
    }
    data = await _post_json("/api/v2/companies/fundamentals/processed", payload)
    activity.heartbeat({"count": len(data)})
    return _save_artifacts(data, layer="stage", dataset="fundamentals")


@activity.defn(name="fetch_fundamentals_prod")
async def fetch_fundamentals_prod(staged_artifacts: List[dict]) -> List[dict]:
    """
    Build production fundamentals from staged artifacts and upload.
    """
    if not staged_artifacts:
        raise ValueError("No staged fundamentals provided for production step")

    results: List[dict] = []
    for artifact in staged_artifacts:
        ticker = artifact.get("ticker")
        start_date = artifact.get("start_date")
        end_date = artifact.get("end_date")
        payload = {
            "tickers": [ticker],
            "start_date": start_date,
            "end_date": end_date,
            "collect": False,
            "data": artifact.get("data", []),
        }
        data = await _post_json("/api/v2/companies/fundamentals/production", payload)
        activity.heartbeat({"ticker": ticker, "count": len(data)})
        results.extend(data)
    return _save_artifacts(results, layer="prod", dataset="fundamentals")


@activity.defn(name="fetch_intraday_raw")
async def fetch_intraday_raw(tickers: List[str], start_date: str, end_date: str, frequency: str) -> List[dict]:
    payload = {
        "tickers": tickers,
        "start_date": start_date,
        "end_date": end_date,
        "frequency": frequency,
        "collect": True,
    }
    data = await _post_json("/api/v2/companies/intraday", payload)
    activity.heartbeat({"count": len(data)})
    return _save_artifacts(data, layer="source", dataset="intraday", freq=frequency)


@activity.defn(name="fetch_intraday_prod")
async def fetch_intraday_prod(raw_artifacts: List[dict], frequency: str) -> List[dict]:
    if not raw_artifacts:
        raise ValueError("No raw intraday artifacts provided for production step")
    results: List[dict] = []
    for artifact in raw_artifacts:
        ticker = artifact.get("ticker")
        start_date = artifact.get("start_date")
        end_date = artifact.get("end_date")
        payload = {
            "tickers": [ticker],
            "start_date": start_date,
            "end_date": end_date,
            "frequency": frequency,
            "collect": False,
            "data": artifact.get("data", []),
        }
        data = await _post_json("/api/v2/companies/intraday/production", payload)
        activity.heartbeat({"ticker": ticker, "count": len(data)})
        results.extend(data)
    return _save_artifacts(results, layer="prod", dataset="intraday", freq=frequency)
