import asyncio
import logging
import json
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
    service_account_key_json=SETTINGS.gcs_service_account_key_json,
    enabled=SETTINGS.upload_enabled,
)

_INTRINIO_HEADERS = (
    {"X-Intrinio-Api-Key": SETTINGS.intrinio_api_key} if SETTINGS.intrinio_api_key else {}
)


def _make_client(stream: bool = False) -> httpx.AsyncClient:
    timeout = SETTINGS.http_stream_timeout if stream else SETTINGS.http_timeout
    return httpx.AsyncClient(timeout=timeout, headers=_INTRINIO_HEADERS)


def _temp_path(object_path: str) -> Path:
    path = Path(SETTINGS.temp_dir) / object_path
    ensure_dir(path.parent)
    return path


def _recent_filings_count(payload: Dict[str, Any]) -> int:
    """
    Count the number of recent SEC filings in a submissions payload.
    """
    recent = payload.get("filings", {}).get("recent", {})
    if not isinstance(recent, dict):
        return 0
    accessions = recent.get("accessionNumber") or []
    return len(accessions) if isinstance(accessions, list) else 0


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


async def _post_json(endpoint: str, payload: Any) -> Any:
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
    Fetch metadata and upload to prod/{instrument}/models/{model_version}.json.
    """
    if activity.is_cancelled():
        raise RuntimeError("fetch_companies_metadata cancelled")
    payload: Dict[str, Any] = {"tickers": tickers} if tickers else {}

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
    )


def _save_artifacts(
    artifacts: List[dict],
    layer: str,
    dataset: str,
    extra_meta: Optional[Dict[str, str]] = None,
    freq: Optional[str] = None,
    include_full_artifact: bool = False,
) -> List[dict]:
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
async def fetch_fundamentals_stage(raw_artifacts: List[dict]) -> List[dict]:
    """
    Build staged fundamentals from raw artifacts and upload.
    """
    if not raw_artifacts:
        raise ValueError("No raw fundamentals provided for staging step")

    results: List[dict] = []
    for artifact in raw_artifacts:
        ticker = artifact.get("ticker")
        start_date = artifact.get("start_date")
        end_date = artifact.get("end_date")
        data_block = artifact.get("data")
        if not data_block and artifact.get("local_path"):
            try:
                loaded = json.loads(Path(artifact["local_path"]).read_text(encoding="utf-8"))
                data_block = loaded.get("data", loaded) if isinstance(loaded, dict) else loaded
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to reload raw fundamentals from %s: %s", artifact["local_path"], exc)
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
    return _save_artifacts(results, layer="stage", dataset="fundamentals")


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
        data_block = artifact.get("data")
        if not data_block and artifact.get("local_path"):
            try:
                loaded = json.loads(Path(artifact["local_path"]).read_text(encoding="utf-8"))
                data_block = loaded.get("data", loaded) if isinstance(loaded, dict) else loaded
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to reload staged fundamentals from %s: %s", artifact["local_path"], exc)
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
        data_block = artifact.get("data")
        if not data_block and artifact.get("local_path"):
            try:
                loaded = json.loads(Path(artifact["local_path"]).read_text(encoding="utf-8"))
                data_block = loaded.get("data", loaded) if isinstance(loaded, dict) else loaded
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to reload raw intraday from %s: %s", artifact["local_path"], exc)
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
    return _save_artifacts(results, layer="prod", dataset="intraday", freq=frequency)
