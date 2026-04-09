from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Optional

from google.api_core.exceptions import PreconditionFailed
from google.cloud.storage import Client as GCSClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import load_settings
from storage_utils import build_object_path, format_iso_date

logger = logging.getLogger(__name__)

LEGACY_ROOT = "source/intraday"
LOCAL_MANIFEST_DIR = Path("/tmp")
MARKET_BAR_GRANULARITY = "day"
REQUESTED_PERIOD = "day"


@dataclass(frozen=True)
class LegacyObjectInfo:
    object_path: str
    ticker: str
    effective_start_date: str
    effective_end_date: str
    generation: str


def _settings_client() -> tuple[Any, GCSClient]:
    settings = load_settings()
    if not settings.gcs_bucket:
        raise ValueError("GCS_BUCKET must be set for migration")
    if settings.gcs_service_account_key_json:
        client = GCSClient.from_service_account_info(json.loads(settings.gcs_service_account_key_json))
    else:
        client = GCSClient()
    return settings, client


def parse_legacy_object_path(object_path: str, generation: str) -> LegacyObjectInfo:
    path = PurePosixPath(object_path)
    parts = path.parts
    try:
        source_index = parts.index("source")
    except ValueError as exc:
        raise ValueError(f"Legacy intraday path is missing source segment: {object_path}") from exc
    if len(parts) < source_index + 4 or parts[source_index + 1] != "intraday":
        raise ValueError(f"Unsupported legacy intraday path: {object_path}")
    ticker = parts[source_index + 2].upper()
    filename = parts[source_index + 3]
    stem = filename.removesuffix(".json")
    prefix = f"{ticker}_"
    if not stem.startswith(prefix):
        raise ValueError(f"Legacy filename does not start with ticker prefix: {object_path}")
    stem_parts = stem.split("_")
    if len(stem_parts) < 4:
        raise ValueError(f"Legacy filename does not contain date range: {object_path}")
    start_date = format_iso_date(stem_parts[-2])
    end_date = format_iso_date(stem_parts[-1])
    return LegacyObjectInfo(
        object_path=object_path,
        ticker=ticker,
        effective_start_date=start_date,
        effective_end_date=end_date,
        generation=str(generation),
    )


def legacy_workflow_id(object_path: str, generation: str) -> str:
    digest = hashlib.sha256(f"{object_path}:{generation}".encode("utf-8")).hexdigest()[:16]
    return f"legacy_{digest}"


def destination_object_path(*, object_info: LegacyObjectInfo, workflow_id: str, gcs_prefix: str) -> str:
    return build_object_path(
        layer="source",
        dataset="prices",
        ticker=object_info.ticker,
        suffix=workflow_id,
        bar_granularity=MARKET_BAR_GRANULARITY,
        effective_end_date=object_info.effective_end_date,
        prefix=gcs_prefix,
    )


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def _optional_bool(value: Any) -> Optional[bool]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _price_row_base(
    *,
    ticker: str,
    universe_key: Optional[str],
    workflow_id: str,
    request_id: str,
    effective_start_date: str,
    effective_end_date: str,
    security: Optional[Dict[str, Any]] = None,
    provider: Optional[str] = None,
) -> Dict[str, Any]:
    security = security or {}
    return {
        "ticker": ticker,
        "date": None,
        "requested_period": REQUESTED_PERIOD,
        "as_of_date": effective_end_date,
        "effective_start_date": effective_start_date,
        "effective_end_date": effective_end_date,
        "bar_granularity": MARKET_BAR_GRANULARITY,
        "universe_key": universe_key,
        "workflow_id": workflow_id,
        "workflow_run_id": workflow_id,
        "request_id": request_id,
        "source_system": "marketio",
        "provider": provider or "marketio",
        "security_id": _optional_str(security.get("id")),
        "company_id": _optional_str(security.get("company_id")),
        "security_code": _optional_str(security.get("code")),
        "security_name": _optional_str(security.get("name")),
        "currency": _optional_str(security.get("currency")),
        "composite_ticker": _optional_str(security.get("composite_ticker")),
        "figi": _optional_str(security.get("figi")),
        "composite_figi": _optional_str(security.get("composite_figi")),
        "share_class_figi": _optional_str(security.get("share_class_figi")),
        "primary_listing": _optional_bool(security.get("primary_listing")),
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "volume": None,
        "adj_open": None,
        "adj_high": None,
        "adj_low": None,
        "adj_close": None,
        "adj_volume": None,
        "dividend": None,
        "factor": None,
        "split_ratio": None,
        "intraperiod": None,
        "change": None,
        "percent_change": None,
        "fifty_two_week_high": None,
        "fifty_two_week_low": None,
    }


def flatten_legacy_payload(
    payload: Any,
    *,
    object_info: LegacyObjectInfo,
    workflow_id: str,
    universe_key: Optional[str],
) -> List[Dict[str, Any]]:
    records: Iterable[Any]
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict):
        records = [payload]
    else:
        return []

    rows: List[Dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        security = record.get("security") if isinstance(record.get("security"), dict) else {}
        provider = _optional_str(record.get("provider")) or _optional_str(record.get("source")) or "marketio"
        base = _price_row_base(
            ticker=object_info.ticker,
            universe_key=universe_key,
            workflow_id=workflow_id,
            request_id=workflow_id,
            effective_start_date=object_info.effective_start_date,
            effective_end_date=object_info.effective_end_date,
            security=security,
            provider=provider,
        )
        stock_prices = record.get("stock_prices")
        if isinstance(stock_prices, list):
            for stock_price in stock_prices:
                if not isinstance(stock_price, dict):
                    continue
                row = dict(base)
                row.update(
                    {
                        "date": _optional_str(stock_price.get("date")),
                        "open": _optional_float(stock_price.get("open")),
                        "high": _optional_float(stock_price.get("high")),
                        "low": _optional_float(stock_price.get("low")),
                        "close": _optional_float(stock_price.get("close")),
                        "volume": _optional_float(stock_price.get("volume")),
                        "adj_open": _optional_float(stock_price.get("adj_open")),
                        "adj_high": _optional_float(stock_price.get("adj_high")),
                        "adj_low": _optional_float(stock_price.get("adj_low")),
                        "adj_close": _optional_float(stock_price.get("adj_close")),
                        "adj_volume": _optional_float(stock_price.get("adj_volume")),
                        "dividend": _optional_float(stock_price.get("dividend")),
                        "factor": _optional_float(stock_price.get("factor")),
                        "split_ratio": _optional_float(stock_price.get("split_ratio")),
                        "intraperiod": _optional_bool(stock_price.get("intraperiod")),
                        "change": _optional_float(stock_price.get("change")),
                        "percent_change": _optional_float(stock_price.get("percent_change")),
                        "fifty_two_week_high": _optional_float(stock_price.get("fifty_two_week_high")),
                        "fifty_two_week_low": _optional_float(stock_price.get("fifty_two_week_low")),
                        "legacy_object_path": object_info.object_path,
                        "legacy_generation": object_info.generation,
                    }
                )
                rows.append(row)
            continue

        data_rows = record.get("data")
        if not isinstance(data_rows, list):
            continue
        for data_row in data_rows:
            if not isinstance(data_row, dict):
                continue
            fields = data_row.get("fields") if isinstance(data_row.get("fields"), dict) else {}
            row = dict(base)
            row.update(
                {
                    "date": _optional_str(data_row.get("date")),
                    "open": _optional_float(fields.get("TR.OPENPRICE")),
                    "high": _optional_float(fields.get("TR.HIGHPRICE")),
                    "low": _optional_float(fields.get("TR.LOWPRICE")),
                    "close": _optional_float(fields.get("TR.CLOSEPRICE")),
                    "volume": _optional_float(fields.get("TR.ACCUMULATEDVOLUME")),
                    "adj_open": _optional_float(fields.get("TR.ADJOPENPRICE")),
                    "adj_high": _optional_float(fields.get("TR.ADJHIGHPRICE")),
                    "adj_low": _optional_float(fields.get("TR.ADJLOWPRICE")),
                    "adj_close": _optional_float(fields.get("TR.ADJCLOSEPRICE")),
                    "adj_volume": _optional_float(fields.get("TR.ADJVOLUME")),
                    "dividend": _optional_float(data_row.get("dividend")),
                    "factor": _optional_float(data_row.get("factor")),
                    "split_ratio": _optional_float(data_row.get("split_ratio")),
                    "intraperiod": _optional_bool(data_row.get("intraperiod")),
                    "change": _optional_float(data_row.get("change")),
                    "percent_change": _optional_float(data_row.get("percent_change")),
                    "fifty_two_week_high": _optional_float(data_row.get("fifty_two_week_high")),
                    "fifty_two_week_low": _optional_float(data_row.get("fifty_two_week_low")),
                    "legacy_object_path": object_info.object_path,
                    "legacy_generation": object_info.generation,
                }
            )
            rows.append(row)
    return rows


def _ndjson_bytes(rows: Iterable[Dict[str, Any]]) -> bytes:
    payload = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    return payload.encode("utf-8")


def _local_manifest_path(*, migration_run_id: str) -> Path:
    return LOCAL_MANIFEST_DIR / f"{migration_run_id}.json"


def _legacy_prefix(*, gcs_prefix: str, prefix: Optional[str]) -> str:
    parts = [p for p in [gcs_prefix, LEGACY_ROOT, (prefix or "").strip("/")] if p]
    return str(PurePosixPath(*parts))


def run_migration(*, prefix: Optional[str], dry_run: bool, universe_key: Optional[str]) -> Dict[str, Any]:
    settings, client = _settings_client()
    bucket = client.bucket(settings.gcs_bucket)
    source_prefix = _legacy_prefix(gcs_prefix=settings.gcs_prefix, prefix=prefix)
    migration_run_id = datetime.now(timezone.utc).strftime("migration_%Y%m%dT%H%M%SZ")

    manifest: Dict[str, Any] = {
        "migration_run_id": migration_run_id,
        "bucket": settings.gcs_bucket,
        "gcs_prefix": settings.gcs_prefix,
        "legacy_prefix": source_prefix,
        "dry_run": dry_run,
        "universe_key": universe_key,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "mappings": [],
        "skipped": [],
        "failures": [],
    }

    for blob in client.list_blobs(settings.gcs_bucket, prefix=source_prefix):
        if not blob.name.endswith(".json"):
            continue
        try:
            info = parse_legacy_object_path(blob.name, str(blob.generation))
            workflow_id = legacy_workflow_id(info.object_path, info.generation)
            dest_path = destination_object_path(
                object_info=info,
                workflow_id=workflow_id,
                gcs_prefix=settings.gcs_prefix,
            )
            if dry_run:
                manifest["mappings"].append(
                    {
                        "source_object_path": info.object_path,
                        "destination_object_path": dest_path,
                        "generation": info.generation,
                        "workflow_id": workflow_id,
                        "record_count": None,
                    }
                )
                continue
            destination_blob = bucket.blob(dest_path)
            payload = json.loads(blob.download_as_bytes().decode("utf-8"))
            rows = flatten_legacy_payload(
                payload,
                object_info=info,
                workflow_id=workflow_id,
                universe_key=universe_key,
            )
            if not rows:
                manifest["skipped"].append(
                    {
                        "source_object_path": info.object_path,
                        "destination_object_path": dest_path,
                        "generation": info.generation,
                        "reason": "no_rows",
                    }
                )
                continue
            record = {
                "source_object_path": info.object_path,
                "destination_object_path": dest_path,
                "generation": info.generation,
                "workflow_id": workflow_id,
                "record_count": len(rows),
            }
            destination_blob.metadata = {
                "dataset": "prices",
                "requested_period": REQUESTED_PERIOD,
                "bar_granularity": MARKET_BAR_GRANULARITY,
                "effective_start_date": info.effective_start_date,
                "effective_end_date": info.effective_end_date,
                "workflow_id": workflow_id,
                "request_id": workflow_id,
                "legacy_object_path": info.object_path,
                "legacy_generation": info.generation,
            }
            try:
                destination_blob.upload_from_string(
                    _ndjson_bytes(rows),
                    content_type="application/x-ndjson",
                    if_generation_match=0,
                )
            except PreconditionFailed:
                manifest["skipped"].append(
                    {
                        "source_object_path": info.object_path,
                        "destination_object_path": dest_path,
                        "generation": info.generation,
                        "reason": "destination_exists",
                    }
                )
                continue
            manifest["mappings"].append(record)
        except Exception as exc:  # noqa: BLE001
            logger.exception("failed to migrate legacy object object_path=%s", blob.name)
            manifest["failures"].append({"source_object_path": blob.name, "error": str(exc)})

    manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
    manifest["migrated_count"] = len(manifest["mappings"])
    manifest["skipped_count"] = len(manifest["skipped"])
    manifest["failure_count"] = len(manifest["failures"])
    LOCAL_MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    local_manifest_path = _local_manifest_path(migration_run_id=migration_run_id)
    manifest["local_manifest_path"] = str(local_manifest_path)
    local_manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Migrate legacy source/intraday objects into source/prices NDJSON layout.")
    parser.add_argument("--prefix", default=None, help="Optional prefix under source/intraday to limit the migration scope.")
    parser.add_argument("--dry-run", action="store_true", help="Enumerate and transform without uploading migrated objects.")
    parser.add_argument(
        "--universe-key",
        default=None,
        help="Optional universe key to stamp into migrated rows. Defaults to the worker's configured UNIVERSE_KEY when omitted.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _build_parser().parse_args(argv)
    settings = load_settings()
    manifest = run_migration(
        prefix=args.prefix,
        dry_run=args.dry_run,
        universe_key=(args.universe_key or settings.universe_key or "").strip().lower() or None,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
