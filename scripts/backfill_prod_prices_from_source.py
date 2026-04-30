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
from price_lake import canonical_price_eod_rows, write_price_eod_parquet
from storage_utils import build_object_path, sanitize_path_segment
from transforms.prices import prod_prices_data

logger = logging.getLogger(__name__)

LOCAL_MANIFEST_DIR = Path("/tmp")
SOURCE_ROOT = "source/prices"
MARKET_BAR_GRANULARITY = "day"
REQUESTED_PERIOD = "day"
PROVIDER_FREQUENCY = "daily"
PRICES_PROD_TRANSFORM_NAME = "prices_prod_transform"
PROD_TRANSFORM_VERSION = "v1"
WORKFLOW_ID_MODES = ("derived", "suffix", "run", "explicit")


@dataclass(frozen=True)
class SourcePriceObjectInfo:
    object_path: str
    ticker: str
    date: str
    granularity: str
    source_workflow_id: str
    generation: str


def _settings_client() -> tuple[Any, GCSClient]:
    settings = load_settings()
    if not settings.gcs_bucket:
        raise ValueError("GCS_BUCKET must be set for prices backfill")
    if settings.gcs_service_account_key_json:
        client = GCSClient.from_service_account_info(json.loads(settings.gcs_service_account_key_json))
    else:
        client = GCSClient()
    return settings, client


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_source_object_path(object_path: str, generation: str) -> SourcePriceObjectInfo:
    path = PurePosixPath(object_path)
    parts = path.parts
    if len(parts) < 6 or parts[0] != "source" or parts[1] != "prices":
        raise ValueError(f"Unsupported source prices path: {object_path}")

    granularity_part = parts[2]
    date_part = parts[3]
    ticker_part = parts[4]
    filename = parts[5]

    if not granularity_part.startswith("granularity="):
        raise ValueError(f"Missing granularity partition in source prices path: {object_path}")
    if not date_part.startswith("date="):
        raise ValueError(f"Missing date partition in source prices path: {object_path}")
    if not ticker_part.startswith("ticker="):
        raise ValueError(f"Missing ticker partition in source prices path: {object_path}")
    if not filename.endswith(".ndjson"):
        raise ValueError(f"Source prices object must be NDJSON: {object_path}")

    return SourcePriceObjectInfo(
        object_path=object_path,
        ticker=ticker_part.split("=", 1)[1].upper(),
        date=date_part.split("=", 1)[1],
        granularity=granularity_part.split("=", 1)[1],
        source_workflow_id=Path(filename).stem,
        generation=str(generation),
    )


def _short_hash(object_path: str, generation: str, length: int = 8) -> str:
    return hashlib.sha256(f"{object_path}:{generation}".encode("utf-8")).hexdigest()[:length]


def output_workflow_id(
    *,
    object_info: SourcePriceObjectInfo,
    mode: str,
    run_workflow_id: str,
    explicit_workflow_id: Optional[str],
    suffix: str,
) -> str:
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in WORKFLOW_ID_MODES:
        raise ValueError(f"Unsupported workflow_id mode: {mode}")

    if normalized_mode == "derived":
        return f"prices_prod_{_short_hash(object_info.object_path, object_info.generation, length=16)}"
    if normalized_mode == "suffix":
        safe_suffix = sanitize_path_segment(suffix or "prod")
        return f"{object_info.source_workflow_id}__{safe_suffix}"
    if normalized_mode == "run":
        return f"{run_workflow_id}__{_short_hash(object_info.object_path, object_info.generation)}"
    if not explicit_workflow_id:
        raise ValueError("--workflow-id is required when --workflow-id-mode explicit")
    safe_base = sanitize_path_segment(explicit_workflow_id)
    return f"{safe_base}__{_short_hash(object_info.object_path, object_info.generation)}"


def destination_object_path(*, object_info: SourcePriceObjectInfo, workflow_id: str, gcs_prefix: str) -> str:
    return build_object_path(
        layer="prod",
        dataset="prices",
        ticker=object_info.ticker,
        suffix=workflow_id,
        bar_granularity=object_info.granularity,
        effective_end_date=object_info.date,
        prefix=gcs_prefix,
    )


def _rehydrate_source_rows(source_rows: Iterable[Any]) -> tuple[List[Dict[str, Any]], List[str]]:
    rehydrated_rows: List[Dict[str, Any]] = []
    requested_fields: List[str] = []
    seen_fields: set[str] = set()
    for source_row in source_rows:
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
            text_name = str(field_name)
            row_payload[text_name] = value
            if text_name not in seen_fields:
                requested_fields.append(text_name)
                seen_fields.add(text_name)
        rehydrated_rows.append(row_payload)
    return rehydrated_rows, requested_fields


def _artifact_context(source_rows: List[Dict[str, Any]], object_info: SourcePriceObjectInfo, universe_key: Optional[str]) -> Dict[str, Any]:
    first_row = source_rows[0] if source_rows else {}
    return {
        "ticker": object_info.ticker,
        "requested_period": _optional_str(first_row.get("requested_period")) or REQUESTED_PERIOD,
        "bar_granularity": _optional_str(first_row.get("bar_granularity")) or object_info.granularity,
        "as_of_date": _optional_str(first_row.get("as_of_date")) or object_info.date,
        "effective_start_date": _optional_str(first_row.get("effective_start_date")) or object_info.date,
        "effective_end_date": _optional_str(first_row.get("effective_end_date")) or object_info.date,
        "provider": _optional_str(first_row.get("provider")),
        "source": _optional_str(first_row.get("source")),
        "ric": _optional_str(first_row.get("ric")),
        "primary_ric": _optional_str(first_row.get("primary_ric")),
        "organization_id": _optional_str(first_row.get("organization_id")),
        "cik_number": _optional_str(first_row.get("cik_number")),
        "universe_key": _optional_str(first_row.get("universe_key")) or universe_key,
    }


def _flatten_prod_rows(
    *,
    source_rows: List[Dict[str, Any]],
    object_info: SourcePriceObjectInfo,
    workflow_id: str,
    request_id: str,
    universe_key: Optional[str],
) -> List[Dict[str, Any]]:
    artifact_context = _artifact_context(source_rows, object_info, universe_key)
    rehydrated_rows, requested_fields = _rehydrate_source_rows(source_rows)
    payload = {
        "ticker": object_info.ticker,
        "ric": artifact_context["ric"],
        "primary_ric": artifact_context["primary_ric"],
        "cik_number": artifact_context["cik_number"],
        "organization_id": artifact_context["organization_id"],
        "frequency": PROVIDER_FREQUENCY,
        "fields": requested_fields,
        "data": rehydrated_rows,
    }
    flattened = prod_prices_data(payload)
    prod_rows: List[Dict[str, Any]] = []
    for row in flattened:
        prod_row = {
            "ticker": object_info.ticker,
            "requested_period": artifact_context["requested_period"],
            "as_of_date": artifact_context["as_of_date"],
            "effective_start_date": artifact_context["effective_start_date"],
            "effective_end_date": artifact_context["effective_end_date"],
            "bar_granularity": artifact_context["bar_granularity"],
            "universe_key": artifact_context["universe_key"],
            "workflow_id": workflow_id,
            "workflow_run_id": workflow_id,
            "request_id": request_id,
            "source_system": "source_prices_backfill",
            "provider": artifact_context["provider"] or "lseg",
            "frequency": PROVIDER_FREQUENCY,
            "ric": artifact_context["ric"],
            "primary_ric": artifact_context["primary_ric"],
            "organization_id": artifact_context["organization_id"],
            "cik_number": artifact_context["cik_number"],
            "source_uri": None,
            "source_object_path": object_info.object_path,
            "source_dataset": "prices",
            "transform_name": PRICES_PROD_TRANSFORM_NAME,
            "transform_version": PROD_TRANSFORM_VERSION,
        }
        for key, value in row.items():
            prod_row[key] = value
        prod_rows.append(prod_row)
    return prod_rows


def _ndjson_bytes(rows: Iterable[Dict[str, Any]]) -> bytes:
    payload = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    return payload.encode("utf-8")


def _local_manifest_path(*, run_id: str) -> Path:
    return LOCAL_MANIFEST_DIR / f"{run_id}.json"


def _source_prefix(*, gcs_prefix: str, prefix: Optional[str]) -> str:
    parts = [p for p in [gcs_prefix, SOURCE_ROOT, (prefix or "").strip("/")] if p]
    return str(PurePosixPath(*parts))


def run_backfill(
    *,
    prefix: Optional[str],
    dry_run: bool,
    workflow_id_mode: str,
    workflow_id: Optional[str],
    workflow_id_suffix: str,
    universe_key: Optional[str],
) -> Dict[str, Any]:
    settings, client = _settings_client()
    bucket = client.bucket(settings.gcs_bucket)
    source_prefix = _source_prefix(gcs_prefix=settings.gcs_prefix, prefix=prefix)
    run_id = datetime.now(timezone.utc).strftime("prices_prod_backfill_%Y%m%dT%H%M%SZ")

    manifest: Dict[str, Any] = {
        "run_id": run_id,
        "bucket": settings.gcs_bucket,
        "gcs_prefix": settings.gcs_prefix,
        "source_prefix": source_prefix,
        "dry_run": dry_run,
        "workflow_id_mode": workflow_id_mode,
        "workflow_id": workflow_id,
        "workflow_id_suffix": workflow_id_suffix,
        "universe_key": universe_key,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "mappings": [],
        "skipped": [],
        "failures": [],
    }

    for blob in client.list_blobs(settings.gcs_bucket, prefix=source_prefix):
        if not blob.name.endswith(".ndjson"):
            continue
        try:
            info = parse_source_object_path(blob.name, str(blob.generation))
            output_id = output_workflow_id(
                object_info=info,
                mode=workflow_id_mode,
                run_workflow_id=run_id,
                explicit_workflow_id=workflow_id,
                suffix=workflow_id_suffix,
            )
            if output_id == info.source_workflow_id:
                raise ValueError(f"Output workflow_id must differ from source workflow_id: {output_id}")
            dest_path = destination_object_path(
                object_info=info,
                workflow_id=output_id,
                gcs_prefix=settings.gcs_prefix,
            )
            mapping = {
                "source_object_path": info.object_path,
                "destination_object_path": dest_path,
                "source_workflow_id": info.source_workflow_id,
                "workflow_id": output_id,
                "generation": info.generation,
                "record_count": None,
            }
            if dry_run:
                manifest["mappings"].append(mapping)
                continue

            source_rows = [
                json.loads(line)
                for line in blob.download_as_text().splitlines()
                if line.strip()
            ]
            prod_rows = _flatten_prod_rows(
                source_rows=source_rows,
                object_info=info,
                workflow_id=output_id,
                request_id=output_id,
                universe_key=universe_key,
            )
            if not prod_rows:
                manifest["skipped"].append(
                    {
                        **mapping,
                        "reason": "no prod rows generated from source artifact",
                    }
                )
                continue

            canonical_rows = canonical_price_eod_rows(
                prod_rows,
                context={
                    "ticker": info.ticker,
                    "date": info.date,
                    "bar_granularity": info.granularity,
                    "requested_period": REQUESTED_PERIOD,
                    "effective_start_date": info.date,
                    "effective_end_date": info.date,
                    "source_dataset": "prices",
                    "source_object_uri": f"gs://{settings.gcs_bucket}/{info.object_path}",
                    "source_object_path": info.object_path,
                    "run_id": run_id,
                },
            )
            local_path = LOCAL_MANIFEST_DIR / dest_path
            write_price_eod_parquet(local_path, canonical_rows)
            destination_blob = bucket.blob(dest_path)
            destination_blob.metadata = {
                "layer": "prod",
                "dataset": "prices",
                "ticker": info.ticker,
                "granularity": info.granularity,
                "date": info.date,
                "workflow_id": output_id,
                "request_id": output_id,
                "workflow_run_id": output_id,
                "source_object_path": info.object_path,
                "source_dataset": "prices",
                "transform_name": PRICES_PROD_TRANSFORM_NAME,
                "transform_version": PROD_TRANSFORM_VERSION,
            }
            try:
                destination_blob.upload_from_filename(
                    str(local_path),
                    content_type="application/octet-stream",
                    if_generation_match=0,
                )
                local_path.unlink(missing_ok=True)
                mapping["record_count"] = len(prod_rows)
                manifest["mappings"].append(mapping)
            except PreconditionFailed:
                manifest["skipped"].append(
                    {
                        **mapping,
                        "reason": "destination already exists",
                    }
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to backfill prod prices from source object %s", getattr(blob, "name", "<unknown>"))
            manifest["failures"].append(
                {
                    "source_object_path": getattr(blob, "name", None),
                    "generation": str(getattr(blob, "generation", "")),
                    "error": str(exc),
                }
            )

    manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
    manifest["migrated_count"] = len(manifest["mappings"])
    manifest["skipped_count"] = len(manifest["skipped"])
    manifest["failure_count"] = len(manifest["failures"])
    local_manifest_path = _local_manifest_path(run_id=run_id)
    manifest["local_manifest_path"] = str(local_manifest_path)
    local_manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    return manifest


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate prod/prices artifacts from stored source/prices NDJSON artifacts."
    )
    parser.add_argument("--prefix", default=None, help="Optional prefix under source/prices to limit the scan.")
    parser.add_argument("--dry-run", action="store_true", help="List planned prod writes without uploading.")
    parser.add_argument(
        "--workflow-id-mode",
        default="derived",
        choices=list(WORKFLOW_ID_MODES),
        help="Strategy for assigning output workflow IDs. Output IDs are always different from the source workflow ID.",
    )
    parser.add_argument(
        "--workflow-id",
        default=None,
        help="Base workflow ID for --workflow-id-mode explicit. The script appends a short source hash.",
    )
    parser.add_argument(
        "--workflow-id-suffix",
        default="prod",
        help="Suffix appended to the source workflow ID when --workflow-id-mode suffix is used.",
    )
    parser.add_argument("--universe-key", default=None, help="Optional universe_key override for output rows.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level (default: INFO).")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    manifest = run_backfill(
        prefix=args.prefix,
        dry_run=args.dry_run,
        workflow_id_mode=args.workflow_id_mode,
        workflow_id=args.workflow_id,
        workflow_id_suffix=args.workflow_id_suffix,
        universe_key=args.universe_key,
    )
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return 0 if manifest["failure_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
