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

LOCAL_MANIFEST_DIR = Path("/tmp")
LEGACY_LAYERS = ("source", "prod")
FUNDAMENTALS_DEFAULT_FREQUENCY = "FQ"
FUNDAMENTALS_DEFAULT_REQUEST_PERIOD = "FQ0"


@dataclass(frozen=True)
class LegacyFundamentalsObjectInfo:
    layer: str
    object_path: str
    ticker: str
    request_start_date: str
    request_end_date: str
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
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def _optional_iso_date(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    try:
        return format_iso_date(value)
    except ValueError:
        return _optional_str(value)


def parse_legacy_object_path(object_path: str, generation: str) -> LegacyFundamentalsObjectInfo:
    path = PurePosixPath(object_path)
    parts = path.parts
    if len(parts) < 4:
        raise ValueError(f"Unsupported fundamentals legacy path: {object_path}")
    layer = parts[0]
    if layer not in LEGACY_LAYERS or parts[1] != "fundamentals":
        raise ValueError(f"Unsupported fundamentals legacy path: {object_path}")
    ticker = parts[2].upper()
    filename = parts[3]
    stem = filename.removesuffix(".json")
    prefix = f"{ticker}_fundamentals_"
    if not stem.startswith(prefix):
        raise ValueError(f"Unsupported fundamentals legacy filename: {object_path}")
    stem_parts = stem.split("_")
    if len(stem_parts) < 4:
        raise ValueError(f"Legacy fundamentals filename missing request dates: {object_path}")
    request_start_date = format_iso_date(stem_parts[-2])
    request_end_date = format_iso_date(stem_parts[-1])
    return LegacyFundamentalsObjectInfo(
        layer=layer,
        object_path=object_path,
        ticker=ticker,
        request_start_date=request_start_date,
        request_end_date=request_end_date,
        generation=str(generation),
    )


def legacy_workflow_id(object_path: str, generation: str) -> str:
    digest = hashlib.sha256(f"{object_path}:{generation}".encode("utf-8")).hexdigest()[:16]
    return f"legacy_{digest}"


def _fundamentals_request_context(payload: Dict[str, Any], object_info: LegacyFundamentalsObjectInfo) -> Dict[str, Any]:
    parameter_overrides = payload.get("parameter_overrides")
    if not isinstance(parameter_overrides, dict):
        parameter_overrides = {}
    requested_period = _optional_str(
        payload.get("requested_period")
        or payload.get("frequency")
        or parameter_overrides.get("Frq")
    ) or FUNDAMENTALS_DEFAULT_FREQUENCY
    return {
        "requested_period": requested_period,
        "request_start_date": _optional_iso_date(
            payload.get("request_start_date")
            or parameter_overrides.get("SDate")
            or payload.get("start_date")
            or object_info.request_start_date
        ),
        "request_end_date": _optional_iso_date(
            payload.get("request_end_date")
            or parameter_overrides.get("EDate")
            or payload.get("end_date")
            or object_info.request_end_date
        ),
        "request_period": _optional_str(payload.get("request_period") or parameter_overrides.get("Period"))
        or FUNDAMENTALS_DEFAULT_REQUEST_PERIOD,
        "request_currency": _optional_str(
            payload.get("request_currency")
            or parameter_overrides.get("Curn")
            or payload.get("currency")
        ),
        "request_scale": _optional_int(
            payload.get("request_scale")
            or parameter_overrides.get("Scale")
            or payload.get("scale")
        ),
        "provider": _optional_str(payload.get("provider")),
        "source": _optional_str(payload.get("source")),
        "ric": _optional_str(payload.get("ric")),
        "primary_ric": _optional_str(payload.get("primary_ric") or payload.get("ric")),
        "organization_id": _optional_str(payload.get("organization_id")),
        "cik_number": _optional_str(payload.get("cik_number")),
        "source_uri": _optional_str(payload.get("source_uri")),
        "source_object_path": _optional_str(payload.get("source_object_path")),
        "source_dataset": _optional_str(payload.get("source_dataset")),
        "transform_name": _optional_str(payload.get("transform_name")),
        "transform_version": _optional_str(payload.get("transform_version")),
    }


def _fundamentals_row_base(
    *,
    workflow_id: str,
    universe_key: Optional[str],
    object_info: LegacyFundamentalsObjectInfo,
    request_context: Dict[str, Any],
) -> Dict[str, Any]:
    row = {
        "ticker": object_info.ticker,
        "universe_key": universe_key,
        "workflow_id": workflow_id,
        "workflow_run_id": workflow_id,
        "request_id": workflow_id,
        "source_system": "legacy_fundamentals_migration",
        "frequency": request_context["requested_period"],
        "requested_period": request_context["requested_period"],
        "request_start_date": request_context["request_start_date"],
        "request_end_date": request_context["request_end_date"],
        "request_period": request_context["request_period"],
    }
    for key in (
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


def flatten_legacy_payload(
    payload: Any,
    *,
    object_info: LegacyFundamentalsObjectInfo,
    workflow_id: str,
    universe_key: Optional[str],
) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    data_rows = payload.get("data")
    if not isinstance(data_rows, list):
        return []
    request_context = _fundamentals_request_context(payload, object_info)
    base = _fundamentals_row_base(
        workflow_id=workflow_id,
        universe_key=universe_key,
        object_info=object_info,
        request_context=request_context,
    )
    rows: List[Dict[str, Any]] = []
    for data_row in data_rows:
        if not isinstance(data_row, dict):
            continue
        period_end_date = _optional_iso_date(data_row.get("period_end_date"))
        if not period_end_date:
            continue
        row = dict(base)
        row.update(data_row)
        period_start_date = _optional_iso_date(row.get("period_start_date"))
        if period_start_date is not None:
            row["period_start_date"] = period_start_date
        row["period_end_date"] = period_end_date
        row["legacy_object_path"] = object_info.object_path
        row["legacy_generation"] = object_info.generation
        rows.append(row)
    return rows


def destination_object_path(
    *,
    layer: str,
    ticker: str,
    requested_period: str,
    end_date: str,
    workflow_id: str,
    gcs_prefix: str,
) -> str:
    return build_object_path(
        layer=layer,
        dataset="fundamentals",
        ticker=ticker,
        suffix=workflow_id,
        requested_period=requested_period,
        effective_end_date=end_date,
        prefix=gcs_prefix,
    )


def _ndjson_bytes(rows: Iterable[Dict[str, Any]]) -> bytes:
    payload = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    return payload.encode("utf-8")


def _local_manifest_path(*, migration_run_id: str) -> Path:
    return LOCAL_MANIFEST_DIR / f"{migration_run_id}.json"


def _legacy_prefix(*, gcs_prefix: str, layer: str, prefix: Optional[str]) -> str:
    parts = [p for p in [gcs_prefix, layer, "fundamentals", (prefix or "").strip("/")] if p]
    return str(PurePosixPath(*parts))


def _selected_layers(layer: str) -> List[str]:
    normalized = str(layer or "both").strip().lower()
    if normalized == "both":
        return list(LEGACY_LAYERS)
    if normalized not in LEGACY_LAYERS:
        raise ValueError("--layer must be one of source, prod, both")
    return [normalized]


def run_migration(*, prefix: Optional[str], dry_run: bool, layer: str, universe_key: Optional[str]) -> Dict[str, Any]:
    settings, client = _settings_client()
    bucket = client.bucket(settings.gcs_bucket)
    migration_run_id = datetime.now(timezone.utc).strftime("fundamentals_migration_%Y%m%dT%H%M%SZ")
    selected_layers = _selected_layers(layer)

    manifest: Dict[str, Any] = {
        "migration_run_id": migration_run_id,
        "bucket": settings.gcs_bucket,
        "gcs_prefix": settings.gcs_prefix,
        "dry_run": dry_run,
        "layer": layer,
        "universe_key": universe_key,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "mappings": [],
        "skipped": [],
        "failures": [],
    }

    for selected_layer in selected_layers:
        source_prefix = _legacy_prefix(gcs_prefix=settings.gcs_prefix, layer=selected_layer, prefix=prefix)
        for blob in client.list_blobs(settings.gcs_bucket, prefix=source_prefix):
            if not blob.name.endswith(".json"):
                continue
            try:
                info = parse_legacy_object_path(blob.name, str(blob.generation))
                workflow_id = legacy_workflow_id(info.object_path, info.generation)
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
                            "generation": info.generation,
                            "reason": "no valid rows with period_end_date",
                        }
                    )
                    continue

                grouped_rows: Dict[str, List[Dict[str, Any]]] = {}
                for row in rows:
                    grouped_rows.setdefault(str(row["period_end_date"]), []).append(row)

                for partition_end_date, partition_rows in sorted(grouped_rows.items()):
                    requested_period = str(partition_rows[0].get("requested_period") or FUNDAMENTALS_DEFAULT_FREQUENCY)
                    dest_path = destination_object_path(
                        layer=info.layer,
                        ticker=info.ticker,
                        requested_period=requested_period,
                        end_date=partition_end_date,
                        workflow_id=workflow_id,
                        gcs_prefix=settings.gcs_prefix,
                    )
                    mapping = {
                        "source_object_path": info.object_path,
                        "destination_object_path": dest_path,
                        "generation": info.generation,
                        "workflow_id": workflow_id,
                        "record_count": len(partition_rows),
                    }
                    if dry_run:
                        manifest["mappings"].append(mapping)
                        continue

                    destination_blob = bucket.blob(dest_path)
                    destination_blob.metadata = {
                        "layer": info.layer,
                        "dataset": "fundamentals",
                        "ticker": info.ticker,
                        "frequency": requested_period,
                        "end_date": partition_end_date,
                        "legacy_object_path": info.object_path,
                        "legacy_generation": info.generation,
                    }
                    try:
                        destination_blob.upload_from_string(
                            _ndjson_bytes(partition_rows),
                            content_type="application/x-ndjson",
                            if_generation_match=0,
                        )
                        manifest["mappings"].append(mapping)
                    except PreconditionFailed:
                        manifest["skipped"].append(
                            {
                                **mapping,
                                "reason": "destination already exists",
                            }
                        )
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to migrate fundamentals object %s", getattr(blob, "name", "<unknown>"))
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
    local_manifest_path = _local_manifest_path(migration_run_id=migration_run_id)
    local_manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    manifest["local_manifest_path"] = str(local_manifest_path)
    local_manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    return manifest


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Migrate legacy fundamentals artifacts into the lake-oriented layout.")
    parser.add_argument("--prefix", default=None, help="Optional ticker prefix to limit migration scope.")
    parser.add_argument("--dry-run", action="store_true", help="List planned migrations without writing new objects.")
    parser.add_argument(
        "--layer",
        default="both",
        choices=["source", "prod", "both"],
        help="Legacy layer scope to migrate.",
    )
    parser.add_argument("--universe-key", default=None, help="Optional universe_key to stamp into migrated rows.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level (default: INFO).")
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    manifest = run_migration(
        prefix=args.prefix,
        dry_run=args.dry_run,
        layer=args.layer,
        universe_key=args.universe_key,
    )
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return 0 if manifest["failure_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
