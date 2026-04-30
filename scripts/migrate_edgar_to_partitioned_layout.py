from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any, Dict, Optional

from google.api_core.exceptions import PreconditionFailed
from google.cloud.storage import Client as GCSClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import load_settings
from storage_utils import build_object_path, sanitize_path_segment

logger = logging.getLogger(__name__)

SOURCE_PREFIX = "source/edgar/"
LOCAL_MANIFEST_DIR = "/tmp"


def _settings_client() -> tuple[Any, GCSClient]:
    settings = load_settings()
    if not settings.gcs_bucket:
        raise ValueError("GCS_BUCKET must be set for EDGAR migration")
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


def _partition_date(blob: Any) -> str:
    timestamp = getattr(blob, "updated", None) or getattr(blob, "time_created", None) or datetime.now(timezone.utc)
    if isinstance(timestamp, datetime):
        return timestamp.astimezone(timezone.utc).date().isoformat()
    return datetime.now(timezone.utc).date().isoformat()


def _ticker_from_payload(payload: Dict[str, Any], object_name: str) -> str:
    ticker = _optional_str(payload.get("ticker"))
    tickers = payload.get("tickers")
    if not ticker and isinstance(tickers, list) and tickers:
        ticker = _optional_str(tickers[0])
    if not ticker:
        cik = _optional_str(payload.get("cik") or payload.get("cik_number"))
        if cik:
            ticker = f"CIK{str(cik).zfill(10)}"
    if not ticker:
        ticker = PurePosixPath(object_name).stem
    return sanitize_path_segment(ticker.upper())


def _legacy_suffix(object_name: str, generation: str) -> str:
    digest = hashlib.sha256(f"{object_name}:{generation}".encode("utf-8")).hexdigest()[:16]
    return f"legacy_{digest}"


def destination_object_path(*, payload: Dict[str, Any], object_name: str, generation: str, date: str, gcs_prefix: str) -> str:
    return build_object_path(
        layer="source",
        dataset="edgar",
        ticker=_ticker_from_payload(payload, object_name),
        suffix=_legacy_suffix(object_name, generation),
        date=date,
        prefix=gcs_prefix,
    )


def run_migration(*, dry_run: bool, delete_source: bool = False) -> Dict[str, Any]:
    settings, client = _settings_client()
    bucket = client.bucket(settings.gcs_bucket)
    source_prefix = str(PurePosixPath(settings.gcs_prefix, SOURCE_PREFIX)) if settings.gcs_prefix else SOURCE_PREFIX
    migration_id = datetime.now(timezone.utc).strftime("edgar_layout_migration_%Y%m%dT%H%M%SZ")
    manifest: Dict[str, Any] = {
        "migration_id": migration_id,
        "bucket": settings.gcs_bucket,
        "source_prefix": source_prefix,
        "dry_run": dry_run,
        "delete_source": delete_source,
        "mappings": [],
        "skipped": [],
        "failures": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
    }

    for blob in client.list_blobs(settings.gcs_bucket, prefix=source_prefix):
        object_path = str(blob.name)
        relative_parts = PurePosixPath(object_path).parts
        if not object_path.endswith(".json") or len(relative_parts) != len(PurePosixPath(source_prefix).parts) + 1:
            continue
        try:
            payload = json.loads(blob.download_as_bytes().decode("utf-8"))
            if not isinstance(payload, dict):
                manifest["skipped"].append({"source_object_path": object_path, "reason": "payload is not a JSON object"})
                continue
            date = _partition_date(blob)
            dest_path = destination_object_path(
                payload=payload,
                object_name=object_path,
                generation=str(blob.generation),
                date=date,
                gcs_prefix=settings.gcs_prefix,
            )
            mapping = {
                "source_object_path": object_path,
                "destination_object_path": dest_path,
                "generation": str(blob.generation),
                "date": date,
                "ticker": _ticker_from_payload(payload, object_path),
            }
            if dry_run:
                manifest["mappings"].append(mapping)
                continue

            destination_blob = bucket.blob(dest_path)
            destination_blob.content_type = "application/json"
            destination_blob.metadata = {
                "layer": "source",
                "dataset": "edgar",
                "date": date,
                "ticker": mapping["ticker"],
                "source_object_path": object_path,
                "source_generation": str(blob.generation),
                "migration_id": migration_id,
            }
            try:
                destination_blob.upload_from_string(
                    json.dumps(payload, ensure_ascii=False),
                    content_type="application/json",
                    if_generation_match=0,
                )
                if delete_source:
                    blob.delete(if_generation_match=blob.generation)
                manifest["mappings"].append(mapping)
            except PreconditionFailed:
                manifest["skipped"].append({**mapping, "reason": "destination already exists"})
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to migrate EDGAR object %s", object_path)
            manifest["failures"].append(
                {
                    "source_object_path": object_path,
                    "generation": str(getattr(blob, "generation", "")),
                    "error": str(exc),
                }
            )

    manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
    manifest["migrated_count"] = len(manifest["mappings"])
    manifest["skipped_count"] = len(manifest["skipped"])
    manifest["failure_count"] = len(manifest["failures"])

    manifest_path = PurePosixPath(LOCAL_MANIFEST_DIR, f"{migration_id}.json")
    with open(str(manifest_path), "w", encoding="utf-8") as fp:
        json.dump(manifest, fp, indent=2, sort_keys=True)
    manifest["local_manifest_path"] = str(manifest_path)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate flat source/edgar JSON objects into the partitioned lake layout.")
    parser.add_argument("--dry-run", action="store_true", help="Preview object mappings without writing destinations.")
    parser.add_argument("--delete-source", action="store_true", help="Delete flat source objects after a successful copy.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    manifest = run_migration(dry_run=args.dry_run, delete_source=args.delete_source)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 1 if manifest["failure_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
