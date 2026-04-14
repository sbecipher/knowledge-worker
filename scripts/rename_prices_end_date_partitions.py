from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional
from uuid import uuid4

from google.api_core.exceptions import NotFound
from google.cloud import storage_control_v2
from google.cloud.storage import Client as GCSClient
from google.oauth2 import service_account

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import load_settings
from storage_utils import format_iso_date

logger = logging.getLogger(__name__)

LOCAL_MANIFEST_DIR = Path("/tmp")
RESOURCE_PROJECT = "_"
PRICES_PARTITION_ROOT = PurePosixPath("source", "prices", "granularity=day")


@dataclass(frozen=True)
class LegacyPricePartitionFolder:
    source_folder_id: str
    date: str


def _settings_clients() -> tuple[Any, GCSClient, storage_control_v2.StorageControlClient]:
    settings = load_settings()
    if not settings.gcs_bucket:
        raise ValueError("GCS_BUCKET must be set for the prices partition rename migration")

    if settings.gcs_service_account_key_json:
        info = json.loads(settings.gcs_service_account_key_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        storage_client = GCSClient(credentials=credentials, project=info.get("project_id"))
        control_client = storage_control_v2.StorageControlClient(credentials=credentials)
    else:
        storage_client = GCSClient()
        control_client = storage_control_v2.StorageControlClient()
    return settings, storage_client, control_client


def _bucket_resource_name(bucket_name: str) -> str:
    return f"projects/{RESOURCE_PROJECT}/buckets/{bucket_name}"


def _folder_resource_name(bucket_name: str, folder_id: str) -> str:
    return f"{_bucket_resource_name(bucket_name)}/folders/{folder_id}"


def _folder_id_from_resource_name(resource_name: str) -> str:
    marker = "/folders/"
    if marker not in resource_name:
        raise ValueError(f"Unsupported folder resource name: {resource_name}")
    return resource_name.split(marker, 1)[1]


def _prices_partition_root(*, gcs_prefix: str) -> str:
    parts = [p for p in [gcs_prefix.strip("/"), str(PRICES_PARTITION_ROOT)] if p]
    return f"{PurePosixPath(*parts)}/"


def _local_manifest_path(*, migration_run_id: str) -> Path:
    return LOCAL_MANIFEST_DIR / f"{migration_run_id}.json"


def _optional_iso_date(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return format_iso_date(text)


def _selected_date(*, date: str, start_date: Optional[str], end_date: Optional[str]) -> bool:
    if start_date and date < start_date:
        return False
    if end_date and date > end_date:
        return False
    return True


def _ensure_hns_enabled(storage_client: GCSClient, bucket_name: str) -> None:
    bucket = storage_client.bucket(bucket_name)
    bucket.reload()
    enabled = bool(bucket._properties.get("hierarchicalNamespace", {}).get("enabled"))
    if not enabled:
        raise ValueError(f"Bucket {bucket_name} does not have hierarchical namespace enabled")


def parse_legacy_partition_folder(folder_id: str) -> LegacyPricePartitionFolder:
    normalized = str(folder_id).strip("/")
    path = PurePosixPath(normalized)
    parts = path.parts
    try:
        source_index = parts.index("source")
    except ValueError as exc:
        raise ValueError(f"Folder path is missing source segment: {folder_id}") from exc
    if len(parts) != source_index + 4:
        raise ValueError(f"Unsupported prices partition folder depth: {folder_id}")
    if parts[source_index + 1] != "prices" or parts[source_index + 2] != "granularity=day":
        raise ValueError(f"Unsupported prices partition folder path: {folder_id}")
    partition_part = parts[source_index + 3]
    if not partition_part.startswith("end_date="):
        raise ValueError(f"Folder is not an end_date partition: {folder_id}")
    partition_date = format_iso_date(partition_part.split("=", 1)[1])
    return LegacyPricePartitionFolder(source_folder_id=f"{normalized}/", date=partition_date)


def destination_folder_id(partition: LegacyPricePartitionFolder) -> str:
    path = PurePosixPath(partition.source_folder_id.rstrip("/"))
    parts = list(path.parts)
    parts[-1] = f"date={partition.date}"
    return f"{PurePosixPath(*parts)}/"


def _destination_exists(
    control_client: storage_control_v2.StorageControlClient,
    *,
    bucket_name: str,
    folder_id: str,
) -> bool:
    try:
        control_client.get_folder(name=_folder_resource_name(bucket_name, folder_id))
        return True
    except NotFound:
        return False


def run_migration(
    *,
    dry_run: bool,
    start_date: Optional[str],
    end_date: Optional[str],
    limit: Optional[int],
) -> Dict[str, Any]:
    settings, storage_client, control_client = _settings_clients()
    _ensure_hns_enabled(storage_client, settings.gcs_bucket)

    normalized_start_date = _optional_iso_date(start_date)
    normalized_end_date = _optional_iso_date(end_date)
    if normalized_start_date and normalized_end_date and normalized_start_date > normalized_end_date:
        raise ValueError("--start-date must be on or before --end-date")

    bucket_resource_name = _bucket_resource_name(settings.gcs_bucket)
    root_prefix = _prices_partition_root(gcs_prefix=settings.gcs_prefix)
    migration_run_id = datetime.now(timezone.utc).strftime("prices_partition_rename_%Y%m%dT%H%M%SZ")

    manifest: Dict[str, Any] = {
        "migration_run_id": migration_run_id,
        "bucket": settings.gcs_bucket,
        "bucket_resource_name": bucket_resource_name,
        "gcs_prefix": settings.gcs_prefix,
        "source_prefix": root_prefix,
        "dry_run": dry_run,
        "start_date": normalized_start_date,
        "end_date": normalized_end_date,
        "limit": limit,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "mappings": [],
        "skipped": [],
        "failures": [],
    }

    request = storage_control_v2.ListFoldersRequest(
        parent=bucket_resource_name,
        prefix=root_prefix,
        delimiter="/",
    )
    selected_count = 0
    executed_count = 0

    for folder in control_client.list_folders(request=request):
        folder_id = _folder_id_from_resource_name(folder.name)
        if folder_id == root_prefix:
            continue
        if folder_id.rstrip("/").split("/")[-1].startswith("date="):
            manifest["skipped"].append(
                {
                    "folder_path": folder_id,
                    "reason": "already_uses_date_partition",
                }
            )
            continue
        try:
            partition = parse_legacy_partition_folder(folder_id)
        except ValueError as exc:
            manifest["skipped"].append(
                {
                    "folder_path": folder_id,
                    "reason": "unsupported_partition_folder",
                    "details": str(exc),
                }
            )
            continue

        if not _selected_date(date=partition.date, start_date=normalized_start_date, end_date=normalized_end_date):
            continue

        selected_count += 1
        destination_id = destination_folder_id(partition)
        mapping = {
            "date": partition.date,
            "source_folder_path": partition.source_folder_id,
            "destination_folder_path": destination_id,
        }

        if dry_run:
            manifest["mappings"].append(mapping)
        else:
            try:
                if _destination_exists(control_client, bucket_name=settings.gcs_bucket, folder_id=destination_id):
                    manifest["skipped"].append({**mapping, "reason": "destination_exists"})
                else:
                    logger.info("renaming folder %s -> %s", partition.source_folder_id, destination_id)
                    rename_request = storage_control_v2.RenameFolderRequest(
                        name=_folder_resource_name(settings.gcs_bucket, partition.source_folder_id),
                        destination_folder_id=destination_id,
                        request_id=str(uuid4()),
                    )
                    operation = control_client.rename_folder(request=rename_request)
                    response = operation.result()
                    executed_count += 1
                    manifest["mappings"].append(
                        {
                            **mapping,
                            "operation_name": getattr(getattr(operation, "operation", None), "name", None),
                            "renamed_folder_resource": getattr(response, "name", None),
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to rename prices partition folder %s", partition.source_folder_id)
                manifest["failures"].append({**mapping, "error": str(exc)})

        if limit is not None and selected_count >= limit:
            break

    manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
    manifest["selected_count"] = selected_count
    manifest["mapping_count"] = len(manifest["mappings"])
    manifest["executed_count"] = executed_count
    manifest["skipped_count"] = len(manifest["skipped"])
    manifest["failure_count"] = len(manifest["failures"])
    LOCAL_MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    local_manifest_path = _local_manifest_path(migration_run_id=migration_run_id)
    manifest["local_manifest_path"] = str(local_manifest_path)
    local_manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Rename HNS prices partition folders from "
            "source/prices/granularity=day/end_date=YYYY-MM-DD/ to "
            "source/prices/granularity=day/date=YYYY-MM-DD/."
        )
    )
    parser.add_argument("--dry-run", action="store_true", help="List planned folder renames without executing them.")
    parser.add_argument("--start-date", default=None, help="Optional inclusive lower bound for partition dates.")
    parser.add_argument("--end-date", default=None, help="Optional inclusive upper bound for partition dates.")
    parser.add_argument("--limit", type=int, default=None, help="Optional maximum number of partition folders to process.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level (default: INFO).")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO), format="%(levelname)s %(message)s")
    manifest = run_migration(
        dry_run=args.dry_run,
        start_date=args.start_date,
        end_date=args.end_date,
        limit=args.limit,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
