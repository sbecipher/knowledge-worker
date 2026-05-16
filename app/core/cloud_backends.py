from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Protocol

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage  # type: ignore

from app.core.config import settings

LOCAL_GCS_ROOT_DIRNAME = "gcs"
LOCAL_BIGQUERY_LOADS_FILENAME = "bigquery_loads.jsonl"


def local_fake_gcp_root() -> Path | None:
    raw = (settings.LOCAL_FAKE_GCP_ROOT or "").strip()
    if not raw:
        return None
    return Path(raw).expanduser().resolve()


def using_local_fake_gcp() -> bool:
    return local_fake_gcp_root() is not None


def parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Expected a gs:// URI, got {gcs_uri!r}")
    bucket_name, _, blob_name = gcs_uri[5:].partition("/")
    if not bucket_name or not blob_name:
        raise ValueError(f"Invalid GCS URI: {gcs_uri!r}")
    return bucket_name, blob_name


def _local_blob_path(root: Path, bucket_name: str, blob_name: str) -> Path:
    return root / LOCAL_GCS_ROOT_DIRNAME / bucket_name / Path(blob_name)


class StorageBackend(Protocol):
    def upload_bytes(
        self,
        bucket_name: str,
        blob_name: str,
        payload: bytes,
        *,
        content_type: str | None = None,
    ) -> str:
        ...

    def promote(self, stage_gcs_uri: str) -> str:
        ...


class BigQueryLoaderBackend(Protocol):
    def load_parquet(self, prod_gcs_uri: str, table_id: str) -> bool:
        ...


class GoogleCloudStorageBackend:
    def upload_bytes(
        self,
        bucket_name: str,
        blob_name: str,
        payload: bytes,
        *,
        content_type: str | None = None,
    ) -> str:
        client = storage.Client(project=settings.PROJECT_ID)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(payload, content_type=content_type)
        return f"gs://{bucket_name}/{blob_name}"

    def promote(self, stage_gcs_uri: str) -> str:
        bucket_name, stage_blob_name = parse_gcs_uri(stage_gcs_uri)
        prod_blob_name = stage_blob_name.replace(
            "stage/knowledge/",
            "prod/knowledge/",
            1,
        )
        if prod_blob_name == stage_blob_name:
            raise ValueError(
                f"Stage URI does not contain 'stage/knowledge/' prefix: {stage_gcs_uri}"
            )

        client = storage.Client(project=settings.PROJECT_ID)
        bucket = client.bucket(bucket_name)
        stage_blob = bucket.blob(stage_blob_name)
        prod_blob = bucket.blob(prod_blob_name)

        token, _, _ = prod_blob.rewrite(stage_blob)
        while token is not None:
            token, _, _ = prod_blob.rewrite(stage_blob, token=token)
        stage_blob.delete()
        return f"gs://{bucket_name}/{prod_blob_name}"


class LocalFilesystemStorageBackend:
    def __init__(self, root: Path) -> None:
        self.root = root

    def upload_bytes(
        self,
        bucket_name: str,
        blob_name: str,
        payload: bytes,
        *,
        content_type: str | None = None,
    ) -> str:
        target_path = _local_blob_path(self.root, bucket_name, blob_name)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(payload)
        return f"gs://{bucket_name}/{blob_name}"

    def promote(self, stage_gcs_uri: str) -> str:
        bucket_name, stage_blob_name = parse_gcs_uri(stage_gcs_uri)
        prod_blob_name = stage_blob_name.replace(
            "stage/knowledge/",
            "prod/knowledge/",
            1,
        )
        if prod_blob_name == stage_blob_name:
            raise ValueError(
                f"Stage URI does not contain 'stage/knowledge/' prefix: {stage_gcs_uri}"
            )

        source_path = _local_blob_path(self.root, bucket_name, stage_blob_name)
        dest_path = _local_blob_path(self.root, bucket_name, prod_blob_name)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, dest_path)
        source_path.unlink()
        return f"gs://{bucket_name}/{prod_blob_name}"


class GoogleBigQueryLoaderBackend:
    def load_parquet(self, prod_gcs_uri: str, table_id: str) -> bool:
        client = bigquery.Client(project=settings.BQ_PROJECT_ID)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )

        load_job = client.load_table_from_uri(
            prod_gcs_uri,
            table_id,
            job_config=job_config,
        )
        load_job.result()
        return True


class LocalBigQueryLoaderBackend:
    def __init__(self, root: Path) -> None:
        self.root = root

    def load_parquet(self, prod_gcs_uri: str, table_id: str) -> bool:
        bucket_name, blob_name = parse_gcs_uri(prod_gcs_uri)
        parquet_path = _local_blob_path(self.root, bucket_name, blob_name)
        dataframe = pd.read_parquet(parquet_path)
        rows = json.loads(dataframe.to_json(orient="records", date_format="iso"))
        log_path = self.root / LOCAL_BIGQUERY_LOADS_FILENAME
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "table_id": table_id,
                        "source_uri": prod_gcs_uri,
                        "row_count": len(rows),
                        "rows": rows,
                    },
                    sort_keys=True,
                )
            )
            handle.write("\n")
        return True


def get_storage_backend() -> StorageBackend:
    root = local_fake_gcp_root()
    if root is not None:
        return LocalFilesystemStorageBackend(root)
    return GoogleCloudStorageBackend()


def get_bigquery_loader_backend() -> BigQueryLoaderBackend:
    root = local_fake_gcp_root()
    if root is not None:
        return LocalBigQueryLoaderBackend(root)
    return GoogleBigQueryLoaderBackend()
