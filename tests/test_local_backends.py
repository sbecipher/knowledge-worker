from __future__ import annotations

import io
import json

import pandas as pd

from app.core.cloud_backends import (
    LOCAL_BIGQUERY_LOADS_FILENAME,
    LocalBigQueryLoaderBackend,
    LocalFilesystemStorageBackend,
)


def test_local_storage_backend_promotes_stage_to_prod(tmp_path) -> None:
    backend = LocalFilesystemStorageBackend(tmp_path)
    stage_uri = backend.upload_bytes(
        "integration-bucket",
        "stage/knowledge/company_metadata/provider=lseg/ticker=MDET/year=2026/doc.parquet",
        b"metadata",
        content_type="application/octet-stream",
    )

    prod_uri = backend.promote(stage_uri)

    assert (
        tmp_path
        / "gcs"
        / "integration-bucket"
        / "prod/knowledge/company_metadata/provider=lseg/ticker=MDET/year=2026/doc.parquet"
    ).read_bytes() == b"metadata"
    assert not (
        tmp_path
        / "gcs"
        / "integration-bucket"
        / "stage/knowledge/company_metadata/provider=lseg/ticker=MDET/year=2026/doc.parquet"
    ).exists()
    assert (
        prod_uri
        == "gs://integration-bucket/prod/knowledge/company_metadata/provider=lseg/ticker=MDET/year=2026/doc.parquet"
    )


def test_local_bigquery_loader_records_rows(tmp_path) -> None:
    storage_backend = LocalFilesystemStorageBackend(tmp_path)
    dataframe = pd.DataFrame(
        [
            {
                "company_ticker": "MDET",
                "provider": "lseg",
                "source_snapshot_date": "2026-05-15",
                "metadata_json": json.dumps({"sector": "Industrials"}),
            }
        ]
    )
    parquet_buffer = io.BytesIO()
    dataframe.to_parquet(parquet_buffer, index=False)
    parquet_uri = storage_backend.upload_bytes(
        "integration-bucket",
        "stage/knowledge/company_metadata/provider=lseg/ticker=MDET/year=2026/doc.parquet",
        parquet_buffer.getvalue(),
    )

    loader = LocalBigQueryLoaderBackend(tmp_path)
    result = loader.load_parquet(
        parquet_uri,
        "sbecipherio.knowledge.company_metadata",
    )

    assert result is True
    records = [
        json.loads(line)
        for line in (tmp_path / LOCAL_BIGQUERY_LOADS_FILENAME)
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    assert records == [
        {
            "row_count": 1,
            "rows": [
                {
                    "company_ticker": "MDET",
                    "metadata_json": '{"sector": "Industrials"}',
                    "provider": "lseg",
                    "source_snapshot_date": "2026-05-15",
                }
            ],
            "source_uri": parquet_uri,
            "table_id": "sbecipherio.knowledge.company_metadata",
        }
    ]
