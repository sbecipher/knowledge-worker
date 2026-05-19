from __future__ import annotations

import argparse
import io
import json
from collections import defaultdict
from datetime import date
from pathlib import PurePosixPath
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

DEFAULT_PROJECT = "sbecipherio"
DEFAULT_DATASET = "knowledge"
DEFAULT_SOURCE_TABLE = "documents"
DEFAULT_TARGET_TABLE = "edgar"
DEFAULT_BUCKET = "sbecipher-intelligence"


EDGAR_EXTRA_FIELDS = [
    bigquery.SchemaField("source_kind", "STRING"),
    bigquery.SchemaField("filing_type", "STRING"),
    bigquery.SchemaField("filing_date", "STRING"),
    bigquery.SchemaField("report_date", "STRING"),
    bigquery.SchemaField("accession_number", "STRING"),
    bigquery.SchemaField("primary_document", "STRING"),
    bigquery.SchemaField("cik", "STRING"),
]
PARQUET_STRING_COLUMNS = {
    "document_id",
    "company_id",
    "company_ticker",
    "title",
    "source_url",
    "source_gcs_uri",
    "gemini_file_uri",
    "document_type",
    "standard_features",
    "source_kind",
    "filing_type",
    "filing_date",
    "report_date",
    "accession_number",
    "primary_document",
    "cik",
}


def _parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// URI, got {gcs_uri!r}")
    bucket_name, _, blob_name = gcs_uri[5:].partition("/")
    if not bucket_name or not blob_name:
        raise ValueError(f"Invalid GCS URI: {gcs_uri!r}")
    return bucket_name, blob_name


def _field_names(schema: Iterable[bigquery.SchemaField]) -> set[str]:
    return {field.name for field in schema}


def _get_table_or_none(client: bigquery.Client, table_id: str) -> bigquery.Table | None:
    try:
        return client.get_table(table_id)
    except NotFound:
        return None


def _ensure_edgar_table(
    client: bigquery.Client,
    source_table_id: str,
    target_table_id: str,
) -> bigquery.Table:
    existing = _get_table_or_none(client, target_table_id)
    if existing is not None:
        return existing

    source_table = client.get_table(source_table_id)
    schema = [
        field for field in source_table.schema if field.name != "__index_level_0__"
    ]
    names = _field_names(schema)
    for field in EDGAR_EXTRA_FIELDS:
        if field.name not in names:
            schema.append(field)

    table = bigquery.Table(target_table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date",
    )
    table.clustering_fields = ["company_ticker", "document_type"]
    return client.create_table(table)


def _edgar_document_query(
    source_table_id: str,
    *,
    source_bucket: str,
    ticker: str | None,
    partition_date: str | None,
    limit: int | None,
) -> tuple[str, bigquery.QueryJobConfig]:
    parameters: list[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter(
            "legacy_source_prefix",
            "STRING",
            f"gs://{source_bucket}/source/knowledge/",
        )
    ]
    filters = [
        "("
        "REGEXP_CONTAINS(LOWER(COALESCE(source_url, '')), "
        r"r'sec\.gov/(archives/edgar|ixviewer)') "
        "OR REGEXP_CONTAINS(LOWER(COALESCE(title, '')), "
        r"r'(10-k|10-q|8-k|def 14a|20-f|40-f|s-1)') "
        "OR REGEXP_CONTAINS(LOWER(COALESCE(source_gcs_uri, '')), "
        r"r'(10-k|10-q|8-k|def 14a|20-f|40-f|s-1)')"
        ")",
        "STARTS_WITH(COALESCE(source_gcs_uri, ''), @legacy_source_prefix)",
    ]
    if ticker:
        filters.append("UPPER(company_ticker) = @ticker")
        parameters.append(
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker.upper())
        )
    if partition_date:
        filters.append("date = @date")
        parameters.append(bigquery.ScalarQueryParameter("date", "DATE", partition_date))

    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT @limit"
        parameters.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))

    query = f"""
    SELECT * EXCEPT(__index_level_0__)
    FROM `{source_table_id}`
    WHERE {' AND '.join(filters)}
    ORDER BY company_ticker, date, document_id
    {limit_sql}
    """
    return query, bigquery.QueryJobConfig(query_parameters=parameters)


def _fetch_candidate_rows(
    client: bigquery.Client,
    source_table_id: str,
    *,
    source_bucket: str,
    ticker: str | None,
    partition_date: str | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    query, job_config = _edgar_document_query(
        source_table_id,
        source_bucket=source_bucket,
        ticker=ticker,
        partition_date=partition_date,
        limit=limit,
    )
    return [
        dict(row.items()) for row in client.query(query, job_config=job_config).result()
    ]


def _row_date(row: dict[str, Any]) -> str:
    value = row.get("date")
    if value is not None and hasattr(value, "isoformat"):
        return value.isoformat()
    if value:
        return str(value)
    year = int(row.get("year") or date.today().year)
    return f"{year}-12-31"


def _source_extension(row: dict[str, Any]) -> str:
    source_uri = str(row.get("source_gcs_uri") or "")
    if source_uri:
        suffix = PurePosixPath(source_uri).suffix.lstrip(".").lower()
        if suffix:
            return suffix
    document_type = str(row.get("document_type") or "").strip().lower()
    return "pdf" if document_type == "pdf" else "html"


def _target_source_blob(row: dict[str, Any]) -> str:
    ticker = str(row.get("company_ticker") or "").strip().upper()
    document_id = str(row["document_id"])
    return (
        f"source/edgar/{ticker}/{_row_date(row)}/"
        f"{document_id}.{_source_extension(row)}"
    )


def _target_prod_blob(row: dict[str, Any]) -> str:
    return f"prod/edgar/v1/date={_row_date(row)}/{row['document_id']}.parquet"


def _normalize_row_for_edgar(
    row: dict[str, Any],
    *,
    source_gcs_uri: str,
) -> dict[str, Any]:
    normalized = dict(row)
    normalized["source_gcs_uri"] = source_gcs_uri
    normalized["source_kind"] = "edgar"
    if isinstance(normalized.get("gemini_chunk_uris"), str):
        try:
            normalized["gemini_chunk_uris"] = json.loads(
                normalized["gemini_chunk_uris"]
            )
        except json.JSONDecodeError:
            normalized["gemini_chunk_uris"] = []
    normalized["date"] = date.fromisoformat(_row_date(normalized))
    return normalized


def _move_source_blob(
    storage_client: storage.Client,
    source_gcs_uri: str,
    target_bucket_name: str,
    target_blob_name: str,
    *,
    dry_run: bool,
    allow_missing_source: bool,
) -> bool:
    if not source_gcs_uri:
        if allow_missing_source:
            return False
        raise ValueError("source_gcs_uri is empty")

    source_bucket_name, source_blob_name = _parse_gcs_uri(source_gcs_uri)
    target_gcs_uri = f"gs://{target_bucket_name}/{target_blob_name}"
    if source_gcs_uri == target_gcs_uri:
        return True
    if dry_run:
        return True

    target_bucket = storage_client.bucket(target_bucket_name)
    target_blob = target_bucket.blob(target_blob_name)
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    if not source_blob.exists():
        if target_blob.exists():
            return True
        if allow_missing_source:
            return False
        raise FileNotFoundError(source_gcs_uri)

    token, _, _ = target_blob.rewrite(source_blob)
    while token is not None:
        token, _, _ = target_blob.rewrite(source_blob, token=token)
    source_blob.delete()
    return True


def _upload_prod_parquet(
    storage_client: storage.Client,
    bucket_name: str,
    row: dict[str, Any],
    *,
    dry_run: bool,
) -> str:
    blob_name = _target_prod_blob(row)
    if dry_run:
        return f"gs://{bucket_name}/{blob_name}"

    parquet_buffer = io.BytesIO()
    df = pd.DataFrame([row])
    if "ingestion_timestamp" in df.columns:
        df["ingestion_timestamp"] = pd.to_datetime(
            df["ingestion_timestamp"], utc=True
        ).astype("datetime64[us, UTC]")
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for column in PARQUET_STRING_COLUMNS.intersection(df.columns):
        df[column] = df[column].astype("string")
    table = pa.Table.from_pandas(df, preserve_index=False)
    if "gemini_chunk_uris" in df.columns:
        chunk_uris = pa.array(
            df["gemini_chunk_uris"].tolist(),
            type=pa.list_(pa.string()),
        )
        column_index = table.schema.get_field_index("gemini_chunk_uris")
        table = table.set_column(column_index, "gemini_chunk_uris", chunk_uris)
    pq.write_table(table, parquet_buffer)

    blob = storage_client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_string(
        parquet_buffer.getvalue(), content_type="application/octet-stream"
    )
    return f"gs://{bucket_name}/{blob_name}"


def _load_edgar_table(
    client: bigquery.Client,
    parquet_uris: list[str],
    target_table_id: str,
) -> None:
    if not parquet_uris:
        return
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    parquet_options = bigquery.ParquetOptions()
    parquet_options.enable_list_inference = True
    job_config.parquet_options = parquet_options
    job = client.load_table_from_uri(
        parquet_uris, target_table_id, job_config=job_config
    )
    job.result()


def migrate_edgar_layout(args: argparse.Namespace) -> int:
    bq_client = bigquery.Client(project=args.project)
    storage_client = storage.Client(project=args.gcp_project or args.project)
    source_table_id = f"{args.project}.{args.dataset}.{args.source_table}"
    target_table_id = f"{args.project}.{args.dataset}.{args.target_table}"

    rows = _fetch_candidate_rows(
        bq_client,
        source_table_id,
        source_bucket=args.source_bucket,
        ticker=args.ticker,
        partition_date=args.date,
        limit=args.limit,
    )
    print(f"candidate_rows={len(rows)}")
    if not rows:
        return 0

    if args.execute:
        _ensure_edgar_table(bq_client, source_table_id, target_table_id)
    else:
        print("dry_run=true")

    parquet_uris: list[str] = []
    planned_by_date: dict[str, int] = defaultdict(int)
    moved_sources = 0
    missing_sources = 0
    for row in rows:
        target_source_blob = _target_source_blob(row)
        target_source_uri = f"gs://{args.source_bucket}/{target_source_blob}"
        planned_by_date[_row_date(row)] += 1
        if not args.skip_gcs:
            moved = _move_source_blob(
                storage_client,
                str(row.get("source_gcs_uri") or ""),
                args.source_bucket,
                target_source_blob,
                dry_run=not args.execute,
                allow_missing_source=args.allow_missing_source,
            )
            if moved:
                moved_sources += 1
            else:
                missing_sources += 1
            normalized_row = _normalize_row_for_edgar(
                row,
                source_gcs_uri=target_source_uri,
            )
            parquet_uris.append(
                _upload_prod_parquet(
                    storage_client,
                    args.prod_bucket,
                    normalized_row,
                    dry_run=not args.execute,
                )
            )
        else:
            parquet_uris.append(f"gs://{args.prod_bucket}/{_target_prod_blob(row)}")

    for partition_date, count in sorted(planned_by_date.items()):
        print(f"date={partition_date} rows={count}")
    source_move_label = "source_moves" if args.execute else "planned_source_moves"
    print(f"{source_move_label}={moved_sources} missing_sources={missing_sources}")
    print(f"prod_parquet_uris={len(parquet_uris)}")

    if args.execute and not args.skip_bq_load:
        _load_edgar_table(bq_client, parquet_uris, target_table_id)
        target_count = next(
            iter(
                bq_client.query(
                    f"SELECT COUNT(*) AS row_count FROM `{target_table_id}`"
                ).result()
            )
        ).row_count
        print(f"target_table={target_table_id} rows={target_count}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Move EDGAR KnowledgeFlow documents into source/prod/BQ EDGAR layout."
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--gcp-project", default=None)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE)
    parser.add_argument("--target-table", default=DEFAULT_TARGET_TABLE)
    parser.add_argument("--source-bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prod-bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--ticker", default=None)
    parser.add_argument("--date", default=None, help="Optional YYYY-MM-DD date filter.")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--skip-bq-load", action="store_true")
    parser.add_argument("--skip-gcs", action="store_true")
    parser.add_argument("--allow-missing-source", action="store_true")
    return migrate_edgar_layout(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
