import re
from google.cloud import bigquery
from google.cloud import storage  # type: ignore
from app.core.config import settings


def run_bulk_load():
    storage_client = storage.Client(project=settings.PROJECT_ID)
    bq_client = bigquery.Client(project=settings.PROJECT_ID)

    bucket_name = settings.PROD_BUCKET
    bucket = storage_client.bucket(bucket_name)

    # 1. Pre-processing: Move any files in prod/knowledge/*.parquet to stage/knowledge/
    print("Scanning for legacy files in prod/knowledge/ ...")
    blobs_in_prod = list(
        storage_client.list_blobs(bucket, prefix="prod/knowledge/", delimiter="/")
    )

    moved_count = 0
    for blob in blobs_in_prod:
        if blob.name.endswith(".parquet"):
            filename = blob.name.split("/")[-1]
            new_name = f"stage/knowledge/{filename}"
            print(f"Moving {blob.name} to {new_name}")
            bucket.copy_blob(blob, bucket, new_name)
            blob.delete()
            moved_count += 1

    if moved_count > 0:
        print(
            f"Moved {moved_count} legacy files from prod/knowledge/ to stage/knowledge/"
        )

    # 2. Check for files in stage/knowledge/
    print("Scanning for files in stage/knowledge/ ...")
    blobs_in_stage = list(storage_client.list_blobs(bucket, prefix="stage/knowledge/"))
    parquet_blobs = [b for b in blobs_in_stage if b.name.endswith(".parquet")]

    if not parquet_blobs:
        print("No files to process in stage/knowledge/")
        return

    print(f"Found {len(parquet_blobs)} files in stage/knowledge/ to process.")

    table_id = f"{settings.BQ_PROJECT_ID}.{settings.BQ_DATASET}.{settings.BQ_TABLE}"
    stage_table_id = f"{settings.BQ_PROJECT_ID}.{settings.BQ_DATASET}.documents_stage"
    source_uri = f"gs://{bucket_name}/stage/knowledge/*.parquet"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    print(f"Starting bulk load job from {source_uri} into {stage_table_id}...")
    load_job = bq_client.load_table_from_uri(
        source_uri, stage_table_id, job_config=job_config
    )
    load_job.result()
    print(f"Successfully loaded {load_job.output_rows} rows into {stage_table_id}.")

    merge_query = f"""
    MERGE `{table_id}` T
    USING `{stage_table_id}` S
    ON T.document_id = S.document_id
    WHEN MATCHED THEN
        UPDATE SET
            company_id = S.company_id,
            company_ticker = S.company_ticker,
            year = S.year,
            title = S.title,
            source_url = S.source_url,
            source_gcs_uri = S.source_gcs_uri,
            document_type = S.document_type,
            standard_features = S.standard_features,
            ingestion_timestamp = S.ingestion_timestamp,
            gemini_file_uri = S.gemini_file_uri
    WHEN NOT MATCHED THEN
        INSERT (document_id, company_id, company_ticker, year, title, source_url, source_gcs_uri, document_type, standard_features, ingestion_timestamp, gemini_file_uri)
        VALUES (S.document_id, S.company_id, S.company_ticker, S.year, S.title, S.source_url, S.source_gcs_uri, S.document_type, S.standard_features, S.ingestion_timestamp, S.gemini_file_uri)
    """

    print("Running MERGE query...")
    query_job = bq_client.query(merge_query)
    query_job.result()
    print("MERGE query completed successfully.")

    # 3. Archival Movement
    print("Archiving processed files...")
    year_re = re.compile(r"_(\d{4})_")
    archived_count = 0
    for blob in parquet_blobs:
        filename = blob.name.split("/")[-1]
        match = year_re.search(filename)
        if match:
            year = match.group(1)
        else:
            print(f"Warning: Could not parse year from {filename}, using 1970")
            year = "1970"

        new_name = f"prod/knowledge/v1/date={year}-12-31/{filename}"
        bucket.copy_blob(blob, bucket, new_name)
        blob.delete()
        archived_count += 1

    print(f"Successfully archived {archived_count} files to prod/knowledge/v1/")


if __name__ == "__main__":
    run_bulk_load()
