import os
from google.cloud import bigquery
from app.core.config import settings


def run_bulk_load():
    client = bigquery.Client(project=settings.PROJECT_ID)
    table_id = f"{settings.PROJECT_ID}.{settings.BQ_DATASET}.{settings.BQ_TABLE}"

    # Load all parquet files from the partitioned prod bucket
    source_uri = f"gs://sbecipher-intelligence/prod/knowledge/v1/*"

    hive_partitioning_options = bigquery.HivePartitioningOptions()
    hive_partitioning_options.mode = "AUTO"
    hive_partitioning_options.source_uri_prefix = (
        f"gs://sbecipher-intelligence/prod/knowledge/v1"
    )

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        hive_partitioning=hive_partitioning_options,
    )

    print(f"Starting bulk load job from {source_uri} into {table_id}...")

    load_job = client.load_table_from_uri(source_uri, table_id, job_config=job_config)

    # Wait for the job to complete
    load_job.result()

    print(f"Successfully loaded {load_job.output_rows} rows into {table_id}.")


if __name__ == "__main__":
    run_bulk_load()
