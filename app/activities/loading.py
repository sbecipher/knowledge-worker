import logging
from temporalio import activity
from google.cloud import bigquery  # type: ignore

from app.core.config import settings

logger = logging.getLogger(__name__)


@activity.defn
def update_knowledge_index(prod_gcs_uri: str) -> bool:
    """
    Loads the Parquet file from the Prod GCS bucket into the BigQuery dataset.
    """
    client = bigquery.Client(project=settings.BQ_PROJECT_ID)
    table_id = f"{settings.BQ_PROJECT_ID}.{settings.BQ_DATASET}.{settings.BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # BigQuery can automatically infer the schema from Parquet
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    logger.info(f"Loading data from {prod_gcs_uri} into {table_id}")

    load_job = client.load_table_from_uri(prod_gcs_uri, table_id, job_config=job_config)

    # Wait for the job to complete
    load_job.result()

    logger.info(f"Successfully loaded {load_job.output_rows} rows to {table_id}.")
    return True
