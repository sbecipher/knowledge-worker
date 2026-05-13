import logging

from google.cloud import bigquery
from temporalio import activity

from app.core.config import settings

logger = logging.getLogger(__name__)


@activity.defn
def update_knowledge_index(prod_gcs_uri: str) -> bool:
    """
    Loads a processed Parquet file from GCS into the BigQuery knowledge table.
    Uses WRITE_APPEND so each document is added incrementally.
    """
    client = bigquery.Client(project=settings.BQ_PROJECT_ID)
    table_id = f"{settings.BQ_PROJECT_ID}.{settings.BQ_DATASET}.{settings.BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    logger.info(
        "Loading %s into %s",
        prod_gcs_uri,
        table_id,
    )

    load_job = client.load_table_from_uri(
        prod_gcs_uri,
        table_id,
        job_config=job_config,
    )
    load_job.result()

    logger.info(
        "Successfully loaded %s rows into %s",
        load_job.output_rows,
        table_id,
    )
    return True
