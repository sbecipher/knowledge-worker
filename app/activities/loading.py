import logging

from temporalio import activity

from app.core.cloud_backends import get_bigquery_loader_backend
from app.core.config import settings

logger = logging.getLogger(__name__)


def _load_parquet_into_table(prod_gcs_uri: str, table_id: str) -> bool:
    logger.info(
        "Loading %s into %s",
        prod_gcs_uri,
        table_id,
    )
    get_bigquery_loader_backend().load_parquet(prod_gcs_uri, table_id)
    logger.info("Successfully loaded %s into %s", prod_gcs_uri, table_id)
    return True


@activity.defn
def update_knowledge_index(prod_gcs_uri: str) -> bool:
    """
    Loads a processed Parquet file from GCS into the BigQuery knowledge table.
    Uses WRITE_APPEND so each document is added incrementally.
    """
    table_id = f"{settings.BQ_PROJECT_ID}.{settings.BQ_DATASET}.{settings.BQ_TABLE}"
    return _load_parquet_into_table(prod_gcs_uri, table_id)


@activity.defn
def update_company_metadata_index(prod_gcs_uri: str) -> bool:
    """
    Loads a company metadata Parquet artifact into the dedicated BigQuery table.
    """
    table_id = (
        f"{settings.BQ_PROJECT_ID}.{settings.BQ_DATASET}."
        f"{settings.BQ_COMPANY_METADATA_TABLE}"
    )
    return _load_parquet_into_table(prod_gcs_uri, table_id)
