import logging

from temporalio import activity

from app.core.cloud_backends import get_storage_backend

logger = logging.getLogger(__name__)


@activity.defn
def promote_to_prod(stage_gcs_uri: str) -> str:
    """
    Promotes a processed Parquet file from its stage prefix to the matching prod prefix.
    Copies the blob then deletes the stage copy.

    Returns the prod GCS URI.
    """
    logger.info("Promoting %s", stage_gcs_uri)
    prod_gcs_uri = get_storage_backend().promote(stage_gcs_uri)
    logger.info("Successfully promoted to %s", prod_gcs_uri)
    return prod_gcs_uri
