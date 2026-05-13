import logging

from google.cloud import storage  # type: ignore
from temporalio import activity

from app.core.config import settings

logger = logging.getLogger(__name__)


def _parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Expected a gs:// URI, got {gcs_uri!r}")
    bucket_name, _, blob_name = gcs_uri[5:].partition("/")
    if not bucket_name or not blob_name:
        raise ValueError(f"Invalid GCS URI: {gcs_uri!r}")
    return bucket_name, blob_name


@activity.defn
def promote_to_prod(stage_gcs_uri: str) -> str:
    """
    Promotes a processed Parquet file from stage/knowledge/ to prod/knowledge/.
    Copies the blob then deletes the stage copy.

    Returns the prod GCS URI.
    """
    bucket_name, stage_blob_name = _parse_gcs_uri(stage_gcs_uri)

    # stage/knowledge/{doc_id}.parquet → prod/knowledge/{doc_id}.parquet
    prod_blob_name = stage_blob_name.replace("stage/knowledge/", "prod/knowledge/", 1)

    if prod_blob_name == stage_blob_name:
        raise ValueError(
            f"Stage URI does not contain 'stage/knowledge/' prefix: {stage_gcs_uri}"
        )

    client = storage.Client(project=settings.PROJECT_ID)
    bucket = client.bucket(bucket_name)

    stage_blob = bucket.blob(stage_blob_name)
    prod_blob = bucket.blob(prod_blob_name)

    # Copy stage → prod
    logger.info("Promoting %s → gs://%s/%s", stage_gcs_uri, bucket_name, prod_blob_name)

    token, _, _ = prod_blob.rewrite(stage_blob)
    while token is not None:
        token, _, _ = prod_blob.rewrite(stage_blob, token=token)

    # Delete the stage copy
    stage_blob.delete()

    prod_gcs_uri = f"gs://{bucket_name}/{prod_blob_name}"
    logger.info("Successfully promoted to %s", prod_gcs_uri)
    return prod_gcs_uri
