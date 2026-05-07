import logging
import httpx
from temporalio import activity
from google.cloud import storage

from app.models.payloads import KnowledgeDocument
from app.core.config import settings

logger = logging.getLogger(__name__)


@activity.defn
async def download_document_to_gcs(doc: KnowledgeDocument) -> str:
    """
    Downloads a document from the given URL and uploads it to the Source GCS bucket.
    """
    client = storage.Client(project=settings.PROJECT_ID)
    bucket = client.bucket(settings.SOURCE_BUCKET)

    # Generate GCS path
    # Path Convention: {company_ticker}/{year}/{title_slug}.{type}
    title_slug = "".join([c if c.isalnum() else "_" for c in doc.title]).strip("_")[
        :100
    ]
    gcs_blob_name = f"{doc.company_ticker}/{doc.year}/{title_slug}.{doc.type}"
    blob = bucket.blob(gcs_blob_name)

    logger.info(f"Downloading file from {doc.filepath}")
    # Download the file
    async with httpx.AsyncClient(timeout=60.0) as http_client:
        response = await http_client.get(str(doc.filepath), follow_redirects=True)
        response.raise_for_status()

        # Upload to GCS
        blob.upload_from_string(
            response.content,
            content_type=response.headers.get(
                "Content-Type", "application/octet-stream"
            ),
        )

    gcs_uri = f"gs://{settings.SOURCE_BUCKET}/{gcs_blob_name}"
    logger.info(f"Successfully uploaded {doc.filepath} to {gcs_uri}")
    return gcs_uri
