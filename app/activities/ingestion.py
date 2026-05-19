import logging
import httpx
from google.cloud import storage  # type: ignore
from temporalio import activity
from temporalio.exceptions import ApplicationError
from app.models.payloads import KnowledgeDocument
from app.core.config import settings
from app.core.knowledge_api import knowledge_api_headers
from app.utils.document_layout import edgar_source_blob_name, is_edgar_document

logger = logging.getLogger(__name__)


def _parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    if not gcs_uri.startswith("gs://"):
        raise ApplicationError(
            f"Expected a gs:// URI, got {gcs_uri!r}",
            non_retryable=True,
        )
    bucket_name, _, blob_name = gcs_uri[5:].partition("/")
    if not bucket_name or not blob_name:
        raise ApplicationError(f"Invalid GCS URI: {gcs_uri!r}", non_retryable=True)
    return bucket_name, blob_name


@activity.defn
async def download_document_to_gcs(doc: KnowledgeDocument) -> str:
    """
    Calls the KnowledgeIO API to download a document via Selenium and upload it to the Source GCS bucket.
    """
    api_url = f"{settings.KNOWLEDGEIO_API_URL.rstrip('/')}/api/v1/scrape/url"

    payload = {
        "year": doc.year,
        "url": str(doc.url),
        "title": doc.title,
        "article_type": doc.type,
        "base_url": doc.base_url,
        "company_name": doc.company_name,
        "company_ticker": doc.company_ticker,
        "company_id": doc.company_id,
    }

    logger.info(f"Calling KnowledgeIO API to download file from {doc.url}")

    async with httpx.AsyncClient(timeout=300.0) as http_client:
        response = await http_client.post(
            api_url,
            json=payload,
            headers=knowledge_api_headers(),
        )
        response.raise_for_status()

        result = response.json()
        gcs_uri = result.get("gcs_uri")
        if not gcs_uri:
            raise ValueError(f"KnowledgeIO API did not return a gcs_uri: {result}")

    logger.info(f"Successfully scraped and uploaded to {gcs_uri}")
    return gcs_uri


@activity.defn
def relocate_edgar_source_to_gcs_layout(
    doc: KnowledgeDocument, source_gcs_uri: str
) -> str:
    """
    Moves a downloaded EDGAR source file into the canonical source/edgar layout.
    """
    if not is_edgar_document(doc):
        return source_gcs_uri

    source_bucket_name, source_blob_name = _parse_gcs_uri(source_gcs_uri)
    target_bucket_name = settings.SOURCE_BUCKET
    target_blob_name = edgar_source_blob_name(doc, source_blob_name)
    target_gcs_uri = f"gs://{target_bucket_name}/{target_blob_name}"
    if source_gcs_uri == target_gcs_uri:
        return source_gcs_uri

    client = storage.Client(project=settings.PROJECT_ID)
    source_bucket = client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    target_bucket = client.bucket(target_bucket_name)
    target_blob = target_bucket.blob(target_blob_name)

    logger.info("Moving EDGAR source %s to %s", source_gcs_uri, target_gcs_uri)
    token, _, _ = target_blob.rewrite(source_blob)
    while token is not None:
        token, _, _ = target_blob.rewrite(source_blob, token=token)
    source_blob.delete()
    return target_gcs_uri
