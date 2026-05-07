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
    Calls the KnowledgeIO API to download a document via Selenium and upload it to the Source GCS bucket.
    """
    api_url = f"{settings.KNOWLEDGEIO_API_URL.rstrip('/')}/api/v1/scrape/url"

    payload = {
        "year": doc.year,
        "url": str(doc.url),
        "title": doc.title,
        "base_url": str(doc.url),
        "company_name": doc.company_name,
        "company_ticker": doc.company_ticker,
        "company_id": doc.company_id,
    }

    logger.info(f"Calling KnowledgeIO API to download file from {doc.url}")

    async with httpx.AsyncClient(timeout=300.0) as http_client:
        response = await http_client.post(api_url, json=payload)
        response.raise_for_status()

        result = response.json()
        gcs_uri = result.get("gcs_uri")
        if not gcs_uri:
            raise ValueError(f"KnowledgeIO API did not return a gcs_uri: {result}")

    logger.info(f"Successfully scraped and uploaded to {gcs_uri}")
    return gcs_uri
