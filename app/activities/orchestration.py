import logging
from typing import List
import httpx
from google.cloud import storage  # type: ignore
from temporalio import activity

from app.models.payloads import CompanyPayload, KnowledgeDocument
from app.core.config import settings

logger = logging.getLogger(__name__)


@activity.defn
async def discover_documents_for_ticker(
    company: CompanyPayload, year: int
) -> List[KnowledgeDocument]:
    """
    Calls the KnowledgeIO API to discover documents for a given company and year.
    """
    api_url = f"{settings.KNOWLEDGEIO_API_URL.rstrip('/')}/api/v1/scrape/articles"

    payload = {
        "year": year,
        "base_url": company.base_url,
        "company_name": company.company_name,
        "company_ticker": company.company_ticker,
        "company_id": "TBD",  # Will be resolved or we can generate a stub, wait let's look at the API requirements
    }

    # Generate a company_id based on the ticker if not provided
    payload["company_id"] = f"com_{company.company_ticker.lower()}"

    logger.info(
        f"Calling KnowledgeIO API to discover documents for {company.company_ticker} for year {year}"
    )

    async with httpx.AsyncClient(timeout=120.0) as http_client:
        try:
            response = await http_client.post(api_url, json=payload)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.info(
                    f"No documents found for {company.company_ticker} in year {year}"
                )
                return []
            logger.error(f"Error fetching documents for {company.company_ticker}: {e}")
            raise e
        except Exception as e:
            logger.error(f"Error fetching documents for {company.company_ticker}: {e}")
            raise e

    documents = []
    for item in data:
        try:
            doc = KnowledgeDocument(**item)
            documents.append(doc)
        except Exception as e:
            logger.warning(f"Skipping invalid document record: {e}")

    logger.info(f"Discovered {len(documents)} documents for {company.company_ticker}")
    return documents


@activity.defn
async def filter_existing_documents(
    documents: List[KnowledgeDocument], year: int
) -> List[KnowledgeDocument]:
    """
    Checks GCS to see which documents already exist and filters them out.
    """
    if not documents:
        return []

    ticker = documents[0].company_ticker
    bucket_name = "sbecipher-intelligence"
    prefix = f"source/knowledge/{ticker}/{year}/"

    logger.info(f"Checking existing documents in gs://{bucket_name}/{prefix}")

    # We use sync GCS client since activity executes in thread pool
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    existing_blobs = list(bucket.list_blobs(prefix=prefix))

    # Extract just the filenames from the existing blobs
    existing_filenames = set()
    for blob in existing_blobs:
        # e.g. source/knowledge/AA/2026/article.html -> article.html
        filename = blob.name.split("/")[-1]
        existing_filenames.add(filename)

    new_documents = []
    for doc in documents:
        # The document filepath usually looks like data/com_aa/2026/filename.ext
        # Or we can just check the URL hash if that's what's used.
        # Let's extract the actual filename we'd upload it as.
        filename = doc.filepath.split("/")[-1]

        if filename in existing_filenames:
            logger.info(f"Document {filename} already exists in GCS. Skipping.")
            continue

        new_documents.append(doc)

    logger.info(
        f"Filtered {len(documents)} down to {len(new_documents)} new documents for {ticker}"
    )
    return new_documents
