import logging
import hashlib
from typing import List
from urllib.parse import urlparse
import httpx
from google.cloud import storage  # type: ignore
from temporalio import activity

from app.core.knowledge_api import knowledge_api_headers
from app.models.payloads import CompanyPayload, KnowledgeDocument
from app.core.config import settings

logger = logging.getLogger(__name__)


def _infer_document_type(item: dict) -> str:
    explicit_type = str(item.get("type") or item.get("article_type") or "").lower()
    if explicit_type in {"pdf", "html"}:
        return explicit_type

    url_path = urlparse(str(item.get("url", ""))).path.lower()
    filepath = str(item.get("filepath", "")).lower()
    if (
        item.get("is_pdf") is True
        or url_path.endswith(".pdf")
        or filepath.endswith(".pdf")
    ):
        return "pdf"

    return "html"


def _build_document_filepath(
    item: dict, company_id: str, year: int, document_type: str
) -> str:
    candidate = str(item.get("filepath") or "").strip()
    if candidate and "://" not in candidate:
        return candidate

    extension = "pdf" if document_type == "pdf" else "html"
    filename = hashlib.md5(str(item["url"]).encode("utf-8")).hexdigest()
    return f"data/{company_id}/{year}/{filename}.{extension}"


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
            response = await http_client.post(
                api_url,
                json=payload,
                headers=knowledge_api_headers(),
            )
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
            document_type = _infer_document_type(item)
            filepath = _build_document_filepath(
                item, payload["company_id"], year, document_type
            )
            doc = KnowledgeDocument(
                title=item["title"],
                url=item["url"],
                date=item.get("date", str(year)),
                type=document_type,
                filepath=filepath,
                company_name=payload["company_name"],
                company_ticker=payload["company_ticker"],
                company_id=payload["company_id"],
                year=year,
                base_url=payload["base_url"],
            )
            documents.append(doc)
        except Exception as e:
            logger.warning(f"Skipping invalid document record: {e}")

    logger.info(f"Discovered {len(documents)} documents for {company.company_ticker}")
    return documents


@activity.defn
async def discover_edgar_documents(
    company: CompanyPayload, year: int
) -> List[KnowledgeDocument]:
    """
    Calls the KnowledgeIO API to discover EDGAR filings for a given company and year.
    Returns 10-K, 10-Q, 8-K, S-1, DEF 14A, 20-F, and 40-F filings as KnowledgeDocument objects.
    """
    api_url = f"{settings.KNOWLEDGEIO_API_URL.rstrip('/')}/api/v1/scrape/edgar/articles"

    payload = {
        "year": year,
        "base_url": company.base_url,
        "company_name": company.company_name,
        "company_ticker": company.company_ticker,
        "company_id": f"com_{company.company_ticker.lower()}",
    }

    logger.info(
        f"Calling KnowledgeIO API to discover EDGAR documents for {company.company_ticker} for year {year}"
    )

    async with httpx.AsyncClient(timeout=120.0) as http_client:
        try:
            response = await http_client.post(
                api_url,
                json=payload,
                headers=knowledge_api_headers(),
            )
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.info(
                    f"No EDGAR documents found for {company.company_ticker} in year {year}"
                )
                return []
            logger.error(f"Error fetching EDGAR documents for {company.company_ticker}: {e}")
            raise e
        except Exception as e:
            logger.error(f"Error fetching EDGAR documents for {company.company_ticker}: {e}")
            raise e

    documents = []
    for item in data:
        try:
            document_type = _infer_document_type(item)
            filepath = _build_document_filepath(
                item, payload["company_id"], year, document_type
            )
            doc = KnowledgeDocument(
                title=item["title"],
                url=item["url"],
                date=item.get("date", str(year)),
                type=document_type,
                filepath=filepath,
                company_name=payload["company_name"],
                company_ticker=payload["company_ticker"],
                company_id=payload["company_id"],
                year=year,
                base_url=payload["base_url"],
            )
            documents.append(doc)
        except Exception as e:
            logger.warning(f"Skipping invalid EDGAR document record: {e}")

    logger.info(f"Discovered {len(documents)} EDGAR documents for {company.company_ticker}")
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
    bucket_name = settings.SOURCE_BUCKET
    prefix = f"source/knowledge/{ticker}/{year}/"

    logger.info(f"Checking existing documents in gs://{bucket_name}/{prefix}")

    # We use sync GCS client since activity executes in thread pool
    client = storage.Client(project=settings.PROJECT_ID)
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
