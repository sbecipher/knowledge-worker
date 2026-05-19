import asyncio
import json
import logging
import argparse
from typing import List, Dict
from google.cloud import storage

from app.models.payloads import CompanyPayload
from app.activities.orchestration import (
    discover_edgar_documents,
    filter_existing_documents,
)
from app.activities.deduplication import check_edgar_document_exists_in_bq
from app.activities.ingestion import (
    download_document_to_gcs,
    relocate_edgar_source_to_gcs_layout,
)
from app.activities.processing import (
    process_document_and_extract_features,
    _document_id,
)
from app.activities.loading import update_edgar_index
from app.activities.promotion import promote_to_prod

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

HOLDINGS_FILES = [
    "gs://sbecipher-intelligence/prod/instruments/provider=ssga/date=2026-05-15/ticker=xme/dataset=holdings/holdings-e65dc1ea19ec.ndjson",
    "gs://sbecipher-intelligence/prod/instruments/provider=ishares/date=2026-05-14/ticker=pick/dataset=holdings/holdings-d4856e9e4cb7.ndjson",
]


def extract_tickers_from_gcs() -> List[Dict[str, str]]:
    """Downloads the NDJSON files from GCS and extracts unique tickers with names."""
    storage_client = storage.Client()
    unique_companies = {}

    for uri in HOLDINGS_FILES:
        logger.info(f"Processing {uri}")
        path = uri.replace("gs://", "")
        bucket_name, blob_name = path.split("/", 1)

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        content = blob.download_as_text()

        for line in content.strip().split("\n"):
            if not line:
                continue
            record = json.loads(line)
            ticker = record.get("ticker")
            name = record.get("name", "")

            if ticker and ticker not in unique_companies:
                if ticker == "-":
                    continue
                unique_companies[ticker] = name

    return [{"ticker": t, "name": n} for t, n in unique_companies.items()]


async def process_document(doc) -> bool:
    """Process a single document sequentially. Returns True if successful."""
    doc_id = _document_id(doc)
    logger.info(f"Processing document {doc_id}")
    try:
        # 0. Deduplication check in BigQuery
        exists = check_edgar_document_exists_in_bq(doc_id)
        if exists:
            logger.warning(f"Document {doc_id} already in BigQuery. Skipping.")
            return True

        # 1. Download to Source GCS
        if doc.downloaded and getattr(doc, "gcs_uri", None):
            source_gcs_uri = doc.gcs_uri
        else:
            source_gcs_uri = await download_document_to_gcs(doc)
        source_gcs_uri = relocate_edgar_source_to_gcs_layout(doc, source_gcs_uri)

        # 2. Process (Document AI, Gemini, Parquet to Stage GCS)
        record = process_document_and_extract_features(doc, source_gcs_uri)
        stage_uri = record.get("prod_gcs_uri")
        if not stage_uri:
            logger.error(f"No stage URI for document {doc_id}")
            return False

        # 3. Load Parquet into BigQuery
        update_edgar_index(stage_uri)

        # 4. Promote stage -> prod
        promote_to_prod(stage_uri)
        logger.info(f"Successfully processed {doc_id}")
        return True
    except Exception as e:
        logger.error(f"Error processing document {doc_id}: {e}", exc_info=True)
        return False


async def process_company(comp: Dict[str, str], years: List[int], concurrency: int):
    ticker = comp["ticker"]
    name = comp["name"]
    payload = CompanyPayload(
        company_ticker=ticker,
        company_name=name,
        base_url=f"https://www.google.com/search?q={name}",
    )

    for year in years:
        logger.info(f"Starting discovery for {ticker} - {year}")
        try:
            discovered_docs = await discover_edgar_documents(payload, year)
            if not discovered_docs:
                logger.info(f"No documents discovered for {ticker} in {year}")
                continue

            new_docs = await filter_existing_documents(discovered_docs, year)
            if not new_docs:
                logger.info(
                    f"All discovered documents for {ticker} in {year} already exist."
                )
                continue

            logger.info(
                f"Found {len(new_docs)} new documents to process for {ticker} in {year}"
            )

            # Process documents with a concurrency limit
            semaphore = asyncio.Semaphore(concurrency)

            async def bounded_process(doc):
                async with semaphore:
                    return await process_document(doc)

            tasks = [bounded_process(doc) for doc in new_docs]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            success_count = sum(1 for r in results if r is True)
            logger.info(
                f"Finished {ticker} - {year}: {success_count}/{len(new_docs)} successful."
            )

        except Exception as e:
            logger.error(
                f"Error discovering/filtering documents for {ticker} in {year}: {e}",
                exc_info=True,
            )


async def run_backfill(dry_run: bool = False, concurrency: int = 5):
    companies = extract_tickers_from_gcs()
    logger.info(f"Extracted {len(companies)} unique companies.")

    if dry_run:
        logger.info("Dry-run mode. Tickers:")
        for comp in companies:
            logger.info(f" - {comp['ticker']}: {comp['name']}")
        return

    years = list(range(2021, 2027))  # 2021 to 2026 (5 years)
    logger.info(
        f"Starting standard backfill for {len(companies)} companies over years {years}"
    )

    for comp in companies:
        await process_company(comp, years, concurrency)

    logger.info("Standard backfill completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill ETF constituents EDGAR data (Standard Script)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print extracted tickers without processing.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of concurrent documents to process.",
    )
    args = parser.parse_args()

    asyncio.run(run_backfill(dry_run=args.dry_run, concurrency=args.concurrency))
