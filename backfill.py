import asyncio
import os
import json
import subprocess
import time
from google.cloud import storage  # type: ignore

# Ensure we use local temporal
os.environ["TEMPORAL_ADDRESS"] = "127.0.0.1:7233"

from temporalio.client import Client  # noqa: E402
from temporalio.worker import Worker  # noqa: E402

# Import our workflow and activities
from app.activities.processing import (
    process_document_and_extract_features,
)  # noqa: E402
from app.workflows.ingestion_workflow import KnowledgeIngestionWorkflow  # noqa: E402
from app.models.payloads import KnowledgeDocument  # noqa: E402
from app.core.config import settings  # noqa: E402
from temporalio import activity  # noqa: E402


@activity.defn(name="update_knowledge_index")
async def mock_update_knowledge_index(prod_gcs_uri: str) -> bool:
    # Just return True to bypass individual BigQuery load jobs
    # We will use bulk_load_bq.py after the backfill is complete
    return True


@activity.defn(name="download_document_to_gcs")
async def mock_download_document_to_gcs(doc: KnowledgeDocument) -> str:
    # Just return the URI directly, skipping the actual download
    return doc.gcs_uri or ""


def kill_existing_workers():
    """Kill any existing Python workers to ensure this script is the only one running."""
    print("Killing any existing local python workers...")
    subprocess.run(["pkill", "-f", "app/main.py"], check=False)
    subprocess.run(["pkill", "-f", "app.main"], check=False)
    time.sleep(1)


async def run_backfill(client: Client):
    print("Loading companies.json...")
    companies_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "knowledgeio", "companies.json"
    )
    if not os.path.exists(companies_path):
        # Fallback to the absolute path if relative fails
        companies_path = "/Users/jlroo/Library/CloudStorage/GoogleDrive-jlrg@sbecipher.com/My Drive/Sbecipher Capital/Cloud/orchestration/knowledgeio/companies.json"

    with open(companies_path, "r") as f:
        companies_data = json.load(f)

    ticker_to_company = {comp["company_ticker"]: comp for comp in companies_data}

    # Initialize GCS client
    gcs_client = storage.Client(project=settings.PROJECT_ID)
    bucket_name = "sbecipher-intelligence"
    prefix = "source/knowledge/"

    print(
        f"Listing blobs in gs://{bucket_name}/stage/knowledge/ to find already processed files..."
    )
    stage_blobs = gcs_client.list_blobs(bucket_name, prefix="stage/knowledge/")
    existing_stage_files = set(blob.name for blob in stage_blobs)
    print(
        f"Found {len(existing_stage_files)} already processed files in stage/knowledge/."
    )

    print(f"Listing source blobs in gs://{bucket_name}/{prefix}...")
    blobs = gcs_client.list_blobs(bucket_name, prefix=prefix)

    documents = []
    import hashlib

    for blob in blobs:
        if blob.name.endswith("/"):
            continue  # skip directories

        # parse source/knowledge/{TICKER}/{YEAR}/{filename}
        parts = blob.name.split("/")
        if len(parts) >= 5:
            ticker = parts[2]
            year_str = parts[3]
            filename = parts[-1]

            comp_info = ticker_to_company.get(ticker)
            if not comp_info:
                print(
                    f"Warning: Ticker {ticker} not found in companies.json, skipping blob {blob.name}"
                )
                continue

            try:
                year = int(year_str)
            except ValueError:
                continue

            company_id = comp_info.get("company_id", ticker)
            stable_hash = hashlib.md5(filename.encode()).hexdigest()[:16]
            doc_id = f"{company_id}_{year}_{stable_hash}"
            expected_stage_path = f"stage/knowledge/{doc_id}.parquet"

            if expected_stage_path in existing_stage_files:
                continue  # Skip already processed files

            # Determine file type
            ext = filename.split(".")[-1].lower() if "." in filename else "unknown"

            doc = KnowledgeDocument(
                title=filename,
                company_name=comp_info["company_name"],
                company_id=company_id,
                company_ticker=ticker,
                year=year,
                url=f"gs://{bucket_name}/{blob.name}",  # Mock URL since we don't have the original
                type=ext,
                filepath=f"gs://{bucket_name}/{blob.name}",
                downloaded=True,  # Crucial flag to skip the downloading step
                gcs_uri=f"gs://{bucket_name}/{blob.name}",
            )
            documents.append(doc)

    # NOTE: Set to 5 to respect Gemini rate limits
    batch_size = 5
    print(
        f"Found {len(documents)} documents to backfill. Proceeding in batches of {batch_size}..."
    )

    total_batches = (len(documents) + batch_size - 1) // batch_size

    for i in range(0, len(documents), batch_size):
        batch = documents[i : i + batch_size]
        print(
            f"\nProcessing batch {i//batch_size + 1} of {total_batches} ({len(batch)} documents)"
        )

        tasks = []
        for doc in batch:
            workflow_id = f"backfill-{doc.company_ticker}-{doc.year}-{hash(doc.title)}-{int(time.time())}"
            tasks.append(
                client.execute_workflow(
                    KnowledgeIngestionWorkflow.run,
                    doc,
                    id=workflow_id,
                    task_queue="knowledge-ingestion-queue-backfill",
                )
            )

        # Wait for the batch of 50 to finish
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for doc, res in zip(batch, results):
            if isinstance(res, Exception):
                print(f"Error processing {doc.gcs_uri}: {res}")
            else:
                print(f"Success for {doc.gcs_uri}: {res}")

        print(f"Batch {i//batch_size + 1} completed.")
        # Brief pause to avoid overwhelming Vertex AI (Gemini) API limits
        await asyncio.sleep(4)


async def main():
    # Ensure this is the only worker running locally
    kill_existing_workers()

    print("Connecting to Temporal at 127.0.0.1:7233...")
    from temporalio.contrib.pydantic import pydantic_data_converter

    client = await Client.connect(
        "127.0.0.1:7233", data_converter=pydantic_data_converter
    )

    import concurrent.futures

    # NOTE: Set to 5 to respect Gemini rate limits
    activity_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

    worker = Worker(
        client,
        task_queue="knowledge-ingestion-queue-backfill",
        workflows=[KnowledgeIngestionWorkflow],
        activities=[
            mock_download_document_to_gcs,
            process_document_and_extract_features,
            mock_update_knowledge_index,
        ],
        activity_executor=activity_executor,
    )

    print("Starting local Temporal Worker inside the backfill script...")
    worker_task = asyncio.create_task(worker.run())

    try:
        await run_backfill(client)
    except Exception as e:
        print(f"Backfill failed: {e}")
    finally:
        print("Shutting down local worker...")
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
