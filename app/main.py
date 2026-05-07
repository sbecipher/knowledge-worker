import asyncio
import logging
from temporalio.client import Client
from temporalio.worker import Worker

from app.activities.ingestion import download_document_to_gcs
from app.activities.processing import process_document_and_extract_features
from app.activities.loading import update_knowledge_index
from app.workflows.ingestion_workflow import KnowledgeIngestionWorkflow

logging.basicConfig(level=logging.INFO)


async def main():
    # Connect to local Temporal server or use env vars for production
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue="knowledge-ingestion-queue",
        workflows=[KnowledgeIngestionWorkflow],
        activities=[
            download_document_to_gcs,
            process_document_and_extract_features,
            update_knowledge_index,
        ],
    )
    logging.info("Starting KnowledgeFlow Worker on queue: knowledge-ingestion-queue")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
