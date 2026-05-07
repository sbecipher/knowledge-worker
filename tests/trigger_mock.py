import asyncio
import logging
import uuid
from temporalio.client import Client

logging.basicConfig(level=logging.INFO)

async def main():
    client = await Client.connect("localhost:7233")
    
    # Mock payload simulating an HTML document ingestion
    mock_payload = {
        "title": "American Battery Materials submits an application for a project development grant under the Defense Production Act—Title III appropriation.",
        "company_name": "American Battery Materials",
        "company_id": "com_Xf530W",
        "company_ticker": "ABM",
        "year": 2026,
        "url": "https://example.com",
        "type": "html",
        "filepath": "https://example.com",
        "downloaded": False,
        "gcs_uri": None
    }
    
    workflow_id = f"mock-ingestion-{uuid.uuid4()}"
    
    logging.info(f"Triggering mock workflow {workflow_id}...")
    
    result = await client.execute_workflow(
        "KnowledgeIngestionWorkflow",
        mock_payload,
        id=workflow_id,
        task_queue="knowledge-ingestion-queue",
    )
    
    logging.info(f"Workflow completed with result: {result}")

if __name__ == "__main__":
    asyncio.run(main())
