import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox._runner import SandboxedWorkflowRunner
from temporalio.worker.workflow_sandbox._restrictions import SandboxRestrictions

from temporal_app.workflows import KnowledgeWorkflow
from temporal_app.activities import list_company_articles, process_company_article, check_api_health

# Task queue name for the worker
TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "knowledge-task-queue")
# Temporal server address
TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")

async def main():
    # Connect to Temporal server
    client = await Client.connect(TEMPORAL_ADDRESS)

    # Create a sandboxed workflow runner that allows importing our app modules
    custom_runner = SandboxedWorkflowRunner(
        restrictions=SandboxRestrictions.default
        .with_passthrough_modules(
            "temporal_app.activities", "temporal_app.workflows"
        )
    )
    # Start a worker for the task queue using the custom runner
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[KnowledgeWorkflow],
        activities=[list_company_articles, process_company_article, check_api_health],
        workflow_runner=custom_runner,
    )
    print(f"Worker started for task queue '{TASK_QUEUE}', connecting to {TEMPORAL_ADDRESS}")
    # Run the worker indefinitely
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())