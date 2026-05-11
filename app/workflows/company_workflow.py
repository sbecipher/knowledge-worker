from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

# Import our activity, passing it through workflow.imported()
with workflow.unsafe.imports_passed_through():
    from app.activities.orchestration import (
        discover_documents_for_ticker,
        filter_existing_documents,
    )
    from app.models.payloads import CompanyPayload
    from app.workflows.ingestion_workflow import KnowledgeIngestionWorkflow


@workflow.defn
class KnowledgeCompanyWorkflow:
    """
    Orchestrates the ingestion of knowledge documents for a specific company and year.
    It calls KnowledgeIO API to discover documents, checks GCS to filter out existing ones,
    and then spawns child workflows for each new document to download and process them.
    """

    @workflow.run
    async def run(self, company: CompanyPayload, year: int) -> dict:
        workflow.logger.info(
            f"Starting company workflow for {company.company_ticker} for year {year}"
        )

        # Step 1: Discover documents via KnowledgeIO API
        discovered_docs = await workflow.execute_activity(
            discover_documents_for_ticker,
            args=[company, year],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=10),
                maximum_interval=timedelta(seconds=60),
                maximum_attempts=3,
            ),
        )

        if not discovered_docs:
            workflow.logger.info(
                f"No documents discovered for {company.company_ticker} in {year}"
            )
            return {
                "company": company.company_ticker,
                "discovered": 0,
                "ingested": 0,
                "message": "No documents found.",
            }

        workflow.logger.info(
            f"Discovered {len(discovered_docs)} documents for {company.company_ticker}"
        )

        # Step 2: Filter out existing documents in GCS
        new_docs = await workflow.execute_activity(
            filter_existing_documents,
            args=[discovered_docs, year],
            start_to_close_timeout=timedelta(minutes=2),
        )

        if not new_docs:
            workflow.logger.info(
                f"All discovered documents for {company.company_ticker} already exist in GCS."
            )
            return {
                "company": company.company_ticker,
                "discovered": len(discovered_docs),
                "ingested": 0,
                "message": "All documents already ingested.",
            }

        workflow.logger.info(
            f"Found {len(new_docs)} new documents to ingest for {company.company_ticker}"
        )

        import asyncio

        # Step 3: Spawn child workflows for each new document
        # We process them concurrently for speed.
        coros = []
        for doc in new_docs:
            filename = doc.filepath.split("/")[-1]
            workflow_id = (
                f"knowledge-ingestion-{company.company_ticker}-{year}-{filename}"
            )

            # Create coroutine for child workflow execution
            coro = workflow.execute_child_workflow(
                KnowledgeIngestionWorkflow.run,
                args=[doc],
                id=workflow_id,
                task_queue=workflow.info().task_queue,
            )
            coros.append(coro)

        # Wait for all child workflows to complete
        results = await asyncio.gather(*coros, return_exceptions=True)

        # Count successes (ignoring exceptions and False returns)
        success_count = sum(1 for r in results if not isinstance(r, Exception) and r)

        workflow.logger.info(
            f"Completed ingestion for {company.company_ticker}. Successfully ingested {success_count}/{len(new_docs)}."
        )

        return {
            "company": company.company_ticker,
            "discovered": len(discovered_docs),
            "ingested": success_count,
            "message": "Ingestion completed.",
        }
