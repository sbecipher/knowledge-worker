from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

# Import our activity, passing it through workflow.imported()
with workflow.unsafe.imports_passed_through():
    from app.activities.loading import update_knowledge_index
    from app.activities.orchestration import (
        discover_documents_for_ticker,
        discover_edgar_documents,
        filter_existing_documents,
    )
    from app.activities.promotion import promote_to_prod
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

        import asyncio
        # Step 1: Discover documents via KnowledgeIO API and SEC EDGAR
        knowledge_io_future = workflow.execute_activity(
            discover_documents_for_ticker,
            args=[company, year],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=10),
                maximum_interval=timedelta(seconds=60),
                maximum_attempts=3,
            ),
        )
        
        edgar_future = workflow.execute_activity(
            discover_edgar_documents,
            args=[company, year],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=10),
                maximum_interval=timedelta(seconds=60),
                maximum_attempts=3,
            ),
        )
        
        # Wait for both discoveries
        knowledge_io_docs, edgar_docs = await asyncio.gather(
            knowledge_io_future, 
            edgar_future,
            return_exceptions=True
        )
        
        discovered_docs = []
        if not isinstance(knowledge_io_docs, Exception) and knowledge_io_docs:
            discovered_docs.extend(knowledge_io_docs)
        else:
            workflow.logger.error(f"Failed to fetch KnowledgeIO docs: {knowledge_io_docs}")
            
        if not isinstance(edgar_docs, Exception) and edgar_docs:
            discovered_docs.extend(edgar_docs)
        else:
            workflow.logger.error(f"Failed to fetch EDGAR docs: {edgar_docs}")

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

        # Collect successful results that have a stage URI
        successful_results = [
            r for r in results
            if not isinstance(r, Exception) and r and r.get("success") and not r.get("skipped")
        ]

        # Count Gate 2 skips (passed GCS filter but already in BQ)
        skipped_results = [
            r for r in results
            if not isinstance(r, Exception) and r and r.get("skipped")
        ]

        workflow.logger.info(
            f"Processing complete for {company.company_ticker}. "
            f"{len(successful_results)} processed, {len(skipped_results)} skipped (already in BQ), "
            f"out of {len(new_docs)} new documents."
        )

        # Step 4: Batch BQ load and stage → prod promotion
        promoted_count = 0
        for result in successful_results:
            stage_uri = result.get("prod_gcs_uri")
            if not stage_uri:
                workflow.logger.warning(
                    f"No stage URI for document {result.get('document_id')}. Skipping load & promotion."
                )
                continue

            try:
                # Load Parquet into BigQuery
                await workflow.execute_activity(
                    update_knowledge_index,
                    stage_uri,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=5),
                        maximum_interval=timedelta(seconds=60),
                        maximum_attempts=3,
                    ),
                )

                # Promote stage → prod
                await workflow.execute_activity(
                    promote_to_prod,
                    stage_uri,
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=5),
                        maximum_interval=timedelta(seconds=30),
                        maximum_attempts=3,
                    ),
                )
                promoted_count += 1
            except Exception as e:
                workflow.logger.error(
                    f"Failed to load/promote document {result.get('document_id')}: {e}"
                )

        workflow.logger.info(
            f"Completed ingestion for {company.company_ticker}. "
            f"Promoted {promoted_count}/{len(successful_results)} documents to prod."
        )

        return {
            "company": company.company_ticker,
            "discovered": len(discovered_docs),
            "processed": len(successful_results),
            "skipped": len(skipped_results),
            "promoted": promoted_count,
            "message": "Ingestion completed.",
        }
