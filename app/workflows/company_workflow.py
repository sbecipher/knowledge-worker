from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

# Import our activity, passing it through workflow.imported()
with workflow.unsafe.imports_passed_through():
    from app.activities.company_metadata import fetch_company_metadata
    from app.activities.loading import (
        update_company_metadata_index,
        update_knowledge_index,
    )
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
    async def run(
        self, company: CompanyPayload, year: int, source: str | None = None
    ) -> dict:
        workflow.logger.info(
            f"Starting company workflow for {company.company_ticker} for year {year} source={source}"
        )

        import asyncio

        requested_sources = _requested_company_sources(source)
        if requested_sources == ("metadata",):
            return await self._run_company_metadata(company, year)

        # Step 1: Discover documents via KnowledgeIO API and SEC EDGAR
        discovery_futures = []
        for requested_source in requested_sources:
            activity_fn = (
                discover_documents_for_ticker
                if requested_source == "articles"
                else discover_edgar_documents
            )
            future = workflow.execute_activity(
                activity_fn,
                args=[company, year],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=10),
                    maximum_interval=timedelta(seconds=60),
                    maximum_attempts=3,
                ),
            )
            discovery_futures.append((requested_source, future))

        results = await asyncio.gather(
            *[future for _, future in discovery_futures],
            return_exceptions=True,
        )

        discovered_docs = []
        source_errors: dict[str, str] = {}
        for (requested_source, _), result in zip(discovery_futures, results):
            if isinstance(result, Exception):
                workflow.logger.error(
                    "Failed to fetch %s docs: %s",
                    requested_source,
                    result,
                )
                source_errors[requested_source] = str(result)
                continue
            if result:
                discovered_docs.extend(result)

        _raise_if_discovery_failed_without_results(
            company.company_ticker,
            requested_sources,
            source_errors,
            discovered_docs_count=len(discovered_docs),
        )

        if not discovered_docs:
            workflow.logger.info(
                f"No documents discovered for {company.company_ticker} in {year}"
            )
            return {
                "company": company.company_ticker,
                "source": source,
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
                "source": source,
                "discovered": len(discovered_docs),
                "ingested": 0,
                "message": "All documents already ingested.",
            }

        workflow.logger.info(
            f"Found {len(new_docs)} new documents to ingest for {company.company_ticker}"
        )

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
            r
            for r in results
            if not isinstance(r, Exception)
            and r
            and r.get("success")
            and not r.get("skipped")
        ]

        # Count Gate 2 skips (passed GCS filter but already in BQ)
        skipped_results = [
            r
            for r in results
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
            "source": source,
            "discovered": len(discovered_docs),
            "processed": len(successful_results),
            "skipped": len(skipped_results),
            "promoted": promoted_count,
            "source_errors": source_errors,
            "message": "Ingestion completed.",
        }

    async def _run_company_metadata(self, company: CompanyPayload, year: int) -> dict:
        metadata_result = await workflow.execute_activity(
            fetch_company_metadata,
            args=[company, year, "lseg"],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=10),
                maximum_interval=timedelta(seconds=60),
                maximum_attempts=3,
            ),
        )

        if metadata_result is None:
            workflow.logger.info(
                "No company metadata discovered for %s in %s",
                company.company_ticker,
                year,
            )
            return {
                "company": company.company_ticker,
                "source": "metadata",
                "discovered": 0,
                "processed": 0,
                "promoted": 0,
                "message": "No company metadata found.",
            }

        await workflow.execute_activity(
            update_company_metadata_index,
            metadata_result.stage_gcs_uri,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=5),
                maximum_interval=timedelta(seconds=60),
                maximum_attempts=3,
            ),
        )
        prod_gcs_uri = await workflow.execute_activity(
            promote_to_prod,
            metadata_result.stage_gcs_uri,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=5),
                maximum_interval=timedelta(seconds=30),
                maximum_attempts=3,
            ),
        )
        return {
            "company": company.company_ticker,
            "source": "metadata",
            "provider": metadata_result.provider,
            "discovered": 1,
            "processed": 1,
            "promoted": 1,
            "metadata_id": metadata_result.metadata_id,
            "prod_gcs_uri": prod_gcs_uri,
            "message": "Company metadata ingestion completed.",
        }


SUPPORTED_COMPANY_SOURCES = frozenset({"articles", "edgar", "metadata"})


def _requested_company_sources(source: str | None) -> tuple[str, ...]:
    if source is None:
        return ("articles", "edgar")
    normalized_source = source.strip().lower()
    if normalized_source not in SUPPORTED_COMPANY_SOURCES:
        raise ApplicationError(
            f"Unsupported company source: {source}",
            non_retryable=True,
        )
    return (normalized_source,)


def _raise_if_discovery_failed_without_results(
    company_ticker: str,
    requested_sources: tuple[str, ...],
    source_errors: dict[str, str],
    discovered_docs_count: int,
) -> None:
    if discovered_docs_count > 0 or not source_errors:
        return
    error_details = "; ".join(
        f"{requested_source}: {source_errors[requested_source]}"
        for requested_source in requested_sources
        if requested_source in source_errors
    )
    raise ApplicationError(
        f"Document discovery failed for {company_ticker}: {error_details}",
    )
