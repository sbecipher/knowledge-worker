from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from app.models.payloads import KnowledgeDocument
    from app.activities.ingestion import download_document_to_gcs
    from app.activities.processing import process_document_and_extract_features
    from app.activities.loading import update_knowledge_index


@workflow.defn
class KnowledgeIngestionWorkflow:
    @workflow.run
    async def run(self, document: KnowledgeDocument) -> dict:
        """
        Orchestrates the ingestion, processing, and loading of a knowledge document.
        """
        # 1. Download to Source GCS
        source_gcs_uri = await workflow.execute_activity(
            download_document_to_gcs,
            document,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        # 2. Process (Document AI, Gemini, Parquet to Prod GCS)
        record = await workflow.execute_activity(
            process_document_and_extract_features,
            args=[document, source_gcs_uri],
            start_to_close_timeout=timedelta(minutes=15),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        # 3. Load Parquet into BigQuery
        success = await workflow.execute_activity(
            update_knowledge_index,
            record["prod_gcs_uri"],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        return {"success": success, "document_id": record["document_id"]}
