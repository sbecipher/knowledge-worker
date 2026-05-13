from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from app.models.payloads import KnowledgeDocument
    from app.activities.ingestion import download_document_to_gcs
    from app.activities.processing import process_document_and_extract_features, _document_id
    from app.activities.deduplication import check_document_exists_in_bq


@workflow.defn
class KnowledgeIngestionWorkflow:
    @workflow.run
    async def run(self, document: KnowledgeDocument) -> dict:
        """
        Orchestrates the ingestion, processing, and loading of a knowledge document.
        """
        workflow.logger.info(
            f"Received document: downloaded={getattr(document, 'downloaded', None)}, gcs_uri={getattr(document, 'gcs_uri', None)}, type={type(document)}"
        )

        doc_id = _document_id(document)

        # 0. Deduplication check in BigQuery
        exists = await workflow.execute_activity(
            check_document_exists_in_bq,
            doc_id,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        if exists:
            workflow.logger.warning(
                f"Document {doc_id} already in BigQuery but passed GCS filter "
                f"(source file may be missing). Skipping reprocessing."
            )
            return {"success": True, "document_id": doc_id, "skipped": True}

        # 1. Download to Source GCS (Skip if already downloaded and GCS URI is provided)
        if document.downloaded and document.gcs_uri:
            source_gcs_uri = document.gcs_uri
        else:
            source_gcs_uri = await workflow.execute_activity(
                download_document_to_gcs,
                document,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=10),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(minutes=2),
                    maximum_attempts=6,
                ),
            )

        # 2. Process (Document AI, Gemini, Parquet to Stage GCS)
        record = await workflow.execute_activity(
            process_document_and_extract_features,
            args=[document, source_gcs_uri],
            start_to_close_timeout=timedelta(minutes=15),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=10),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(minutes=2),
                maximum_attempts=15,
            ),
        )

        return {"success": True, "document_id": record["document_id"], "prod_gcs_uri": record["prod_gcs_uri"], "skipped": False}
