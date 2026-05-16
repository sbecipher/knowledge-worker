from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from app.activities.company_metadata import fetch_company_metadata
from app.activities.deduplication import check_document_exists_in_bq
from app.activities.ingestion import download_document_to_gcs
from app.activities.loading import (
    update_company_metadata_index,
    update_knowledge_index,
)
from app.activities.promotion import promote_to_prod
from app.activities.orchestration import (
    discover_documents_for_ticker,
    discover_edgar_documents,
    filter_existing_documents,
)
from app.activities.processing import process_document_and_extract_features
from app.core.config import Settings, settings
from app.workflows.company_workflow import KnowledgeCompanyWorkflow
from app.workflows.ingestion_workflow import KnowledgeIngestionWorkflow

logger = logging.getLogger(__name__)

WORKFLOWS = [KnowledgeIngestionWorkflow, KnowledgeCompanyWorkflow]
ACTIVITIES = [
    check_document_exists_in_bq,
    download_document_to_gcs,
    process_document_and_extract_features,
    update_knowledge_index,
    update_company_metadata_index,
    promote_to_prod,
    discover_documents_for_ticker,
    discover_edgar_documents,
    filter_existing_documents,
    fetch_company_metadata,
]


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path in {"/", "/health", "/healthz"}:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        return


def configure_logging(current_settings: Settings | None = None) -> None:
    current_settings = current_settings or settings
    level = os.getenv("LOG_LEVEL") or current_settings.LOG_LEVEL
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def start_health_server() -> ThreadingHTTPServer | None:
    port_value = os.getenv("HEALTHCHECK_PORT") or os.getenv("PORT")
    if not port_value:
        return None
    try:
        port = int(port_value)
    except ValueError as exc:
        raise ValueError("HEALTHCHECK_PORT/PORT must be an integer") from exc

    server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Health server listening on %s", port)
    return server


async def connect_temporal(current_settings: Settings | None = None) -> Client:
    current_settings = current_settings or settings
    logger.info(
        "Connecting to Temporal at %s for task queue %s",
        current_settings.TEMPORAL_ADDRESS,
        current_settings.TEMPORAL_TASK_QUEUE,
    )
    return await Client.connect(
        current_settings.TEMPORAL_ADDRESS,
        data_converter=pydantic_data_converter,
    )


def create_activity_executor(
    current_settings: Settings | None = None,
) -> ThreadPoolExecutor:
    current_settings = current_settings or settings
    return ThreadPoolExecutor(max_workers=current_settings.ACTIVITY_EXECUTOR_THREADS)


def create_knowledge_worker(
    client: Client,
    current_settings: Settings | None = None,
    activity_executor: ThreadPoolExecutor | None = None,
) -> Worker:
    current_settings = current_settings or settings
    worker_kwargs = {
        "client": client,
        "task_queue": current_settings.TEMPORAL_TASK_QUEUE,
        "workflows": WORKFLOWS,
        "activities": ACTIVITIES,
    }
    if activity_executor is not None:
        worker_kwargs["activity_executor"] = activity_executor
    if current_settings.MAX_CONCURRENT_ACTIVITIES is not None:
        worker_kwargs[
            "max_concurrent_activities"
        ] = current_settings.MAX_CONCURRENT_ACTIVITIES
    if current_settings.MAX_CONCURRENT_WORKFLOW_TASKS is not None:
        worker_kwargs[
            "max_concurrent_workflow_tasks"
        ] = current_settings.MAX_CONCURRENT_WORKFLOW_TASKS
    if current_settings.MAX_CACHED_WORKFLOWS is not None:
        worker_kwargs["max_cached_workflows"] = current_settings.MAX_CACHED_WORKFLOWS
    return Worker(**worker_kwargs)
