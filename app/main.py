import asyncio
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from temporalio.client import Client
from temporalio.worker import Worker

from app.activities.ingestion import download_document_to_gcs
from app.activities.processing import process_document_and_extract_features
from app.activities.loading import update_knowledge_index
from app.workflows.ingestion_workflow import KnowledgeIngestionWorkflow


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


def start_health_server() -> None:
    port_value = os.getenv("HEALTHCHECK_PORT") or os.getenv("PORT")
    if not port_value:
        return
    try:
        port = int(port_value)
    except ValueError as exc:
        raise ValueError("HEALTHCHECK_PORT/PORT must be an integer") from exc
    server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logging.info("Health server listening on %s", port)


async def main():
    # Setup logging level from env
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Start healthcheck server if configured
    start_health_server()

    # Connect to local Temporal server or use env vars for production
    temporal_address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    client = await Client.connect(temporal_address)

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
