import asyncio
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from temporalio.client import Client
from temporalio.worker import Worker

from activities import (
    check_marketio_health,
    fetch_companies_metadata,
    fetch_edgar_source,
    fetch_fundamentals_prod,
    fetch_fundamentals_raw,
    fetch_fundamentals_stage,
    fetch_intraday_prod,
    fetch_intraday_raw,
)
from config import load_settings
from workflows import MarketDataWorkflow

SETTINGS = load_settings()
logger = logging.getLogger(__name__)


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


def _configure_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def _start_health_server() -> None:
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
    logger.info("Health server listening on %s", port)


async def main() -> None:
    _start_health_server()
    client = await Client.connect(SETTINGS.temporal_address)
    worker = Worker(
        client,
        task_queue=SETTINGS.temporal_task_queue,
        workflows=[MarketDataWorkflow],
        activities=[
            check_marketio_health,
            fetch_companies_metadata,
            fetch_edgar_source,
            fetch_fundamentals_raw,
            fetch_fundamentals_stage,
            fetch_fundamentals_prod,
            fetch_intraday_raw,
            fetch_intraday_prod,
        ],
    )
    logger.info(
        "Worker started for task queue '%s', connecting to %s",
        SETTINGS.temporal_task_queue,
        SETTINGS.temporal_address,
    )
    await worker.run()


if __name__ == "__main__":
    _configure_logging()
    asyncio.run(main())
