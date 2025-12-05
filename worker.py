import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from activities import (
    check_marketio_health,
    fetch_companies_metadata,
    fetch_fundamentals_prod,
    fetch_fundamentals_raw,
    fetch_fundamentals_stage,
    fetch_intraday_prod,
    fetch_intraday_raw,
)
from config import load_settings
from workflows import MarketDataWorkflow

SETTINGS = load_settings()


async def main() -> None:
    client = await Client.connect(SETTINGS.temporal_address)
    worker = Worker(
        client,
        task_queue=SETTINGS.temporal_task_queue,
        workflows=[MarketDataWorkflow],
        activities=[
            check_marketio_health,
            fetch_companies_metadata,
            fetch_fundamentals_raw,
            fetch_fundamentals_stage,
            fetch_fundamentals_prod,
            fetch_intraday_raw,
            fetch_intraday_prod,
        ],
    )
    print(
        f"Worker started for task queue '{SETTINGS.temporal_task_queue}', connecting to {SETTINGS.temporal_address}"
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
