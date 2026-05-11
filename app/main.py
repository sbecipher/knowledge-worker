import asyncio
import logging

from app.runtime import (
    configure_logging,
    connect_temporal,
    create_activity_executor,
    create_knowledge_worker,
    start_health_server,
)
from app.core.config import settings


async def main():
    configure_logging(settings)
    start_health_server()

    client = await connect_temporal(settings)
    with create_activity_executor(settings) as activity_executor:
        worker = create_knowledge_worker(
            client,
            settings,
            activity_executor,
        )
        logging.info(
            "Starting KnowledgeFlow Worker on queue: %s",
            settings.TEMPORAL_TASK_QUEUE,
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
