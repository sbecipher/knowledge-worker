import os
import argparse
import asyncio
import sys

import httpx
from datetime import datetime

from temporalio.client import Client
from temporal_app.workflows import KnowledgeWorkflow

# Cron schedule expressions for supported intervals
CRON_SCHEDULES = {
    "weekly": "0 9 * * 1",                  # Every Monday at 9 AM
    "four_weeks": "0 9 1 * *",              # Approximate monthly: 1st of each month at 9 AM
    "quarterly": "0 9 1 1,4,7,10 *",        # Jan 1, Apr 1, Jul 1, Oct 1 at 9 AM
    "annually": "0 9 1 1 *"                 # Jan 1 at 9 AM
}
# Supported schedule options including one-time run
SCHEDULE_OPTIONS = ["once"] + list(CRON_SCHEDULES.keys())

async def main():
    parser = argparse.ArgumentParser(
        description="Start KnowledgeWorkflow with a one-time or cron schedule"
    )
    parser.add_argument(
        "--companies",
        type=str,
        default="aa,amr,feam",
        help="Comma-separated company codes (aa, amr, feam)",
    )
    parser.add_argument(
        "--years",
        type=str,
        required=True,
        help="Comma-separated list of years or 'current', e.g. 2021,2022,current",
    )
    parser.add_argument(
        "--schedule",
        type=str,
        choices=SCHEDULE_OPTIONS,
        required=True,
        help=f"Schedule interval: {', '.join(SCHEDULE_OPTIONS)}",
    )
    parser.add_argument(
        "--workflow-id",
        type=str,
        default=None,
        help="Unique workflow ID (defaults to knowledge_{schedule})",
    )
    parser.add_argument(
        "--task-queue",
        type=str,
        default=os.getenv("TEMPORAL_TASK_QUEUE", "knowledge-task-queue"),
        help="Task queue for the workflow",
    )
    parser.add_argument(
        "--address",
        type=str,
        default=os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
        help="Temporal server address",
    )
    args = parser.parse_args()

    # Health check: ensure Companies Knowledge Data API is reachable
    knowledge_api = os.getenv("KNOWLEDGE_API_URL", "http://localhost:8000").rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=10.0) as http_client:
            resp = await http_client.get(knowledge_api)
            resp.raise_for_status()
    except Exception as e:
        print(f"Error: Knowledge API health check failed at {knowledge_api}: {e}")
        sys.exit(1)

    companies = [c.strip() for c in args.companies.split(",") if c.strip()]
    # Parse years, supporting 'current' to refer to the current year
    years = []
    for y in args.years.split(","):
        y = y.strip()
        if not y:
            continue
        if y.lower() == "current":
            years.append(datetime.now().year)
        else:
            years.append(int(y))

    client = await Client.connect(args.address)
    wf_id = args.workflow_id or f"knowledge_{args.schedule}"
    # Start workflow: one-time or cron recurring
    if args.schedule == "once":
        execution = await client.start_workflow(
            KnowledgeWorkflow.run,
            args=[companies, years],
            id=wf_id,
            task_queue=args.task_queue,
        )
        print(
            f"Started one-time workflow {wf_id} ({execution.id}) "
            f"for companies {companies}, years {years}"
        )
    else:
        cron = CRON_SCHEDULES[args.schedule]
        execution = await client.start_workflow(
            KnowledgeWorkflow.run,
            args=[companies, years],
            id=wf_id,
            task_queue=args.task_queue,
            cron_schedule=cron,
        )
        print(
            f"Started workflow {wf_id} ({execution.id}) with schedule '{args.schedule}' ({cron}) "
            f"for companies {companies}, years {years}"
        )

if __name__ == "__main__":
    asyncio.run(main())