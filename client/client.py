import argparse
import asyncio
import os
from datetime import datetime, timezone
from typing import List, Optional

from temporalio.client import Client

DEFAULT_TASK_QUEUE = "marketio-task-queue"
DEFAULT_TEMPORAL_ADDRESS = "localhost:7233"
DEFAULT_WORKFLOW_NAME = "MarketDataWorkflow"


def _env_str(name: str) -> str:
    return os.getenv(name, "").strip()


def _env_or(name: str, default: str) -> str:
    value = _env_str(name)
    return value if value else default


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start MarketDataWorkflow with Marketio API")
    parser.add_argument("--tickers", type=str, required=True, help="Comma-separated tickers, e.g., AA,NUE")
    parser.add_argument("--start-date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--intraday-frequency",
        type=str,
        default="daily",
        help="Intraday frequency (daily, weekly, monthly)",
    )
    parser.add_argument(
        "--fundamentals-mode",
        type=str,
        choices=["raw", "stage", "prod", "none"],
        default="prod",
        help="Fundamentals pipeline depth",
    )
    parser.add_argument(
        "--intraday-mode",
        type=str,
        choices=["raw", "prod", "none"],
        default="prod",
        help="Intraday pipeline depth (use 'none' to skip intraday)",
    )
    parser.add_argument(
        "--edgar-source",
        action="store_true",
        help="Set to fetch EDGAR submissions (source=True) for each ticker",
    )
    parser.add_argument(
        "--workflow-id",
        type=str,
        default=None,
        help="Workflow ID (defaults to market_data_{timestamp})",
    )
    parser.add_argument(
        "--workflow-name",
        type=str,
        default=None,
        help="Workflow name (defaults to env/TEMPORAL_WORKFLOW or MarketDataWorkflow)",
    )
    parser.add_argument(
        "--task-queue",
        type=str,
        default=None,
        help=f"Temporal task queue (defaults to env/TEMPORAL_TASK_QUEUE or {DEFAULT_TASK_QUEUE})",
    )
    parser.add_argument(
        "--address",
        type=str,
        default=None,
        help="Temporal server address (defaults to env/TEMPORAL_ADDRESS or localhost:7233)",
    )
    exclusive = parser.add_mutually_exclusive_group()
    exclusive.add_argument(
        "--metadata-only",
        action="store_true",
        help="Fetch only metadata and skip per-ticker fundamentals/intraday/edgar",
    )
    exclusive.add_argument(
        "--edgar-only",
        action="store_true",
        help="Fetch only EDGAR submissions (forces --edgar-source) and skip fundamentals/intraday",
    )
    return parser.parse_args(argv)


def _parse_tickers(raw: str) -> List[str]:
    return [t.strip().upper() for t in raw.split(",") if t.strip()]


async def run_with_args(args: argparse.Namespace) -> None:
    now_utc = datetime.now(timezone.utc)
    workflow_id = args.workflow_id or f"market_data_{now_utc.strftime('%Y%m%dT%H%M%SZ')}"
    task_queue = args.task_queue or _env_or("TEMPORAL_TASK_QUEUE", DEFAULT_TASK_QUEUE)
    address = args.address or _env_or("TEMPORAL_ADDRESS", DEFAULT_TEMPORAL_ADDRESS)
    workflow_name = args.workflow_name or _env_or("TEMPORAL_WORKFLOW", DEFAULT_WORKFLOW_NAME)

    tickers = _parse_tickers(args.tickers)
    if args.edgar_only:
        args.edgar_source = True

    client = await Client.connect(address)
    execution = await client.start_workflow(
        workflow_name,
        args=[
            tickers,
            args.start_date,
            args.end_date,
            args.intraday_frequency,
            args.fundamentals_mode,
            args.intraday_mode,
            args.edgar_source,
            args.metadata_only,
            args.edgar_only,
        ],
        id=workflow_id,
        task_queue=task_queue,
    )
    print(
        f"Started workflow {workflow_id} ({execution.id}) for tickers={tickers} "
        f"window={args.start_date}..{args.end_date} intraday_freq={args.intraday_frequency} "
        f"fundamentals_mode={args.fundamentals_mode} intraday_mode={args.intraday_mode} "
        f"edgar_source={args.edgar_source} metadata_only={args.metadata_only} edgar_only={args.edgar_only}"
    )


async def main() -> None:
    args = parse_args()
    await run_with_args(args)


if __name__ == "__main__":
    asyncio.run(main())
