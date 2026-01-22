import asyncio
import os
from typing import List

import client


def _env_str(name: str) -> str:
    return os.getenv(name, "").strip()


def _env_bool(name: str) -> bool:
    value = _env_str(name)
    return value.lower() in {"1", "true", "yes", "y", "on"}


def _require_env(name: str) -> str:
    value = _env_str(name)
    if not value:
        raise SystemExit(f"Missing required env var: {name}")
    return value


def _build_argv() -> List[str]:
    argv: List[str] = []

    argv += ["--tickers", _require_env("TICKERS")]
    argv += ["--start-date", _require_env("START_DATE")]
    argv += ["--end-date", _require_env("END_DATE")]

    intraday_frequency = _env_str("INTRADAY_FREQUENCY")
    if intraday_frequency:
        argv += ["--intraday-frequency", intraday_frequency]

    fundamentals_mode = _env_str("FUNDAMENTALS_MODE")
    if fundamentals_mode:
        argv += ["--fundamentals-mode", fundamentals_mode]

    intraday_mode = _env_str("INTRADAY_MODE")
    if intraday_mode:
        argv += ["--intraday-mode", intraday_mode]

    workflow_id = _env_str("WORKFLOW_ID")
    if workflow_id:
        argv += ["--workflow-id", workflow_id]

    task_queue = _env_str("TEMPORAL_TASK_QUEUE") or _env_str("TASK_QUEUE")
    if task_queue:
        argv += ["--task-queue", task_queue]

    address = _env_str("TEMPORAL_ADDRESS")
    if address:
        argv += ["--address", address]

    if _env_bool("EDGAR_SOURCE"):
        argv.append("--edgar-source")
    if _env_bool("METADATA_ONLY"):
        argv.append("--metadata-only")
    if _env_bool("EDGAR_ONLY"):
        argv.append("--edgar-only")

    return argv


def main() -> None:
    argv = _build_argv()
    args = client.parse_args(argv)
    asyncio.run(client.run_with_args(args))


if __name__ == "__main__":
    main()
