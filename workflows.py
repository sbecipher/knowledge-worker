import asyncio
from datetime import timedelta
from typing import Dict, List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

from config import load_settings
from activities import (
    check_marketio_health,
    fetch_companies_metadata,
    fetch_fundamentals_prod,
    fetch_fundamentals_raw,
    fetch_fundamentals_stage,
    fetch_intraday_prod,
    fetch_intraday_raw,
)

SETTINGS = load_settings()

# Retry policies tuned for HTTP work
SHORT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
)
LONG_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=120),
    maximum_attempts=3,
)


@workflow.defn
class MarketDataWorkflow:
    @workflow.run
    async def run(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
        intraday_frequency: str = "daily",
        fundamentals_mode: str = "prod",
        intraday_mode: str = "prod",
    ) -> Dict[str, List[dict]]:
        """
        Orchestrates metadata, fundamentals (raw->stage->prod), and intraday (raw->prod).
        """
        await workflow.execute_activity(
            check_marketio_health,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=SHORT_RETRY,
        )

        # Metadata once per run
        metadata_result = await workflow.execute_activity(
            fetch_companies_metadata,
            args=[tickers],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=SHORT_RETRY,
        )

        results: Dict[str, List[dict]] = {"metadata": [metadata_result]}

        async def process_ticker(ticker: str) -> Dict[str, List[dict]]:
            ticker_results: Dict[str, List[dict]] = {}
            # Fundamentals path
            fundamentals_raw = await workflow.execute_activity(
                fetch_fundamentals_raw,
                args=[[ticker], start_date, end_date],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=LONG_RETRY,
            ) if fundamentals_mode in {"raw", "stage", "prod"} else []

            if fundamentals_mode in {"stage", "prod"}:
                fundamentals_stage = await workflow.execute_activity(
                    fetch_fundamentals_stage,
                    args=[[ticker], start_date, end_date],
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=LONG_RETRY,
                )
            else:
                fundamentals_stage = []

            if fundamentals_mode == "prod":
                fundamentals_prod = await workflow.execute_activity(
                    fetch_fundamentals_prod,
                    args=[fundamentals_stage],
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=LONG_RETRY,
                )
            else:
                fundamentals_prod = []

            ticker_results["fundamentals_raw"] = fundamentals_raw
            ticker_results["fundamentals_stage"] = fundamentals_stage
            ticker_results["fundamentals_prod"] = fundamentals_prod

            # Intraday path
            intraday_raw = await workflow.execute_activity(
                fetch_intraday_raw,
                args=[[ticker], start_date, end_date, intraday_frequency],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=LONG_RETRY,
            ) if intraday_mode in {"raw", "prod"} else []

            if intraday_mode == "prod":
                intraday_prod = await workflow.execute_activity(
                    fetch_intraday_prod,
                    args=[intraday_raw, intraday_frequency],
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=LONG_RETRY,
                )
            else:
                intraday_prod = []

            ticker_results["intraday_raw"] = intraday_raw
            ticker_results["intraday_prod"] = intraday_prod
            return ticker_results

        tasks = [process_ticker(ticker) for ticker in tickers]
        ticker_outputs = await asyncio.gather(*tasks)
        for ticker, output in zip(tickers, ticker_outputs):
            results[ticker] = output
        return results
