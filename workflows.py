import asyncio
from datetime import timedelta
from typing import Dict, List

from temporalio import workflow
from temporalio.common import RetryPolicy

# Activity names are used instead of importing the activity module because the Temporal
# workflow sandbox blocks dependencies used by activities (e.g., httpx/sniffio).
CHECK_MARKETIO_HEALTH = "check_marketio_health"
FETCH_COMPANIES_METADATA = "fetch_companies_metadata"
FETCH_FUNDAMENTALS_RAW = "fetch_fundamentals_raw"
FETCH_FUNDAMENTALS_STAGE = "fetch_fundamentals_stage"
FETCH_FUNDAMENTALS_PROD = "fetch_fundamentals_prod"
FETCH_INTRADAY_RAW = "fetch_intraday_raw"
FETCH_INTRADAY_PROD = "fetch_intraday_prod"

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
            CHECK_MARKETIO_HEALTH,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=SHORT_RETRY,
        )

        # Metadata once per run
        metadata_result = await workflow.execute_activity(
            FETCH_COMPANIES_METADATA,
            args=[tickers],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=SHORT_RETRY,
        )

        results: Dict[str, List[dict]] = {"metadata": [metadata_result]}

        async def process_ticker(ticker: str) -> Dict[str, List[dict]]:
            ticker_results: Dict[str, List[dict]] = {}
            # Fundamentals path
            fundamentals_raw = await workflow.execute_activity(
                FETCH_FUNDAMENTALS_RAW,
                args=[[ticker], start_date, end_date],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=LONG_RETRY,
            ) if fundamentals_mode in {"raw", "stage", "prod"} else []

            if fundamentals_mode in {"stage", "prod"}:
                fundamentals_stage = await workflow.execute_activity(
                    FETCH_FUNDAMENTALS_STAGE,
                    args=[[ticker], start_date, end_date],
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=LONG_RETRY,
                )
            else:
                fundamentals_stage = []

            if fundamentals_mode == "prod":
                fundamentals_prod = await workflow.execute_activity(
                    FETCH_FUNDAMENTALS_PROD,
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
                FETCH_INTRADAY_RAW,
                args=[[ticker], start_date, end_date, intraday_frequency],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=LONG_RETRY,
            ) if intraday_mode in {"raw", "prod"} else []

            if intraday_mode == "prod":
                intraday_prod = await workflow.execute_activity(
                    FETCH_INTRADAY_PROD,
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
