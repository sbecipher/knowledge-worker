import asyncio
from datetime import timedelta
from typing import Dict, List

from temporalio import workflow
from temporalio.common import RetryPolicy

# Activity names are used instead of importing the activity module because the Temporal
# workflow sandbox blocks dependencies used by activities (e.g., httpx/sniffio).
CHECK_MARKETIO_HEALTH = "check_marketio_health"
FETCH_COMPANIES_METADATA = "fetch_companies_metadata"
FETCH_EDGAR_SOURCE = "fetch_edgar_source"
FETCH_FUNDAMENTALS_RAW = "fetch_fundamentals_raw"
FETCH_FUNDAMENTALS_STAGE = "fetch_fundamentals_stage"
FETCH_FUNDAMENTALS_PROD = "fetch_fundamentals_prod"
FETCH_INTRADAY_RAW = "fetch_intraday_raw"
FETCH_INTRADAY_PROD = "fetch_intraday_prod"

MAX_CONCURRENT_TICKERS = 5

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
        edgar_source: bool = False,
        metadata_only: bool = False,
        edgar_only: bool = False,
    ) -> Dict[str, List[dict]]:
        """
        Orchestrates metadata, fundamentals (raw->stage->prod), intraday (raw->prod), and optional EDGAR pulls.
        """
        if metadata_only and edgar_only:
            raise ValueError("metadata_only and edgar_only cannot both be true")

        await workflow.execute_activity(
            CHECK_MARKETIO_HEALTH,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=SHORT_RETRY,
        )

        metadata_result = await workflow.execute_activity(
            FETCH_COMPANIES_METADATA,
            args=[tickers],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=SHORT_RETRY,
        )
        results: Dict[str, List[dict]] = {"metadata": [metadata_result]}

        if metadata_only:
            return results

        ticker_ciks: Dict[str, str] = {}
        if isinstance(metadata_result, dict):
            ciks_map = metadata_result.get("ciks") or {}
            if isinstance(ciks_map, dict):
                ticker_ciks = {
                    str(ticker).upper(): str(cik).zfill(10)
                    for ticker, cik in ciks_map.items()
                    if ticker and cik
                }

        do_edgar = edgar_only or edgar_source
        do_fundamentals = fundamentals_mode in {"raw", "stage", "prod"} and not edgar_only
        do_intraday = intraday_mode in {"raw", "prod"} and not edgar_only

        async def process_ticker(ticker: str) -> Dict[str, List[dict]]:
            ticker_results: Dict[str, List[dict]] = {}
            edgar_kwargs = {}
            cik_value = ticker_ciks.get(ticker.upper())
            if cik_value:
                edgar_kwargs["ciks"] = [cik_value]
            else:
                edgar_kwargs["tickers"] = [ticker]

            edgar_payload = await workflow.execute_activity(
                FETCH_EDGAR_SOURCE,
                args=[edgar_kwargs.get("tickers"), edgar_kwargs.get("ciks")],
                start_to_close_timeout=timedelta(minutes=3),
                retry_policy=LONG_RETRY,
            ) if do_edgar else []
            # Fundamentals path
            fundamentals_raw = await workflow.execute_activity(
                FETCH_FUNDAMENTALS_RAW,
                args=[[ticker], start_date, end_date],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=LONG_RETRY,
            ) if do_fundamentals else []

            if fundamentals_mode in {"stage", "prod"} and do_fundamentals:
                fundamentals_stage = await workflow.execute_activity(
                    FETCH_FUNDAMENTALS_STAGE,
                    args=[fundamentals_raw],
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=LONG_RETRY,
                )
            else:
                fundamentals_stage = []

            if fundamentals_mode == "prod" and do_fundamentals:
                fundamentals_prod = await workflow.execute_activity(
                    FETCH_FUNDAMENTALS_PROD,
                    args=[fundamentals_stage],
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=LONG_RETRY,
                )
            else:
                fundamentals_prod = []

            ticker_results["edgar_source"] = edgar_payload
            ticker_results["fundamentals_raw"] = fundamentals_raw
            ticker_results["fundamentals_stage"] = fundamentals_stage
            ticker_results["fundamentals_prod"] = fundamentals_prod

            # Intraday path
            intraday_raw = await workflow.execute_activity(
                FETCH_INTRADAY_RAW,
                args=[[ticker], start_date, end_date, intraday_frequency],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=LONG_RETRY,
            ) if do_intraday else []

            if intraday_mode == "prod" and do_intraday:
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

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TICKERS)

        async def process_ticker_limited(ticker: str) -> Dict[str, List[dict]]:
            async with semaphore:
                return await process_ticker(ticker)

        tasks = [process_ticker_limited(ticker) for ticker in tickers]
        ticker_outputs = await asyncio.gather(*tasks)
        for ticker, output in zip(tickers, ticker_outputs):
            results[ticker] = output
        return results
