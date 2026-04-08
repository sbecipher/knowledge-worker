import asyncio
from datetime import timedelta
from typing import Any, Dict, List

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from models import (
    DEFAULT_INTRADAY_FREQUENCY,
    ExecutionMetadata,
    MarketDataRequest,
)

# Activity names are used instead of importing the activity module because the Temporal
# workflow sandbox blocks dependencies used by activities (e.g., httpx/google-cloud-*).
CHECK_MARKETIO_HEALTH = "check_marketio_health"
FETCH_COMPANIES_METADATA = "fetch_companies_metadata"
FETCH_EDGAR_SOURCE = "fetch_edgar_source"
FETCH_FUNDAMENTALS_RAW = "fetch_fundamentals_raw"
FETCH_FUNDAMENTALS_STAGE = "fetch_fundamentals_stage"
FETCH_FUNDAMENTALS_PROD = "fetch_fundamentals_prod"
FETCH_INTRADAY_RAW = "fetch_intraday_raw"
FETCH_INTRADAY_PROD = "fetch_intraday_prod"

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


def _error_result(exc: Exception) -> Dict[str, str]:
    cause = getattr(exc, "cause", None)
    if cause is not None:
        cause_type = getattr(cause, "type", None) or cause.__class__.__name__
        return {
            "error": str(cause),
            "type": str(cause_type),
            "outer_type": exc.__class__.__name__,
        }
    return {"error": str(exc), "type": exc.__class__.__name__}


def _validate_request(request: MarketDataRequest) -> None:
    if request.metadata_only and request.edgar_only:
        raise ApplicationError(
            "metadata_only and edgar_only cannot both be true",
            type="InvalidRequest",
            non_retryable=True,
        )
    if not request.universe_key:
        raise ApplicationError("universe_key is required", type="InvalidRequest", non_retryable=True)
    if request.intraday_frequency != DEFAULT_INTRADAY_FREQUENCY:
        raise ApplicationError(
            "intraday_frequency must be one of: daily, eod",
            type="InvalidRequest",
            non_retryable=True,
        )
    if request.fundamentals_mode == "stage":
        raise ApplicationError(
            "fundamentals_mode='stage' is no longer supported by the Marketio API",
            type="InvalidRequest",
            non_retryable=True,
        )
    if request.fundamentals_mode not in {"none", "raw", "prod"}:
        raise ApplicationError(
            f"Unsupported fundamentals_mode: {request.fundamentals_mode}",
            type="InvalidRequest",
            non_retryable=True,
        )
    if request.intraday_mode not in {"none", "raw", "prod"}:
        raise ApplicationError(
            f"Unsupported intraday_mode: {request.intraday_mode}",
            type="InvalidRequest",
            non_retryable=True,
        )


@workflow.defn
class MarketDataWorkflow:
    @workflow.run
    async def run(self, request_payload: Dict[str, Any]) -> Dict[str, Any]:
        request = MarketDataRequest.from_payload(request_payload)
        _validate_request(request)

        info = workflow.info()
        execution = ExecutionMetadata(
            request_id=request.request_id or info.workflow_id,
            workflow_id=info.workflow_id,
            workflow_run_id=info.run_id,
        )
        execution_payload = execution.to_payload()

        await workflow.execute_activity(
            CHECK_MARKETIO_HEALTH,
            args=[execution_payload],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=SHORT_RETRY,
        )

        metadata_result = await workflow.execute_activity(
            FETCH_COMPANIES_METADATA,
            args=[request.tickers, request.universe_key, execution_payload],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=SHORT_RETRY,
        )
        results: Dict[str, Any] = {"metadata": [metadata_result], "request_id": execution.request_id}

        if request.metadata_only:
            return results

        ticker_ciks: Dict[str, str] = {}
        ticker_rics: Dict[str, str] = {}
        if isinstance(metadata_result, dict):
            ciks_map = metadata_result.get("ciks") or {}
            if isinstance(ciks_map, dict):
                ticker_ciks = {
                    str(ticker).upper(): str(cik).zfill(10)
                    for ticker, cik in ciks_map.items()
                    if ticker and cik
                }
            rics_map = metadata_result.get("rics") or {}
            if isinstance(rics_map, dict):
                ticker_rics = {
                    str(ticker).upper(): str(ric).strip().upper()
                    for ticker, ric in rics_map.items()
                    if ticker and ric
                }
        workflow_tickers = request.tickers
        if not workflow_tickers and isinstance(metadata_result, dict):
            workflow_tickers = [
                str(ticker).upper()
                for ticker in metadata_result.get("tickers", [])
                if str(ticker).strip()
            ]

        do_edgar = request.edgar_only or request.edgar_source
        do_fundamentals = request.fundamentals_mode in {"raw", "prod"} and not request.edgar_only
        do_intraday = request.intraday_mode in {"raw", "prod"} and not request.edgar_only

        async def process_ticker(ticker: str) -> Dict[str, List[dict]]:
            ticker_results: Dict[str, List[dict]] = {}
            edgar_kwargs: Dict[str, List[str]] = {}
            cik_value = ticker_ciks.get(ticker.upper())
            ric_value = ticker_rics.get(ticker.upper())
            if cik_value:
                edgar_kwargs["ciks"] = [cik_value]
            else:
                edgar_kwargs["tickers"] = [ticker]

            edgar_payload = (
                await workflow.execute_activity(
                    FETCH_EDGAR_SOURCE,
                    args=[
                        edgar_kwargs.get("tickers"),
                        edgar_kwargs.get("ciks"),
                        request.universe_key,
                        execution_payload,
                    ],
                    start_to_close_timeout=timedelta(minutes=3),
                    retry_policy=LONG_RETRY,
                )
                if do_edgar
                else []
            )

            fundamentals_raw = (
                await workflow.execute_activity(
                    FETCH_FUNDAMENTALS_RAW,
                    args=[
                        ticker,
                        ric_value,
                        request.start_date,
                        request.end_date,
                        request.universe_key,
                        execution_payload,
                    ],
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=LONG_RETRY,
                )
                if do_fundamentals
                else []
            )
            fundamentals_stage: List[dict] = []

            if request.fundamentals_mode == "prod" and do_fundamentals:
                fundamentals_prod = await workflow.execute_activity(
                    FETCH_FUNDAMENTALS_PROD,
                    args=[fundamentals_raw, request.universe_key, execution_payload],
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=LONG_RETRY,
                )
            else:
                fundamentals_prod = []

            ticker_results["edgar_source"] = edgar_payload
            ticker_results["fundamentals_raw"] = fundamentals_raw
            ticker_results["fundamentals_stage"] = fundamentals_stage
            ticker_results["fundamentals_prod"] = fundamentals_prod

            intraday_raw = (
                await workflow.execute_activity(
                    FETCH_INTRADAY_RAW,
                    args=[
                        ticker,
                        ric_value,
                        request.start_date,
                        request.end_date,
                        request.intraday_frequency,
                        request.universe_key,
                        execution_payload,
                    ],
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=LONG_RETRY,
                )
                if do_intraday
                else []
            )

            if request.intraday_mode == "prod" and do_intraday:
                intraday_prod = await workflow.execute_activity(
                    FETCH_INTRADAY_PROD,
                    args=[
                        intraday_raw,
                        request.intraday_frequency,
                        request.universe_key,
                        execution_payload,
                    ],
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=LONG_RETRY,
                )
            else:
                intraday_prod = []

            ticker_results["intraday_raw"] = intraday_raw
            ticker_results["intraday_prod"] = intraday_prod
            return ticker_results

        semaphore = asyncio.Semaphore(request.max_concurrent_tickers)

        async def process_ticker_limited(ticker: str) -> Dict[str, List[dict]]:
            async with semaphore:
                return await process_ticker(ticker)

        tasks = [process_ticker_limited(ticker) for ticker in workflow_tickers]
        ticker_outputs = await asyncio.gather(*tasks, return_exceptions=True)
        for ticker, output in zip(workflow_tickers, ticker_outputs):
            if isinstance(output, Exception):
                results[ticker] = [_error_result(output)]
            else:
                results[ticker] = output
        return results
