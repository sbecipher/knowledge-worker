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
LOAD_ACTIVE_UNIVERSE_INDEX = "load_active_universe_index"
RESOLVE_COMPANY_IDENTIFIERS = "resolve_company_identifiers"
PERSIST_COMPANY_METADATA = "persist_company_metadata"
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
    if request.metadata_mode not in {"none", "source"}:
        raise ApplicationError(
            f"Unsupported metadata_mode: {request.metadata_mode}",
            type="InvalidRequest",
            non_retryable=True,
        )
    requires_universe_key = (
        not request.tickers
        or request.metadata_only
        or request.metadata_mode == "source"
        or request.edgar_only
        or request.edgar_source
    )
    if requires_universe_key and not request.universe_key:
        raise ApplicationError(
            "universe_key is required for metadata, EDGAR, or full-universe runs",
            type="InvalidRequest",
            non_retryable=True,
        )
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

        metadata_requested = request.metadata_only or request.metadata_mode == "source"
        workflow_tickers = list(request.tickers)
        ticker_ciks: Dict[str, str] = {}
        ticker_rics: Dict[str, str] = {}
        active_index: Dict[str, Any] = {}
        identifier_resolution: Dict[str, Any] = {}
        metadata_refs_by_ticker: Dict[str, List[dict]] = {}
        results: Dict[str, Any] = {
            "request_id": execution.request_id,
            "identifiers": {
                "ciks": {},
                "rics": {},
                "missing_from_active": [],
                "missing_from_provider": [],
                "active_source_uri": None,
                "active_source_object_path": None,
            },
        }

        if not workflow_tickers:
            active_index = await workflow.execute_activity(
                LOAD_ACTIVE_UNIVERSE_INDEX,
                args=[request.universe_key, execution_payload],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=SHORT_RETRY,
            )
            if isinstance(active_index, dict):
                workflow_tickers = [
                    str(ticker).upper()
                    for ticker in active_index.get("tickers", [])
                    if str(ticker).strip()
                ]
                rics_map = active_index.get("rics") or {}
                if isinstance(rics_map, dict):
                    ticker_rics.update(
                        {
                            str(ticker).upper(): str(ric).strip().upper()
                            for ticker, ric in rics_map.items()
                            if ticker and ric
                        }
                    )
                results["identifiers"]["active_source_uri"] = active_index.get("active_source_uri")
                results["identifiers"]["active_source_object_path"] = active_index.get("active_source_object_path")
                results["identifiers"]["rics"] = dict(ticker_rics)

        if not workflow_tickers:
            raise ApplicationError(
                "No tickers available for workflow execution",
                type="InvalidRequest",
                non_retryable=True,
            )

        needs_identifier_resolution = request.edgar_only or request.edgar_source or metadata_requested
        if needs_identifier_resolution:
            identifier_resolution = await workflow.execute_activity(
                RESOLVE_COMPANY_IDENTIFIERS,
                args=[workflow_tickers, request.universe_key, metadata_requested, execution_payload],
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=SHORT_RETRY,
            )
            if isinstance(identifier_resolution, dict):
                ciks_map = identifier_resolution.get("ciks") or {}
                if isinstance(ciks_map, dict):
                    ticker_ciks = {
                        str(ticker).upper(): str(cik).zfill(10)
                        for ticker, cik in ciks_map.items()
                        if ticker and cik
                    }
                rics_map = identifier_resolution.get("rics") or {}
                if isinstance(rics_map, dict):
                    ticker_rics.update(
                        {
                            str(ticker).upper(): str(ric).strip().upper()
                            for ticker, ric in rics_map.items()
                            if ticker and ric
                        }
                    )
                results["identifiers"] = {
                    "ciks": dict(ticker_ciks),
                    "rics": dict(ticker_rics),
                    "missing_from_active": list(identifier_resolution.get("missing_from_active") or []),
                    "missing_from_provider": list(identifier_resolution.get("missing_from_provider") or []),
                    "active_source_uri": identifier_resolution.get("active_source_uri")
                    or results["identifiers"].get("active_source_uri"),
                    "active_source_object_path": identifier_resolution.get("active_source_object_path")
                    or results["identifiers"].get("active_source_object_path"),
                }

        if metadata_requested:
            if not isinstance(identifier_resolution, dict) or not identifier_resolution:
                raise ApplicationError(
                    "Metadata persistence requires identifier resolution results",
                    type="WorkflowStateError",
                    non_retryable=True,
                )
            metadata_summary = await workflow.execute_activity(
                PERSIST_COMPANY_METADATA,
                args=[identifier_resolution, request.universe_key, execution_payload],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=LONG_RETRY,
            )
            if isinstance(metadata_summary, dict):
                metadata_refs = metadata_summary.get("artifacts_by_ticker") or {}
                if isinstance(metadata_refs, dict):
                    metadata_refs_by_ticker = {
                        str(ticker).upper(): artifacts
                        for ticker, artifacts in metadata_refs.items()
                        if isinstance(artifacts, list)
                    }
                results["metadata"] = {
                    "manifest_uri": metadata_summary.get("manifest_uri"),
                    "manifest_object_path": metadata_summary.get("manifest_object_path"),
                    "persisted_tickers": list(metadata_summary.get("persisted_tickers") or []),
                }

        def _base_ticker_result(ticker: str) -> Dict[str, Any]:
            payload: Dict[str, Any] = {
                "edgar_source": [],
                "fundamentals_raw": [],
                "fundamentals_stage": [],
                "fundamentals_prod": [],
                "intraday_raw": [],
                "intraday_prod": [],
            }
            metadata_source = metadata_refs_by_ticker.get(ticker.upper())
            if metadata_source:
                payload["metadata_source"] = metadata_source
            return payload

        if request.metadata_only:
            for ticker in workflow_tickers:
                results[ticker] = _base_ticker_result(ticker)
            return results

        do_edgar = request.edgar_only or request.edgar_source
        do_fundamentals = request.fundamentals_mode in {"raw", "prod"} and not request.edgar_only
        do_intraday = request.intraday_mode in {"raw", "prod"} and not request.edgar_only

        async def process_ticker(ticker: str) -> Dict[str, Any]:
            ticker_results = _base_ticker_result(ticker)
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

        async def process_ticker_limited(ticker: str) -> Dict[str, Any]:
            async with semaphore:
                return await process_ticker(ticker)

        tasks = [process_ticker_limited(ticker) for ticker in workflow_tickers]
        ticker_outputs = await asyncio.gather(*tasks, return_exceptions=True)
        for ticker, output in zip(workflow_tickers, ticker_outputs):
            base_result = _base_ticker_result(ticker)
            if isinstance(output, Exception):
                base_result["error"] = _error_result(output)
                results[ticker] = base_result
            else:
                results[ticker] = output
        return results
