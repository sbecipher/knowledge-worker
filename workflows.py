import asyncio
from datetime import date as date_cls, timedelta
from typing import Any, Dict, List

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from models import (
    DEFAULT_PERIOD,
    ExecutionMetadata,
    MarketDataRequest,
)

# Activity names are used instead of importing the activity module because the Temporal
# workflow sandbox blocks dependencies used by activities (e.g., httpx/google-cloud-*).
CHECK_MARKETIO_HEALTH = "check_marketio_health"
LOAD_ACTIVE_UNIVERSE_INDEX = "load_active_universe_index"
RESOLVE_COMPANY_IDENTIFIERS = "resolve_company_identifiers"
PERSIST_COMPANY_METADATA = "persist_company_metadata"
PERSIST_LAYER_MANIFESTS = "persist_layer_manifests"
FETCH_EDGAR_SOURCE = "fetch_edgar_source"
FETCH_FUNDAMENTALS_RAW = "fetch_fundamentals_raw"
FETCH_FUNDAMENTALS_STAGE = "fetch_fundamentals_stage"
FETCH_FUNDAMENTALS_PROD = "fetch_fundamentals_prod"
FETCH_PRICES_RAW = "fetch_prices_raw"
FETCH_PRICES_PROD = "fetch_prices_prod"

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
SIDE_EFFECT_HEARTBEAT_TIMEOUT = timedelta(minutes=2)


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
    if request.market_mode not in {"none", "raw", "prod"}:
        raise ApplicationError(
            f"Unsupported market_mode: {request.market_mode}",
            type="InvalidRequest",
            non_retryable=True,
        )
    if request.period not in {"day", "week", "month", "quarter"}:
        raise ApplicationError(
            f"Unsupported period: {request.period}",
            type="InvalidRequest",
            non_retryable=True,
        )
    if request.fundamentals_mode != "none":
        if not request.start_date or not request.end_date:
            raise ApplicationError(
                "start_date and end_date are required when fundamentals_mode is not none",
                type="InvalidRequest",
                non_retryable=True,
            )
        try:
            start_value = date_cls.fromisoformat(request.start_date)
            end_value = date_cls.fromisoformat(request.end_date)
        except ValueError as exc:
            raise ApplicationError(
                "start_date and end_date must be YYYY-MM-DD",
                type="InvalidRequest",
                non_retryable=True,
            ) from exc
        if start_value > end_value:
            raise ApplicationError(
                "start_date must be on or before end_date",
                type="InvalidRequest",
                non_retryable=True,
            )
    if request.market_mode != "none":
        if not request.as_of_date:
            raise ApplicationError(
                "as_of_date is required when market_mode is not none",
                type="InvalidRequest",
                non_retryable=True,
            )
        try:
            date_cls.fromisoformat(request.as_of_date)
        except ValueError as exc:
            raise ApplicationError(
                "as_of_date must be YYYY-MM-DD",
                type="InvalidRequest",
                non_retryable=True,
            ) from exc


def _reject_legacy_payload_fields(request_payload: Dict[str, Any]) -> None:
    legacy_fields = [
        field
        for field in ("intraday_mode", "intraday_frequency")
        if field in request_payload
    ]
    if legacy_fields:
        raise ApplicationError(
            "Legacy fields are no longer supported: intraday_mode, intraday_frequency. "
            "Use market_mode, period, and as_of_date instead.",
            type="InvalidRequest",
            non_retryable=True,
        )


@workflow.defn
class MarketDataWorkflow:
    @workflow.run
    async def run(self, request_payload: Dict[str, Any]) -> Dict[str, Any]:
        _reject_legacy_payload_fields(request_payload)
        try:
            request = MarketDataRequest.from_payload(request_payload)
        except ValueError as exc:
            raise ApplicationError(
                str(exc),
                type="InvalidRequest",
                non_retryable=True,
            ) from exc
        _validate_request(request)

        info = workflow.info()
        execution = ExecutionMetadata(
            request_id=request.request_id or info.workflow_id,
            workflow_id=info.workflow_id,
            workflow_run_id=info.run_id,
        )
        execution_payload = execution.to_payload()
        use_artifact_partition_date = workflow.patched("artifact-partition-date-heartbeats-v1")
        artifact_execution_payload = (
            {
                **execution_payload,
                "artifact_partition_date": workflow.now().date().isoformat(),
            }
            if use_artifact_partition_date
            else execution_payload
        )
        side_effect_activity_options = (
            {"heartbeat_timeout": SIDE_EFFECT_HEARTBEAT_TIMEOUT}
            if use_artifact_partition_date
            else {}
        )

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
        ticker_exchange_codes: Dict[str, str] = {}
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
            "manifests": {
                "source": [],
                "prod": [],
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
                exchange_codes = active_index.get("exchange_codes") or {}
                if isinstance(exchange_codes, dict):
                    ticker_exchange_codes = {
                        str(ticker).upper(): str(exchange_code).strip().upper()
                        for ticker, exchange_code in exchange_codes.items()
                        if ticker and exchange_code
                    }
                results["identifiers"]["active_source_uri"] = active_index.get("active_source_uri")
                results["identifiers"]["active_source_object_path"] = active_index.get("active_source_object_path")

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
                args=[identifier_resolution, request.universe_key, artifact_execution_payload],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=LONG_RETRY,
                **side_effect_activity_options,
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
                    "persisted_tickers": list(metadata_summary.get("persisted_tickers") or []),
                }

        def _base_ticker_result(ticker: str) -> Dict[str, Any]:
            payload: Dict[str, Any] = {
                "edgar_source": [],
                "fundamentals_raw": [],
                "fundamentals_stage": [],
                "fundamentals_prod": [],
                "prices_raw": [],
                "prices_prod": [],
            }
            metadata_source = metadata_refs_by_ticker.get(ticker.upper())
            if metadata_source:
                payload["metadata_source"] = metadata_source
            return payload

        def _collect_artifact_refs() -> List[dict]:
            artifact_refs: List[dict] = []
            artifact_fields = (
                "metadata_source",
                "edgar_source",
                "fundamentals_raw",
                "fundamentals_stage",
                "fundamentals_prod",
                "prices_raw",
                "prices_prod",
            )
            for ticker in workflow_tickers:
                ticker_payload = results.get(ticker)
                if not isinstance(ticker_payload, dict):
                    continue
                for field in artifact_fields:
                    artifacts = ticker_payload.get(field)
                    if not isinstance(artifacts, list):
                        continue
                    artifact_refs.extend(
                        artifact
                        for artifact in artifacts
                        if isinstance(artifact, dict)
                    )
            return artifact_refs

        async def _persist_workflow_manifests() -> None:
            artifact_refs = _collect_artifact_refs()
            if not artifact_refs:
                return
            manifest_summary = await workflow.execute_activity(
                PERSIST_LAYER_MANIFESTS,
                args=[artifact_refs, request.universe_key, execution_payload],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=LONG_RETRY,
                **side_effect_activity_options,
            )
            if isinstance(manifest_summary, dict):
                results["manifests"] = {
                    "source": list(manifest_summary.get("source") or []),
                    "prod": list(manifest_summary.get("prod") or []),
                }

        if request.metadata_only:
            for ticker in workflow_tickers:
                results[ticker] = _base_ticker_result(ticker)
            await _persist_workflow_manifests()
            return results

        do_edgar = request.edgar_only or request.edgar_source
        do_fundamentals = request.fundamentals_mode in {"raw", "prod"} and not request.edgar_only
        do_market = request.market_mode in {"raw", "prod"} and not request.edgar_only

        async def process_ticker(ticker: str) -> Dict[str, Any]:
            ticker_results = _base_ticker_result(ticker)
            edgar_kwargs: Dict[str, List[str]] = {}
            cik_value = ticker_ciks.get(ticker.upper())
            ric_value = ticker_rics.get(ticker.upper())
            exchange_code = ticker_exchange_codes.get(ticker.upper())
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
                        artifact_execution_payload,
                    ],
                    start_to_close_timeout=timedelta(minutes=3),
                    retry_policy=LONG_RETRY,
                    **side_effect_activity_options,
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
                    **side_effect_activity_options,
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
                    **side_effect_activity_options,
                )
            else:
                fundamentals_prod = []

            ticker_results["edgar_source"] = edgar_payload
            ticker_results["fundamentals_raw"] = fundamentals_raw
            ticker_results["fundamentals_stage"] = fundamentals_stage
            ticker_results["fundamentals_prod"] = fundamentals_prod

            prices_raw = (
                await workflow.execute_activity(
                    FETCH_PRICES_RAW,
                    args=[
                        ticker,
                        ric_value,
                        request.as_of_date,
                        request.period,
                        exchange_code,
                        request.universe_key,
                        execution_payload,
                    ],
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=LONG_RETRY,
                    **side_effect_activity_options,
                )
                if do_market
                else []
            )

            if request.market_mode == "prod" and do_market:
                prices_prod = await workflow.execute_activity(
                    FETCH_PRICES_PROD,
                    args=[
                        prices_raw,
                        request.universe_key,
                        execution_payload,
                    ],
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=LONG_RETRY,
                    **side_effect_activity_options,
                )
            else:
                prices_prod = []

            ticker_results["prices_raw"] = prices_raw
            ticker_results["prices_prod"] = prices_prod
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
        await _persist_workflow_manifests()
        return results
