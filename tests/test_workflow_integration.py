import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from typing import Any, Callable, Dict, List

import pytest
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from workflows import MarketDataWorkflow


def _artifact_ref(ticker: str, dataset: str, layer: str, count: int = 1) -> Dict[str, Any]:
    return {
        "uri": f"gs://bucket/{layer}/{dataset}/{ticker}/artifact.json",
        "object_path": f"{layer}/{dataset}/{ticker}/artifact.json",
        "layer": layer,
        "dataset": dataset,
        "universe_key": "mmh5r1",
        "request_id": "req-123",
        "workflow_id": "wf-123",
        "workflow_run_id": "run-123",
        "ticker": ticker,
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "record_count": count,
    }


async def _run_workflow(
    request_payload: Dict[str, Any],
    activities_impl: List[Callable[..., Any]],
) -> Dict[str, Any]:
    task_queue = "marketflow-test-task-queue"
    try:
        env = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as exc:
        pytest.skip(f"Temporal test server unavailable in this environment: {exc}")
    async with env:
        activity_executor = ThreadPoolExecutor(max_workers=8)
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[MarketDataWorkflow],
            activities=activities_impl,
            activity_executor=activity_executor,
        )
        worker_task = asyncio.create_task(worker.run())
        try:
            handle = await env.client.start_workflow(
                MarketDataWorkflow.run,
                args=[request_payload],
                id=f"workflow-{request_payload['request_id']}",
                task_queue=task_queue,
            )
            return await handle.result()
        finally:
            worker_task.cancel()
            with suppress(asyncio.CancelledError):
                await worker_task
            activity_executor.shutdown(wait=True)


def test_metadata_only_workflow() -> None:
    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

    @activity.defn(name="fetch_companies_metadata")
    def fetch_companies_metadata(
        tickers: List[str],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "ciks": {ticker: "0000123456" for ticker in tickers},
            "rics": {ticker: f"{ticker}.N" for ticker in tickers},
            "record_count": len(tickers),
            "tickers": tickers,
        }

    result = asyncio.run(
        _run_workflow(
            {
                "universe_key": "mmh5r1",
                "tickers": ["AA"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "metadata_only": True,
                "request_id": "req-metadata",
            },
            [check_marketio_health, fetch_companies_metadata],
        )
    )
    assert result["request_id"] == "req-metadata"
    assert result["metadata"][0]["record_count"] == 1
    assert "AA" not in result


def test_edgar_only_workflow() -> None:
    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

    @activity.defn(name="fetch_companies_metadata")
    def fetch_companies_metadata(
        tickers: List[str],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "ciks": {ticker: "0000123456" for ticker in tickers},
            "rics": {ticker: f"{ticker}.N" for ticker in tickers},
            "record_count": len(tickers),
            "tickers": tickers,
        }

    @activity.defn(name="fetch_edgar_source")
    def fetch_edgar_source(
        tickers: List[str],
        ciks: List[str],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        ticker = tickers[0] if tickers else "AA"
        return [_artifact_ref(ticker, "edgar", "source")]

    result = asyncio.run(
        _run_workflow(
            {
                "universe_key": "mmh5r1",
                "tickers": ["AA"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "edgar_only": True,
                "request_id": "req-edgar",
            },
            [check_marketio_health, fetch_companies_metadata, fetch_edgar_source],
        )
    )
    assert result["AA"]["edgar_source"][0]["dataset"] == "edgar"
    assert result["AA"]["fundamentals_raw"] == []
    assert result["AA"]["intraday_raw"] == []


def test_full_pipeline_workflow() -> None:
    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

    @activity.defn(name="fetch_companies_metadata")
    def fetch_companies_metadata(
        tickers: List[str],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "ciks": {ticker: "0000123456" for ticker in tickers},
            "rics": {ticker: f"{ticker}.N" for ticker in tickers},
            "record_count": len(tickers),
            "tickers": tickers,
        }

    @activity.defn(name="fetch_fundamentals_raw")
    def fetch_fundamentals_raw(
        ticker: str,
        ric: str,
        start_date: str,
        end_date: str,
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        ref = _artifact_ref(ticker, "fundamentals", "source")
        ref["ric"] = ric
        ref["primary_ric"] = ric
        return [ref]

    @activity.defn(name="fetch_fundamentals_prod")
    def fetch_fundamentals_prod(
        raw_artifacts: List[Dict[str, Any]],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        return [_artifact_ref(raw_artifacts[0]["ticker"], "fundamentals", "prod")]

    @activity.defn(name="fetch_intraday_raw")
    def fetch_intraday_raw(
        ticker: str,
        ric: str,
        start_date: str,
        end_date: str,
        frequency: str,
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        ref = _artifact_ref(ticker, "intraday", "source")
        ref["ric"] = ric
        ref["primary_ric"] = ric
        return [ref]

    @activity.defn(name="fetch_intraday_prod")
    def fetch_intraday_prod(
        raw_artifacts: List[Dict[str, Any]],
        frequency: str,
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        return [_artifact_ref(raw_artifacts[0]["ticker"], "intraday", "prod")]

    result = asyncio.run(
        _run_workflow(
            {
                "universe_key": "mmh5r1",
                "tickers": ["AA"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "request_id": "req-full",
            },
            [
                check_marketio_health,
                fetch_companies_metadata,
                fetch_fundamentals_raw,
                fetch_fundamentals_prod,
                fetch_intraday_raw,
                fetch_intraday_prod,
            ],
        )
    )
    assert result["AA"]["fundamentals_prod"][0]["layer"] == "prod"
    assert result["AA"]["fundamentals_stage"] == []
    assert result["AA"]["intraday_prod"][0]["dataset"] == "intraday"


def test_partial_failure_isolated_per_ticker() -> None:
    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

    @activity.defn(name="fetch_companies_metadata")
    def fetch_companies_metadata(
        tickers: List[str],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "ciks": {ticker: "0000123456" for ticker in tickers},
            "rics": {ticker: f"{ticker}.N" for ticker in tickers},
            "record_count": len(tickers),
            "tickers": tickers,
        }

    @activity.defn(name="fetch_fundamentals_raw")
    def fetch_fundamentals_raw(
        ticker: str,
        ric: str,
        start_date: str,
        end_date: str,
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        if ticker == "NUE":
            raise ApplicationError("invalid upstream data", non_retryable=True)
        ref = _artifact_ref(ticker, "fundamentals", "source")
        ref["ric"] = ric
        ref["primary_ric"] = ric
        return [ref]

    @activity.defn(name="fetch_intraday_raw")
    def fetch_intraday_raw(
        ticker: str,
        ric: str,
        start_date: str,
        end_date: str,
        frequency: str,
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        ref = _artifact_ref(ticker, "intraday", "source")
        ref["ric"] = ric
        ref["primary_ric"] = ric
        return [ref]

    result = asyncio.run(
        _run_workflow(
            {
                "universe_key": "mmh5r1",
                "tickers": ["AA", "NUE"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "fundamentals_mode": "raw",
                "intraday_mode": "raw",
                "request_id": "req-partial",
            },
            [check_marketio_health, fetch_companies_metadata, fetch_fundamentals_raw, fetch_intraday_raw],
        )
    )
    assert result["AA"]["fundamentals_raw"][0]["ticker"] == "AA"
    assert result["NUE"][0]["type"] == "ApplicationError"


def test_eod_frequency_is_normalized_before_intraday_activity() -> None:
    captured: Dict[str, str] = {}

    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

    @activity.defn(name="fetch_companies_metadata")
    def fetch_companies_metadata(
        tickers: List[str],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "ciks": {ticker: "0000123456" for ticker in tickers},
            "rics": {ticker: f"{ticker}.N" for ticker in tickers},
            "record_count": len(tickers),
            "tickers": tickers,
        }

    @activity.defn(name="fetch_intraday_raw")
    def fetch_intraday_raw(
        ticker: str,
        ric: str,
        start_date: str,
        end_date: str,
        frequency: str,
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        captured["frequency"] = frequency
        ref = _artifact_ref(ticker, "intraday", "source")
        ref["ric"] = ric
        ref["primary_ric"] = ric
        return [ref]

    result = asyncio.run(
        _run_workflow(
            {
                "universe_key": "mmh5r1",
                "tickers": ["AA"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "fundamentals_mode": "none",
                "intraday_mode": "raw",
                "intraday_frequency": "eod",
                "request_id": "req-eod",
            },
            [check_marketio_health, fetch_companies_metadata, fetch_intraday_raw],
        )
    )

    assert captured["frequency"] == "daily"
    assert result["AA"]["intraday_raw"][0]["ticker"] == "AA"


def test_downstream_processing_uses_metadata_tickers_when_request_omits_tickers() -> None:
    captured: Dict[str, str] = {}

    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

    @activity.defn(name="fetch_companies_metadata")
    def fetch_companies_metadata(
        tickers: List[str],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        assert tickers == []
        return {
            "ciks": {"AA": "0000123456"},
            "rics": {"AA": "AA.N"},
            "record_count": 1,
            "tickers": ["AA"],
        }

    @activity.defn(name="fetch_fundamentals_raw")
    def fetch_fundamentals_raw(
        ticker: str,
        ric: str,
        start_date: str,
        end_date: str,
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        captured["ticker"] = ticker
        ref = _artifact_ref(ticker, "fundamentals", "source")
        ref["ric"] = ric
        ref["primary_ric"] = ric
        return [ref]

    result = asyncio.run(
        _run_workflow(
            {
                "universe_key": "mmh5r1",
                "tickers": [],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "fundamentals_mode": "raw",
                "intraday_mode": "none",
                "request_id": "req-full-universe",
            },
            [check_marketio_health, fetch_companies_metadata, fetch_fundamentals_raw],
        )
    )

    assert captured["ticker"] == "AA"
    assert result["AA"]["fundamentals_raw"][0]["ticker"] == "AA"
