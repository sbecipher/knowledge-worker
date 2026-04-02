import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from typing import Any, Callable, Dict, List

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
        "instrument": "mm-h5r1",
        "model_version": "metadata",
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
    async with await WorkflowEnvironment.start_time_skipping() as env:
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
        instrument: str,
        model_version: str,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {"ciks": {ticker: "0000123456" for ticker in tickers}, "record_count": len(tickers)}

    result = asyncio.run(
        _run_workflow(
            {
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
        instrument: str,
        model_version: str,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {"ciks": {ticker: "0000123456" for ticker in tickers}, "record_count": len(tickers)}

    @activity.defn(name="fetch_edgar_source")
    def fetch_edgar_source(
        tickers: List[str],
        ciks: List[str],
        instrument: str,
        model_version: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        ticker = tickers[0] if tickers else "AA"
        return [_artifact_ref(ticker, "edgar", "source")]

    result = asyncio.run(
        _run_workflow(
            {
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
        instrument: str,
        model_version: str,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {"ciks": {ticker: "0000123456" for ticker in tickers}, "record_count": len(tickers)}

    @activity.defn(name="fetch_fundamentals_raw")
    def fetch_fundamentals_raw(
        tickers: List[str],
        start_date: str,
        end_date: str,
        instrument: str,
        model_version: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        return [_artifact_ref(tickers[0], "fundamentals", "source")]

    @activity.defn(name="fetch_fundamentals_stage")
    def fetch_fundamentals_stage(
        raw_artifacts: List[Dict[str, Any]],
        instrument: str,
        model_version: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        return [_artifact_ref(raw_artifacts[0]["ticker"], "fundamentals", "stage")]

    @activity.defn(name="fetch_fundamentals_prod")
    def fetch_fundamentals_prod(
        staged_artifacts: List[Dict[str, Any]],
        instrument: str,
        model_version: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        return [_artifact_ref(staged_artifacts[0]["ticker"], "fundamentals", "prod")]

    @activity.defn(name="fetch_intraday_raw")
    def fetch_intraday_raw(
        tickers: List[str],
        start_date: str,
        end_date: str,
        frequency: str,
        instrument: str,
        model_version: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        return [_artifact_ref(tickers[0], "intraday", "source")]

    @activity.defn(name="fetch_intraday_prod")
    def fetch_intraday_prod(
        raw_artifacts: List[Dict[str, Any]],
        frequency: str,
        instrument: str,
        model_version: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        return [_artifact_ref(raw_artifacts[0]["ticker"], "intraday", "prod")]

    result = asyncio.run(
        _run_workflow(
            {
                "tickers": ["AA"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "request_id": "req-full",
            },
            [
                check_marketio_health,
                fetch_companies_metadata,
                fetch_fundamentals_raw,
                fetch_fundamentals_stage,
                fetch_fundamentals_prod,
                fetch_intraday_raw,
                fetch_intraday_prod,
            ],
        )
    )
    assert result["AA"]["fundamentals_prod"][0]["layer"] == "prod"
    assert result["AA"]["intraday_prod"][0]["dataset"] == "intraday"


def test_partial_failure_isolated_per_ticker() -> None:
    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

    @activity.defn(name="fetch_companies_metadata")
    def fetch_companies_metadata(
        tickers: List[str],
        instrument: str,
        model_version: str,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {"ciks": {ticker: "0000123456" for ticker in tickers}, "record_count": len(tickers)}

    @activity.defn(name="fetch_fundamentals_raw")
    def fetch_fundamentals_raw(
        tickers: List[str],
        start_date: str,
        end_date: str,
        instrument: str,
        model_version: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        ticker = tickers[0]
        if ticker == "NUE":
            raise ApplicationError("invalid upstream data", non_retryable=True)
        return [_artifact_ref(ticker, "fundamentals", "source")]

    @activity.defn(name="fetch_intraday_raw")
    def fetch_intraday_raw(
        tickers: List[str],
        start_date: str,
        end_date: str,
        frequency: str,
        instrument: str,
        model_version: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        return [_artifact_ref(tickers[0], "intraday", "source")]

    result = asyncio.run(
        _run_workflow(
            {
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
