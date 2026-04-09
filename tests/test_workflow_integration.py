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


def test_metadata_only_workflow_persists_metadata_only() -> None:
    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

    @activity.defn(name="resolve_company_identifiers")
    def resolve_company_identifiers(
        tickers: List[str],
        universe_key: str,
        include_metadata_rows: bool,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        assert tickers == ["AA"]
        assert include_metadata_rows is True
        return {
            "active_source_uri": "gs://bucket/prod/models/mmh5r1/active.json",
            "active_source_object_path": "prod/models/mmh5r1/active.json",
            "tickers": ["AA"],
            "ciks": {"AA": "0000123456"},
            "rics": {"AA": "AA.N"},
            "rows_by_ticker": {"AA": {"ticker": "AA"}},
            "missing_from_active": [],
            "missing_from_provider": [],
        }

    @activity.defn(name="persist_company_metadata")
    def persist_company_metadata(
        identifier_resolution: Dict[str, Any],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "manifest_uri": "gs://bucket/source/metadata/manifests/wf-123.json",
            "manifest_object_path": "source/metadata/manifests/wf-123.json",
            "persisted_tickers": ["AA"],
            "artifacts_by_ticker": {"AA": [_artifact_ref("AA", "metadata", "source")]},
        }

    result = asyncio.run(
        _run_workflow(
            {
                "universe_key": "mmh5r1",
                "tickers": ["AA"],
                "fundamentals_mode": "none",
                "market_mode": "none",
                "metadata_only": True,
                "request_id": "req-metadata",
            },
            [check_marketio_health, resolve_company_identifiers, persist_company_metadata],
        )
    )

    assert result["request_id"] == "req-metadata"
    assert result["metadata"]["manifest_object_path"] == "source/metadata/manifests/wf-123.json"
    assert result["AA"]["metadata_source"][0]["dataset"] == "metadata"
    assert result["AA"]["prices_raw"] == []


def test_edgar_only_workflow_resolves_identifiers_without_persisting_metadata() -> None:
    captured: Dict[str, Any] = {}

    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

    @activity.defn(name="resolve_company_identifiers")
    def resolve_company_identifiers(
        tickers: List[str],
        universe_key: str,
        include_metadata_rows: bool,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        assert include_metadata_rows is False
        return {
            "active_source_uri": "gs://bucket/prod/models/mmh5r1/active.json",
            "active_source_object_path": "prod/models/mmh5r1/active.json",
            "tickers": ["AA"],
            "ciks": {"AA": "0000123456"},
            "rics": {"AA": "AA.N"},
            "rows_by_ticker": {},
            "missing_from_active": [],
            "missing_from_provider": [],
        }

    @activity.defn(name="fetch_edgar_source")
    def fetch_edgar_source(
        tickers: List[str],
        ciks: List[str],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        captured["tickers"] = tickers
        captured["ciks"] = ciks
        return [_artifact_ref("AA", "edgar", "source")]

    result = asyncio.run(
        _run_workflow(
            {
                "universe_key": "mmh5r1",
                "tickers": ["AA"],
                "fundamentals_mode": "none",
                "market_mode": "none",
                "edgar_only": True,
                "request_id": "req-edgar",
            },
            [check_marketio_health, resolve_company_identifiers, fetch_edgar_source],
        )
    )

    assert "metadata" not in result
    assert captured["tickers"] is None
    assert captured["ciks"] == ["0000123456"]
    assert result["identifiers"]["ciks"] == {"AA": "0000123456"}
    assert result["AA"]["edgar_source"][0]["dataset"] == "edgar"


def test_prices_only_explicit_ticker_skips_identifier_resolution() -> None:
    captured: Dict[str, Any] = {}

    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

    @activity.defn(name="fetch_prices_raw")
    def fetch_prices_raw(
        ticker: str,
        ric: str,
        as_of_date: str,
        period: str,
        exchange_code: str,
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        captured["ticker"] = ticker
        captured["ric"] = ric
        ref = _artifact_ref(ticker, "prices", "source")
        ref["ric"] = ric
        return [ref]

    result = asyncio.run(
        _run_workflow(
            {
                "universe_key": "mmh5r1",
                "tickers": ["AA"],
                "as_of_date": "2024-01-31",
                "fundamentals_mode": "none",
                "market_mode": "raw",
                "request_id": "req-prices-only",
            },
            [check_marketio_health, fetch_prices_raw],
        )
    )

    assert captured["ticker"] == "AA"
    assert captured["ric"] is None
    assert result["AA"]["prices_raw"][0]["ticker"] == "AA"
    assert result["identifiers"]["ciks"] == {}


def test_prices_only_explicit_ticker_can_omit_universe_key() -> None:
    captured: Dict[str, Any] = {}

    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

    @activity.defn(name="fetch_prices_raw")
    def fetch_prices_raw(
        ticker: str,
        ric: str,
        as_of_date: str,
        period: str,
        exchange_code: str,
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        captured["ticker"] = ticker
        captured["universe_key"] = universe_key
        ref = _artifact_ref(ticker, "prices", "source")
        return [ref]

    result = asyncio.run(
        _run_workflow(
            {
                "tickers": ["AA"],
                "as_of_date": "2024-01-31",
                "fundamentals_mode": "none",
                "market_mode": "raw",
                "request_id": "req-prices-no-universe",
            },
            [check_marketio_health, fetch_prices_raw],
        )
    )

    assert captured["ticker"] == "AA"
    assert captured["universe_key"] is None
    assert result["AA"]["prices_raw"][0]["ticker"] == "AA"


def test_full_universe_non_edgar_uses_active_universe_for_ticker_expansion_only() -> None:
    captured: Dict[str, Any] = {}

    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

    @activity.defn(name="load_active_universe_index")
    def load_active_universe_index(
        universe_key: str,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "active_source_uri": "gs://bucket/prod/models/mmh5r1/active.json",
            "active_source_object_path": "prod/models/mmh5r1/active.json",
            "tickers": ["AA"],
            "rics": {"AA": "AA.N"},
            "record_count": 1,
            "universe_key": universe_key,
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
        captured["ric"] = ric
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
                "market_mode": "none",
                "request_id": "req-full-universe",
            },
            [check_marketio_health, load_active_universe_index, fetch_fundamentals_raw],
        )
    )

    assert captured["ticker"] == "AA"
    assert captured["ric"] is None
    assert result["identifiers"]["active_source_object_path"] == "prod/models/mmh5r1/active.json"
    assert result["identifiers"]["rics"] == {}
    assert result["AA"]["fundamentals_raw"][0]["ticker"] == "AA"


def test_combined_metadata_source_and_prices_reuses_identifier_resolution() -> None:
    captured: Dict[str, Any] = {}

    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

    @activity.defn(name="resolve_company_identifiers")
    def resolve_company_identifiers(
        tickers: List[str],
        universe_key: str,
        include_metadata_rows: bool,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        captured["include_metadata_rows"] = include_metadata_rows
        return {
            "active_source_uri": "gs://bucket/prod/models/mmh5r1/active.json",
            "active_source_object_path": "prod/models/mmh5r1/active.json",
            "tickers": ["AA"],
            "ciks": {"AA": "0000123456"},
            "rics": {"AA": "AA.N"},
            "rows_by_ticker": {"AA": {"ticker": "AA", "primary_ric": "AA.N"}},
            "missing_from_active": [],
            "missing_from_provider": [],
        }

    @activity.defn(name="persist_company_metadata")
    def persist_company_metadata(
        identifier_resolution: Dict[str, Any],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "manifest_uri": "gs://bucket/source/metadata/manifests/wf-123.json",
            "manifest_object_path": "source/metadata/manifests/wf-123.json",
            "persisted_tickers": ["AA"],
            "artifacts_by_ticker": {"AA": [_artifact_ref("AA", "metadata", "source")]},
        }

    @activity.defn(name="fetch_prices_raw")
    def fetch_prices_raw(
        ticker: str,
        ric: str,
        as_of_date: str,
        period: str,
        exchange_code: str,
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        captured["ric"] = ric
        ref = _artifact_ref(ticker, "prices", "source")
        ref["ric"] = ric
        ref["primary_ric"] = ric
        return [ref]

    result = asyncio.run(
        _run_workflow(
            {
                "universe_key": "mmh5r1",
                "tickers": ["AA"],
                "as_of_date": "2024-01-31",
                "fundamentals_mode": "none",
                "market_mode": "raw",
                "metadata_mode": "source",
                "request_id": "req-combined",
            },
            [check_marketio_health, resolve_company_identifiers, persist_company_metadata, fetch_prices_raw],
        )
    )

    assert captured["include_metadata_rows"] is True
    assert captured["ric"] == "AA.N"
    assert result["metadata"]["persisted_tickers"] == ["AA"]
    assert result["AA"]["metadata_source"][0]["dataset"] == "metadata"
    assert result["AA"]["prices_raw"][0]["ric"] == "AA.N"


def test_partial_failure_isolated_per_ticker() -> None:
    @activity.defn(name="check_marketio_health")
    def check_marketio_health(execution: Dict[str, Any]) -> None:
        return None

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
        return [ref]

    result = asyncio.run(
        _run_workflow(
            {
                "universe_key": "mmh5r1",
                "tickers": ["AA", "NUE"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "fundamentals_mode": "raw",
                "market_mode": "none",
                "request_id": "req-partial",
            },
            [check_marketio_health, fetch_fundamentals_raw],
        )
    )

    assert result["AA"]["fundamentals_raw"][0]["ticker"] == "AA"
    assert result["NUE"]["error"]["type"] == "ApplicationError"
    assert result["NUE"]["fundamentals_raw"] == []
