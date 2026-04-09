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
    if dataset == "metadata":
        object_path = f"{layer}/metadata/end_date=2026-04-09/ticker={ticker}/artifact.json"
    elif dataset == "edgar":
        object_path = f"{layer}/edgar/end_date=2026-04-09/ticker={ticker}/artifact.json"
    elif dataset == "fundamentals":
        object_path = f"{layer}/fundamentals/frequency=FQ/end_date=2024-01-31/ticker={ticker}/artifact.ndjson"
    elif dataset == "prices":
        object_path = f"{layer}/prices/granularity=day/end_date=2024-01-31/ticker={ticker}/artifact.ndjson"
    else:
        object_path = f"{layer}/{dataset}/{ticker}/artifact.json"
    return {
        "uri": f"gs://bucket/{object_path}",
        "object_path": object_path,
        "layer": layer,
        "dataset": dataset,
        "universe_key": "mmh5r1",
        "request_id": "req-123",
        "workflow_id": "wf-123",
        "workflow_run_id": "run-123",
        "ticker": ticker,
        "start_date": "2024-01-01",
        "end_date": "2026-04-09" if dataset in {"metadata", "edgar"} else "2024-01-31",
        "record_count": count,
    }


def _manifest_summary(layer: str, end_date: str, artifact_count: int, datasets: List[str]) -> Dict[str, Any]:
    return {
        "end_date": end_date,
        "manifest_uri": f"gs://bucket/{layer}/manifests/end_date={end_date}/wf-123.json",
        "manifest_object_path": f"{layer}/manifests/end_date={end_date}/wf-123.json",
        "artifact_count": artifact_count,
        "datasets": datasets,
    }


def _persist_layer_manifests_activity(
    response: Dict[str, List[Dict[str, Any]]],
    captured: Dict[str, Any] | None = None,
) -> Callable[..., Any]:
    @activity.defn(name="persist_layer_manifests")
    def persist_layer_manifests(
        artifacts: List[Dict[str, Any]],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> Dict[str, List[Dict[str, Any]]]:
        if captured is not None:
            captured["artifacts"] = artifacts
            captured["universe_key"] = universe_key
            captured["execution"] = execution
        return response

    return persist_layer_manifests


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
            "persisted_tickers": ["AA"],
            "artifacts_by_ticker": {"AA": [_artifact_ref("AA", "metadata", "source")]},
        }

    persist_layer_manifests = _persist_layer_manifests_activity(
        {
            "source": [_manifest_summary("source", "2026-04-09", 1, ["metadata"])],
            "prod": [],
        },
        captured,
    )

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
            [check_marketio_health, resolve_company_identifiers, persist_company_metadata, persist_layer_manifests],
        )
    )

    assert result["request_id"] == "req-metadata"
    assert result["metadata"]["persisted_tickers"] == ["AA"]
    assert result["manifests"]["source"] == [_manifest_summary("source", "2026-04-09", 1, ["metadata"])]
    assert result["manifests"]["prod"] == []
    assert result["AA"]["metadata_source"][0]["dataset"] == "metadata"
    assert result["AA"]["prices_raw"] == []
    assert [artifact["dataset"] for artifact in captured["artifacts"]] == ["metadata"]


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

    persist_layer_manifests = _persist_layer_manifests_activity(
        {
            "source": [_manifest_summary("source", "2026-04-09", 1, ["edgar"])],
            "prod": [],
        }
    )

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
            [check_marketio_health, resolve_company_identifiers, fetch_edgar_source, persist_layer_manifests],
        )
    )

    assert "metadata" not in result
    assert captured["tickers"] is None
    assert captured["ciks"] == ["0000123456"]
    assert result["identifiers"]["ciks"] == {"AA": "0000123456"}
    assert result["AA"]["edgar_source"][0]["dataset"] == "edgar"
    assert result["manifests"]["source"] == [_manifest_summary("source", "2026-04-09", 1, ["edgar"])]
    assert result["manifests"]["prod"] == []


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

    persist_layer_manifests = _persist_layer_manifests_activity(
        {
            "source": [_manifest_summary("source", "2024-01-31", 1, ["prices"])],
            "prod": [],
        }
    )

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
            [check_marketio_health, fetch_prices_raw, persist_layer_manifests],
        )
    )

    assert captured["ticker"] == "AA"
    assert captured["ric"] is None
    assert result["AA"]["prices_raw"][0]["ticker"] == "AA"
    assert result["identifiers"]["ciks"] == {}
    assert result["manifests"]["source"] == [_manifest_summary("source", "2024-01-31", 1, ["prices"])]


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

    persist_layer_manifests = _persist_layer_manifests_activity(
        {
            "source": [_manifest_summary("source", "2024-01-31", 1, ["prices"])],
            "prod": [],
        }
    )

    result = asyncio.run(
        _run_workflow(
            {
                "tickers": ["AA"],
                "as_of_date": "2024-01-31",
                "fundamentals_mode": "none",
                "market_mode": "raw",
                "request_id": "req-prices-no-universe",
            },
            [check_marketio_health, fetch_prices_raw, persist_layer_manifests],
        )
    )

    assert captured["ticker"] == "AA"
    assert captured["universe_key"] is None
    assert result["AA"]["prices_raw"][0]["ticker"] == "AA"
    assert result["manifests"]["source"] == [_manifest_summary("source", "2024-01-31", 1, ["prices"])]


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

    persist_layer_manifests = _persist_layer_manifests_activity(
        {
            "source": [_manifest_summary("source", "2024-01-31", 1, ["fundamentals"])],
            "prod": [],
        }
    )

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
            [check_marketio_health, load_active_universe_index, fetch_fundamentals_raw, persist_layer_manifests],
        )
    )

    assert captured["ticker"] == "AA"
    assert captured["ric"] is None
    assert result["identifiers"]["active_source_object_path"] == "prod/models/mmh5r1/active.json"
    assert result["identifiers"]["rics"] == {}
    assert result["AA"]["fundamentals_raw"][0]["ticker"] == "AA"
    assert result["manifests"]["source"] == [_manifest_summary("source", "2024-01-31", 1, ["fundamentals"])]


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

    persist_layer_manifests = _persist_layer_manifests_activity(
        {
            "source": [
                _manifest_summary("source", "2024-01-31", 1, ["prices"]),
                _manifest_summary("source", "2026-04-09", 1, ["metadata"]),
            ],
            "prod": [],
        }
    )

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
            [
                check_marketio_health,
                resolve_company_identifiers,
                persist_company_metadata,
                fetch_prices_raw,
                persist_layer_manifests,
            ],
        )
    )

    assert captured["include_metadata_rows"] is True
    assert captured["ric"] == "AA.N"
    assert result["metadata"]["persisted_tickers"] == ["AA"]
    assert result["AA"]["metadata_source"][0]["dataset"] == "metadata"
    assert result["AA"]["prices_raw"][0]["ric"] == "AA.N"
    assert result["manifests"]["source"] == [
        _manifest_summary("source", "2024-01-31", 1, ["prices"]),
        _manifest_summary("source", "2026-04-09", 1, ["metadata"]),
    ]
    assert result["manifests"]["prod"] == []


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

    persist_layer_manifests = _persist_layer_manifests_activity(
        {
            "source": [_manifest_summary("source", "2024-01-31", 1, ["fundamentals"])],
            "prod": [],
        }
    )

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
            [check_marketio_health, fetch_fundamentals_raw, persist_layer_manifests],
        )
    )

    assert result["AA"]["fundamentals_raw"][0]["ticker"] == "AA"
    assert result["NUE"]["error"]["type"] == "ApplicationError"
    assert result["NUE"]["fundamentals_raw"] == []
    assert result["manifests"]["source"] == [_manifest_summary("source", "2024-01-31", 1, ["fundamentals"])]


def test_workflow_keeps_multiple_fundamentals_refs_per_ticker() -> None:
    captured: Dict[str, Any] = {}

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
        first = _artifact_ref(ticker, "fundamentals", "source")
        first["object_path"] = "source/fundamentals/frequency=FQ/end_date=2026-03-31/ticker=AA/wf-123.ndjson"
        first["end_date"] = "2026-03-31"
        second = _artifact_ref(ticker, "fundamentals", "source")
        second["object_path"] = "source/fundamentals/frequency=FQ/end_date=2026-06-30/ticker=AA/wf-123.ndjson"
        second["end_date"] = "2026-06-30"
        return [first, second]

    persist_layer_manifests = _persist_layer_manifests_activity(
        {
            "source": [
                _manifest_summary("source", "2026-03-31", 1, ["fundamentals"]),
                _manifest_summary("source", "2026-06-30", 1, ["fundamentals"]),
            ],
            "prod": [],
        },
        captured,
    )

    result = asyncio.run(
        _run_workflow(
            {
                "universe_key": "mmh5r1",
                "tickers": ["AA"],
                "start_date": "2026-01-01",
                "end_date": "2026-12-31",
                "fundamentals_mode": "raw",
                "market_mode": "none",
                "request_id": "req-fundamentals-multi",
            },
            [check_marketio_health, fetch_fundamentals_raw, persist_layer_manifests],
        )
    )

    assert [ref["end_date"] for ref in result["AA"]["fundamentals_raw"]] == ["2026-03-31", "2026-06-30"]
    assert result["manifests"]["source"] == [
        _manifest_summary("source", "2026-03-31", 1, ["fundamentals"]),
        _manifest_summary("source", "2026-06-30", 1, ["fundamentals"]),
    ]
    assert [artifact["end_date"] for artifact in captured["artifacts"]] == ["2026-03-31", "2026-06-30"]


def test_full_run_emits_source_and_prod_manifests() -> None:
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
        ref = _artifact_ref(ticker, "fundamentals", "source")
        ref["end_date"] = "2024-01-31"
        return [ref]

    @activity.defn(name="fetch_fundamentals_prod")
    def fetch_fundamentals_prod(
        source_refs: List[Dict[str, Any]],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        ref = _artifact_ref("AA", "fundamentals", "prod")
        ref["end_date"] = "2024-01-31"
        return [ref]

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
        ref = _artifact_ref(ticker, "prices", "source")
        ref["end_date"] = "2024-01-31"
        return [ref]

    @activity.defn(name="fetch_prices_prod")
    def fetch_prices_prod(
        source_refs: List[Dict[str, Any]],
        universe_key: str,
        execution: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        ref = _artifact_ref("AA", "prices", "prod")
        ref["end_date"] = "2024-01-31"
        return [ref]

    persist_layer_manifests = _persist_layer_manifests_activity(
        {
            "source": [_manifest_summary("source", "2024-01-31", 2, ["fundamentals", "prices"])],
            "prod": [_manifest_summary("prod", "2024-01-31", 2, ["fundamentals", "prices"])],
        }
    )

    result = asyncio.run(
        _run_workflow(
            {
                "universe_key": "mmh5r1",
                "tickers": ["AA"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "as_of_date": "2024-01-31",
                "fundamentals_mode": "prod",
                "market_mode": "prod",
                "request_id": "req-full-run",
            },
            [
                check_marketio_health,
                fetch_fundamentals_raw,
                fetch_fundamentals_prod,
                fetch_prices_raw,
                fetch_prices_prod,
                persist_layer_manifests,
            ],
        )
    )

    assert result["manifests"]["source"] == [_manifest_summary("source", "2024-01-31", 2, ["fundamentals", "prices"])]
    assert result["manifests"]["prod"] == [_manifest_summary("prod", "2024-01-31", 2, ["fundamentals", "prices"])]
    assert result["AA"]["fundamentals_raw"][0]["layer"] == "source"
    assert result["AA"]["fundamentals_prod"][0]["layer"] == "prod"
    assert result["AA"]["prices_raw"][0]["layer"] == "source"
    assert result["AA"]["prices_prod"][0]["layer"] == "prod"
