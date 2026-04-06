from typing import Any, Dict, List

import pytest
from temporalio.exceptions import ApplicationError

import activities


def _execution_payload() -> Dict[str, str]:
    return {
        "request_id": "req-123",
        "workflow_id": "wf-123",
        "workflow_run_id": "run-123",
    }


def test_required_ticker_raises_non_retryable_error() -> None:
    with pytest.raises(ApplicationError) as exc_info:
        activities._required_ticker({}, "raw fundamentals")
    assert exc_info.value.non_retryable is True


def test_fetch_fundamentals_stage_is_unsupported() -> None:
    with pytest.raises(ApplicationError, match="no longer supported") as exc_info:
        activities.fetch_fundamentals_stage([], execution=_execution_payload())
    assert exc_info.value.non_retryable is True


def test_load_artifact_payload_uses_durable_gcs_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        activities.UPLOADER,
        "download_json",
        lambda object_path: {"data": [{"ticker": "AA", "value": 1}], "object_path": object_path},
    )
    payload = activities._load_artifact_payload(
        {
            "uri": "gs://bucket/source/fundamentals/AA/file.json",
            "object_path": "source/fundamentals/AA/file.json",
        },
        "load failed",
    )
    assert payload == [{"ticker": "AA", "value": 1}]


def test_fetch_companies_metadata_uses_current_route_and_builds_cik_and_ric_maps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_calls: List[tuple[str, Dict[str, Any]]] = []

    def fake_post(endpoint: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        captured_calls.append((endpoint, payload))
        return [
            {
                "ticker": "AA",
                "cik_number": "0001675149",
                "primary_ric": "AA.N",
                "ric": "AA",
            }
        ]

    monkeypatch.setattr(activities, "_post_json", fake_post)

    result = activities.fetch_companies_metadata(
        tickers=["AA"],
        instrument="mm-h5r1",
        model_version="metadata",
        execution=_execution_payload(),
    )

    assert captured_calls == [(activities.MARKETIO_ROUTE_COMPANIES, {"tickers": ["AA"]})]
    assert result["ciks"] == {"AA": "0001675149"}
    assert result["rics"] == {"AA": "AA.N"}


def test_fetch_edgar_source_uses_current_route(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_calls: List[tuple[str, Dict[str, Any]]] = []

    def fake_post(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        captured_calls.append((endpoint, payload))
        return {
            "ticker": "AA",
            "filings": {"recent": {"accessionNumber": ["0001", "0002"]}},
        }

    monkeypatch.setattr(activities, "_post_json", fake_post)

    result = activities.fetch_edgar_source(
        tickers=["AA"],
        execution=_execution_payload(),
    )

    assert captured_calls == [(activities.MARKETIO_ROUTE_EDGAR_RAW, {"ticker": "AA"})]
    assert result[0]["record_count"] == 2
    assert result[0]["dataset"] == "edgar"


def test_fetch_fundamentals_prod_uses_current_route_and_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_calls: List[tuple[str, Dict[str, Any]]] = []

    def fake_post(endpoint: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        captured_calls.append((endpoint, payload))
        return [
            {
                "ticker": "AA",
                "ric": "AA.N",
                "primary_ric": "AA.N",
                "organization_id": "4295904304",
                "cik_number": "0001675149",
                "start_date": "2026-04-02",
                "end_date": "2026-04-02",
                "record_count": 1,
                "page_count": 1,
                "data": [{"value": 1}],
            }
        ]

    monkeypatch.setattr(activities, "_post_json", fake_post)

    result = activities.fetch_fundamentals_prod(
        [
            {
                "ticker": "AA",
                "ric": "AA.N",
                "primary_ric": "AA.N",
                "start_date": "2026-04-02",
                "end_date": "2026-04-02",
            }
        ],
        execution=_execution_payload(),
    )

    assert captured_calls == [
        (
            activities.MARKETIO_ROUTE_FUNDAMENTALS_PROD,
            {"ric": "AA.N", "start_date": "2026-04-02", "end_date": "2026-04-02"},
        )
    ]
    assert result[0]["ric"] == "AA.N"
    assert result[0]["organization_id"] == "4295904304"


def test_fetch_intraday_raw_retries_empty_field_responses_once(monkeypatch: pytest.MonkeyPatch) -> None:
    responses: List[List[Dict[str, Any]]] = [
        [
            {
                "ticker": "AA",
                "ric": "AA.N",
                "primary_ric": "AA.N",
                "start_date": "2026-04-02",
                "end_date": "2026-04-02",
                "record_count": 0,
                "page_count": 1,
                "frequency": "daily",
                "provider": "lseg",
                "source": "lseg",
                "field_count": 0,
                "fields": [],
                "data": [],
            }
        ],
        [
            {
                "ticker": "AA",
                "ric": "AA.N",
                "primary_ric": "AA.N",
                "organization_id": "4295904304",
                "cik_number": "0001675149",
                "start_date": "2026-04-02",
                "end_date": "2026-04-02",
                "record_count": 1,
                "page_count": 1,
                "frequency": "daily",
                "provider": "lseg",
                "source": "lseg",
                "field_count": 1,
                "fields": ["TR.CLOSEPRICE"],
                "data": [
                    {
                        "date": "2026-04-02",
                        "instrument": "AA.N",
                        "fields": {"TR.CLOSEPRICE": 71.53},
                    }
                ],
            }
        ],
    ]
    captured_calls: List[tuple[str, Dict[str, Any]]] = []

    def fake_post(endpoint: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        captured_calls.append((endpoint, payload))
        return responses.pop(0)

    monkeypatch.setattr(activities, "_post_json", fake_post)
    monkeypatch.setattr(activities.time, "sleep", lambda _: None)

    result = activities.fetch_intraday_raw(
        "AA",
        "AA.N",
        "2026-04-02",
        "2026-04-02",
        "daily",
        execution=_execution_payload(),
    )

    assert len(captured_calls) == 2
    assert all(call[0] == activities.MARKETIO_ROUTE_MARKET_DAILY_RAW for call in captured_calls)
    assert captured_calls[0][1] == {
        "ric": "AA.N",
        "source": "lseg",
        "start_date": "2026-04-02",
        "end_date": "2026-04-02",
        "frequency": "daily",
    }
    assert result[0]["dataset"] == "intraday"
    assert result[0]["provider"] == "lseg"
    assert result[0]["source"] == "lseg"
    assert result[0]["ric"] == "AA.N"
    assert result[0]["field_count"] == 1


def test_fetch_intraday_raw_raises_retryable_error_after_empty_field_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_post(endpoint: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        assert endpoint == activities.MARKETIO_ROUTE_MARKET_DAILY_RAW
        return [
            {
                "ticker": "AA",
                "ric": "AA.N",
                "primary_ric": "AA.N",
                "start_date": "2026-04-02",
                "end_date": "2026-04-02",
                "record_count": 1,
                "page_count": 1,
                "frequency": "daily",
                "provider": "lseg",
                "source": "lseg",
                "field_count": 1,
                "fields": ["TR.CLOSEPRICE"],
                "data": [{"date": "2026-04-02", "instrument": "AA.N", "fields": {}}],
            }
        ]

    monkeypatch.setattr(activities, "_post_json", fake_post)
    monkeypatch.setattr(activities.time, "sleep", lambda _: None)

    with pytest.raises(ApplicationError) as exc_info:
        activities.fetch_intraday_raw(
            "AA",
            "AA.N",
            "2026-04-02",
            "2026-04-02",
            "daily",
            execution=_execution_payload(),
        )

    assert exc_info.value.type == activities.MARKETIO_MARKET_EMPTY_RESPONSE_TYPE
    assert exc_info.value.non_retryable is False
