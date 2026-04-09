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


def test_load_active_universe_index_uses_active_universe_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        activities.UPLOADER,
        "download_json",
        lambda object_path: [{"ticker": "AA", "ric": "AA"}, {"ticker": "NUE", "primary_ric": "NUE.N"}],
    )

    result = activities.load_active_universe_index(
        universe_key="mmh5r1",
        execution=_execution_payload(),
    )

    assert result["active_source_object_path"] == "prod/models/mmh5r1/active.json"
    assert result["tickers"] == ["AA", "NUE"]
    assert result["rics"] == {"AA": "AA", "NUE": "NUE.N"}


def test_resolve_company_identifiers_normalizes_metadata_and_returns_cik_ric_maps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_calls: List[tuple[str, Dict[str, Any]]] = []

    def fake_post(endpoint: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        captured_calls.append((endpoint, payload))
        return [{"ticker": "AA", "cik_number": "0001675149", "primary_ric": "AA.N"}]

    monkeypatch.setattr(activities, "_post_json", fake_post)
    monkeypatch.setattr(
        activities.UPLOADER,
        "download_json",
        lambda object_path: [{"ticker": "AA", "ric": "AA"}],
    )

    result = activities.resolve_company_identifiers(
        tickers=["AA"],
        universe_key="mmh5r1",
        include_metadata_rows=True,
        execution=_execution_payload(),
    )

    assert captured_calls == [
        (activities.MARKETIO_ROUTE_COMPANIES, {"tickers": ["AA"]})
    ]
    assert result["tickers"] == ["AA"]
    assert result["ciks"] == {"AA": "0001675149"}
    assert result["rics"] == {"AA": "AA.N"}
    row = result["rows_by_ticker"]["AA"]
    assert row["ticker"] == "AA"
    assert row["universe_key"] == "mmh5r1"
    assert row["sic_code"] is None
    assert row["raw"]["active_universe_row"]["ticker"] == "AA"


def test_resolve_company_identifiers_can_skip_metadata_rows_when_not_persisting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        activities.UPLOADER,
        "download_json",
        lambda object_path: [{"ticker": "AA", "ric": "AA"}],
    )
    monkeypatch.setattr(
        activities,
        "_post_json",
        lambda endpoint, payload: [{"ticker": "AA", "cik_number": "0001675149", "primary_ric": "AA.N"}],
    )

    result = activities.resolve_company_identifiers(
        tickers=["AA"],
        universe_key="mmh5r1",
        include_metadata_rows=False,
        execution=_execution_payload(),
    )

    assert result["ciks"] == {"AA": "0001675149"}
    assert result["rows_by_ticker"] == {}


def test_persist_company_metadata_writes_per_ticker_artifacts_and_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uploaded_paths: List[str] = []

    def fake_upload(local_path, object_path, metadata=None):
        uploaded_paths.append(object_path)
        return f"gs://bucket/{object_path}"

    monkeypatch.setattr(activities.UPLOADER, "upload_file", fake_upload)

    result = activities.persist_company_metadata(
        {
            "active_source_uri": "gs://bucket/prod/models/mmh5r1/active.json",
            "active_source_object_path": "prod/models/mmh5r1/active.json",
            "tickers": ["AA"],
            "ciks": {"AA": "0001675149"},
            "rics": {"AA": "AA.N"},
            "rows_by_ticker": {
                "AA": {
                    "ticker": "AA",
                    "universe_key": "mmh5r1",
                    "organization_id": "5051045063",
                    "cik_number": "0001675149",
                    "ric": "AA",
                    "primary_ric": "AA.N",
                    "provider": "lseg",
                    "source": "marketio",
                    "raw": {"active_universe_row": {"ticker": "AA"}},
                }
            },
            "missing_from_active": [],
            "missing_from_provider": [],
            "universe_key": "mmh5r1",
        },
        universe_key="mmh5r1",
        execution=_execution_payload(),
    )

    assert result["persisted_tickers"] == ["AA"]
    assert result["artifacts_by_ticker"]["AA"][0]["object_path"] == "source/metadata/AA/AA_wf-123.json"
    assert result["manifest_object_path"] == "source/metadata/manifests/wf-123.json"
    assert uploaded_paths == [
        "source/metadata/AA/AA_wf-123.json",
        "source/metadata/manifests/wf-123.json",
    ]


def test_resolve_market_window_day_uses_last_completed_session() -> None:
    window = activities._resolve_market_window(period="day", as_of_date="2024-01-31", exchange_code="NYQ")

    assert window == {
        "requested_period": "day",
        "bar_granularity": "day",
        "as_of_date": "2024-01-31",
        "effective_start_date": "2024-01-31",
        "effective_end_date": "2024-01-31",
        "calendar": "XNYS",
    }


def test_resolve_market_window_week_and_month_follow_trading_calendar() -> None:
    week_window = activities._resolve_market_window(period="week", as_of_date="2024-01-31", exchange_code="NYQ")
    month_window = activities._resolve_market_window(period="month", as_of_date="2024-01-31", exchange_code="NYQ")
    quarter_window = activities._resolve_market_window(period="quarter", as_of_date="2024-04-10", exchange_code="NYQ")

    assert week_window["effective_start_date"] == "2024-01-29"
    assert week_window["effective_end_date"] == "2024-01-31"
    assert month_window["effective_start_date"] == "2024-01-02"
    assert month_window["effective_end_date"] == "2024-01-31"
    assert quarter_window["effective_start_date"] == "2024-04-01"
    assert quarter_window["effective_end_date"] == "2024-04-10"


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


def test_fetch_prices_raw_retries_empty_field_responses_once(monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr(
        activities,
        "_resolve_market_window",
        lambda **_: {
            "requested_period": "day",
            "bar_granularity": "day",
            "as_of_date": "2026-04-02",
            "effective_start_date": "2026-04-02",
            "effective_end_date": "2026-04-02",
            "calendar": "XNYS",
        },
    )

    result = activities.fetch_prices_raw(
        "AA",
        "AA.N",
        "2026-04-02",
        "day",
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
    assert result[0]["dataset"] == "prices"
    assert result[0]["provider"] == "lseg"
    assert result[0]["source"] == "lseg"
    assert result[0]["ric"] == "AA.N"
    assert result[0]["bar_granularity"] == "day"
    assert result[0]["requested_period"] == "day"
    assert result[0]["record_count"] == 1


def test_fetch_prices_raw_raises_retryable_error_after_empty_field_retry(
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
    monkeypatch.setattr(
        activities,
        "_resolve_market_window",
        lambda **_: {
            "requested_period": "day",
            "bar_granularity": "day",
            "as_of_date": "2026-04-02",
            "effective_start_date": "2026-04-02",
            "effective_end_date": "2026-04-02",
            "calendar": "XNYS",
        },
    )

    with pytest.raises(ApplicationError) as exc_info:
        activities.fetch_prices_raw(
            "AA",
            "AA.N",
            "2026-04-02",
            "day",
            execution=_execution_payload(),
        )

    assert exc_info.value.type == activities.MARKETIO_MARKET_EMPTY_RESPONSE_TYPE
    assert exc_info.value.non_retryable is False
