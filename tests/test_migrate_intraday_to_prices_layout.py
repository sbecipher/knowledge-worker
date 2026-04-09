from scripts.migrate_intraday_to_prices_layout import (
    LegacyObjectInfo,
    destination_object_path,
    flatten_legacy_payload,
    legacy_workflow_id,
    parse_legacy_object_path,
)


def test_parse_legacy_object_path_extracts_ticker_and_dates() -> None:
    info = parse_legacy_object_path(
        "source/intraday/AA/AA_eod_20141007_20141007.json",
        "12345",
    )

    assert info == LegacyObjectInfo(
        object_path="source/intraday/AA/AA_eod_20141007_20141007.json",
        ticker="AA",
        effective_start_date="2014-10-07",
        effective_end_date="2014-10-07",
        generation="12345",
    )


def test_legacy_workflow_id_is_deterministic() -> None:
    workflow_id = legacy_workflow_id("source/intraday/AA/AA_eod_20141007_20141007.json", "12345")

    assert workflow_id == legacy_workflow_id("source/intraday/AA/AA_eod_20141007_20141007.json", "12345")
    assert workflow_id.startswith("legacy_")


def test_destination_object_path_uses_partitioned_prices_layout() -> None:
    info = LegacyObjectInfo(
        object_path="source/intraday/AA/AA_eod_20141007_20141007.json",
        ticker="AA",
        effective_start_date="2014-10-07",
        effective_end_date="2014-10-07",
        generation="12345",
    )

    path = destination_object_path(object_info=info, workflow_id="legacy_deadbeefdeadbe", gcs_prefix="")

    assert path == "source/prices/granularity=day/end_date=2014-10-07/ticker=AA/legacy_deadbeefdeadbe.ndjson"


def test_flatten_legacy_payload_emits_bigquery_friendly_rows() -> None:
    info = LegacyObjectInfo(
        object_path="source/intraday/AA/AA_eod_20141007_20141007.json",
        ticker="AA",
        effective_start_date="2014-10-07",
        effective_end_date="2014-10-07",
        generation="12345",
    )
    payload = [
        {
            "security": {
                "id": 1,
                "company_id": 2,
                "code": "AA",
                "name": "Alcoa Corp",
                "currency": "USD",
                "primary_listing": True,
            },
            "stock_prices": [
                {
                    "date": "2014-10-07",
                    "open": 14.0,
                    "high": 15.0,
                    "low": 13.0,
                    "close": 14.5,
                    "volume": 100.0,
                }
            ],
        }
    ]

    rows = flatten_legacy_payload(payload, object_info=info, workflow_id="legacy_deadbeefdeadbe", universe_key="mmh5r1")

    assert rows == [
        {
            "ticker": "AA",
            "date": "2014-10-07",
            "requested_period": "day",
            "as_of_date": "2014-10-07",
            "effective_start_date": "2014-10-07",
            "effective_end_date": "2014-10-07",
            "bar_granularity": "day",
            "universe_key": "mmh5r1",
            "workflow_id": "legacy_deadbeefdeadbe",
            "workflow_run_id": "legacy_deadbeefdeadbe",
            "request_id": "legacy_deadbeefdeadbe",
            "source_system": "marketio",
            "provider": "marketio",
            "security_id": "1",
            "company_id": "2",
            "security_code": "AA",
            "security_name": "Alcoa Corp",
            "currency": "USD",
            "composite_ticker": None,
            "figi": None,
            "composite_figi": None,
            "share_class_figi": None,
            "primary_listing": True,
            "open": 14.0,
            "high": 15.0,
            "low": 13.0,
            "close": 14.5,
            "volume": 100.0,
            "adj_open": None,
            "adj_high": None,
            "adj_low": None,
            "adj_close": None,
            "adj_volume": None,
            "dividend": None,
            "factor": None,
            "split_ratio": None,
            "intraperiod": None,
            "change": None,
            "percent_change": None,
            "fifty_two_week_high": None,
            "fifty_two_week_low": None,
            "legacy_object_path": "source/intraday/AA/AA_eod_20141007_20141007.json",
            "legacy_generation": "12345",
        }
    ]
