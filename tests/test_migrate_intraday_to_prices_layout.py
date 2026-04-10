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

    assert path == "source/prices/granularity=day/date=2014-10-07/ticker=AA/legacy_deadbeefdeadbe.ndjson"


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
            "source_system": "legacy_intraday_migration",
            "source": "intrinio",
            "provider": "intrinio",
            "frequency": "daily",
            "instrument": "AA",
            "fields": {
                "TR.OPENPRICE": 14.0,
                "TR.CLOSEPRICE": 14.5,
                "TR.HIGHPRICE": 15.0,
                "TR.LOWPRICE": 13.0,
                "TR.VOLUME": 100.0,
                "TR.PRICE52WEEKHIGH": None,
                "TR.PRICE52WEEKLOW": None,
                "TR.PRICEPCTCHG1D": None,
                "TR.TOTALRETURN1D": None,
                "TR.PRICE52WKHIGHFLG1D": None,
                "TR.PRICE52WKLOWFLG1D": None,
            },
            "legacy_object_path": "source/intraday/AA/AA_eod_20141007_20141007.json",
            "legacy_generation": "12345",
        }
    ]
