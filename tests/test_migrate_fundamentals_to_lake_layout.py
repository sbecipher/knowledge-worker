from scripts.migrate_fundamentals_to_lake_layout import (
    LegacyFundamentalsObjectInfo,
    destination_object_path,
    flatten_legacy_payload,
    legacy_workflow_id,
    parse_legacy_object_path,
)


def test_parse_legacy_fundamentals_object_path_extracts_layer_ticker_and_request_dates() -> None:
    info = parse_legacy_object_path(
        "source/fundamentals/AA/AA_fundamentals_20240101_20241231.json",
        "12345",
    )

    assert info == LegacyFundamentalsObjectInfo(
        layer="source",
        object_path="source/fundamentals/AA/AA_fundamentals_20240101_20241231.json",
        ticker="AA",
        request_start_date="2024-01-01",
        request_end_date="2024-12-31",
        generation="12345",
    )


def test_legacy_fundamentals_workflow_id_is_deterministic() -> None:
    workflow_id = legacy_workflow_id("prod/fundamentals/AA/AA_fundamentals_20240101_20241231.json", "12345")

    assert workflow_id == legacy_workflow_id("prod/fundamentals/AA/AA_fundamentals_20240101_20241231.json", "12345")
    assert workflow_id.startswith("legacy_")


def test_destination_object_path_uses_partitioned_fundamentals_layout() -> None:
    path = destination_object_path(
        layer="prod",
        ticker="AA",
        requested_period="FQ",
        end_date="2025-03-31",
        workflow_id="legacy_deadbeefdeadbe",
        gcs_prefix="",
    )

    assert path == "prod/fundamentals/frequency=FQ/end_date=2025-03-31/ticker=AA/legacy_deadbeefdeadbe.ndjson"


def test_flatten_legacy_source_fundamentals_payload_emits_partitionable_rows() -> None:
    info = LegacyFundamentalsObjectInfo(
        layer="source",
        object_path="source/fundamentals/AA/AA_fundamentals_20240101_20241231.json",
        ticker="AA",
        request_start_date="2024-01-01",
        request_end_date="2024-12-31",
        generation="12345",
    )
    payload = {
        "ticker": "AA",
        "ric": "AA.N",
        "primary_ric": "AA.N",
        "organization_id": "4295904304",
        "cik_number": "0001675149",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "frequency": "FQ",
        "provider": "lseg",
        "source": "lseg",
        "parameter_overrides": {"Period": "FQ0:FQ-4", "Curn": "USD", "Scale": 6},
        "data": [
            {
                "instrument": "AA.N",
                "statement": "income_statement",
                "name": "TR.F.TotRevenue",
                "period_start_date": "2024-01-01",
                "period_end_date": "2024-03-31",
                "financial_period_absolute": "FY2024Q1",
                "std_income_statement_all": 1000.0,
            },
            {
                "instrument": "AA.N",
                "statement": "income_statement",
                "name": "TR.F.TotRevenue",
                "period_start_date": "2024-04-01",
                "period_end_date": None,
                "financial_period_absolute": "FY2024Q2",
                "std_income_statement_all": 1100.0,
            },
        ],
    }

    rows = flatten_legacy_payload(payload, object_info=info, workflow_id="legacy_deadbeefdeadbe", universe_key="mmh5r1")

    assert rows == [
        {
            "ticker": "AA",
            "universe_key": "mmh5r1",
            "workflow_id": "legacy_deadbeefdeadbe",
            "workflow_run_id": "legacy_deadbeefdeadbe",
            "request_id": "legacy_deadbeefdeadbe",
            "source_system": "legacy_fundamentals_migration",
            "frequency": "FQ",
            "requested_period": "FQ",
            "request_start_date": "2024-01-01",
            "request_end_date": "2024-12-31",
            "request_period": "FQ0:FQ-4",
            "request_currency": "USD",
            "request_scale": 6,
            "provider": "lseg",
            "source": "lseg",
            "ric": "AA.N",
            "primary_ric": "AA.N",
            "organization_id": "4295904304",
            "cik_number": "0001675149",
            "instrument": "AA.N",
            "statement": "income_statement",
            "name": "TR.F.TotRevenue",
            "period_start_date": "2024-01-01",
            "period_end_date": "2024-03-31",
            "financial_period_absolute": "FY2024Q1",
            "std_income_statement_all": 1000.0,
            "legacy_object_path": "source/fundamentals/AA/AA_fundamentals_20240101_20241231.json",
            "legacy_generation": "12345",
        }
    ]


def test_flatten_legacy_prod_fundamentals_payload_keeps_lineage_fields() -> None:
    info = LegacyFundamentalsObjectInfo(
        layer="prod",
        object_path="prod/fundamentals/AA/AA_fundamentals_20240101_20241231.json",
        ticker="AA",
        request_start_date="2024-01-01",
        request_end_date="2024-12-31",
        generation="12345",
    )
    payload = {
        "ticker": "AA",
        "ric": "AA.N",
        "primary_ric": "AA.N",
        "organization_id": "4295904304",
        "cik_number": "0001675149",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "frequency": "FQ",
        "provider": "lseg",
        "source_uri": "gs://bucket/source/fundamentals/AA/AA_fundamentals_20240101_20241231.json",
        "source_object_path": "source/fundamentals/AA/AA_fundamentals_20240101_20241231.json",
        "source_dataset": "fundamentals",
        "transform_name": "fundamentals_prod_transform",
        "transform_version": "v1",
        "data": [
            {
                "instrument": "AA.N",
                "financial_period_absolute": "FY2024Q1",
                "period_start_date": "2024-01-01",
                "period_end_date": "2024-03-31",
                "is_tot_revenue": 1000.0,
            }
        ],
    }

    rows = flatten_legacy_payload(payload, object_info=info, workflow_id="legacy_deadbeefdeadbe", universe_key="mmh5r1")

    assert rows == [
        {
            "ticker": "AA",
            "universe_key": "mmh5r1",
            "workflow_id": "legacy_deadbeefdeadbe",
            "workflow_run_id": "legacy_deadbeefdeadbe",
            "request_id": "legacy_deadbeefdeadbe",
            "source_system": "legacy_fundamentals_migration",
            "frequency": "FQ",
            "requested_period": "FQ",
            "request_start_date": "2024-01-01",
            "request_end_date": "2024-12-31",
            "request_period": "FQ0",
            "provider": "lseg",
            "ric": "AA.N",
            "primary_ric": "AA.N",
            "organization_id": "4295904304",
            "cik_number": "0001675149",
            "source_uri": "gs://bucket/source/fundamentals/AA/AA_fundamentals_20240101_20241231.json",
            "source_object_path": "source/fundamentals/AA/AA_fundamentals_20240101_20241231.json",
            "source_dataset": "fundamentals",
            "transform_name": "fundamentals_prod_transform",
            "transform_version": "v1",
            "instrument": "AA.N",
            "financial_period_absolute": "FY2024Q1",
            "period_start_date": "2024-01-01",
            "period_end_date": "2024-03-31",
            "is_tot_revenue": 1000.0,
            "legacy_object_path": "prod/fundamentals/AA/AA_fundamentals_20240101_20241231.json",
            "legacy_generation": "12345",
        }
    ]
