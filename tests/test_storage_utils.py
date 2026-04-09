from models import ArtifactRef
from storage_utils import (
    build_active_universe_object_path,
    build_metadata_manifest_object_path,
    build_object_path,
)


def test_models_snapshot_path_uses_request_suffix() -> None:
    path = build_object_path(
        layer="prod",
        dataset="models",
        universe_key="mmh5r1",
        suffix="request-123",
    )
    assert path == "prod/models/mmh5r1/metadata/request-123.json"


def test_active_universe_path_uses_universe_key() -> None:
    assert build_active_universe_object_path("mmh5r1") == "prod/models/mmh5r1/active.json"


def test_metadata_source_path_uses_ticker_and_workflow_id() -> None:
    path = build_object_path(
        layer="source",
        dataset="metadata",
        universe_key="mmh5r1",
        ticker="AA",
        suffix="wf-123",
    )
    assert path == "source/metadata/AA/AA_wf-123.json"


def test_metadata_manifest_path_uses_workflow_id() -> None:
    assert build_metadata_manifest_object_path("wf-123") == "source/metadata/manifests/wf-123.json"


def test_artifact_ref_round_trip_payload() -> None:
    ref = ArtifactRef(
        uri="gs://bucket/source/fundamentals/frequency=FQ/end_date=2024-03-31/ticker=AA/wf-123.ndjson",
        object_path="source/fundamentals/frequency=FQ/end_date=2024-03-31/ticker=AA/wf-123.ndjson",
        layer="source",
        dataset="fundamentals",
        universe_key="mmh5r1",
        request_id="req-123",
        workflow_id="wf-123",
        workflow_run_id="run-123",
        ticker="AA",
        start_date="2024-01-01",
        end_date="2024-03-31",
        requested_period="FQ",
        request_start_date="2024-01-01",
        request_end_date="2024-12-31",
        request_period="FQ0:FQ-4",
        request_currency="USD",
        request_scale=6,
        record_count=3,
    )
    assert ArtifactRef(**ref.to_payload()) == ref


def test_fundamentals_path_uses_partitioned_layout_and_ndjson_extension() -> None:
    path = build_object_path(
        layer="source",
        dataset="fundamentals",
        universe_key="mmh5r1",
        ticker="AA",
        suffix="wf-123",
        requested_period="FQ",
        effective_end_date="2025-03-31",
    )
    assert path == "source/fundamentals/frequency=FQ/end_date=2025-03-31/ticker=AA/wf-123.ndjson"


def test_edgar_path_uses_request_date_suffix() -> None:
    path = build_object_path(
        layer="source",
        dataset="edgar",
        universe_key="mmh5r1",
        ticker="AA",
        suffix="edgar_20260407",
    )
    assert path == "source/edgar/AA/AA_edgar_20260407.json"


def test_prices_path_uses_partitioned_layout_and_ndjson_extension() -> None:
    path = build_object_path(
        layer="source",
        dataset="prices",
        universe_key="mmh5r1",
        ticker="AA",
        suffix="wf-123",
        bar_granularity="day",
        effective_end_date="2024-01-31",
    )
    assert path == "source/prices/granularity=day/end_date=2024-01-31/ticker=AA/wf-123.ndjson"
