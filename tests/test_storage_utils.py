from models import ArtifactRef
from storage_utils import build_active_universe_object_path, build_object_path


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


def test_artifact_ref_round_trip_payload() -> None:
    ref = ArtifactRef(
        uri="gs://bucket/source/fundamentals/AA/AA_fundamentals_20240101_20240131.json",
        object_path="source/fundamentals/AA/AA_fundamentals_20240101_20240131.json",
        layer="source",
        dataset="fundamentals",
        universe_key="mmh5r1",
        request_id="req-123",
        workflow_id="wf-123",
        workflow_run_id="run-123",
        ticker="AA",
        start_date="2024-01-01",
        end_date="2024-01-31",
        record_count=3,
    )
    assert ArtifactRef(**ref.to_payload()) == ref


def test_fundamentals_path_uses_dataset_slug_between_ticker_and_dates() -> None:
    path = build_object_path(
        layer="source",
        dataset="fundamentals",
        universe_key="mmh5r1",
        ticker="AA",
        start_date="2024-01-01",
        end_date="2024-01-31",
    )
    assert path == "source/fundamentals/AA/AA_fundamentals_20240101_20240131.json"


def test_edgar_path_uses_request_date_suffix() -> None:
    path = build_object_path(
        layer="source",
        dataset="edgar",
        universe_key="mmh5r1",
        ticker="AA",
        suffix="edgar_20260407",
    )
    assert path == "source/edgar/AA/AA_edgar_20260407.json"
