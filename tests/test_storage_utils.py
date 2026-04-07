from models import ArtifactRef
from storage_utils import build_object_path


def test_models_snapshot_path_uses_request_suffix() -> None:
    path = build_object_path(
        layer="prod",
        instrument="mm-h5r1",
        dataset="models",
        model_version="metadata",
        suffix="request-123",
    )
    assert path == "prod/models/mm-h5r1/metadata/request-123.json"


def test_artifact_ref_round_trip_payload() -> None:
    ref = ArtifactRef(
        uri="gs://bucket/source/fundamentals/AA/AA_fundamentals_20240101_20240131.json",
        object_path="source/fundamentals/AA/AA_fundamentals_20240101_20240131.json",
        layer="source",
        dataset="fundamentals",
        instrument="mm-h5r1",
        model_version="metadata",
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
        instrument="mm-h5r1",
        dataset="fundamentals",
        ticker="AA",
        start_date="2024-01-01",
        end_date="2024-01-31",
    )
    assert path == "source/fundamentals/AA/AA_fundamentals_20240101_20240131.json"


def test_edgar_path_uses_request_date_suffix() -> None:
    path = build_object_path(
        layer="source",
        instrument="mm-h5r1",
        dataset="edgar",
        ticker="AA",
        suffix="edgar_20260407",
    )
    assert path == "source/edgar/AA/AA_edgar_20260407.json"
