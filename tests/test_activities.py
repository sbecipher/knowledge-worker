import pytest
from temporalio.exceptions import ApplicationError

import activities


def test_required_ticker_raises_non_retryable_error() -> None:
    with pytest.raises(ApplicationError) as exc_info:
        activities._required_ticker({}, "raw fundamentals")
    assert exc_info.value.non_retryable is True


def test_fetch_fundamentals_stage_rejects_empty_inputs() -> None:
    with pytest.raises(ApplicationError) as exc_info:
        activities.fetch_fundamentals_stage(
            [],
            execution={
                "request_id": "req-123",
                "workflow_id": "wf-123",
                "workflow_run_id": "run-123",
            },
        )
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
