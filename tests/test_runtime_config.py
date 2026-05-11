from __future__ import annotations

import asyncio
import http.client

import pytest

from app.activities import orchestration
from app.core.config import Settings
from app.models.payloads import KnowledgeDocument
from app import runtime


def test_settings_accepts_cloud_run_task_queue() -> None:
    settings = Settings(
        _env_file=None,
        TEMPORAL_ADDRESS="temporal.internal:7233",
        TEMPORAL_TASK_QUEUE="knowledge-cloud-run-task-queue",
        ACTIVITY_EXECUTOR_THREADS=3,
    )

    assert settings.TEMPORAL_ADDRESS == "temporal.internal:7233"
    assert settings.TEMPORAL_TASK_QUEUE == "knowledge-cloud-run-task-queue"
    assert settings.ACTIVITY_EXECUTOR_THREADS == 3


def test_activity_executor_uses_configured_thread_count() -> None:
    settings = Settings(_env_file=None, ACTIVITY_EXECUTOR_THREADS=2)

    executor = runtime.create_activity_executor(settings)
    try:
        assert executor._max_workers == 2
    finally:
        executor.shutdown(wait=True)


def test_worker_registration_uses_configured_task_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    class FakeWorker:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(runtime, "Worker", FakeWorker)
    settings = Settings(
        _env_file=None,
        TEMPORAL_TASK_QUEUE="knowledge-cloud-run-task-queue",
        MAX_CONCURRENT_ACTIVITIES=4,
        MAX_CONCURRENT_WORKFLOW_TASKS=5,
        MAX_CACHED_WORKFLOWS=6,
    )

    worker = runtime.create_knowledge_worker(client=object(), current_settings=settings)

    assert isinstance(worker, FakeWorker)
    assert captured["task_queue"] == "knowledge-cloud-run-task-queue"
    assert captured["workflows"] == runtime.WORKFLOWS
    assert captured["activities"] == runtime.ACTIVITIES
    assert captured["max_concurrent_activities"] == 4
    assert captured["max_concurrent_workflow_tasks"] == 5
    assert captured["max_cached_workflows"] == 6


def test_health_server_serves_healthz(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HEALTHCHECK_PORT", "0")
    server = runtime.start_health_server()
    assert server is not None

    try:
        port = server.server_address[1]
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        connection.request("GET", "/healthz")
        response = connection.getresponse()
        body = response.read()
        connection.close()

        assert response.status == 200
        assert body == b"ok"
    finally:
        server.shutdown()
        server.server_close()


def test_filter_existing_documents_uses_configured_source_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = {}

    class FakeBlob:
        name = "source/knowledge/AA/2026/existing.html"

    class FakeBucket:
        def list_blobs(self, prefix: str):
            seen["prefix"] = prefix
            return [FakeBlob()]

    class FakeStorageClient:
        def __init__(self, project: str):
            seen["project"] = project

        def bucket(self, bucket_name: str):
            seen["bucket_name"] = bucket_name
            return FakeBucket()

    monkeypatch.setattr(orchestration.settings, "PROJECT_ID", "data-cipher")
    monkeypatch.setattr(orchestration.settings, "SOURCE_BUCKET", "source-bucket")
    monkeypatch.setattr(orchestration.storage, "Client", FakeStorageClient)

    documents = [
        KnowledgeDocument(
            title="Existing",
            company_name="Alcoa",
            company_id="com_aa",
            company_ticker="AA",
            base_url="https://example.com",
            year=2026,
            url="https://example.com/existing",
            type="html",
            filepath="data/com_aa/2026/existing.html",
        ),
        KnowledgeDocument(
            title="New",
            company_name="Alcoa",
            company_id="com_aa",
            company_ticker="AA",
            base_url="https://example.com",
            year=2026,
            url="https://example.com/new",
            type="html",
            filepath="data/com_aa/2026/new.html",
        ),
    ]

    result = asyncio.run(orchestration.filter_existing_documents(documents, 2026))

    assert seen == {
        "project": "data-cipher",
        "bucket_name": "source-bucket",
        "prefix": "source/knowledge/AA/2026/",
    }
    assert [document.title for document in result] == ["New"]
