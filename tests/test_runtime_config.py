from __future__ import annotations

import asyncio
import http.client

import pytest

from app.activities import deduplication
from app.activities import ingestion
from app.activities import loading
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


def test_settings_accepts_gemini_pdf_chunk_config() -> None:
    settings = Settings(
        _env_file=None,
        GEMINI_MODEL="gemini-test-model",
        GEMINI_PDF_MAX_BYTES=100,
        GEMINI_PDF_CHUNK_TARGET_BYTES=80,
        GEMINI_CHUNK_BUCKET="chunk-bucket",
        GEMINI_CHUNK_PREFIX="tmp/chunks",
    )

    assert settings.GEMINI_MODEL == "gemini-test-model"
    assert settings.GEMINI_PDF_MAX_BYTES == 100
    assert settings.GEMINI_PDF_CHUNK_TARGET_BYTES == 80
    assert settings.GEMINI_CHUNK_BUCKET == "chunk-bucket"
    assert settings.GEMINI_CHUNK_PREFIX == "tmp/chunks"


def test_activity_executor_uses_configured_thread_count() -> None:
    settings = Settings(_env_file=None, ACTIVITY_EXECUTOR_THREADS=2)

    executor = runtime.create_activity_executor(settings)
    try:
        assert executor._max_workers == 2
    finally:
        executor.shutdown(wait=True)


def test_worker_registration_uses_configured_task_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    result = orchestration.filter_existing_documents(documents, 2026)

    assert seen == {
        "project": "data-cipher",
        "bucket_name": "source-bucket",
        "prefix": "source/knowledge/AA/2026/",
    }
    assert [document.title for document in result] == ["New"]


def test_infer_document_type_from_article_item_flags() -> None:
    assert orchestration._infer_document_type({"is_pdf": True}) == "pdf"
    assert (
        orchestration._infer_document_type(
            {"url": "https://example.com/news/release.pdf?download=1"}
        )
        == "pdf"
    )
    assert (
        orchestration._infer_document_type({"url": "https://example.com/news"})
        == "html"
    )


def test_build_document_filepath_hashes_remote_article_items() -> None:
    filepath = orchestration._build_document_filepath(
        {"url": "https://example.com/releases/q1-results"},
        company_id="com_aa",
        year=2026,
        document_type="html",
    )

    assert filepath == "data/com_aa/2026/069c957e404a5f5fbc1e713ef0df2cd9.html"


def test_download_document_to_gcs_passes_article_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = {}

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"gcs_uri": "gs://bucket/object"}

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url: str, json: dict, headers: dict):
            captured["url"] = url
            captured["json"] = json
            captured["headers"] = headers
            return FakeResponse()

    monkeypatch.setattr(
        ingestion.settings, "KNOWLEDGEIO_API_URL", "https://knowledgeio.example"
    )
    monkeypatch.setattr(
        ingestion, "knowledge_api_headers", lambda: {"Authorization": "Bearer token"}
    )
    monkeypatch.setattr(ingestion.httpx, "AsyncClient", FakeAsyncClient)

    doc = KnowledgeDocument(
        title="Quarterly results",
        company_name="Alcoa",
        company_id="com_aa",
        company_ticker="AA",
        base_url="https://example.com",
        year=2026,
        url="https://example.com/releases/results.pdf",
        type="pdf",
        filepath="data/com_aa/2026/results.pdf",
    )

    result = asyncio.run(ingestion.download_document_to_gcs(doc))

    assert result == "gs://bucket/object"
    assert captured["url"] == "https://knowledgeio.example/api/v1/scrape/url"
    assert captured["headers"] == {"Authorization": "Bearer token"}
    assert captured["json"]["article_type"] == "pdf"


def test_update_knowledge_index_uses_configured_bq_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = {}

    class FakeLoader:
        def load_parquet(self, uri: str, table_id: str) -> bool:
            seen["uri"] = uri
            seen["table_id"] = table_id
            seen["called"] = True
            return True

    monkeypatch.setattr(loading.settings, "BQ_PROJECT_ID", "sbecipherio")
    monkeypatch.setattr(loading.settings, "BQ_DATASET", "knowledge")
    monkeypatch.setattr(loading.settings, "BQ_TABLE", "documents")
    monkeypatch.setattr(loading, "get_bigquery_loader_backend", lambda: FakeLoader())

    result = loading.update_knowledge_index("gs://bucket/prod/knowledge/doc.parquet")

    assert result is True
    assert seen == {
        "uri": "gs://bucket/prod/knowledge/doc.parquet",
        "table_id": "sbecipherio.knowledge.documents",
        "called": True,
    }


def test_update_company_metadata_index_uses_dedicated_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = {}

    class FakeLoader:
        def load_parquet(self, uri: str, table_id: str) -> bool:
            seen["uri"] = uri
            seen["table_id"] = table_id
            seen["called"] = True
            return True

    monkeypatch.setattr(loading.settings, "BQ_PROJECT_ID", "sbecipherio")
    monkeypatch.setattr(loading.settings, "BQ_DATASET", "knowledge")
    monkeypatch.setattr(
        loading.settings, "BQ_COMPANY_METADATA_TABLE", "company_metadata"
    )
    monkeypatch.setattr(loading, "get_bigquery_loader_backend", lambda: FakeLoader())

    result = loading.update_company_metadata_index(
        "gs://bucket/prod/knowledge/company_metadata/doc.parquet"
    )

    assert result is True
    assert seen == {
        "uri": "gs://bucket/prod/knowledge/company_metadata/doc.parquet",
        "table_id": "sbecipherio.knowledge.company_metadata",
        "called": True,
    }


def test_check_document_exists_uses_bq_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = {}

    class FakeResults:
        total_rows = 1

    class FakeQueryJob:
        def result(self):
            return FakeResults()

    class FakeBigQueryClient:
        def __init__(self, project: str):
            seen["project"] = project

        def query(self, query: str, job_config):
            seen["query"] = query
            seen["job_config"] = job_config
            return FakeQueryJob()

    monkeypatch.setattr(deduplication.settings, "BQ_PROJECT_ID", "sbecipherio")
    monkeypatch.setattr(deduplication.settings, "BQ_DATASET", "knowledge")
    monkeypatch.setattr(deduplication.settings, "BQ_TABLE", "documents")
    monkeypatch.setattr(deduplication.bigquery, "Client", FakeBigQueryClient)

    result = deduplication.check_document_exists_in_bq("doc-123")

    assert result is True
    assert seen["project"] == "sbecipherio"
    assert "`sbecipherio.knowledge.documents`" in seen["query"]
