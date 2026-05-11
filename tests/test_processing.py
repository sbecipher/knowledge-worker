from __future__ import annotations

import io
import json
from typing import Any

import pytest
from temporalio.exceptions import ApplicationError
from pypdf import PdfWriter

from app.activities import processing
from app.models.payloads import KnowledgeDocument


def _pdf_bytes(page_count: int) -> bytes:
    writer = PdfWriter()
    for _ in range(page_count):
        writer.add_blank_page(width=72, height=72)

    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def _document(document_type: str = "pdf") -> KnowledgeDocument:
    return KnowledgeDocument(
        title="Quarterly results",
        company_name="Alcoa",
        company_id="com_aa",
        company_ticker="AA",
        base_url="https://example.com",
        year=2026,
        url="https://example.com/results.pdf",
        type=document_type,
        filepath="data/com_aa/2026/results.pdf",
    )


class _FakeBlob:
    def __init__(self, size: int | None = None) -> None:
        self.size = size
        self.name = "source/knowledge/AA/2026/results.pdf"
        self.reload_called = False

    def reload(self) -> None:
        self.reload_called = True
        self.size = 123


def test_blob_size_detection_reloads_missing_size() -> None:
    blob = _FakeBlob()

    assert processing._get_blob_size(blob) == 123
    assert blob.reload_called is True


def test_pdf_size_threshold_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(processing.settings, "GEMINI_PDF_MAX_BYTES", 100)

    assert (
        processing._get_blob_size(_FakeBlob(size=99))
        <= processing.settings.GEMINI_PDF_MAX_BYTES
    )
    assert (
        processing._get_blob_size(_FakeBlob(size=101))
        > processing.settings.GEMINI_PDF_MAX_BYTES
    )


def test_gemini_chunk_bucket_defaults_to_prod_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(processing.settings, "PROD_BUCKET", "prod-bucket")
    monkeypatch.setattr(processing.settings, "GEMINI_CHUNK_BUCKET", None)

    assert processing._gemini_chunk_bucket_name() == "prod-bucket"


def test_sample_pdf_size_error_is_not_transient() -> None:
    error = RuntimeError(
        "Transient API Error: 400 INVALID_ARGUMENT. {'error': {'code': 400, "
        "'message': 'The file size of "
        "`gs://sbecipher-intelligence/source/knowledge/AMR/2021/2021_untitled_1750436998.pdf` "
        "is 185245993, which exceeds max allowed file size of 52428800 for "
        "`application/pdf` files. Reduce the file size by lowering the image "
        "resolution, splitting the PDF into multiple files, and so on.', "
        "'status': 'INVALID_ARGUMENT'}}"
    )

    assert processing._is_pdf_file_size_error(error) is True
    assert processing._is_transient_gemini_error(error) is False


def test_invalid_argument_after_splitting_is_non_retryable() -> None:
    error = RuntimeError("400 INVALID_ARGUMENT unsupported PDF content")

    with pytest.raises(ApplicationError) as exc_info:
        processing._coerce_or_raise_gemini_error(
            error,
            allow_empty_invalid_argument=False,
        )

    assert exc_info.value.non_retryable is True


def test_split_pdf_bytes_keeps_chunks_under_target() -> None:
    single_page_size = len(_pdf_bytes(1))
    two_page_size = len(_pdf_bytes(2))
    target_bytes = max(single_page_size, two_page_size - 1)

    chunks = processing._split_pdf_bytes(_pdf_bytes(3), target_bytes)

    assert len(chunks) >= 2
    assert all(len(chunk) <= target_bytes for chunk in chunks)


def test_split_and_upload_pdf_chunks_keeps_uploaded_bytes_under_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    pdf_bytes = _pdf_bytes(3)
    target_bytes = max(len(_pdf_bytes(1)), len(_pdf_bytes(2)) - 1)
    uploads: dict[str, bytes] = {}

    class FakeSourceBlob:
        def download_to_filename(self, path: str) -> None:
            tmp_file = tmp_path / "source.pdf"
            tmp_file.write_bytes(pdf_bytes)
            with open(tmp_file, "rb") as source, open(path, "wb") as destination:
                destination.write(source.read())

    class FakeChunkBlob:
        def __init__(self, name: str) -> None:
            self.name = name

        def upload_from_string(self, data: bytes, content_type: str) -> None:
            assert content_type == "application/pdf"
            uploads[self.name] = data

    class FakeBucket:
        def blob(self, name: str) -> FakeChunkBlob:
            return FakeChunkBlob(name)

    class FakeStorageClient:
        def bucket(self, bucket_name: str) -> FakeBucket:
            assert bucket_name == "chunk-bucket"
            return FakeBucket()

    monkeypatch.setattr(
        processing.settings, "GEMINI_PDF_CHUNK_TARGET_BYTES", target_bytes
    )
    monkeypatch.setattr(processing.settings, "GEMINI_CHUNK_BUCKET", "chunk-bucket")
    monkeypatch.setattr(processing.settings, "GEMINI_CHUNK_PREFIX", "tmp/chunks")

    chunk_uris = processing._split_and_upload_pdf_chunks(
        FakeStorageClient(),
        FakeSourceBlob(),
        "doc",
    )

    assert chunk_uris == [f"gs://chunk-bucket/{name}" for name in uploads.keys()]
    assert len(uploads) >= 2
    assert all(len(data) <= target_bytes for data in uploads.values())


def test_oversized_pdf_routing_uses_chunks_not_source_uri(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(processing.settings, "GEMINI_PDF_MAX_BYTES", 100)
    generated_uris: list[str] = []
    chunk_uris = [
        "gs://chunk-bucket/stage/knowledge/gemini_chunks/doc/part-00001.pdf",
        "gs://chunk-bucket/stage/knowledge/gemini_chunks/doc/part-00002.pdf",
    ]

    monkeypatch.setattr(
        processing,
        "_blob_from_gcs_uri",
        lambda storage_client, uri: _FakeBlob(size=101),
    )
    monkeypatch.setattr(
        processing,
        "_split_and_upload_pdf_chunks",
        lambda storage_client, source_blob, doc_id: chunk_uris,
    )

    def fake_generate_features_from_uri(
        genai_client: Any,
        gcs_uri: str,
        mime_type: str,
    ) -> processing.StandardFeatures:
        generated_uris.append(gcs_uri)
        return processing.StandardFeatures(
            summary=f"features for {gcs_uri}",
            key_entities=[gcs_uri],
            topics=["topic"],
        )

    monkeypatch.setattr(
        processing,
        "_generate_features_from_uri",
        fake_generate_features_from_uri,
    )

    def fake_merge_chunk_features(
        genai_client: Any,
        chunk_features: list[processing.StandardFeatures],
    ) -> processing.StandardFeatures:
        assert len(chunk_features) == 2
        return processing.StandardFeatures(
            summary="merged",
            key_entities=["Alcoa"],
            topics=["earnings"],
        )

    monkeypatch.setattr(processing, "_merge_chunk_features", fake_merge_chunk_features)

    features, metadata = processing._extract_standard_features(
        _document("pdf"),
        "gs://source-bucket/source.pdf",
        storage_client=object(),
        genai_client=object(),
        doc_id="doc",
    )

    assert generated_uris == chunk_uris
    assert features.summary == "merged"
    assert metadata["gemini_file_uri"] == "gs://source-bucket/source.pdf"
    assert json.loads(metadata["gemini_chunk_uris"]) == chunk_uris
    assert metadata["gemini_chunk_count"] == 2


def test_small_pdf_uses_direct_source_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(processing.settings, "GEMINI_PDF_MAX_BYTES", 100)
    generated_uris: list[str] = []

    monkeypatch.setattr(
        processing,
        "_blob_from_gcs_uri",
        lambda storage_client, uri: _FakeBlob(size=99),
    )

    def fake_generate_features_from_uri(
        genai_client: Any,
        gcs_uri: str,
        mime_type: str,
    ) -> processing.StandardFeatures:
        generated_uris.append(gcs_uri)
        return processing.StandardFeatures(
            summary="direct",
            key_entities=[],
            topics=[],
        )

    monkeypatch.setattr(
        processing,
        "_generate_features_from_uri",
        fake_generate_features_from_uri,
    )

    features, metadata = processing._extract_standard_features(
        _document("pdf"),
        "gs://source-bucket/source.pdf",
        storage_client=object(),
        genai_client=object(),
        doc_id="doc",
    )

    assert generated_uris == ["gs://source-bucket/source.pdf"]
    assert features.summary == "direct"
    assert metadata == {
        "gemini_file_uri": "gs://source-bucket/source.pdf",
        "gemini_chunk_uris": "[]",
        "gemini_chunk_count": 0,
    }


def test_merge_chunk_features_uses_gemini_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeModels:
        def generate_content(self, model: str, contents: list[Any], config: Any):
            captured["model"] = model
            captured["contents"] = contents
            captured["config"] = config

            class Response:
                text = json.dumps(
                    {
                        "summary": "merged summary",
                        "key_entities": ["Alcoa"],
                        "topics": ["earnings"],
                    }
                )

            return Response()

    class FakeGenAIClient:
        models = FakeModels()

    monkeypatch.setattr(processing.settings, "GEMINI_MODEL", "gemini-test-model")

    features = processing._merge_chunk_features(
        FakeGenAIClient(),
        [
            processing.StandardFeatures(
                summary="chunk one",
                key_entities=["Alcoa"],
                topics=["earnings"],
            ),
            processing.StandardFeatures(
                summary="chunk two",
                key_entities=["Alcoa"],
                topics=["guidance"],
            ),
        ],
    )

    assert features.summary == "merged summary"
    assert captured["model"] == "gemini-test-model"
    assert captured["config"].response_mime_type == "application/json"
    assert "chunk one" in captured["contents"][1]
    assert "chunk two" in captured["contents"][1]
