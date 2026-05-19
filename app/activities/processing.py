import logging
import json
import io
import re
import tempfile
from datetime import date, datetime, timezone
from typing import Any, Sequence

import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore
from temporalio import activity
from temporalio.exceptions import ApplicationError
from google.cloud import storage  # type: ignore
from google import genai
from pydantic import BaseModel, Field
from pypdf import PdfReader, PdfWriter

from app.models.payloads import KnowledgeDocument
from app.core.config import settings
from app.utils.document_layout import (
    document_id,
    document_partition_date,
    is_edgar_document,
    stage_blob_name as build_stage_blob_name,
)

logger = logging.getLogger(__name__)


class StandardFeatures(BaseModel):
    summary: str = Field(description="1-2 paragraph executive summary")
    key_entities: list[str] = Field(description="List of key entities mentioned")
    topics: list[str] = Field(description="List of main topics covered")


GEMINI_PROMPT = """
You are a financial analyst. Read the following document and extract the standard features requested.
Do NOT hallucinate. If the information is not present, return an empty string or empty list.
Return a valid JSON object matching this schema:
{
  "summary": "1-2 paragraph executive summary",
  "key_entities": ["list of key entities mentioned"],
  "topics": ["list of main topics covered"]
}

Document Text:
"""

GEMINI_MERGE_PROMPT = """
You are a financial analyst. Merge the following per-part document feature JSON records into one
document-level feature object. Deduplicate key entities and topics. Preserve only information
supported by the per-part records.
"""

TRANSIENT_CODE_RE = re.compile(r"(?<!\d)(429|500|502|503|504)(?!\d)")
TRANSIENT_TERMS = (
    "RESOURCE_EXHAUSTED",
    "UNAVAILABLE",
    "DEADLINE_EXCEEDED",
    "INTERNAL",
    "BAD GATEWAY",
    "SERVICE UNAVAILABLE",
    "GATEWAY TIMEOUT",
    "TIMEOUT",
    "TIMED OUT",
)
PARQUET_STRING_COLUMNS = {
    "document_id",
    "company_id",
    "company_ticker",
    "title",
    "source_url",
    "source_gcs_uri",
    "gemini_file_uri",
    "document_type",
    "standard_features",
    "source_kind",
    "filing_type",
    "filing_date",
    "report_date",
    "accession_number",
    "primary_document",
    "cik",
}


def _document_id(doc: KnowledgeDocument) -> str:
    return document_id(doc)


def _mime_type(doc: KnowledgeDocument) -> str:
    return "application/pdf" if doc.type.lower() == "pdf" else "text/html"


def _parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    if not gcs_uri.startswith("gs://"):
        raise ApplicationError(
            f"Expected a gs:// URI, got {gcs_uri!r}",
            non_retryable=True,
        )
    bucket_name, _, blob_name = gcs_uri[5:].partition("/")
    if not bucket_name or not blob_name:
        raise ApplicationError(
            f"Invalid GCS URI: {gcs_uri!r}",
            non_retryable=True,
        )
    return bucket_name, blob_name


def _blob_from_gcs_uri(storage_client: storage.Client, gcs_uri: str) -> Any:
    bucket_name, blob_name = _parse_gcs_uri(gcs_uri)
    return storage_client.bucket(bucket_name).blob(blob_name)


def _get_blob_size(blob: Any) -> int:
    if getattr(blob, "size", None) is None:
        blob.reload()
    size = getattr(blob, "size", None)
    if size is None:
        raise ApplicationError(
            f"Could not determine GCS object size for {getattr(blob, 'name', 'unknown')}",
            non_retryable=True,
        )
    return int(size)


def _gemini_chunk_bucket_name() -> str:
    return settings.GEMINI_CHUNK_BUCKET or settings.PROD_BUCKET


def _gemini_response_config() -> genai.types.GenerateContentConfig:
    return genai.types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=StandardFeatures,
    )


def _generate_features_from_contents(
    genai_client: Any,
    contents: list[Any],
) -> StandardFeatures:
    response = genai_client.models.generate_content(
        model=settings.GEMINI_MODEL,
        contents=contents,
        config=_gemini_response_config(),
    )
    if not response.text:
        raise ValueError("Gemini returned empty response text")
    features = json.loads(response.text)
    return StandardFeatures(**features)


def _generate_features_from_uri(
    genai_client: Any,
    gcs_uri: str,
    mime_type: str,
) -> StandardFeatures:
    return _generate_features_from_contents(
        genai_client,
        [
            GEMINI_PROMPT,
            genai.types.Part.from_uri(file_uri=gcs_uri, mime_type=mime_type),
        ],
    )


def _error_text(error: BaseException) -> str:
    return str(error)


def _is_invalid_argument_error(error: BaseException) -> bool:
    error_text = _error_text(error).upper()
    return "INVALID_ARGUMENT" in error_text or bool(
        re.search(r"(?<!\d)400(?!\d)", error_text)
    )


def _is_pdf_file_size_error(error: BaseException) -> bool:
    error_text = _error_text(error).upper()
    return (
        _is_invalid_argument_error(error)
        and "APPLICATION/PDF" in error_text
        and "EXCEEDS MAX ALLOWED FILE SIZE" in error_text
    )


def _is_transient_gemini_error(error: BaseException) -> bool:
    if _is_pdf_file_size_error(error):
        return False
    error_text = _error_text(error).upper()
    return any(term in error_text for term in TRANSIENT_TERMS) or bool(
        TRANSIENT_CODE_RE.search(error_text)
    )


def _coerce_or_raise_gemini_error(
    error: BaseException,
    *,
    allow_empty_invalid_argument: bool,
) -> StandardFeatures:
    if _is_transient_gemini_error(error):
        logger.warning(
            "Transient Gemini error. Raising RuntimeError for retry: %s", error
        )
        raise RuntimeError(f"Transient Gemini error: {error}") from error
    if _is_invalid_argument_error(error) and allow_empty_invalid_argument:
        logger.warning(
            "Gemini rejected document with INVALID_ARGUMENT. Generating empty features. Error: %s",
            error,
        )
        return StandardFeatures(
            summary="Document parsing failed or document is empty.",
            key_entities=[],
            topics=[],
        )
    logger.error("Permanent Gemini parsing error: %s", error)
    raise ApplicationError(
        f"Permanent Gemini parsing error: {error}",
        non_retryable=True,
    ) from error


def _write_pdf_pages(pages: Sequence[Any]) -> bytes:
    writer = PdfWriter()
    for page in pages:
        writer.add_page(page)
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def _split_pdf_bytes(pdf_bytes: bytes, target_bytes: int) -> list[bytes]:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    if not reader.pages:
        raise ApplicationError("PDF has no pages to process", non_retryable=True)

    chunks: list[bytes] = []
    current_pages: list[Any] = []

    for page in reader.pages:
        candidate_pages = [*current_pages, page]
        candidate_bytes = _write_pdf_pages(candidate_pages)
        if len(candidate_bytes) <= target_bytes:
            current_pages = candidate_pages
            continue

        if not current_pages:
            raise ApplicationError(
                f"A single PDF page is {len(candidate_bytes)} bytes, above the "
                f"configured Gemini chunk target of {target_bytes} bytes",
                non_retryable=True,
            )

        chunks.append(_write_pdf_pages(current_pages))
        current_pages = [page]
        single_page_bytes = _write_pdf_pages(current_pages)
        if len(single_page_bytes) > target_bytes:
            raise ApplicationError(
                f"A single PDF page is {len(single_page_bytes)} bytes, above the "
                f"configured Gemini chunk target of {target_bytes} bytes",
                non_retryable=True,
            )

    if current_pages:
        chunks.append(_write_pdf_pages(current_pages))
    return chunks


def _split_and_upload_pdf_chunks(
    storage_client: storage.Client,
    source_blob: Any,
    doc_id: str,
) -> list[str]:
    with tempfile.TemporaryDirectory() as temp_dir:
        source_path = f"{temp_dir}/source.pdf"
        if hasattr(source_blob, "download_to_filename"):
            source_blob.download_to_filename(source_path)
            with open(source_path, "rb") as source_file:
                pdf_bytes = source_file.read()
        else:
            pdf_bytes = source_blob.download_as_bytes()

    chunks = _split_pdf_bytes(pdf_bytes, settings.GEMINI_PDF_CHUNK_TARGET_BYTES)
    chunk_bucket_name = _gemini_chunk_bucket_name()
    chunk_bucket = storage_client.bucket(chunk_bucket_name)
    chunk_prefix = settings.GEMINI_CHUNK_PREFIX.strip("/")

    chunk_uris = []
    for index, chunk_bytes in enumerate(chunks, start=1):
        blob_name = f"{chunk_prefix}/{doc_id}/part-{index:05d}.pdf"
        chunk_blob = chunk_bucket.blob(blob_name)
        chunk_blob.upload_from_string(
            chunk_bytes,
            content_type="application/pdf",
        )
        chunk_uris.append(f"gs://{chunk_bucket_name}/{blob_name}")

    return chunk_uris


def _merge_chunk_features(
    genai_client: Any,
    chunk_features: Sequence[StandardFeatures],
) -> StandardFeatures:
    if not chunk_features:
        raise ApplicationError("No Gemini chunk features to merge", non_retryable=True)
    if len(chunk_features) == 1:
        return chunk_features[0]

    merge_payload = [
        {"chunk": index, **features.model_dump()}
        for index, features in enumerate(chunk_features, start=1)
    ]
    try:
        return _generate_features_from_contents(
            genai_client,
            [
                GEMINI_MERGE_PROMPT,
                json.dumps(merge_payload, separators=(",", ":")),
            ],
        )
    except Exception as error:
        return _coerce_or_raise_gemini_error(
            error,
            allow_empty_invalid_argument=False,
        )


def _direct_metadata(source_gcs_uri: str) -> dict[str, Any]:
    return {
        "gemini_file_uri": source_gcs_uri,
        "gemini_chunk_uris": [],
        "gemini_chunk_count": 0,
    }


def _chunk_metadata(source_gcs_uri: str, chunk_uris: Sequence[str]) -> dict[str, Any]:
    return {
        "gemini_file_uri": source_gcs_uri,
        "gemini_chunk_uris": list(chunk_uris),
        "gemini_chunk_count": len(chunk_uris),
    }


def _document_date(doc: KnowledgeDocument) -> date:
    return date.fromisoformat(document_partition_date(doc))


def _edgar_metadata(doc: KnowledgeDocument) -> dict[str, Any]:
    if not is_edgar_document(doc):
        return {}
    return {
        "source_kind": doc.source_kind,
        "filing_type": doc.filing_type,
        "filing_date": doc.filing_date,
        "report_date": doc.report_date,
        "accession_number": doc.accession_number,
        "primary_document": doc.primary_document,
        "cik": doc.cik,
    }


def _parquet_bytes(dataframe: pd.DataFrame) -> bytes:
    for column in PARQUET_STRING_COLUMNS.intersection(dataframe.columns):
        dataframe[column] = dataframe[column].astype("string")
    table = pa.Table.from_pandas(dataframe, preserve_index=False)
    if "gemini_chunk_uris" in dataframe.columns:
        chunk_uris = pa.array(
            dataframe["gemini_chunk_uris"].tolist(),
            type=pa.list_(pa.string()),
        )
        column_index = table.schema.get_field_index("gemini_chunk_uris")
        table = table.set_column(column_index, "gemini_chunk_uris", chunk_uris)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)
    return parquet_buffer.getvalue()


def _extract_oversized_pdf_features(
    storage_client: storage.Client,
    genai_client: Any,
    source_blob: Any,
    source_gcs_uri: str,
    doc_id: str,
) -> tuple[StandardFeatures, dict[str, Any]]:
    logger.info(
        "Splitting oversized PDF for Gemini: source=%s size=%s",
        source_gcs_uri,
        getattr(source_blob, "size", None),
    )
    chunk_uris = _split_and_upload_pdf_chunks(storage_client, source_blob, doc_id)
    chunk_features = []
    for chunk_uri in chunk_uris:
        try:
            chunk_features.append(
                _generate_features_from_uri(
                    genai_client,
                    chunk_uri,
                    "application/pdf",
                )
            )
        except Exception as error:
            _coerce_or_raise_gemini_error(
                error,
                allow_empty_invalid_argument=False,
            )
            raise

    return _merge_chunk_features(genai_client, chunk_features), _chunk_metadata(
        source_gcs_uri,
        chunk_uris,
    )


def _extract_standard_features(
    doc: KnowledgeDocument,
    source_gcs_uri: str,
    storage_client: storage.Client,
    genai_client: Any,
    doc_id: str,
) -> tuple[StandardFeatures, dict[str, Any]]:
    mime_type = _mime_type(doc)

    if mime_type == "application/pdf":
        source_blob = _blob_from_gcs_uri(storage_client, source_gcs_uri)
        source_size = _get_blob_size(source_blob)
        if source_size > settings.GEMINI_PDF_MAX_BYTES:
            return _extract_oversized_pdf_features(
                storage_client,
                genai_client,
                source_blob,
                source_gcs_uri,
                doc_id,
            )

    try:
        return (
            _generate_features_from_uri(genai_client, source_gcs_uri, mime_type),
            _direct_metadata(source_gcs_uri),
        )
    except Exception as error:
        if mime_type == "application/pdf" and _is_pdf_file_size_error(error):
            source_blob = _blob_from_gcs_uri(storage_client, source_gcs_uri)
            return _extract_oversized_pdf_features(
                storage_client,
                genai_client,
                source_blob,
                source_gcs_uri,
                doc_id,
            )
        return (
            _coerce_or_raise_gemini_error(
                error,
                allow_empty_invalid_argument=True,
            ),
            _direct_metadata(source_gcs_uri),
        )


@activity.defn
def process_document_and_extract_features(
    doc: KnowledgeDocument, source_gcs_uri: str
) -> dict:
    """
    Downloads the raw file from Source GCS, extracts text using Document AI,
    uses Gemini to generate standard features, uploads to Gemini File Search,
    and saves the features as a Parquet file in the Prod GCS bucket.
    """
    client = storage.Client(project=settings.PROJECT_ID)

    genai_client = genai.Client(
        vertexai=True, project=settings.PROJECT_ID, location="global"
    )
    doc_id = _document_id(doc)

    validated_features, gemini_metadata = _extract_standard_features(
        doc,
        source_gcs_uri,
        client,
        genai_client,
        doc_id,
    )

    record = {
        "document_id": doc_id,
        "company_id": doc.company_id,
        "company_ticker": doc.company_ticker,
        "year": doc.year,
        "title": doc.title,
        "source_url": str(doc.url),
        "source_gcs_uri": source_gcs_uri,
        "document_type": doc.type,
        "standard_features": validated_features.model_dump_json(),
        "ingestion_timestamp": datetime.now(timezone.utc),
        "date": _document_date(doc),
    }
    record.update(gemini_metadata)
    record.update(_edgar_metadata(doc))

    df = pd.DataFrame([record])
    # Cast to microsecond resolution for BigQuery compatibility
    df["ingestion_timestamp"] = df["ingestion_timestamp"].astype("datetime64[us, UTC]")
    df["date"] = pd.to_datetime(df["date"]).dt.date

    stage_bucket = client.bucket(settings.PROD_BUCKET)
    stage_blob_name = build_stage_blob_name(doc, doc_id)

    stage_blob = stage_bucket.blob(stage_blob_name)
    stage_blob.upload_from_string(
        _parquet_bytes(df), content_type="application/octet-stream"
    )

    stage_gcs_uri = f"gs://{settings.PROD_BUCKET}/{stage_blob_name}"
    # Keep the legacy key name for workflow compatibility, but point it to stage.
    record["prod_gcs_uri"] = stage_gcs_uri

    logger.info(f"Successfully processed document and saved to {stage_gcs_uri}")
    return record
