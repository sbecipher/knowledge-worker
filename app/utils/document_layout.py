from __future__ import annotations

import hashlib
from datetime import date
from pathlib import PurePosixPath

from app.models.payloads import KnowledgeDocument

EDGAR_SOURCE_KIND = "edgar"


def source_kind(doc: KnowledgeDocument) -> str:
    return (doc.source_kind or "articles").strip().lower()


def is_edgar_document(doc: KnowledgeDocument) -> bool:
    return source_kind(doc) == EDGAR_SOURCE_KIND


def document_id(doc: KnowledgeDocument) -> str:
    hash_source = doc.title
    if is_edgar_document(doc) and doc.accession_number:
        hash_source = f"{doc.accession_number}:{doc.primary_document or ''}"
    stable_hash = hashlib.md5(hash_source.encode("utf-8")).hexdigest()[:16]
    return f"{doc.company_id}_{doc.year}_{stable_hash}"


def document_partition_date(doc: KnowledgeDocument) -> str:
    raw_value = (doc.date or "").strip()
    if raw_value:
        try:
            return date.fromisoformat(raw_value).isoformat()
        except ValueError:
            pass
    return f"{doc.year}-12-31"


def source_prefix(doc: KnowledgeDocument) -> str:
    ticker = doc.company_ticker.upper()
    if is_edgar_document(doc):
        return f"source/edgar/{ticker}/{document_partition_date(doc)}/"
    return f"source/knowledge/{ticker}/{doc.year}/"


def source_filename(doc: KnowledgeDocument) -> str:
    if is_edgar_document(doc):
        extension = source_extension(doc)
        return f"{document_id(doc)}.{extension}"
    return PurePosixPath(doc.filepath).name


def source_extension(
    doc: KnowledgeDocument, source_blob_name: str | None = None
) -> str:
    if source_blob_name:
        suffix = PurePosixPath(source_blob_name).suffix.lstrip(".").lower()
        if suffix:
            return suffix
    explicit_type = doc.type.lower().strip()
    if explicit_type == "pdf":
        return "pdf"
    return "html"


def edgar_source_blob_name(
    doc: KnowledgeDocument,
    source_blob_name: str | None = None,
) -> str:
    return f"{source_prefix(doc)}{document_id(doc)}.{source_extension(doc, source_blob_name)}"


def stage_blob_name(doc: KnowledgeDocument, doc_id: str) -> str:
    if is_edgar_document(doc):
        return f"stage/edgar/v1/date={document_partition_date(doc)}/{doc_id}.parquet"
    return f"stage/knowledge/{doc_id}.parquet"
