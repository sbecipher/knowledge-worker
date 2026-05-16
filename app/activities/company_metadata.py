from __future__ import annotations

import hashlib
import io
import json
import logging
from datetime import datetime, timezone

import httpx
import pandas as pd
from temporalio import activity
from temporalio.exceptions import ApplicationError

from app.core.config import settings
from app.core.cloud_backends import get_storage_backend
from app.core.knowledge_api import knowledge_api_headers
from app.models.payloads import CompanyMetadataArtifact, CompanyPayload

logger = logging.getLogger(__name__)

SUPPORTED_METADATA_PROVIDERS = frozenset({"lseg"})


def _normalize_metadata_provider(provider: str) -> str:
    normalized = provider.strip().lower()
    if normalized not in SUPPORTED_METADATA_PROVIDERS:
        raise ApplicationError(
            f"Unsupported metadata provider: {provider}",
            non_retryable=True,
        )
    return normalized


def _metadata_id(company: CompanyPayload, year: int, provider: str) -> str:
    stable_input = f"{company.company_id}|{company.company_ticker}|{company.base_url}|{year}|{provider}"
    stable_hash = hashlib.md5(stable_input.encode("utf-8")).hexdigest()[:16]
    return f"{company.company_id}_{year}_{provider}_{stable_hash}"


def _stage_company_metadata_artifact(
    artifact: CompanyMetadataArtifact,
) -> str:
    row = {
        "metadata_id": artifact.metadata_id,
        "company_ticker": artifact.company_ticker,
        "company_name": artifact.company_name,
        "company_id": artifact.company_id,
        "base_url": artifact.base_url,
        "year": artifact.year,
        "provider": artifact.provider,
        "matched_on": artifact.matched_on,
        "source_snapshot_uri": artifact.source_snapshot_uri,
        "source_snapshot_date": artifact.source_snapshot_date,
        "metadata_json": json.dumps(artifact.metadata, sort_keys=True),
        "source_record_json": json.dumps(artifact.source_record, sort_keys=True),
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    parquet_buffer = io.BytesIO()
    pd.DataFrame([row]).to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    blob_name = (
        "stage/knowledge/company_metadata/"
        f"provider={artifact.provider}/ticker={artifact.company_ticker}/"
        f"year={artifact.year}/{artifact.metadata_id}.parquet"
    )

    return get_storage_backend().upload_bytes(
        settings.PROD_BUCKET,
        blob_name,
        parquet_buffer.getvalue(),
        content_type="application/octet-stream",
    )


@activity.defn
def fetch_company_metadata(
    company: CompanyPayload,
    year: int,
    provider: str = "lseg",
) -> CompanyMetadataArtifact | None:
    normalized_provider = _normalize_metadata_provider(provider)
    api_url = f"{settings.KNOWLEDGEIO_API_URL.rstrip('/')}/api/v1/metadata/company"
    payload = {
        "provider": normalized_provider,
        "company_ticker": company.company_ticker,
        "company_name": company.company_name,
        "company_id": company.company_id,
        "base_url": company.base_url,
    }

    logger.info(
        "Fetching company metadata for %s provider=%s",
        company.company_ticker,
        normalized_provider,
    )

    with httpx.Client(timeout=60.0) as http_client:
        try:
            response = http_client.post(
                api_url,
                json=payload,
                headers=knowledge_api_headers(),
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 404:
                logger.info(
                    "No company metadata found for %s provider=%s",
                    company.company_ticker,
                    normalized_provider,
                )
                return None
            if 400 <= status_code < 500:
                raise ApplicationError(
                    f"Metadata request was rejected by KnowledgeIO: {exc}",
                    non_retryable=True,
                ) from exc
            raise RuntimeError(
                f"KnowledgeIO metadata request failed with status {status_code}: {exc}"
            ) from exc
        except httpx.HTTPError as exc:
            raise RuntimeError(
                f"KnowledgeIO metadata request failed for {company.company_ticker}: {exc}"
            ) from exc

    data = response.json()
    if not isinstance(data, dict):
        raise ApplicationError(
            f"KnowledgeIO metadata response must be an object, got {type(data).__name__}",
            non_retryable=True,
        )

    artifact = CompanyMetadataArtifact(
        metadata_id=_metadata_id(company, year, normalized_provider),
        company_ticker=str(data["company_ticker"]).strip().upper(),
        company_name=str(data["company_name"]).strip(),
        company_id=str(data["company_id"]).strip(),
        base_url=str(data["base_url"]).strip(),
        year=year,
        provider=normalized_provider,
        matched_on=str(data["matched_on"]).strip(),
        source_snapshot_uri=str(data["source_snapshot_uri"]).strip(),
        source_snapshot_date=data.get("source_snapshot_date"),
        metadata=data.get("metadata") or {},
        source_record=data.get("source_record") or {},
        stage_gcs_uri="",
    )
    stage_gcs_uri = _stage_company_metadata_artifact(artifact)
    return artifact.model_copy(update={"stage_gcs_uri": stage_gcs_uri})
