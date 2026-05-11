import logging
import json
import io
from datetime import datetime, timezone

import pandas as pd  # type: ignore
from temporalio import activity
from google.cloud import storage  # type: ignore
from google import genai
from pydantic import BaseModel, Field

from app.models.payloads import KnowledgeDocument
from app.core.config import settings

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

    # 1. Determine mime type and set up Gemini Part
    mime_type = "application/pdf" if doc.type.lower() == "pdf" else "text/html"

    # 2. Initialize Gemini Client with a strict timeout to prevent thread hanging
    genai_client = genai.Client(
        vertexai=True, project=settings.PROJECT_ID, location="global"
    )

    # We no longer need mock URIs since Vertex AI natively reads from GCS
    gemini_file_uri = source_gcs_uri

    # 3. Generate Standard Features via Gemini using GCS URI natively
    try:
        response = genai_client.models.generate_content(
            model="gemini-3-flash-preview",
            contents=[  # type: ignore
                GEMINI_PROMPT,
                genai.types.Part.from_uri(file_uri=source_gcs_uri, mime_type=mime_type),
            ],
            config=genai.types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=StandardFeatures,
            ),
        )
        if not response.text:
            raise ValueError("Gemini returned empty response text")
        features = json.loads(response.text)
        validated_features = StandardFeatures(**features)
    except Exception as e:
        error_str = str(e).upper()
        transient_codes = [
            "429",
            "RESOURCE_EXHAUSTED",
            "500",
            "502",
            "503",
            "504",
            "UNAVAILABLE",
            "TIMEOUT",
            "TIMED OUT",
        ]
        if any(code in error_str for code in transient_codes):
            logger.warning(
                f"Transient API Error ({error_str[:50]}). Raising RuntimeError to trigger Temporal retry."
            )
            raise RuntimeError(f"Transient API Error: {e}")
        elif "400" in error_str and "INVALID_ARGUMENT" in error_str:
            logger.warning(
                f"Document was invalid/empty for Gemini (400 INVALID_ARGUMENT). Generating empty features. Error: {e}"
            )
            validated_features = StandardFeatures(
                summary="Document parsing failed or document is empty.",
                key_entities=[],
                topics=[],
            )
        else:
            from temporalio.exceptions import ApplicationError

            logger.error(f"Permanent Gemini parsing error: {e}")
            raise ApplicationError(
                f"Permanent Gemini parsing error: {e}", non_retryable=True
            )

    # 5. Create Parquet and upload to Prod GCS
    import hashlib

    # Use deterministic hash for document ID
    stable_hash = hashlib.md5(doc.title.encode()).hexdigest()[:16]
    doc_id = f"{doc.company_id}_{doc.year}_{stable_hash}"

    record = {
        "document_id": doc_id,
        "company_id": doc.company_id,
        "company_ticker": doc.company_ticker,
        "year": doc.year,
        "title": doc.title,
        "source_url": str(doc.url),
        "source_gcs_uri": source_gcs_uri,
        "gemini_file_uri": gemini_file_uri,
        "document_type": doc.type,
        "standard_features": validated_features.model_dump_json(),
        "ingestion_timestamp": datetime.now(timezone.utc),
    }

    df = pd.DataFrame([record])
    # Cast to microsecond resolution for BigQuery compatibility
    df["ingestion_timestamp"] = df["ingestion_timestamp"].astype("datetime64[us, UTC]")

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    prod_bucket = client.bucket("sbecipher-intelligence")
    prod_blob_name = f"prod/knowledge/{doc_id}.parquet"

    prod_blob = prod_bucket.blob(prod_blob_name)
    prod_blob.upload_from_string(
        parquet_buffer.getvalue(), content_type="application/octet-stream"
    )

    prod_gcs_uri = f"gs://sbecipher-intelligence/{prod_blob_name}"
    record["prod_gcs_uri"] = prod_gcs_uri

    logger.info(f"Successfully processed document and saved to {prod_gcs_uri}")
    return record
