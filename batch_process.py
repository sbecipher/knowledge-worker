import os
import json
import hashlib
from datetime import datetime, timezone
import pandas as pd  # type: ignore
import io
from google.cloud import storage  # type: ignore
from app.core.config import settings
from app.activities.processing import StandardFeatures


def get_companies_data():
    from app.utils.metadata import get_latest_companies
    return {comp["company_ticker"]: comp for comp in get_latest_companies()}


def parse_batch_output():
    ticker_to_company = get_companies_data()
    gcs_client = storage.Client(project=settings.PROJECT_ID)
    bucket_name = "sbecipher-intelligence"
    bucket = gcs_client.bucket(bucket_name)

    print(
        "Looking for output JSONL files in gs://sbecipher-intelligence/batch/knowledge/output/..."
    )
    blobs = list(gcs_client.list_blobs(bucket_name, prefix="batch/knowledge/output/"))

    jsonl_blobs = [
        b for b in blobs if b.name.endswith(".jsonl") and "predictions" in b.name
    ]
    if not jsonl_blobs:
        print("No prediction output files found. Is the batch job finished?")
        return

    print(f"Found {len(jsonl_blobs)} output files. Processing...")

    success_count = 0
    error_count = 0

    for blob in jsonl_blobs:
        print(f"Processing {blob.name}...")
        content = blob.download_as_text()

        for line in content.strip().split("\n"):
            if not line.strip():
                continue
            data = json.loads(line)

            # Extract request info
            request = data.get("request", {})
            try:
                # Find the fileData part
                contents = request.get("contents", [])[0]
                file_uri = None
                for part in contents.get("parts", []):
                    fd = part.get("fileData")
                    if fd:
                        file_uri = fd["fileUri"]
                        break
            except Exception as e:
                print(f"Error parsing request structure: {e}")
                continue

            if not file_uri:
                print("Could not find fileUri in request.")
                continue

            # e.g. gs://sbecipher-intelligence/source/knowledge/TICKER/YEAR/filename
            prefix = f"gs://{bucket_name}/source/knowledge/"
            if not file_uri.startswith(prefix):
                continue

            path_part = file_uri[len(prefix) :]
            parts = path_part.split("/")
            ticker = parts[0]
            year_str = parts[1]
            filename = parts[-1]

            comp_info = ticker_to_company.get(ticker)
            company_id = comp_info.get("company_id", ticker) if comp_info else ticker
            try:
                year = int(year_str)
            except:
                year = 0

            stable_hash = hashlib.md5(filename.encode()).hexdigest()[:16]
            doc_id = f"{company_id}_{year}_{stable_hash}"

            # Check if this document was already processed (idempotency)
            prod_blob_name = f"stage/knowledge/{doc_id}.parquet"
            if bucket.blob(prod_blob_name).exists():
                # Already processed
                continue

            # Check response — Vertex AI batch includes an empty "status" key on
            # successful predictions, so test the *value*, not just key presence.
            error_val = data.get("error") or data.get("status")
            if error_val:
                print(f"Error for {doc_id}: {error_val}")
                error_count += 1
                continue

            response = data.get("response", {})
            try:
                text_response = response["candidates"][0]["content"]["parts"][0]["text"]
            except (KeyError, IndexError):
                print(f"Malformed response for {doc_id}: {response}")
                error_count += 1
                continue

            # Clean up the response if it contains markdown code blocks
            text_response = text_response.strip()
            if text_response.startswith("```json"):
                text_response = text_response[7:]
            if text_response.startswith("```"):
                text_response = text_response[3:]
            if text_response.endswith("```"):
                text_response = text_response[:-3]

            try:
                features_dict = json.loads(text_response.strip())
                validated_features = StandardFeatures(**features_dict)
            except Exception as e:
                print(
                    f"Validation error for {doc_id}: {e}\nResponse was: {text_response}"
                )
                error_count += 1
                continue

            # Create the Parquet
            ext = filename.split(".")[-1].lower() if "." in filename else "unknown"

            record = {
                "document_id": doc_id,
                "company_id": company_id,
                "company_ticker": ticker,
                "year": year,
                "title": filename,
                "source_url": file_uri,
                "source_gcs_uri": file_uri,
                "gemini_file_uri": file_uri,
                "document_type": ext,
                "standard_features": validated_features.model_dump_json(),
                "ingestion_timestamp": datetime.now(timezone.utc),
            }

            df = pd.DataFrame([record])
            df["ingestion_timestamp"] = df["ingestion_timestamp"].astype(
                "datetime64[us, UTC]"
            )

            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)

            prod_blob = bucket.blob(prod_blob_name)
            prod_blob.upload_from_string(
                parquet_buffer.getvalue(), content_type="application/octet-stream"
            )
            success_count += 1

            # Print periodic progress
            if success_count % 100 == 0:
                print(f"Processed {success_count} valid parquet files...")

    print(f"Finished processing. Success: {success_count}, Errors: {error_count}")


if __name__ == "__main__":
    parse_batch_output()
