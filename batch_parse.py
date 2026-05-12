import argparse
import json
import io
import os
import pandas as pd  # type: ignore
from datetime import datetime, timezone
from google.cloud import storage  # type: ignore
from app.core.config import settings

def _document_id(doc: dict) -> str:
    # Need hashlib for MD5
    import hashlib
    stable_hash = hashlib.md5(doc["title"].encode()).hexdigest()[:16]
    return f"{doc['company_id']}_{doc['year']}_{stable_hash}"

def main():
    parser = argparse.ArgumentParser(description="Parse Vertex AI Batch Job output")
    parser.add_argument("--audit-file", required=True, help="Path to missing_documents.json")
    parser.add_argument("--output-uri", required=True, help="GCS URI to the output JSONL file or directory")
    args = parser.parse_args()

    with open(args.audit_file, "r") as f:
        missing_docs = json.load(f)

    # Build mapping from gcs_uri to doc
    uri_to_doc = {}
    for doc in missing_docs:
        gcs_uri = doc.get("gcs_uri") or doc.get("url") or doc.get("filepath")
        if gcs_uri:
            uri_to_doc[gcs_uri] = doc

    client = storage.Client(project=settings.PROJECT_ID)
    bucket_name = settings.PROD_BUCKET
    stage_bucket = client.bucket(bucket_name)

    # Download output JSONL
    # If the output_uri is a directory (e.g., gs://bucket/path/to/output/), find the actual JSONL
    if not args.output_uri.startswith("gs://"):
        raise ValueError("output_uri must start with gs://")

    path_parts = args.output_uri[5:].split("/", 1)
    if len(path_parts) != 2:
        raise ValueError("Invalid GCS URI")
    
    out_bucket_name, out_prefix = path_parts
    out_bucket = client.bucket(out_bucket_name)

    blobs = list(out_bucket.list_blobs(prefix=out_prefix))
    jsonl_blobs = [b for b in blobs if b.name.endswith(".jsonl")]
    if not jsonl_blobs:
        print(f"No .jsonl files found at {args.output_uri}")
        return

    success_count = 0
    error_count = 0

    for jsonl_blob in jsonl_blobs:
        print(f"Processing {jsonl_blob.name}...")
        content = jsonl_blob.download_as_string().decode("utf-8")
        for line in content.strip().split("\n"):
            if not line:
                continue
            
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                print("Failed to decode JSONL row")
                error_count += 1
                continue

            # Extract GCS URI from request to match doc
            request_obj = row.get("request", {})
            parts = request_obj.get("contents", [{}])[0].get("parts", [])
            gcs_uri = None
            for p in parts:
                if "fileData" in p:
                    gcs_uri = p["fileData"].get("fileUri")
                    break
            
            if not gcs_uri or gcs_uri not in uri_to_doc:
                print(f"Could not find matching document for URI: {gcs_uri}")
                error_count += 1
                continue

            doc = uri_to_doc[gcs_uri]
            
            # Check for error in response
            if "error" in row:
                print(f"Error in Gemini response for {gcs_uri}: {row['error']}")
                error_count += 1
                continue

            # Extract response features
            response_obj = row.get("response", {})
            candidates = response_obj.get("candidates", [])
            if not candidates:
                print(f"No candidates returned for {gcs_uri}")
                error_count += 1
                continue

            content_parts = candidates[0].get("content", {}).get("parts", [])
            if not content_parts:
                print(f"No content parts returned for {gcs_uri}")
                error_count += 1
                continue
            
            try:
                features_text = content_parts[0].get("text", "")
                if not features_text:
                    raise ValueError("Empty text")
                
                # Strip markdown blocks if Gemini returns ```json ... ```
                features_text = features_text.strip()
                if features_text.startswith("```json"):
                    features_text = features_text[7:]
                if features_text.startswith("```"):
                    features_text = features_text[3:]
                if features_text.endswith("```"):
                    features_text = features_text[:-3]

                features_json = json.loads(features_text)
            except Exception as e:
                print(f"Failed to parse Gemini output JSON for {gcs_uri}: {e}")
                error_count += 1
                continue

            doc_id = _document_id(doc)
            
            record = {
                "document_id": doc_id,
                "company_id": doc.get("company_id"),
                "company_ticker": doc.get("company_ticker"),
                "year": doc.get("year"),
                "title": doc.get("title"),
                "source_url": doc.get("url") or "",
                "source_gcs_uri": gcs_uri,
                "document_type": doc.get("type", "unknown"),
                "standard_features": json.dumps(features_json),
                "ingestion_timestamp": datetime.now(timezone.utc),
                "gemini_file_uri": gcs_uri,
                "gemini_chunk_uris": "[]",
                "gemini_chunk_count": 0,
            }

            df = pd.DataFrame([record])
            df["ingestion_timestamp"] = df["ingestion_timestamp"].astype("datetime64[us, UTC]")

            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)

            stage_blob_name = f"stage/knowledge/{doc_id}.parquet"
            stage_blob = stage_bucket.blob(stage_blob_name)
            stage_blob.upload_from_string(
                parquet_buffer.getvalue(), content_type="application/octet-stream"
            )

            stage_gcs_uri = f"gs://{bucket_name}/{stage_blob_name}"
            print(f"Successfully processed and uploaded {stage_gcs_uri}")
            success_count += 1

    print(f"Batch parse complete. Successes: {success_count}, Errors: {error_count}")

if __name__ == "__main__":
    main()
