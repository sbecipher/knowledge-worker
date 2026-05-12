import argparse
import json
import os
from google.cloud import storage  # type: ignore
from google import genai
from google.genai import types

from app.core.config import settings
from app.activities.processing import GEMINI_PROMPT

def generate_jsonl(audit_file: str, jsonl_file: str):
    with open(audit_file, "r") as f:
        missing_docs = json.load(f)

    valid_docs = 0
    with open(jsonl_file, "w") as out:
        for doc in missing_docs:
            mime_type = "application/pdf" if doc.get("type", "unknown").lower() == "pdf" else "text/html"
            gcs_uri = doc.get("gcs_uri") or doc.get("url") or doc.get("filepath")
            if not gcs_uri or not gcs_uri.startswith("gs://"):
                continue

            request_obj = {
                "request": {
                    "contents": [
                        {
                            "role": "user",
                            "parts": [
                                {"text": GEMINI_PROMPT},
                                {"fileData": {"fileUri": gcs_uri, "mimeType": mime_type}}
                            ]
                        }
                    ],
                    "generationConfig": {
                        "responseMimeType": "application/json",
                        "responseSchema": {
                            "type": "OBJECT",
                            "properties": {
                                "summary": {"type": "STRING", "description": "1-2 paragraph executive summary"},
                                "key_entities": {"type": "ARRAY", "items": {"type": "STRING"}, "description": "List of key entities mentioned"},
                                "topics": {"type": "ARRAY", "items": {"type": "STRING"}, "description": "List of main topics covered"}
                            },
                            "required": ["summary", "key_entities", "topics"]
                        }
                    }
                }
            }
            out.write(json.dumps(request_obj) + "\n")
            valid_docs += 1
    return valid_docs

def main():
    parser = argparse.ArgumentParser(description="Submit a Gemini batch prediction job")
    parser.add_argument("--audit-file", required=True, help="Path to missing_documents.json")
    parser.add_argument("--model", default="gemini-2.5-pro", help="Model to use (e.g., gemini-2.5-pro)")
    args = parser.parse_args()

    print(f"Preparing batch requests using model {args.model}")
    jsonl_filename = "batch_input.jsonl"
    count = generate_jsonl(args.audit_file, jsonl_filename)
    print(f"Generated {count} valid requests in {jsonl_filename}")

    if count == 0:
        print("No valid documents found. Exiting.")
        return

    # Upload to GCS
    storage_client = storage.Client(project=settings.PROJECT_ID)
    bucket = storage_client.bucket(settings.PROD_BUCKET)
    blob_name = "stage/knowledge/batch_jobs/batch_input.jsonl"
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(jsonl_filename)
    gcs_input_uri = f"gs://{settings.PROD_BUCKET}/{blob_name}"
    print(f"Uploaded to {gcs_input_uri}")

    # Submit Batch Job
    print("Submitting Vertex AI Batch Job...")
    # NOTE: using us-central1 as the location, standard for Vertex
    genai_client = genai.Client(vertexai=True, project=settings.PROJECT_ID, location="us-central1")
    
    dest_uri = f"gs://{settings.PROD_BUCKET}/stage/knowledge/batch_jobs/output/"
    
    job = genai_client.batches.create(
        model=args.model,
        src=gcs_input_uri,
        config=types.CreateBatchJobConfig(dest=dest_uri)
    )
    print(f"Batch Job created successfully! Job Name: {job.name}")
    print(f"Job State: {job.state}")
    print(f"Output will be delivered to: {dest_uri}")

if __name__ == "__main__":
    main()
