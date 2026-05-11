import os
import json
import hashlib
from google.cloud import storage  # type: ignore
from google import genai
from app.core.config import settings
from app.activities.processing import StandardFeatures, GEMINI_PROMPT


def get_companies_data():
    companies_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "knowledgeio", "companies.json"
    )
    if not os.path.exists(companies_path):
        companies_path = "/Users/jlroo/Library/CloudStorage/GoogleDrive-jlrg@sbecipher.com/My Drive/Sbecipher Capital/Cloud/orchestration/knowledgeio/companies.json"

    with open(companies_path, "r") as f:
        return json.load(f)


def generate_jsonl_payload():
    companies_data = get_companies_data()
    ticker_to_company = {comp["company_ticker"]: comp for comp in companies_data}

    gcs_client = storage.Client(project=settings.PROJECT_ID)
    bucket_name = "sbecipher-intelligence"

    print(f"Listing blobs in gs://{bucket_name}/stage/knowledge/...")
    stage_blobs = gcs_client.list_blobs(bucket_name, prefix="stage/knowledge/")
    existing_stage_files = set(blob.name for blob in stage_blobs)
    print(f"Found {len(existing_stage_files)} already processed files.")

    print(f"Listing source blobs in gs://{bucket_name}/source/knowledge/...")
    source_blobs = gcs_client.list_blobs(bucket_name, prefix="source/knowledge/")

    schema = StandardFeatures.model_json_schema()
    if "$defs" in schema:
        # Vertex AI schema does not support $defs out of the box, but ours is simple
        pass

    # Vertex AI Batch Prediction JSONL format
    # {"request": {"contents": [{"role": "user", "parts": [{"text": "..."}, {"fileData": {"mimeType": "...", "fileUri": "..."}}]}], "generationConfig": {"responseMimeType": "application/json", "responseSchema": {...}}}}

    requests = []

    for blob in source_blobs:
        if blob.name.endswith("/"):
            continue

        parts = blob.name.split("/")
        if len(parts) >= 5:
            ticker = parts[2]
            year_str = parts[3]
            filename = parts[-1]

            comp_info = ticker_to_company.get(ticker)
            if not comp_info:
                continue

            try:
                year = int(year_str)
            except ValueError:
                continue

            company_id = comp_info.get("company_id", ticker)
            stable_hash = hashlib.md5(filename.encode()).hexdigest()[:16]
            doc_id = f"{company_id}_{year}_{stable_hash}"
            expected_stage_path = f"stage/knowledge/{doc_id}.parquet"

            if expected_stage_path in existing_stage_files:
                continue

            ext = filename.split(".")[-1].lower() if "." in filename else "unknown"
            mime_type = "application/pdf" if ext == "pdf" else "text/html"
            gcs_uri = f"gs://{bucket_name}/{blob.name}"

            req = {
                "request": {
                    "contents": [
                        {
                            "role": "user",
                            "parts": [
                                {"text": GEMINI_PROMPT},
                                {
                                    "fileData": {
                                        "mimeType": mime_type,
                                        "fileUri": gcs_uri,
                                    }
                                },
                            ],
                        }
                    ],
                    "generationConfig": {
                        "responseMimeType": "application/json",
                        "responseSchema": schema,
                    },
                }
            }
            requests.append(req)

    print(f"Found {len(requests)} remaining documents to backfill.")

    jsonl_path = "batch_input.jsonl"
    with open(jsonl_path, "w") as f:
        for req in requests:
            f.write(json.dumps(req) + "\n")

    print(f"Wrote {len(requests)} requests to {jsonl_path}")
    return jsonl_path, len(requests)


def submit_batch_job(jsonl_path):
    gcs_client = storage.Client(project=settings.PROJECT_ID)
    bucket = gcs_client.bucket("sbecipher-intelligence")

    # Upload jsonl to GCS
    gcs_input_path = "batch/knowledge/input.jsonl"
    blob = bucket.blob(gcs_input_path)
    print(f"Uploading {jsonl_path} to gs://sbecipher-intelligence/{gcs_input_path}...")
    blob.upload_from_filename(jsonl_path)
    print("Upload complete.")

    gcs_input_uri = f"gs://sbecipher-intelligence/{gcs_input_path}"
    gcs_output_prefix = "gs://sbecipher-intelligence/batch/knowledge/output/"

    print(f"Submitting batch job to Vertex AI...")
    genai_client = genai.Client(
        vertexai=True, project=settings.PROJECT_ID, location="us-central1"
    )

    try:
        job = genai_client.batches.create(
            model="gemini-2.5-flash",
            src=gcs_input_uri,
            config=genai.types.CreateBatchJobConfig(
                dest=gcs_output_prefix, displayName="knowledge-backfill-batch-2"
            ),
        )
        print(f"Batch job submitted successfully! Job Name: {job.name}")
        print(f"Check the GCP Console for progress.")
    except Exception as e:
        print(f"Error submitting batch job: {e}")


if __name__ == "__main__":
    jsonl_path, count = generate_jsonl_payload()
    if count > 0:
        submit_batch_job(jsonl_path)
    else:
        print("No remaining documents to process.")
