import os
import json
from google.cloud import storage


def delete_failed_files():
    file_path = "/Users/jlroo/.gemini/antigravity/brain/07519639-746f-4ba7-8e2f-995bbb0f4a3a/.system_generated/steps/503/output.txt"
    bucket_name = "sbecipher-intelligence"

    with open(file_path, "r") as f:
        lines = f.readlines()

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    deleted_count = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue

        try:
            data = json.loads(line)
            doc_id = data.get("document_id")
            if doc_id:
                blob_name = f"stage/knowledge/{doc_id}.parquet"
                blob = bucket.blob(blob_name)
                if blob.exists():
                    blob.delete()
                    print(f"Deleted {blob_name}")
                    deleted_count += 1
                else:
                    print(f"Blob {blob_name} does not exist.")
        except json.JSONDecodeError:
            print(f"Skipping line, not valid JSON: {line}")

    print(f"Finished. Deleted {deleted_count} files.")


if __name__ == "__main__":
    delete_failed_files()
