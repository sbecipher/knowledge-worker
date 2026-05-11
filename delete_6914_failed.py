import os
import json
from google.cloud import storage, bigquery
from concurrent.futures import ThreadPoolExecutor


def delete_failed_files():
    bq_client = bigquery.Client()
    storage_client = storage.Client()
    bucket_name = "sbecipher-intelligence"
    bucket = storage_client.bucket(bucket_name)

    query = """
    SELECT document_id
    FROM `sbecipherio.knowledge.documents`
    WHERE standard_features LIKE '%Rate limit exhausted%'
    """
    print("Executing BigQuery query...")
    query_job = bq_client.query(query)
    results = query_job.result()

    doc_ids = [row.document_id for row in results]
    print(f"Found {len(doc_ids)} failed documents.")

    def delete_blob(doc_id):
        blob_name = f"stage/knowledge/{doc_id}.parquet"
        blob = bucket.blob(blob_name)
        if blob.exists():
            blob.delete()
            return True
        return False

    deleted_count = 0
    print("Deleting from GCS...")

    with ThreadPoolExecutor(max_workers=20) as executor:
        results = executor.map(delete_blob, doc_ids)
        for r in results:
            if r:
                deleted_count += 1

    print(f"Finished. Deleted {deleted_count} files.")


if __name__ == "__main__":
    delete_failed_files()
