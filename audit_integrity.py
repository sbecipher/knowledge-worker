import os
import json
import hashlib
from google.cloud import storage  # type: ignore


def main():
    print("Loading companies.json...")
    companies_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "knowledgeio", "companies.json"
    )
    if not os.path.exists(companies_path):
        companies_path = "/Users/jlroo/Library/CloudStorage/GoogleDrive-jlrg@sbecipher.com/My Drive/Sbecipher Capital/Cloud/orchestration/knowledgeio/companies.json"

    with open(companies_path, "r") as f:
        companies_data = json.load(f)

    ticker_to_company = {comp["company_ticker"]: comp for comp in companies_data}

    gcs_client = storage.Client()
    bucket_name = "sbecipher-intelligence"
    bucket = gcs_client.bucket(bucket_name)

    print("Fetching prod/knowledge/v1/ blobs...")
    prod_blobs = bucket.list_blobs(prefix="prod/knowledge/v1/")
    # Extract just the filenames (e.g. TICKER_YEAR_hash.parquet)
    existing_prod_files = set(
        blob.name.split("/")[-1]
        for blob in prod_blobs
        if blob.name.endswith(".parquet")
    )
    print(f"Found {len(existing_prod_files)} files in prod/knowledge/v1/")

    print("Fetching source/knowledge/ blobs...")
    source_blobs = bucket.list_blobs(prefix="source/knowledge/")

    missing_docs = []
    total_source = 0

    for blob in source_blobs:
        if blob.name.endswith("/"):
            continue

        parts = blob.name.split("/")
        if len(parts) >= 5:
            total_source += 1
            ticker = parts[2]
            year_str = parts[3]
            filename = parts[-1]

            comp_info = ticker_to_company.get(ticker)
            if not comp_info:
                print(f"Warning: Ticker {ticker} not found in companies.json, skipping")
                continue

            try:
                year = int(year_str)
            except ValueError:
                continue

            company_id = comp_info.get("company_id", ticker)
            stable_hash = hashlib.md5(filename.encode()).hexdigest()[:16]
            doc_id = f"{company_id}_{year}_{stable_hash}"
            expected_filename = f"{doc_id}.parquet"

            if expected_filename not in existing_prod_files:
                ext = filename.split(".")[-1].lower() if "." in filename else "unknown"
                missing_docs.append(
                    {
                        "title": filename,
                        "company_name": comp_info["company_name"],
                        "company_id": company_id,
                        "company_ticker": ticker,
                        "base_url": comp_info.get("base_url", ""),
                        "year": year,
                        "url": f"gs://{bucket_name}/{blob.name}",
                        "type": ext,
                        "filepath": f"gs://{bucket_name}/{blob.name}",
                        "downloaded": True,
                        "gcs_uri": f"gs://{bucket_name}/{blob.name}",
                    }
                )

    print(
        f"Audit complete. Total source files: {total_source}. Missing in prod: {len(missing_docs)}"
    )

    with open("missing_documents.json", "w") as f:
        json.dump(missing_docs, f, indent=2)
    print("Wrote missing_documents.json")


if __name__ == "__main__":
    main()
