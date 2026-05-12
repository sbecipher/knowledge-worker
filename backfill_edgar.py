import argparse
import asyncio
import os
import json
import hashlib
import time
from datetime import datetime
import httpx
from google.cloud import storage

from app.core.config import settings
from batch_prepare import generate_jsonl_payload, submit_batch_job

SEC_USER_AGENT = "Sbecipher Capital jlrg@sbecipher.com"
TARGET_FORMS = {"10-K", "10-Q", "8-K", "S-1", "DEF 14A", "20-F", "40-F"}
BUCKET_NAME = "sbecipher-intelligence"

async def fetch_sec_tickers(http_client):
    headers = {"User-Agent": SEC_USER_AGENT}
    print("Fetching SEC company_tickers.json...")
    r = await http_client.get("https://www.sec.gov/files/company_tickers.json", headers=headers)
    r.raise_for_status()
    data = r.json()
    ticker_to_cik = {}
    for entry in data.values():
        ticker = entry.get("ticker", "").upper()
        cik = str(entry.get("cik_str"))
        ticker_to_cik[ticker] = cik
    return ticker_to_cik

async def download_file(http_client, url, bucket, gcs_path):
    headers = {"User-Agent": SEC_USER_AGENT}
    try:
        response = await http_client.get(url, headers=headers)
        response.raise_for_status()
        
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(response.content, content_type=response.headers.get("Content-Type", "text/html"))
        return True
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return False

async def main():
    parser = argparse.ArgumentParser(description="Backfill missing EDGAR documents.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be downloaded without doing it")
    parser.add_argument("--download-only", action="store_true", help="Only download files to source, skip batch processing")
    parser.add_argument("--submit-batch-only", action="store_true", help="Skip downloading, only generate and submit the batch payload")
    args = parser.parse_args()

    if args.submit_batch_only:
        print("\n--- Generating Vertex AI Batch Payload ---")
        jsonl_path, count = generate_jsonl_payload()
        if count > 0:
            print("\n--- Submitting Batch Job ---")
            submit_batch_job(jsonl_path)
        else:
            print("\nNo remaining documents to process via Vertex AI.")
        return

    from app.utils.metadata import get_latest_companies
    print("Fetching latest companies metadata from GCS...")
    companies_data = get_latest_companies()

    gcs_client = storage.Client(project=settings.PROJECT_ID)
    bucket = gcs_client.bucket(BUCKET_NAME)

    print(f"Listing existing source files in gs://{BUCKET_NAME}/source/knowledge/ ...")
    source_blobs = list(bucket.list_blobs(prefix="source/knowledge/"))
    existing_paths = {blob.name for blob in source_blobs}
    print(f"Found {len(existing_paths)} existing source files.")

    current_year = datetime.now().year
    start_year = current_year - 10

    documents_to_download = []
    skipped_companies = []

    async with httpx.AsyncClient(timeout=60.0) as http_client:
        ticker_to_cik = await fetch_sec_tickers(http_client)

        for comp in companies_data:
            ticker = comp["company_ticker"].upper()
            cik = ticker_to_cik.get(ticker)
            if not cik:
                print(f"Warning: CIK not found for {ticker}")
                skipped_companies.append({"ticker": ticker, "reason": "CIK not found"})
                continue

            padded_cik = cik.zfill(10)
            print(f"Fetching SEC submissions for {ticker} (CIK {padded_cik})...")
            headers = {"User-Agent": SEC_USER_AGENT}
            
            try:
                url = f"https://data.sec.gov/submissions/CIK{padded_cik}.json"
                response = await http_client.get(url, headers=headers)
                response.raise_for_status()
                submissions_data = response.json()
            except Exception as e:
                print(f"Failed to fetch EDGAR submissions for {ticker}: {e}")
                skipped_companies.append({"ticker": ticker, "reason": f"Failed to fetch submissions: {e}"})
                continue
            
            # SEC API rate limiting: Max 10 requests per second.
            await asyncio.sleep(0.2)

            filings = submissions_data.get("filings", {})
            recent = filings.get("recent", {})
            
            forms = recent.get("form", [])
            accession_numbers = recent.get("accessionNumber", [])
            primary_documents = recent.get("primaryDocument", [])
            report_dates = recent.get("reportDate", [])
            filing_dates = recent.get("filingDate", [])

            for i in range(len(forms)):
                form = forms[i]
                if form in TARGET_FORMS:
                    report_date = report_dates[i] if i < len(report_dates) and report_dates[i] else ""
                    filing_date = filing_dates[i] if i < len(filing_dates) and filing_dates[i] else ""
                    date_to_check = report_date or filing_date
                    
                    if not date_to_check:
                        continue
                    
                    try:
                        doc_year = int(date_to_check.split("-")[0])
                    except ValueError:
                        continue

                    if start_year <= doc_year <= current_year:
                        acc_num = accession_numbers[i]
                        acc_num_no_dashes = acc_num.replace("-", "")
                        primary_doc = primary_documents[i]
                        
                        doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_num_no_dashes}/{primary_doc}"
                        
                        ext = "html" if doc_url.lower().endswith(".htm") or doc_url.lower().endswith(".html") else "pdf"
                        filename = hashlib.md5(doc_url.encode("utf-8")).hexdigest() + f".{ext}"
                        gcs_path = f"source/knowledge/{ticker}/{doc_year}/{filename}"

                        if gcs_path not in existing_paths:
                            documents_to_download.append({
                                "ticker": ticker,
                                "year": doc_year,
                                "form": form,
                                "url": doc_url,
                                "gcs_path": gcs_path
                            })

    if skipped_companies:
        print(f"\nSkipped {len(skipped_companies)} companies. Writing to skipped_edgar_companies.json")
        with open("skipped_edgar_companies.json", "w") as f:
            json.dump(skipped_companies, f, indent=2)

    print(f"\nDiscovered {len(documents_to_download)} new EDGAR documents to download.")

    if args.dry_run:
        print("\n--- DRY RUN: Documents to download ---")
        for doc in documents_to_download[:50]:
            print(f"[{doc['ticker']} {doc['year']}] {doc['form']} -> gs://{BUCKET_NAME}/{doc['gcs_path']}")
        if len(documents_to_download) > 50:
            print(f"... and {len(documents_to_download) - 50} more.")
        print("\nSkipping downloads and batch job submission due to --dry-run.")
        return

    print("\n--- Starting Downloads ---")
    async with httpx.AsyncClient(timeout=120.0) as http_client:
        for i, doc in enumerate(documents_to_download):
            print(f"[{i+1}/{len(documents_to_download)}] Downloading {doc['ticker']} {doc['form']} ({doc['year']})...")
            success = await download_file(http_client, doc["url"], bucket, doc["gcs_path"])
            if success:
                existing_paths.add(doc["gcs_path"])
            # SEC rate limit is 10 req/sec, let's wait a bit to be safe
            await asyncio.sleep(0.2)

    if args.download_only:
        print("\nSkipping batch job submission due to --download-only.")
        return

    print("\n--- Generating Vertex AI Batch Payload ---")
    jsonl_path, count = generate_jsonl_payload()
    
    if count > 0:
        print("\n--- Submitting Batch Job ---")
        submit_batch_job(jsonl_path)
    else:
        print("\nNo remaining documents to process via Vertex AI.")

if __name__ == "__main__":
    asyncio.run(main())
