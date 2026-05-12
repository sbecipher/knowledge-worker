import json
import logging
from typing import List, Dict, Any
from google.cloud import storage

logger = logging.getLogger(__name__)

COMPANIES_GCS_BUCKET = "sbecipher-intelligence"
COMPANIES_GCS_PREFIX = "source/instruments/metadata/"

def get_latest_companies() -> List[Dict[str, Any]]:
    """
    Fetches the latest companies.json from the central GCS bucket.
    The path follows the pattern:
    gs://sbecipher-intelligence/source/instruments/metadata/date=<latest_date>/companies.json
    """
    try:
        client = storage.Client()
        # List all blobs under the prefix to find the latest date partition
        # We assume the 'date=YYYY-MM-DD' folders can be sorted lexicographically
        blobs = list(client.list_blobs(COMPANIES_GCS_BUCKET, prefix=COMPANIES_GCS_PREFIX))
        
        # Filter for companies.json
        company_blobs = [b for b in blobs if b.name.endswith("companies.json")]
        
        if not company_blobs:
            raise FileNotFoundError(f"No companies.json found under gs://{COMPANIES_GCS_BUCKET}/{COMPANIES_GCS_PREFIX}")
            
        # Sort by name, assuming the date partition allows chronological sorting
        # e.g., source/instruments/metadata/date=2026-05-12/companies.json
        company_blobs.sort(key=lambda b: b.name, reverse=True)
        latest_blob = company_blobs[0]
        
        logger.info(f"Loading companies from: gs://{COMPANIES_GCS_BUCKET}/{latest_blob.name}")
        content = latest_blob.download_as_text()
        return json.loads(content)
        
    except Exception as e:
        logger.error(f"Failed to fetch companies.json from GCS: {e}")
        raise
