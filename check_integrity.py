import pandas as pd
from google.cloud import storage


def check_integrity():
    client = storage.Client()
    bucket = client.bucket("sbecipher-intelligence")

    # List files in the stage directory
    blobs = list(bucket.list_blobs(prefix="stage/knowledge/", max_results=200))
    blobs = [b for b in blobs if b.name.endswith('.parquet')]

    if not blobs:
        print("No files found in stage/knowledge/.")
        return

    print(f"Found {len(blobs)} parquet files. Checking the 5 most recent...")

    # Sort by updated time, newest first
    blobs.sort(key=lambda x: x.updated, reverse=True)

    for i, blob in enumerate(blobs[:5]):
        print(f"\nChecking: {blob.name}")
        local_path = f"/tmp/check_{i}.parquet"
        blob.download_to_filename(local_path)

        try:
            df = pd.read_parquet(local_path)
            print(f"  Shape: {df.shape}")
            print(f"  Columns: {list(df.columns)}")
            if "features" in df.columns:
                print(f"  Features check: {df['features'].iloc[0] != None} (Not None)")
            else:
                for col in df.columns:
                    if col not in ["company_ticker", "year", "source_url"]:
                        print(f"  Sample {col}: {df[col].iloc[0]}")
            print("  Status: VALID")
        except Exception as e:
            print(f"  Status: CORRUPT ({e})")


if __name__ == "__main__":
    check_integrity()
