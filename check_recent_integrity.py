import pandas as pd
from google.cloud import storage
from datetime import datetime, timezone


def check_recent_integrity():
    client = storage.Client()
    bucket = client.bucket("sbecipher-intelligence")

    # List files in the stage directory
    blobs = list(bucket.list_blobs(prefix="stage/knowledge/"))

    # Sort by updated time, newest first
    blobs.sort(key=lambda x: x.updated, reverse=True)

    now = datetime.now(timezone.utc)
    recent_blobs = [
        b for b in blobs if (now - b.updated).total_seconds() < 3600 * 2
    ]  # within last 2 hours

    if not recent_blobs:
        print(f"Total files: {len(blobs)}. No files created in the last 2 hours.")
        print(
            f"Most recent file was modified at {blobs[0].updated if blobs else 'N/A'}"
        )
        return

    print(
        f"Found {len(recent_blobs)} files generated in the last 2 hours. Checking the 5 most recent..."
    )

    for i, blob in enumerate(recent_blobs[:5]):
        print(f"\nChecking: {blob.name} (Updated: {blob.updated})")
        local_path = f"/tmp/check_recent_{i}.parquet"
        blob.download_to_filename(local_path)

        try:
            df = pd.read_parquet(local_path)
            print(f"  Shape: {df.shape}")
            if "standard_features" in df.columns:
                print(
                    f"  Features check: {df['standard_features'].iloc[0] != None} (Not None)"
                )
            print("  Status: VALID")
        except Exception as e:
            print(f"  Status: CORRUPT ({e})")


if __name__ == "__main__":
    check_recent_integrity()
