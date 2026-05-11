from google.cloud import storage
from datetime import datetime, timezone


def main():
    client = storage.Client()
    bucket = client.bucket("sbecipher-intelligence")

    # List files in the stage directory
    blobs = list(bucket.list_blobs(prefix="stage/knowledge/"))

    now = datetime.now(timezone.utc)
    # Check how many were created in the last 24 hours
    recent_blobs = [b for b in blobs if (now - b.updated).total_seconds() < 3600 * 24]

    print(f"Total files in stage/knowledge/: {len(blobs)}")
    print(f"Files processed in the last 24 hours: {len(recent_blobs)}")


if __name__ == "__main__":
    main()
