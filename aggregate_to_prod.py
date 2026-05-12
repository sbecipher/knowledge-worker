import os
import uuid
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from google.cloud import storage


def run_aggregation():
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "data-cipher")
    bucket_name = "sbecipher-intelligence"
    stage_path = f"gs://{bucket_name}/stage/knowledge/"
    prod_path = f"gs://{bucket_name}/prod/knowledge/v1/"

    print(f"Reading all staging parquets from {stage_path} ...")

    import gcsfs
    fs = gcsfs.GCSFileSystem(project=project_id)

    # Read all parquet files from the staging directory
    try:
        files = fs.glob(f"{bucket_name}/stage/knowledge/*.parquet")
        if not files:
            print("No parquet files found in staging.")
            return
        df = pd.concat([pd.read_parquet(f"gs://{f}") for f in files], ignore_index=True)
    except Exception as e:
        print(f"Failed to read staging files or no files found: {e}")
        return

    print(f"Loaded {len(df)} records from staging.")

    # Create the 'date' partition column. For backfill, we use the end of the year.
    # df['year'] is the year of the document
    df["date"] = df["year"].astype(str) + "-12-31"

    # We want to write to partitioned dataset: prod/knowledge/v1/date=YYYY-12-31/...
    print(f"Writing partitioned dataset to {prod_path} ...")

    # Define a custom basename template to match user's request:
    # part-00000-backfill-YYYY-12-31-hash.snappy.parquet
    # We can use pyarrow's dataset API to write this.
    table = pa.Table.from_pandas(df)

    # Using gcsfs explicitly for pyarrow
    import gcsfs

    fs = gcsfs.GCSFileSystem(project=project_id)

    # Since we want a specific file pattern, it's easiest to write each partition individually
    dates = df["date"].unique()
    for d in dates:
        partition_df = df[df["date"] == d]
        partition_table = pa.Table.from_pandas(partition_df)

        # Format: part-00000-backfill-{date}-{uuid}.snappy.parquet
        run_timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        unique_id = uuid.uuid4().hex[:8]
        filename = f"part-00000-backfill-{d}-{run_timestamp}-{unique_id}.snappy.parquet"

        # Build GCS path
        # GCSFileSystem expects path without gs://
        partition_path = f"{bucket_name}/prod/knowledge/v1/date={d}/{filename}"

        print(f"Writing {len(partition_df)} records to {partition_path} ...")

        # Write to GCS
        with fs.open(partition_path, "wb") as f:
            pq.write_table(partition_table, f, compression="snappy")

    print("Aggregation complete!")


if __name__ == "__main__":
    run_aggregation()
