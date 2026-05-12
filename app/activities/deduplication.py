from google.cloud import bigquery
from app.core.config import settings
from temporalio import activity


@activity.defn(name="check_document_exists_in_bq")
async def check_document_exists_in_bq(document_id: str) -> bool:
    """
    Checks if a document with the given document_id already exists in BigQuery.
    Returns True if it exists, False otherwise.
    """
    client = bigquery.Client(project=settings.PROJECT_ID)
    table_id = f"{settings.PROJECT_ID}.{settings.BQ_DATASET}.{settings.BQ_TABLE}"

    query = f"""
    SELECT 1
    FROM `{table_id}`
    WHERE document_id = @document_id
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("document_id", "STRING", document_id)
        ]
    )
    
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    
    return results.total_rows > 0
