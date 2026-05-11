from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_ID: str = "sbecipherio"
    REGION: str = "us-central1"
    SOURCE_BUCKET: str = "sbecipher-intelligence"
    PROD_BUCKET: str = "sbecipher-intelligence"
    BQ_DATASET: str = "knowledge"
    BQ_TABLE: str = "documents"
    KNOWLEDGEIO_API_URL: str = "http://knowledgeio-api:8000"
    TEMPORAL_ADDRESS: str = "localhost:7233"
    TEMPORAL_TASK_QUEUE: str = "knowledge-ingestion-queue"
    LOG_LEVEL: str = "INFO"
    ACTIVITY_EXECUTOR_THREADS: int = 10
    MAX_CONCURRENT_ACTIVITIES: int | None = None
    MAX_CONCURRENT_WORKFLOW_TASKS: int | None = None
    MAX_CACHED_WORKFLOWS: int | None = None

    model_config = {"env_file": ".env"}


settings = Settings()
