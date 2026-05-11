from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_ID: str = "data-cipher"
    BQ_PROJECT_ID: str = "sbecipherio"
    REGION: str = "us-central1"
    SOURCE_BUCKET: str = "sbecipher-intelligence"
    PROD_BUCKET: str = "sbecipher-intelligence"
    BQ_DATASET: str = "knowledge"
    BQ_TABLE: str = "documents"
    KNOWLEDGEIO_API_URL: str = "https://knowledgeio-875978034496.us-central1.run.app"
    KNOWLEDGEIO_API_AUDIENCE: str | None = (
        "https://knowledgeio-875978034496.us-central1.run.app"
    )
    TEMPORAL_ADDRESS: str = "localhost:7233"
    TEMPORAL_TASK_QUEUE: str = "knowledge-ingestion-queue"
    LOG_LEVEL: str = "INFO"
    ACTIVITY_EXECUTOR_THREADS: int = 10
    MAX_CONCURRENT_ACTIVITIES: int | None = None
    MAX_CONCURRENT_WORKFLOW_TASKS: int | None = None
    MAX_CACHED_WORKFLOWS: int | None = None
    GEMINI_MODEL: str = "gemini-3-flash-preview"
    GEMINI_PDF_MAX_BYTES: int = 52428800
    GEMINI_PDF_CHUNK_TARGET_BYTES: int = 45000000
    GEMINI_CHUNK_BUCKET: str | None = None
    GEMINI_CHUNK_PREFIX: str = "stage/knowledge/gemini_chunks"

    model_config = {"env_file": ".env"}


settings = Settings()
