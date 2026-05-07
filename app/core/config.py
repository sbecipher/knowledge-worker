from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_ID: str = "sbecipherio"
    REGION: str = "us-central1"
    SOURCE_BUCKET: str = "sbecipher-knowledge-source"
    PROD_BUCKET: str = "sbecipher-knowledge-prod"
    BQ_DATASET: str = "knowledge"
    BQ_TABLE: str = "documents"
    KNOWLEDGEIO_API_URL: str = "http://knowledgeio-api:8000"
    TEMPORAL_ADDRESS: str = "172.0.0.4:7233"

    model_config = {"env_file": ".env"}


settings = Settings()
