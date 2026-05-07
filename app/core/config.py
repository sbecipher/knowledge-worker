import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_ID: str = os.getenv("PROJECT_ID", "sbecipherio")
    REGION: str = os.getenv("REGION", "us-central1")
    SOURCE_BUCKET: str = os.getenv("SOURCE_BUCKET", "sbecipher-knowledge-source")
    PROD_BUCKET: str = os.getenv("PROD_BUCKET", "sbecipher-knowledge-prod")
    BQ_DATASET: str = os.getenv("BQ_DATASET", "knowledge")
    BQ_TABLE: str = os.getenv("BQ_TABLE", "documents")

    class Config:
        env_file = ".env"


settings = Settings()
