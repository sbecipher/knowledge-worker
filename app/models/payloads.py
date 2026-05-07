from pydantic import BaseModel
from typing import Optional


class KnowledgeDocument(BaseModel):
    title: str
    company_name: str
    company_id: str
    company_ticker: str
    year: int
    url: str
    type: str  # e.g., 'html', 'pdf'
    filepath: str
    downloaded: bool = False
    gcs_uri: Optional[str] = None
