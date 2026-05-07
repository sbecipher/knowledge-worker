from pydantic import BaseModel, Field


class KnowledgeDocument(BaseModel):
    title: str = Field(description="Title of the document")
    company_name: str = Field(description="Name of the company")
    company_id: str = Field(description="Internal company ID")
    company_ticker: str = Field(description="Stock ticker of the company")
    year: int = Field(description="Year of the document")
    url: str = Field(description="Source URL of the document")
    type: str = Field(description="Document type, e.g., 'html', 'pdf'")
    filepath: str = Field(description="Local or remote filepath for the document")
    downloaded: bool = Field(
        default=False, description="Flag indicating if the document has been downloaded"
    )
    gcs_uri: str | None = Field(
        default=None, description="GCS URI where the document is stored"
    )
