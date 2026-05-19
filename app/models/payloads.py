from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator


class CompanyPayload(BaseModel):
    company_ticker: str = Field(description="The stock ticker symbol")
    company_name: str = Field(description="The full name of the company")
    company_id: str | None = Field(
        default=None, description="The unique internal identifier"
    )
    base_url: str = Field(description="The base URL of the company's webpage")

    @model_validator(mode="after")
    def normalize_company(self) -> "CompanyPayload":
        self.company_ticker = self.company_ticker.strip().upper()
        self.company_name = self.company_name.strip()
        self.base_url = self.base_url.strip()
        if not self.company_id or not self.company_id.strip():
            self.company_id = f"com_{self.company_ticker.lower()}"
        else:
            self.company_id = self.company_id.strip()
        return self


class KnowledgeDocument(BaseModel):
    title: str = Field(description="Title of the document")
    company_name: str = Field(description="Name of the company")
    company_id: str = Field(description="Internal company ID")
    company_ticker: str = Field(description="Stock ticker of the company")
    base_url: str = Field(description="The base URL of the company's webpage")
    year: int = Field(description="Year of the document")
    date: str | None = Field(
        default=None, description="Publication date of the document"
    )
    url: str = Field(description="Source URL of the document")
    type: str = Field(description="Document type, e.g., 'html', 'pdf'")
    filepath: str = Field(description="Local or remote filepath for the document")
    downloaded: bool = Field(
        default=False, description="Flag indicating if the document has been downloaded"
    )
    gcs_uri: str | None = Field(
        default=None, description="GCS URI where the document is stored"
    )
    source_kind: str = Field(
        default="articles",
        description="Logical source family, e.g. articles or edgar",
    )
    filing_type: str | None = Field(
        default=None, description="SEC filing form type, e.g. 10-K or 10-Q"
    )
    filing_date: str | None = Field(
        default=None, description="SEC filing date when available"
    )
    report_date: str | None = Field(
        default=None, description="SEC report date when available"
    )
    accession_number: str | None = Field(
        default=None, description="SEC accession number when available"
    )
    primary_document: str | None = Field(
        default=None, description="SEC primary document filename when available"
    )
    cik: str | None = Field(default=None, description="SEC CIK when available")

    @model_validator(mode="after")
    def normalize_document(self) -> "KnowledgeDocument":
        self.company_ticker = self.company_ticker.strip().upper()
        self.company_id = self.company_id.strip()
        self.type = self.type.strip().lower()
        self.source_kind = (self.source_kind or "articles").strip().lower()
        for field_name in (
            "filing_type",
            "filing_date",
            "report_date",
            "accession_number",
            "primary_document",
            "cik",
        ):
            value = getattr(self, field_name)
            if isinstance(value, str):
                stripped = value.strip()
                setattr(self, field_name, stripped or None)
        if self.filing_type:
            self.filing_type = self.filing_type.upper()
        if self.cik and self.cik.isdigit():
            self.cik = self.cik.zfill(10)
        return self


class CompanyMetadataArtifact(BaseModel):
    metadata_id: str = Field(description="Stable identifier for the metadata artifact")
    company_ticker: str = Field(description="The stock ticker symbol")
    company_name: str = Field(description="The full name of the company")
    company_id: str = Field(description="The unique internal identifier")
    base_url: str = Field(description="The base URL of the company's webpage")
    year: int = Field(description="Requested year for the ingestion run")
    provider: str = Field(description="Metadata provider, e.g. lseg")
    matched_on: str = Field(description="How the company record was matched")
    source_snapshot_uri: str = Field(description="Snapshot artifact used upstream")
    source_snapshot_date: str | None = Field(
        default=None, description="Date partition of the source snapshot"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Structured provider metadata fields"
    )
    source_record: dict[str, Any] = Field(
        default_factory=dict, description="Raw matched provider record"
    )
    stage_gcs_uri: str = Field(
        description="Stage parquet artifact written by the worker"
    )
