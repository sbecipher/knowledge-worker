from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


DEFAULT_PERIOD = "day"
DEFAULT_FUNDAMENTALS_MODE = "prod"
DEFAULT_MARKET_MODE = "prod"
DEFAULT_METADATA_MODE = "none"
DEFAULT_MAX_CONCURRENT_TICKERS = 5


def normalize_period(value: Any) -> str:
    text = str(value or DEFAULT_PERIOD).strip().lower()
    return text or DEFAULT_PERIOD


class PayloadModel(BaseModel):
    """Base for Temporal payload models that also emit OpenAPI-ready JSON Schema."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
    )

    def to_payload(self) -> Dict[str, Any]:
        return self.model_dump(mode="json")


class MarketDataRequest(PayloadModel):
    universe_key: Optional[str] = Field(default=None, description="Universe key used for active-universe lookups.")
    tickers: List[str] = Field(
        default_factory=list,
        description="Ticker symbols to process. Empty means load the active universe.",
    )
    start_date: Optional[str] = Field(default=None, description="Inclusive fundamentals start date in YYYY-MM-DD.")
    end_date: Optional[str] = Field(default=None, description="Inclusive fundamentals end date in YYYY-MM-DD.")
    as_of_date: Optional[str] = Field(default=None, description="Market data date in YYYY-MM-DD.")
    period: str = Field(default=DEFAULT_PERIOD, description="Requested market data period.")
    fundamentals_mode: str = Field(default=DEFAULT_FUNDAMENTALS_MODE, description="Fundamentals processing mode.")
    market_mode: str = Field(default=DEFAULT_MARKET_MODE, description="Market data processing mode.")
    metadata_mode: str = Field(default=DEFAULT_METADATA_MODE, description="Company metadata persistence mode.")
    edgar_source: bool = Field(default=False, description="Fetch EDGAR source artifacts alongside other work.")
    metadata_only: bool = Field(default=False, description="Persist metadata without prices or fundamentals.")
    edgar_only: bool = Field(default=False, description="Fetch only EDGAR source artifacts.")
    request_id: Optional[str] = Field(default=None, description="Optional caller-provided request identifier.")
    max_concurrent_tickers: int = Field(
        default=DEFAULT_MAX_CONCURRENT_TICKERS,
        ge=1,
        description="Maximum number of tickers processed concurrently by the workflow.",
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra={
            "example": {
                "universe_key": "mmh5r1",
                "tickers": ["AA", "NUE"],
                "start_date": "2026-01-01",
                "end_date": "2026-03-31",
                "as_of_date": "2026-04-02",
                "period": "day",
                "fundamentals_mode": "prod",
                "market_mode": "prod",
                "metadata_mode": "none",
            }
        },
        populate_by_name=True,
    )

    @field_validator("universe_key", "start_date", "end_date", "as_of_date", "request_id", mode="before")
    @classmethod
    def _normalize_optional_str(cls, value: Any) -> Optional[str]:
        return _optional_str(value)

    @field_validator("tickers", mode="before")
    @classmethod
    def _normalize_tickers(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if not isinstance(value, (list, tuple, set)):
            raise ValueError("tickers must be an array of strings")
        return [ticker for ticker in (str(item).strip().upper() for item in value) if ticker]

    @field_validator("period", mode="before")
    @classmethod
    def _normalize_period(cls, value: Any) -> str:
        return normalize_period(value)

    @field_validator("fundamentals_mode", mode="before")
    @classmethod
    def _normalize_fundamentals_mode(cls, value: Any) -> str:
        return str(value or DEFAULT_FUNDAMENTALS_MODE).strip().lower()

    @field_validator("market_mode", mode="before")
    @classmethod
    def _normalize_market_mode(cls, value: Any) -> str:
        return str(value or DEFAULT_MARKET_MODE).strip().lower()

    @field_validator("metadata_mode", mode="before")
    @classmethod
    def _normalize_metadata_mode(cls, value: Any) -> str:
        return str(value or DEFAULT_METADATA_MODE).strip().lower()

    @field_validator("max_concurrent_tickers", mode="before")
    @classmethod
    def _normalize_max_concurrent_tickers(cls, value: Any) -> int:
        if value in (None, ""):
            return DEFAULT_MAX_CONCURRENT_TICKERS
        return max(1, int(value))

    @model_validator(mode="before")
    @classmethod
    def _metadata_only_requests_source_metadata(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        metadata_only = data.get("metadata_only", False)
        if isinstance(metadata_only, str):
            metadata_only = metadata_only.strip().lower() in {"true", "1", "yes", "y", "on"}
        if metadata_only:
            return {**data, "metadata_mode": "source"}
        return data

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "MarketDataRequest":
        return cls.model_validate(payload)


class ExecutionMetadata(PayloadModel):
    request_id: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    workflow_run_id: str = Field(min_length=1)


class ArtifactRef(PayloadModel):
    uri: str = Field(min_length=1)
    object_path: str = Field(min_length=1)
    layer: Literal["source", "prod"]
    dataset: str = Field(min_length=1)
    universe_key: str = Field(min_length=1)
    request_id: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    workflow_run_id: str = Field(min_length=1)
    ticker: Optional[str] = None
    start_date: Optional[str] = None
    date: Optional[str] = None
    requested_period: Optional[str] = None
    bar_granularity: Optional[str] = None
    as_of_date: Optional[str] = None
    effective_start_date: Optional[str] = None
    effective_end_date: Optional[str] = None
    request_start_date: Optional[str] = None
    request_end_date: Optional[str] = None
    request_period: Optional[str] = None
    request_currency: Optional[str] = None
    request_scale: Optional[int] = None
    record_count: int = Field(default=0, ge=0)
    local_path: Optional[str] = None
    provider: Optional[str] = None
    source: Optional[str] = None
    ric: Optional[str] = None
    primary_ric: Optional[str] = None
    organization_id: Optional[str] = None
    cik_number: Optional[str] = None
    field_count: Optional[int] = None
    page_count: Optional[int] = None
    active_source_uri: Optional[str] = None
    active_source_object_path: Optional[str] = None
    source_uri: Optional[str] = None
    source_object_path: Optional[str] = None
    source_dataset: Optional[str] = None
    transform_name: Optional[str] = None
    transform_version: Optional[str] = None

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra={
            "example": {
                "uri": "gs://bucket/source/prices/granularity=day/date=2026-04-02/ticker=AA/wf-123.ndjson",
                "object_path": "source/prices/granularity=day/date=2026-04-02/ticker=AA/wf-123.ndjson",
                "layer": "source",
                "dataset": "prices",
                "universe_key": "mmh5r1",
                "request_id": "req-123",
                "workflow_id": "wf-123",
                "workflow_run_id": "run-123",
                "ticker": "AA",
                "date": "2026-04-02",
                "record_count": 25,
            }
        },
        populate_by_name=True,
    )


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
