from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional


DEFAULT_PERIOD = "day"
DEFAULT_FUNDAMENTALS_MODE = "prod"
DEFAULT_MARKET_MODE = "prod"
DEFAULT_METADATA_MODE = "none"
DEFAULT_MAX_CONCURRENT_TICKERS = 5


def normalize_period(value: Any) -> str:
    text = str(value or DEFAULT_PERIOD).strip().lower()
    return text or DEFAULT_PERIOD


@dataclass(frozen=True)
class MarketDataRequest:
    universe_key: Optional[str]
    tickers: List[str]
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    as_of_date: Optional[str] = None
    period: str = DEFAULT_PERIOD
    fundamentals_mode: str = DEFAULT_FUNDAMENTALS_MODE
    market_mode: str = DEFAULT_MARKET_MODE
    metadata_mode: str = DEFAULT_METADATA_MODE
    edgar_source: bool = False
    metadata_only: bool = False
    edgar_only: bool = False
    request_id: Optional[str] = None
    max_concurrent_tickers: int = DEFAULT_MAX_CONCURRENT_TICKERS

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "MarketDataRequest":
        metadata_only = bool(payload.get("metadata_only", False))
        metadata_mode = str(payload.get("metadata_mode") or DEFAULT_METADATA_MODE).strip().lower()
        if metadata_only:
            metadata_mode = "source"
        return cls(
            universe_key=_optional_str(payload.get("universe_key")),
            tickers=[str(ticker).strip().upper() for ticker in payload.get("tickers", []) if str(ticker).strip()],
            start_date=_optional_str(payload.get("start_date")),
            end_date=_optional_str(payload.get("end_date")),
            as_of_date=_optional_str(payload.get("as_of_date")),
            period=normalize_period(payload.get("period")),
            fundamentals_mode=str(payload.get("fundamentals_mode") or DEFAULT_FUNDAMENTALS_MODE).strip().lower(),
            market_mode=str(payload.get("market_mode") or DEFAULT_MARKET_MODE).strip().lower(),
            metadata_mode=metadata_mode,
            edgar_source=bool(payload.get("edgar_source", False)),
            metadata_only=metadata_only,
            edgar_only=bool(payload.get("edgar_only", False)),
            request_id=_optional_str(payload.get("request_id")),
            max_concurrent_tickers=max(1, int(payload.get("max_concurrent_tickers") or DEFAULT_MAX_CONCURRENT_TICKERS)),
        )

    def to_payload(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionMetadata:
    request_id: str
    workflow_id: str
    workflow_run_id: str

    def to_payload(self) -> Dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class ArtifactRef:
    uri: str
    object_path: str
    layer: str
    dataset: str
    universe_key: str
    request_id: str
    workflow_id: str
    workflow_run_id: str
    ticker: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
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
    record_count: int = 0
    local_path: Optional[str] = None
    provider: Optional[str] = None
    source: Optional[str] = None
    ric: Optional[str] = None
    primary_ric: Optional[str] = None
    organization_id: Optional[str] = None
    cik_number: Optional[str] = None
    field_count: Optional[int] = None
    page_count: Optional[int] = None
    source_uri: Optional[str] = None
    source_object_path: Optional[str] = None
    source_dataset: Optional[str] = None
    transform_name: Optional[str] = None
    transform_version: Optional[str] = None

    def to_payload(self) -> Dict[str, Any]:
        return asdict(self)


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
