from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional


DEFAULT_INTRADAY_FREQUENCY = "daily"
DEFAULT_FUNDAMENTALS_MODE = "prod"
DEFAULT_INTRADAY_MODE = "prod"
DEFAULT_MAX_CONCURRENT_TICKERS = 5


def normalize_intraday_frequency(value: Any) -> str:
    text = str(value or DEFAULT_INTRADAY_FREQUENCY).strip().lower()
    if text == "eod":
        return DEFAULT_INTRADAY_FREQUENCY
    return text or DEFAULT_INTRADAY_FREQUENCY


@dataclass(frozen=True)
class MarketDataRequest:
    tickers: List[str]
    start_date: str
    end_date: str
    intraday_frequency: str = DEFAULT_INTRADAY_FREQUENCY
    fundamentals_mode: str = DEFAULT_FUNDAMENTALS_MODE
    intraday_mode: str = DEFAULT_INTRADAY_MODE
    edgar_source: bool = False
    metadata_only: bool = False
    edgar_only: bool = False
    instrument: Optional[str] = None
    model_version: Optional[str] = None
    request_id: Optional[str] = None
    max_concurrent_tickers: int = DEFAULT_MAX_CONCURRENT_TICKERS

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "MarketDataRequest":
        return cls(
            tickers=[str(ticker).strip().upper() for ticker in payload.get("tickers", []) if str(ticker).strip()],
            start_date=str(payload["start_date"]),
            end_date=str(payload["end_date"]),
            intraday_frequency=normalize_intraday_frequency(payload.get("intraday_frequency")),
            fundamentals_mode=str(payload.get("fundamentals_mode") or DEFAULT_FUNDAMENTALS_MODE).strip().lower(),
            intraday_mode=str(payload.get("intraday_mode") or DEFAULT_INTRADAY_MODE).strip().lower(),
            edgar_source=bool(payload.get("edgar_source", False)),
            metadata_only=bool(payload.get("metadata_only", False)),
            edgar_only=bool(payload.get("edgar_only", False)),
            instrument=_optional_str(payload.get("instrument")),
            model_version=_optional_str(payload.get("model_version")),
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
    instrument: str
    model_version: str
    request_id: str
    workflow_id: str
    workflow_run_id: str
    ticker: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    frequency: Optional[str] = None
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

    def to_payload(self) -> Dict[str, Any]:
        return asdict(self)


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
