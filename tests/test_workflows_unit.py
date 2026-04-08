import pytest
from temporalio.exceptions import ApplicationError

from models import MarketDataRequest
from workflows import _validate_request


def test_validate_request_rejects_conflicting_modes() -> None:
    request = MarketDataRequest(
        universe_key="mmh5r1",
        tickers=["AA"],
        start_date="2024-01-01",
        end_date="2024-01-31",
        metadata_only=True,
        edgar_only=True,
    )
    with pytest.raises(ApplicationError, match="metadata_only and edgar_only cannot both be true") as exc_info:
        _validate_request(request)
    assert exc_info.value.non_retryable is True


def test_market_data_request_normalizes_eod_frequency() -> None:
    request = MarketDataRequest.from_payload(
        {
            "universe_key": "mmh5r1",
            "tickers": ["AA"],
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "intraday_frequency": "eod",
        }
    )
    assert request.intraday_frequency == "daily"


def test_validate_request_rejects_non_daily_intraday_frequency() -> None:
    request = MarketDataRequest(
        universe_key="mmh5r1",
        tickers=["AA"],
        start_date="2024-01-01",
        end_date="2024-01-31",
        intraday_frequency="weekly",
    )
    with pytest.raises(ApplicationError, match="intraday_frequency must be one of: daily, eod") as exc_info:
        _validate_request(request)
    assert exc_info.value.non_retryable is True


def test_validate_request_rejects_unknown_metadata_mode() -> None:
    request = MarketDataRequest(
        universe_key="mmh5r1",
        tickers=["AA"],
        start_date="2024-01-01",
        end_date="2024-01-31",
        metadata_mode="full",
    )
    with pytest.raises(ApplicationError, match="Unsupported metadata_mode") as exc_info:
        _validate_request(request)
    assert exc_info.value.non_retryable is True


def test_validate_request_rejects_removed_fundamentals_stage_mode() -> None:
    request = MarketDataRequest(
        universe_key="mmh5r1",
        tickers=["AA"],
        start_date="2024-01-01",
        end_date="2024-01-31",
        fundamentals_mode="stage",
    )
    with pytest.raises(ApplicationError, match="fundamentals_mode='stage' is no longer supported") as exc_info:
        _validate_request(request)
    assert exc_info.value.non_retryable is True
