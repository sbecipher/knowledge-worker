import pytest
from temporalio.exceptions import ApplicationError

from models import MarketDataRequest
from workflows import _reject_legacy_payload_fields, _validate_request


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


def test_validate_request_allows_missing_universe_key_for_explicit_ticker_non_metadata_non_edgar_run() -> None:
    request = MarketDataRequest(
        universe_key=None,
        tickers=["AA"],
        start_date="2024-01-01",
        end_date="2024-01-31",
        fundamentals_mode="raw",
        market_mode="none",
    )
    _validate_request(request)


def test_validate_request_requires_universe_key_for_full_universe_run() -> None:
    request = MarketDataRequest(
        universe_key=None,
        tickers=[],
        start_date="2024-01-01",
        end_date="2024-01-31",
        fundamentals_mode="raw",
        market_mode="none",
    )
    with pytest.raises(
        ApplicationError,
        match="universe_key is required for metadata, EDGAR, or full-universe runs",
    ) as exc_info:
        _validate_request(request)
    assert exc_info.value.non_retryable is True


def test_validate_request_requires_universe_key_for_edgar_run() -> None:
    request = MarketDataRequest(
        universe_key=None,
        tickers=["AA"],
        start_date="2024-01-01",
        end_date="2024-01-31",
        edgar_source=True,
    )
    with pytest.raises(
        ApplicationError,
        match="universe_key is required for metadata, EDGAR, or full-universe runs",
    ) as exc_info:
        _validate_request(request)
    assert exc_info.value.non_retryable is True


def test_validate_request_requires_universe_key_for_metadata_persistence() -> None:
    request = MarketDataRequest(
        universe_key=None,
        tickers=["AA"],
        start_date="2024-01-01",
        end_date="2024-01-31",
        metadata_mode="source",
        fundamentals_mode="none",
        market_mode="none",
    )
    with pytest.raises(
        ApplicationError,
        match="universe_key is required for metadata, EDGAR, or full-universe runs",
    ) as exc_info:
        _validate_request(request)
    assert exc_info.value.non_retryable is True


def test_market_data_request_defaults_day_period() -> None:
    request = MarketDataRequest.from_payload(
        {
            "universe_key": "mmh5r1",
            "tickers": ["AA"],
            "as_of_date": "2024-01-31",
            "period": "day",
            "fundamentals_mode": "none",
        }
    )
    assert request.period == "day"


def test_market_data_request_normalizes_payload_for_openapi_contract() -> None:
    request = MarketDataRequest.from_payload(
        {
            "universe_key": " MMH5R1 ",
            "tickers": [" aa ", "", "nue"],
            "metadata_only": True,
            "fundamentals_mode": None,
            "market_mode": None,
            "max_concurrent_tickers": 0,
        }
    )

    assert request.universe_key == "MMH5R1"
    assert request.tickers == ["AA", "NUE"]
    assert request.metadata_mode == "source"
    assert request.fundamentals_mode == "prod"
    assert request.market_mode == "prod"
    assert request.max_concurrent_tickers == 1


def test_market_data_request_rejects_non_openapi_payload_shape() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        MarketDataRequest.from_payload({"tickers": ["AA"], "unknown": True})

    with pytest.raises(ValueError, match="tickers must be an array of strings"):
        MarketDataRequest.from_payload({"tickers": "AA"})


def test_market_data_request_schema_is_openapi_ready() -> None:
    schema = MarketDataRequest.model_json_schema()

    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert schema["properties"]["tickers"]["type"] == "array"
    assert schema["properties"]["max_concurrent_tickers"]["minimum"] == 1
    assert "example" in schema


def test_validate_request_rejects_unknown_period() -> None:
    request = MarketDataRequest(
        universe_key="mmh5r1",
        tickers=["AA"],
        fundamentals_mode="none",
        as_of_date="2024-01-31",
        period="year",
    )
    with pytest.raises(ApplicationError, match="Unsupported period") as exc_info:
        _validate_request(request)
    assert exc_info.value.non_retryable is True


def test_validate_request_requires_as_of_date_for_market_runs() -> None:
    request = MarketDataRequest(
        universe_key="mmh5r1",
        tickers=["AA"],
        fundamentals_mode="none",
        market_mode="raw",
        as_of_date=None,
    )
    with pytest.raises(ApplicationError, match="as_of_date is required when market_mode is not none") as exc_info:
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


def test_reject_legacy_payload_fields_rejects_intraday_fields() -> None:
    with pytest.raises(ApplicationError, match="Legacy fields are no longer supported") as exc_info:
        _reject_legacy_payload_fields({"tickers": ["AA"], "intraday_mode": "raw"})
    assert exc_info.value.non_retryable is True
