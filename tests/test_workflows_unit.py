import pytest

from models import MarketDataRequest
from workflows import _validate_request


def test_validate_request_rejects_conflicting_modes() -> None:
    request = MarketDataRequest(
        tickers=["AA"],
        start_date="2024-01-01",
        end_date="2024-01-31",
        metadata_only=True,
        edgar_only=True,
    )
    with pytest.raises(ValueError, match="metadata_only and edgar_only cannot both be true"):
        _validate_request(request)
