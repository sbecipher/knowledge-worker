from __future__ import annotations

import httpx
import pytest

from app.activities import company_metadata
from app.models.payloads import CompanyPayload


def test_fetch_company_metadata_returns_stage_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = {}

    class FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "provider": "lseg",
                "company_ticker": "AA",
                "company_name": "Alcoa",
                "company_id": "com_aa",
                "base_url": "https://aa.example.com",
                "matched_on": "company_ticker+base_url",
                "source_snapshot_uri": "gs://bucket/source/instruments/metadata/date=2026-05-01/companies.json",
                "source_snapshot_date": "2026-05-01",
                "metadata": {"sector": "Materials"},
                "source_record": {"sector": "Materials"},
            }

    class FakeHttpClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url: str, json: dict, headers: dict):
            captured["url"] = url
            captured["json"] = json
            captured["headers"] = headers
            return FakeResponse()

    monkeypatch.setattr(
        company_metadata.settings,
        "KNOWLEDGEIO_API_URL",
        "https://knowledgeio.example",
    )
    monkeypatch.setattr(
        company_metadata,
        "knowledge_api_headers",
        lambda: {"Authorization": "Bearer token"},
    )
    monkeypatch.setattr(company_metadata.httpx, "Client", FakeHttpClient)
    monkeypatch.setattr(
        company_metadata,
        "_stage_company_metadata_artifact",
        lambda artifact: (
            "gs://bucket/stage/knowledge/company_metadata/provider=lseg/"
            "ticker=AA/year=2026/com_aa_2026_lseg_abcdef1234567890.parquet"
        ),
    )

    result = company_metadata.fetch_company_metadata(
        CompanyPayload(
            company_ticker="aa",
            company_name="Alcoa",
            base_url="https://aa.example.com",
        ),
        year=2026,
    )

    assert result is not None
    assert result.provider == "lseg"
    assert result.company_id == "com_aa"
    assert result.stage_gcs_uri.startswith(
        "gs://bucket/stage/knowledge/company_metadata/"
    )
    assert captured["url"] == "https://knowledgeio.example/api/v1/metadata/company"
    assert captured["json"]["company_id"] == "com_aa"
    assert captured["headers"] == {"Authorization": "Bearer token"}


def test_fetch_company_metadata_returns_none_on_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = httpx.Request(
        "POST", "https://knowledgeio.example/api/v1/metadata/company"
    )
    response = httpx.Response(404, request=request)

    class FakeHttpClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url: str, json: dict, headers: dict):
            raise httpx.HTTPStatusError("missing", request=request, response=response)

    monkeypatch.setattr(company_metadata.httpx, "Client", FakeHttpClient)
    monkeypatch.setattr(
        company_metadata,
        "knowledge_api_headers",
        lambda: {},
    )
    monkeypatch.setattr(
        company_metadata.settings,
        "KNOWLEDGEIO_API_URL",
        "https://knowledgeio.example",
    )

    result = company_metadata.fetch_company_metadata(
        CompanyPayload(
            company_ticker="aa",
            company_name="Alcoa",
            base_url="https://aa.example.com",
        ),
        year=2026,
    )

    assert result is None


def test_fetch_company_metadata_raises_retryable_error_on_500(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = httpx.Request(
        "POST", "https://knowledgeio.example/api/v1/metadata/company"
    )
    response = httpx.Response(500, request=request)

    class FakeHttpClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url: str, json: dict, headers: dict):
            raise httpx.HTTPStatusError(
                "server error", request=request, response=response
            )

    monkeypatch.setattr(company_metadata.httpx, "Client", FakeHttpClient)
    monkeypatch.setattr(
        company_metadata.settings,
        "KNOWLEDGEIO_API_URL",
        "https://knowledgeio.example",
    )
    monkeypatch.setattr(company_metadata, "knowledge_api_headers", lambda: {})

    with pytest.raises(RuntimeError, match="status 500"):
        company_metadata.fetch_company_metadata(
            CompanyPayload(
                company_ticker="aa",
                company_name="Alcoa",
                base_url="https://aa.example.com",
            ),
            year=2026,
        )


def test_fetch_company_metadata_rejects_invalid_provider() -> None:
    with pytest.raises(Exception, match="Unsupported metadata provider"):
        company_metadata.fetch_company_metadata(
            CompanyPayload(
                company_ticker="aa",
                company_name="Alcoa",
                base_url="https://aa.example.com",
            ),
            year=2026,
            provider="unsupported",
        )
