from __future__ import annotations

import pytest
from temporalio.exceptions import ApplicationError

from app.workflows import company_workflow


def test_requested_company_sources_defaults_to_articles_and_edgar() -> None:
    assert company_workflow._requested_company_sources(None) == (
        "articles",
        "edgar",
    )


def test_requested_company_sources_accepts_metadata() -> None:
    assert company_workflow._requested_company_sources("metadata") == ("metadata",)


def test_requested_company_sources_rejects_invalid_source() -> None:
    with pytest.raises(ApplicationError, match="Unsupported company source"):
        company_workflow._requested_company_sources("newsletter")


def test_discovery_failure_without_results_raises() -> None:
    with pytest.raises(ApplicationError, match="Document discovery failed for AA"):
        company_workflow._raise_if_discovery_failed_without_results(
            company_ticker="AA",
            requested_sources=("articles",),
            source_errors={"articles": "boom"},
            discovered_docs_count=0,
        )


def test_discovery_failure_with_results_does_not_raise() -> None:
    company_workflow._raise_if_discovery_failed_without_results(
        company_ticker="AA",
        requested_sources=("articles", "edgar"),
        source_errors={"edgar": "boom"},
        discovered_docs_count=2,
    )
