# Code Review Notes

This document captures a deep review of the Temporal worker focusing on correctness, security, performance, scalability, and maintainability.

## Key findings

1. **Critical**: Untrusted ticker/identifier values can flow into object paths without sanitization, enabling object-key injection and path confusion in GCS layouts.
2. **High**: The workflow fan-out uses `asyncio.gather` without per-ticker isolation, so one ticker failure fails the entire run.
3. **High**: Startup hard-fails when Secret Manager retrieval fails, even for runs that do not require Intrinio access.
4. **Medium**: Processing activities accept partially-formed artifacts and may send `tickers=[None]` to APIs.
5. **Medium**: Local artifact files are never cleaned up, which can exhaust disk over time.
6. **Low**: `build_object_path(..., suffix=...)` defines an unused argument, increasing confusion.

## Positive design choices

- Authentication token caching is protected by an async lock and includes retry invalidation flow.
- Activity boundaries are clear and map well to raw/stage/prod transitions.
- GCS metadata attachment supports lineage tracking (`layer`, `dataset`, `run_id`, `instrument`, `model_version`).
