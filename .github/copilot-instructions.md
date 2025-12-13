# Copilot Instructions: MarketFlow Temporal Orchestration

## Architecture Overview

**MarketFlow** is a Temporal-based workflow orchestrator that pulls market data (metadata, fundamentals, intraday) from the Marketio API and writes hierarchical JSON artifacts to Google Cloud Storage (GCS).

### Key Components

- **`workflows.py`**: Defines `MarketDataWorkflow` using Temporal's async Python SDK. The workflow orchestrates parallel ticker processing with three data pipelines (metadata once per run, fundamentals raw→stage→prod, intraday raw→prod).
- **`activities.py`**: Implements actual work units (activities) that call the Marketio API endpoints and save results to GCS/local temp. Activities are named constants in `workflows.py` to bypass Temporal's sandbox restrictions on imports.
- **`client.py`**: CLI entry point to start workflows. Takes tickers, date range, and mode flags (fundamentals_mode, intraday_mode, intraday_frequency).
- **`worker.py`**: Registers the workflow and activities with a Temporal worker pool listening on the task queue.
- **`config.py`**: Centralized settings loader using environment variables; all runtime config flows through the `Settings` dataclass.
- **`storage_utils.py`**: Utilities for building hierarchical GCS paths, formatting dates (YYYYMMDD), writing/uploading JSON artifacts, and embedding metadata.

### Data Flow

```
Client (client.py) → Temporal Server ← Worker (worker.py)
                          ↓
                      Workflow (workflows.py)
                          ↓
                  [Health Check + Metadata]
                          ↓
            [Per-Ticker: Fundamentals + Intraday]
                    ↓         ↓
              Marketio API   GCS Upload
                    ↓         ↓
              Artifacts + Metadata
```

## Critical Patterns

### 1. Temporal Workflow Sandbox Isolation
Activities import external libraries (httpx, google-cloud-storage); workflows do not. Activity names are referenced as string constants in `workflows.py` to avoid import restrictions. See `workflows.py:6-16` for activity name definitions.

### 2. Hierarchical GCS Storage with Metadata
Paths follow: `{prefix}/{layer}/{dataset}/{ticker}/{freq}/{start}_{end}.json`
- **Layers**: source (raw API output) → stage (processed) → prod (final)
- **Datasets**: fundamentals, intraday, models
- **Metadata**: All uploads embed GCS object metadata including `ticker`, `layer`, `dataset`, `instrument`, `run_id`, `cik`, `company_id` (see `_metadata_base()` in activities.py:41-57).

### 3. Multi-Stage Fundamentals Pipeline
Fundamentals flow through three activities (`fetch_fundamentals_raw`, `fetch_fundamentals_stage`, `fetch_fundamentals_prod`), each calling a different Marketio API endpoint. The prod stage accepts pre-processed data from stage and calls `/api/v2/companies/fundamentals/production`. See `activities.py:160-191` for the pattern.

### 4. Artifact Pass-Through & Lazy Loading
Activities return artifact summaries (ticker, dates, paths, URIs, record_count) rather than full JSON data to minimize memory. When an activity needs original data (e.g., fundamentals_prod), it reloads from local temp if not in-memory. See `activities.py:183-188` for lazy loading pattern.

### 5. Retry Policies Tuned for HTTP
- **SHORT_RETRY** (3 attempts, 5s→30s backoff): Health checks, metadata
- **LONG_RETRY** (3 attempts, 10s→120s backoff): Data pulls (fundamentals, intraday)
See `workflows.py:18-28`.

### 6. Parallel Ticker Processing
The workflow uses `asyncio.gather()` to process multiple tickers concurrently within a single workflow execution. See `workflows.py:103-115`.

## Key Files & Patterns

| File | Key Pattern |
|------|-------------|
| [workflows.py](workflows.py#L38-L115) | `async def run()` orchestrates activities with retry policies; `process_ticker()` parallelizes work. |
| [activities.py](activities.py#L56-L93) | Health/metadata activities; `_save_artifacts()` writes JSON and uploads with metadata. |
| [activities.py](activities.py#L160-L191) | `fetch_fundamentals_prod()` reloads staged data if needed before calling production endpoint. |
| [storage_utils.py](storage_utils.py#L26-L50) | `build_object_path()` normalizes layers, datasets, tickers, dates (YYYYMMDD), and prefixes. |
| [client.py](client.py#L41-L68) | CLI validates modes (raw/stage/prod for fundamentals; raw/prod/none for intraday). |

## Development Workflow

### Setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# Start Temporal server: temporal server start-dev (in separate terminal)
# Start Marketio API: uvicorn app.main:app --reload (in separate terminal)
```

### Running Workflows
```bash
# Full run (all stages)
python client.py --tickers AA,NUE --start-date 2025-11-01 --end-date 2025-12-31 \
  --fundamentals-mode prod --intraday-mode prod --intraday-frequency daily

# Fundamentals only
python client.py --tickers AA --start-date 2025-11-01 --end-date 2025-12-31 \
  --fundamentals-mode prod --intraday-mode none
```

### Testing & Debugging
- Worker logs activity heartbeats (record counts, ticker names) for monitoring.
- GCS upload can be disabled via `UPLOAD_ENABLED=false` to test locally.
- Temp directory (`TEMP_DIR=tmp`) persists artifacts for inspection.
- Artifact summaries (returned by activities) include `local_path` for quick debugging.

## Important Implementation Notes

- **Environment Variables** flow through `config.Settings`; always use `SETTINGS.<attr>` rather than direct `os.getenv()`.
- **Date Normalization**: Use `format_date()` to convert input dates to YYYYMMDD; ensure consistency in GCS paths.
- **Activity Heartbeats**: Call `activity.heartbeat()` periodically to signal liveness and provide monitoring context.
- **Error Propagation**: Activities raise exceptions (e.g., `ValueError` for missing data); Temporal retries per policy before failing the workflow.
- **GCS Metadata Embedding**: All uploads include a metadata dict via `UPLOADER.upload_file(path, object_path, metadata=dict)`. This enables downstream filtering/querying.
