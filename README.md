# Marketio Temporal Pipeline

Temporal Python worker that orchestrates Marketio API pulls (metadata, EDGAR submissions, fundamentals, intraday) and writes artifacts to GCS in a hierarchical layout per instrument.

## Prerequisites

- Python 3.9+
- Temporal server (e.g., `temporal server start-dev`)
- Marketio API running (e.g., `uvicorn app.main:app --reload`)
- GCS bucket + service account key for uploads

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Config (env)

```bash
export MARKETIO_API_URL=http://localhost:8000
export GCS_BUCKET=sbecipher-intelligence
export GCS_PREFIX=dev            # optional
export INSTRUMENT=ssga-xme
export MODEL_VERSION=1125v
export TEMP_DIR=tmp
export UPLOAD_ENABLED=true
export GCS_SERVICE_ACCOUNT_KEY_PATH=/path/to/key.json
export INTRINIO_API_KEY=your_key                # required for metadata/fundamentals
export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_TASK_QUEUE=market-data-task-queue
```

## GCS layout (hierarchical)

- Metadata: `prod/models/{model_version}.json`
- EDGAR submissions: `source/edgar/{TICKER}/{TICKER}.json`
- Fundamentals:  
  - Raw: `source/fundamentals/{TICKER}/{start}_{end}.json`  
  - Stage: `stage/fundamentals/{TICKER}/{start}_{end}.json`  
  - Prod: `prod/fundamentals/{TICKER}/{start}_{end}.json`  
- Intraday:  
  - Raw: `source/intraday/{TICKER}/{freq}/{start}_{end}.json`  
  - Prod: `prod/intraday/{TICKER}/{freq}/{start}_{end}.json`

Dates use `YYYYMMDD`; tickers uppercase; freq lowercase. Stage is required for fundamentals prod.

## Run the worker

```bash
python worker.py
```

## Start a workflow

```bash
python client.py \
  --tickers AA,NUE \
  --start-date 2020-01-01 \
  --end-date 2020-12-31 \
  --intraday-frequency daily \
  --fundamentals-mode prod \
  --intraday-mode prod \
  --edgar-source             # optional: pull SEC submissions (source=True)
```

Modes:

- Fundamentals: `raw` (source only), `stage` (processed), `prod` (stage → prod), `none` (skip fundamentals)
- Intraday: `raw` (source), `prod` (raw → prod), `none` (skip intraday)
- EDGAR: toggled via `--edgar-source` to fetch SEC submissions (source=True) per ticker or `--edgar-only` to fetch just EDGAR
- Metadata-only: `--metadata-only` fetches just metadata and exits early

Fundamentals honor the workflow `--start-date/--end-date` window (with `filed_after` nudged forward only if the company’s first trade date is later).

### Common runs

- Intraday only (skip fundamentals):  
  `python client.py --tickers AA --start-date 2025-11-01 --end-date 2025-12-31 --fundamentals-mode none --intraday-mode prod --intraday-frequency daily`
- Metadata only:  
  `python client.py --tickers AA --start-date 2025-11-01 --end-date 2025-12-31 --fundamentals-mode none --intraday-mode none --metadata-only`
- EDGAR only (raw SEC submissions):  
  `python client.py --tickers AA --start-date 2025-11-01 --end-date 2025-12-31 --fundamentals-mode none --intraday-mode none --edgar-only`
- Metadata + EDGAR submissions (adds SEC submissions to the regular fundamentals run):  
  `python client.py --tickers AA --start-date 2024-01-01 --end-date 2024-12-31 --fundamentals-mode prod --intraday-mode none --edgar-source`
- Fundamentals + Intraday (full run):  
  `python client.py --tickers AA,NUE --start-date 2025-11-01 --end-date 2025-12-31 --fundamentals-mode prod --intraday-mode prod --intraday-frequency daily`
- Fundamentals only:  
  `python client.py --tickers AA --start-date 2025-11-01 --end-date 2025-12-31 --fundamentals-mode prod --intraday-mode none`

## What the workflow does (per ticker)

- Health check `/health`
- Metadata → write/upload companies file (always runs; EDGAR uses CIKs from metadata when present)
- EDGAR submissions → per ticker raw SEC submissions (source=True) uploaded under `source/edgar/` when `--edgar-source/--edgar-only`
- Fundamentals path: raw → stage → prod (from staged data) unless `--fundamentals-mode none` or `--edgar-only`
- Intraday path: raw → prod unless `--intraday-mode none` or `--edgar-only`
- Uploads JSON artifacts with metadata in GCS object metadata (instrument, layer, ticker, window, run_id).

## Activities

- `check_marketio_health`: Ensure the Marketio API is reachable.
- `fetch_companies_metadata`: Pull company metadata and upload the consolidated model file.
- `fetch_edgar_source`: Download raw SEC submissions (source=True) for tickers/CIKs and upload per ticker under `source/edgar/`.
- `fetch_fundamentals_raw` / `fetch_fundamentals_stage` / `fetch_fundamentals_prod`: Pull fundamentals through raw → stage → prod.
- `fetch_intraday_raw` / `fetch_intraday_prod`: Pull historical prices and flatten them for production.
