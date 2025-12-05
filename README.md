# Marketio Temporal Pipeline

Temporal Python worker that orchestrates Marketio API pulls (metadata, fundamentals, intraday) and writes artifacts to GCS in a hierarchical layout per instrument.

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
export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_TASK_QUEUE=market-data-task-queue
```

## GCS layout (hierarchical)
- Metadata: `prod/{instrument}/models/companies_{model_version}.json`
- Fundamentals:  
  - Raw: `source/{instrument}/fundamentals/{TICKER}/{start}_{end}.json`  
  - Stage: `stage/{instrument}/fundamentals/{TICKER}/{start}_{end}.json`  
  - Prod: `prod/{instrument}/fundamentals/{TICKER}/{start}_{end}.json`  
- Intraday:  
  - Raw: `source/{instrument}/intraday/{TICKER}/{freq}/{start}_{end}.json`  
  - Prod: `prod/{instrument}/intraday/{TICKER}/{freq}/{start}_{end}.json`

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
  --intraday-mode prod
```

Modes:
- Fundamentals: `raw` (source only), `stage` (processed), `prod` (stage → prod)
- Intraday: `raw` (source), `prod` (raw → prod)

## What the workflow does
- Health check `/health`
- Metadata → write/upload companies file
- Per ticker:
  - Fundamentals path: raw → stage → prod (from staged data)
  - Intraday path: raw → prod
- Uploads JSON artifacts with metadata in GCS object metadata (instrument, layer, ticker, window, run_id).
