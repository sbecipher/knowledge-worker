# Marketio Temporal Pipeline

Temporal Python worker that orchestrates Marketio API pulls (metadata, EDGAR submissions, fundamentals, intraday) and writes artifacts to GCS in a hierarchical layout per instrument.

## Prerequisites

- Python 3.13+
- Temporal server (e.g., `temporal server start-dev`)
- Marketio API running (e.g., `uvicorn app.main:app --reload`)
- GCS bucket + workload identity on the Cloud Run service account for uploads

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
export INTRINIO_API_KEY=your_key                # required for metadata/fundamentals
export TEMPORAL_ADDRESS=temporal.sbecipher.io:7233
export TEMPORAL_TASK_QUEUE=market-data-task-queue
export LOG_LEVEL=INFO
export HEALTHCHECK_PORT=8080                    # optional; defaults to PORT when set
export MARKETIO_API_URL=https://marketio-875978034496.us-central1.run.app
```

## GCS layout (hierarchical)

- Metadata: `prod/models/{INSTRUMENT}/{model_version}.json`
- EDGAR submissions: `source/edgar/{TICKER}/{TICKER}.json`
- Fundamentals:  
  - Raw: `source/fundamentals/{TICKER}/{start}_{end}.json`  
  - Stage: `stage/fundamentals/{TICKER}/{start}_{end}.json`  
  - Prod: `prod/fundamentals/{TICKER}/{start}_{end}.json`  
- Intraday (examples):  
  - Daily (eod): `source/intraday/{TICKER}/{TICKER}_eod_{start}_{end}.json`  
  - Weekly: `source/week/{TICKER}/{TICKER}_wk_{start}_{end}.json`  
  - Monthly: `source/month/{TICKER}/{TICKER}_mth_{start}_{end}.json`  
  - Quarterly: `source/quarter/{TICKER}/{TICKER}_qtr_{start}_{end}.json`  
  - Prod mirrors the same directory and filename structure under `prod/`

Dates use `YYYYMMDD`; tickers uppercase; freq lowercase. Stage is required for fundamentals prod.

## Run the worker

1) Export the required env vars (see above) or use a local `.env`.
2) Start the worker:

```bash
python worker.py
```

3) Optional health check (only if `HEALTHCHECK_PORT` or `PORT` is set):

```bash
curl -s http://localhost:8080/healthz
```

Keep the worker running and start workflows from another terminal using `client.py`.

## Container build

```bash
docker build -t marketflow-worker .
docker run --rm -p 8080:8080 --env-file .env marketflow-worker
```

## Cloud Build / Cloud Run

`cloudbuild.yaml` builds and deploys a container image on each trigger. Configure the Cloud Run
service with the required environment variables and set a minimum instance count so the worker
does not scale to zero.

Cloud Run uses workload identity, so no service account JSON key is needed. The Cloud Run
service account must have access to the GCS bucket and Secret Manager.

## Cloud Run targets (worker service + client job)

- Worker service uses `Dockerfile` and runs `worker.py` (long-lived).
- Client job uses `Dockerfile.client` and runs `client_job.py` (one-shot).

Example job setup (env-driven wrapper):

```bash
gcloud run jobs create marketflow-client \
  --image gcr.io/$PROJECT_ID/marketflow-client \
  --region us-central1 \
  --service-account 875978034496-compute@developer.gserviceaccount.com \
  --set-env-vars \
TICKERS=AA,NUE,START_DATE=2024-01-01,END_DATE=2024-01-31,\
INTRADAY_FREQUENCY=daily,FUNDAMENTALS_MODE=prod,INTRADAY_MODE=prod,\
TEMPORAL_ADDRESS=temporal.sbecipher.io:7233,TEMPORAL_TASK_QUEUE=marketio-task-queue

gcloud run jobs execute marketflow-client --region us-central1
```

## Cloud Scheduler trigger examples (Cloud Run Jobs)

Prereqs:

- Enable Cloud Scheduler in the project.
- Grant the scheduler service account permission to run jobs (`run.jobs.run`), e.g. `roles/run.developer`.

Example 1: trigger the job on a schedule with the job defaults (no overrides):

```bash
gcloud scheduler jobs create http marketflow-client-daily \
  --location us-central1 \
  --schedule "0 5 * * *" \
  --uri "https://run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/marketflow-client:run" \
  --http-method POST \
  --oauth-service-account-email scheduler@${PROJECT_ID}.iam.gserviceaccount.com \
  --oauth-token-scope https://www.googleapis.com/auth/cloud-platform
```

Example 2: trigger with per-run env overrides (no job update required):

```bash
cat > /tmp/marketflow-client-overrides.json <<'EOF'
{
  "overrides": {
    "containerOverrides": [
      {
        "env": [
          { "name": "TICKERS", "value": "AA,NUE" },
          { "name": "START_DATE", "value": "2024-01-01" },
          { "name": "END_DATE", "value": "2024-01-31" },
          { "name": "FUNDAMENTALS_MODE", "value": "prod" },
          { "name": "INTRADAY_MODE", "value": "prod" },
          { "name": "INTRADAY_FREQUENCY", "value": "daily" }
        ]
      }
    ]
  }
}
EOF

gcloud scheduler jobs create http marketflow-client-monthly \
  --location us-central1 \
  --schedule "0 6 1 * *" \
  --uri "https://run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/marketflow-client:run" \
  --http-method POST \
  --oauth-service-account-email scheduler@${PROJECT_ID}.iam.gserviceaccount.com \
  --oauth-token-scope https://www.googleapis.com/auth/cloud-platform \
  --headers "Content-Type: application/json" \
  --message-body "$(cat /tmp/marketflow-client-overrides.json)"
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
