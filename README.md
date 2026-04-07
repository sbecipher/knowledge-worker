# Marketio Temporal Pipeline

Temporal Python worker that orchestrates Marketio API pulls (metadata, EDGAR raw, fundamentals raw/production, and market daily raw/production) and writes artifacts to GCS in a hierarchical layout per instrument.

This worker now uses:

- A structured workflow request payload instead of positional workflow arguments
- Synchronous Temporal activities executed on a worker thread pool
- Durable artifact references (`gs://...` or local fallback) between stages
- Run-scoped metadata snapshots keyed by `request_id`

## Prerequisites

- Python 3.13+
- Temporal server (e.g., `temporal server start-dev`)
- Marketio API running (e.g., `uvicorn app.main:app --reload`)
- GCS bucket + credentials (service account JSON or workload identity on VM) for uploads
- Secret Manager access to `projects/875978034496/secrets/marketio-data-api-intrinio`

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Config (env)

```bash
export MARKETIO_API_URL=http://localhost:8000
export MARKETIO_REQUIRE_AUTH=false            # optional; auto-detects https/non-localhost when unset
export GCS_BUCKET=sbecipher-intelligence
export GCS_PREFIX=dev            # optional
export INSTRUMENT=ssga-xme
export MODEL_VERSION=1125v
export TEMP_DIR=tmp
export UPLOAD_ENABLED=true
export CLEANUP_LOCAL_ARTIFACTS=true            # optional; remove temp files after successful uploads
export TEMPORAL_ADDRESS=127.0.0.1:7233
export TEMPORAL_TASK_QUEUE=market-data-task-queue
export LOG_LEVEL=INFO
export HEALTHCHECK_PORT=8080                    # optional; defaults to PORT when set
export ACTIVITY_EXECUTOR_THREADS=16             # optional; sync activity thread pool size
export MAX_CONCURRENT_ACTIVITIES=16             # optional
export MAX_CONCURRENT_WORKFLOW_TASKS=100        # optional
export MAX_CACHED_WORKFLOWS=1000                # optional
# Optional Intrinio overrides
# export INTRINIO_API_KEY=...
# export INTRINIO_SECRET_MANAGER_ENABLED=false
# Use the public Marketio URL on a VM if needed.
```

The worker loads `INTRINIO_API_KEY` from GCP Secret Manager
(`projects/875978034496/secrets/marketio-data-api-intrinio`) unless
`INTRINIO_SECRET_MANAGER_ENABLED=false` or `INTRINIO_API_KEY` is provided via env.

## GCS layout (hierarchical)

- Metadata snapshots: `prod/models/{INSTRUMENT}/{model_version}/{request_id}.json`
- EDGAR submissions: `source/edgar/{TICKER}/{TICKER}_edgar_{date}.json`
- Fundamentals:  
  - Raw: `source/fundamentals/{TICKER}/{TICKER}_fundamentals_{start}_{end}.json`
  - Prod: `prod/fundamentals/{TICKER}/{TICKER}_fundamentals_{start}_{end}.json`
- Market daily artifacts are still stored under the existing `intraday` namespace for compatibility:
  - Raw daily (eod layout): `source/intraday/{TICKER}/{TICKER}_eod_{start}_{end}.json`
  - Prod daily (eod layout): `prod/intraday/{TICKER}/{TICKER}_eod_{start}_{end}.json`

Dates use `YYYYMMDD`; tickers uppercase; freq lowercase. Regular workflow runs do not update a canonical latest metadata file.

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

Keep the worker running and start workflows from another terminal using the
sibling `../client/` project (or your Cloud Function).

### Example workstreams

Run these from the sibling `client/` directory while the worker is running:

- Metadata only:

```bash
python3 client.py \
  --tickers AA \
  --start-date 2026-04-01 \
  --end-date 2026-04-01 \
  --fundamentals-mode none \
  --intraday-mode none \
  --metadata-only
```

- EDGAR only:

```bash
python3 client.py \
  --tickers AA \
  --start-date 2026-04-01 \
  --end-date 2026-04-01 \
  --fundamentals-mode none \
  --intraday-mode none \
  --edgar-only
```

- Fundamentals raw only:

```bash
python3 client.py \
  --tickers AA \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --fundamentals-mode raw \
  --intraday-mode none
```

- Fundamentals production only:

```bash
python3 client.py \
  --tickers AA \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --fundamentals-mode prod \
  --intraday-mode none
```

- Market daily raw only:

```bash
python3 client.py \
  --tickers AA \
  --start-date 2026-04-02 \
  --end-date 2026-04-02 \
  --fundamentals-mode none \
  --intraday-mode raw \
  --intraday-frequency eod
```

- Market daily production only:

```bash
python3 client.py \
  --tickers AA \
  --start-date 2026-04-02 \
  --end-date 2026-04-02 \
  --fundamentals-mode none \
  --intraday-mode prod \
  --intraday-frequency daily
```

- Full run (metadata + fundamentals prod + market daily prod):

```bash
python3 client.py \
  --tickers AA,NUE \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --fundamentals-mode prod \
  --intraday-mode prod \
  --intraday-frequency daily \
  --edgar-source
```

## Tests

Install the dev dependencies and run the worker test suite:

```bash
python -m pip install -r requirements-dev.txt
pytest -q
```

Replay verification uses `tests/histories/` when history fixtures are present:

```bash
pytest -q tests/test_replay.py
```

## Container build

```bash
docker build -t marketflow-worker .
docker run --rm -p 8080:8080 --env-file .env marketflow-worker
```

## Run worker on the Temporal VM (Docker Compose)

Use the worker container as a long-lived service on the same VM as the Temporal
server. The compose file uses host networking so the worker can reach
`127.0.0.1:7233` on the VM.

```bash
# Example env file (required values shown)
cat > .env.worker <<'EOF'
MARKETFLOW_WORKER_IMAGE=sbecipher/marketflow-worker:v1.0.0
TEMPORAL_ADDRESS=127.0.0.1:7233
TEMPORAL_TASK_QUEUE=marketio-task-queue
MARKETIO_API_URL=https://marketio-875978034496.us-central1.run.app
GCS_BUCKET=sbecipher-intelligence
UPLOAD_ENABLED=true
EOF

# Uses host networking so 127.0.0.1 resolves to the VM's Temporal server.
docker compose --env-file .env.worker -f docker-compose.yml up -d
```

Notes:
- Host networking is supported on Linux (works well on a VM). On macOS/Windows,
  use a Docker network instead and set `TEMPORAL_ADDRESS` to the Temporal service name.

## Push a private image to Docker Hub

1) Create a private repo on Docker Hub, e.g. `sbecipher/marketflow-worker`.
2) Authenticate and push:

```bash
docker login
docker build -t sbecipher/marketflow-worker:latest .
docker push sbecipher/marketflow-worker:latest
```

3) If you want to pin a release tag:

```bash
docker tag sbecipher/marketflow-worker:latest sbecipher/marketflow-worker:v1.0.0
docker push sbecipher/marketflow-worker:v1.0.0
```

4) Update the VM to pull the private image:

```bash
docker login
docker compose --env-file .env.worker -f docker-compose.yml pull
docker compose --env-file .env.worker -f docker-compose.yml up -d
```

## Run the worker via systemd (auto-start on boot)

The repo includes a systemd unit template at `systemd/marketflow.service`.
Copy it to the VM, adjust the paths if needed, then enable it:

```bash
sudo mkdir -p /opt/marketflow
sudo cp -R . /opt/marketflow
sudo cp systemd/marketflow.service /etc/systemd/system/marketflow.service
sudo systemctl daemon-reload
sudo systemctl enable --now marketflow
```

To refresh after pulling a new image:

```bash
sudo systemctl restart marketflow
```

## VM deploy helper script

Use the helper to pull the latest image and restart the worker:

```bash
chmod +x scripts/deploy.sh
APP_DIR=/opt/marketflow ./scripts/deploy.sh
```

To update the image tag and deploy in one step:

```bash
APP_DIR=/opt/marketflow ./scripts/deploy.sh --tag v1.0.0
```

## VM install checklist (Debian 13 trixie, compose + systemd)

```bash
# 1) Install Docker + Compose plugin
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin
sudo systemctl enable --now docker

# 2) Create app directory and copy repo
sudo mkdir -p /opt/marketflow
sudo cp -R . /opt/marketflow

# 3) Create env file
sudo tee /opt/marketflow/.env.worker >/dev/null <<'EOF'
MARKETFLOW_WORKER_IMAGE=sbecipher/marketflow-worker:v1.0.0
TEMPORAL_ADDRESS=127.0.0.1:7233
TEMPORAL_TASK_QUEUE=marketio-task-queue
MARKETIO_API_URL=https://marketio-875978034496.us-central1.run.app
GCS_BUCKET=sbecipher-intelligence
UPLOAD_ENABLED=true
EOF

# 4) Docker Hub auth + pull
sudo docker login
sudo docker compose --env-file /opt/marketflow/.env.worker -f /opt/marketflow/docker-compose.yml pull

# 5) Install systemd unit and start
sudo cp /opt/marketflow/systemd/marketflow.service /etc/systemd/system/marketflow.service
sudo systemctl daemon-reload
sudo systemctl enable --now marketflow
```

## Client (Cloud Function)

The workflow starter now lives in the sibling `../client/` project so it can be
deployed separately (for example, as a Cloud Function in another project). The
client uses the Temporal gRPC address and task queue from environment
variables; all run-specific parameters can be passed via the request payload to
your function.

Minimum connection settings:

```bash
TEMPORAL_ADDRESS=temporal.sbecipher.io:7233
TEMPORAL_TASK_QUEUE=marketio-task-queue
TEMPORAL_WORKFLOW=MarketDataWorkflow
```

Inputs to pass per run (query/body → function args):

```text
tickers,start_date,end_date,intraday_frequency,fundamentals_mode,intraday_mode,edgar_source,metadata_only,edgar_only,workflow_id,request_id
```

See the sibling client project's `README.md` for local CLI usage and examples.
Its `function.py` exposes the `marketflow_handler` HTTP entrypoint for Cloud
Functions.

## What the workflow does (per ticker)

- Health check `/health`
- Metadata → write/upload run-scoped companies snapshot (always runs; EDGAR uses CIKs from metadata when present)
- EDGAR submissions → per ticker raw SEC submissions uploaded under `source/edgar/` when `--edgar-source/--edgar-only`
- Fundamentals path: raw → prod unless `--fundamentals-mode none` or `--edgar-only`
- Market daily path: raw → prod unless `--intraday-mode none` or `--edgar-only`
- `intraday_frequency` accepts `daily` or `eod` only; `eod` is normalized to `daily` for the Marketio API
- `fundamentals_mode=stage` and non-daily market frequencies are rejected as invalid requests
- Market daily raw retries once locally when the API returns a 200 with unusable empty-field payloads, then falls back to Temporal activity retries if the response remains empty
- Uploads JSON artifacts with metadata in GCS object metadata (`request_id`, `workflow_id`, `workflow_run_id`, `instrument`, `layer`, `ticker`, `window`).

## Activities

- `check_marketio_health`: Ensure the Marketio API is reachable.
- `fetch_companies_metadata`: Pull company metadata from `/api/v2/companies` and upload the consolidated model file.
- `fetch_edgar_source`: Download raw SEC submissions from `/api/v2/edgar/raw` for tickers/CIKs and upload per ticker under `source/edgar/`.
- `fetch_fundamentals_raw` / `fetch_fundamentals_prod`: Pull fundamentals through `/api/v2/fundamentals/raw` and `/api/v2/fundamentals/production`.
- `fetch_intraday_raw` / `fetch_intraday_prod`: Pull market daily data from `/api/v2/market/daily/raw` and `/api/v2/market/daily/production`, while preserving the existing `intraday` storage layout.
