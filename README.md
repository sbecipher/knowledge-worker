# Marketio Temporal Pipeline

Temporal Python worker that orchestrates Marketio API pulls (metadata, EDGAR raw, fundamentals raw/production, and market daily raw/production) and writes artifacts to GCS in a hierarchical layout per universe.

This worker now uses:

- A structured workflow request payload instead of positional workflow arguments
- Synchronous Temporal activities executed on a worker thread pool
- Durable artifact references (`gs://...` or local fallback) between stages
- Independent identifier resolution and metadata persistence
- Per-ticker metadata source artifacts plus a run manifest when metadata persistence is requested

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
export UNIVERSE_KEY=mmh5r1
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

- Active universe input: `prod/models/{UNIVERSE_KEY}/active.json`
- Metadata source artifacts: `source/metadata/{TICKER}/{TICKER}_{workflow_id}.json`
- Metadata manifests: `source/metadata/manifests/{workflow_id}.json`
- EDGAR submissions: `source/edgar/{TICKER}/{TICKER}_edgar_{date}.json`
- Fundamentals:  
  - Raw: `source/fundamentals/{TICKER}/{TICKER}_fundamentals_{start}_{end}.json`
  - Prod: `prod/fundamentals/{TICKER}/{TICKER}_fundamentals_{start}_{end}.json`
- Prices lake layout:
  - Raw: `source/prices/granularity=day/end_date=YYYY-MM-DD/ticker={TICKER}/{workflow_id}.ndjson`
  - Prod: `prod/prices/granularity=day/end_date=YYYY-MM-DD/ticker={TICKER}/{workflow_id}.ndjson`

The worker reads `active.json` as the authoritative universe membership input,
resolves identifiers only when needed, and persists metadata only when
`metadata_only` or `metadata_mode=source` is requested. `universe_key` is
required for full-universe, EDGAR, or metadata runs, but explicit-ticker
prices/fundamentals runs can omit it. For non-EDGAR prices/fundamentals runs,
`active.json` is used only for ticker expansion, not as an implicit RIC
override. Price rows are stored as NDJSON and follow the current Marketio
contracts: raw files repeat Marketio raw artifact metadata and store
`date`/`instrument` plus a provider `fields` object, while prod files repeat
artifact metadata and store `date`/`instrument` plus the canonical Marketio
snake_case daily market fields. Both layouts include `requested_period`,
`as_of_date`, `effective_start_date`, `effective_end_date`, and
`bar_granularity=day` for BigQuery ingestion.

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
  --fundamentals-mode none \
  --market-mode none \
  --metadata-only
```

- Persist metadata alongside prices:

```bash
python3 client.py \
  --tickers AA \
  --as-of-date 2026-04-02 \
  --fundamentals-mode none \
  --market-mode raw \
  --period month \
  --metadata-mode source
```

- EDGAR only:

```bash
python3 client.py \
  --tickers AA \
  --fundamentals-mode none \
  --market-mode none \
  --edgar-only
```

- Fundamentals raw only:

```bash
python3 client.py \
  --tickers AA \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --fundamentals-mode raw \
  --market-mode none
```

- Fundamentals production only:

```bash
python3 client.py \
  --tickers AA \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --fundamentals-mode prod \
  --market-mode none
```

- Prices raw only:

```bash
python3 client.py \
  --tickers AA \
  --as-of-date 2026-04-02 \
  --fundamentals-mode none \
  --market-mode raw \
  --period week
```

- Prices production only:

```bash
python3 client.py \
  --tickers AA \
  --as-of-date 2026-04-02 \
  --fundamentals-mode none \
  --market-mode prod \
  --period quarter
```

- Full run (fundamentals prod + prices prod):

```bash
python3 client.py \
  --tickers AA,NUE \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --as-of-date 2026-04-02 \
  --fundamentals-mode prod \
  --market-mode prod \
  --period month \
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
TEMPORAL_ADDRESS=172.0.0.4:7233
TEMPORAL_TASK_QUEUE=marketio-task-queue
TEMPORAL_WORKFLOW=MarketDataWorkflow
```

Inputs to pass per run (query/body → function args):

```text
tickers,start_date,end_date,as_of_date,period,fundamentals_mode,market_mode,metadata_mode,edgar_source,metadata_only,edgar_only,workflow_id,request_id
```

See the sibling client project's `README.md` for local CLI usage and examples.
Its `function.py` exposes the `marketflow_handler` HTTP entrypoint for Cloud
Functions.

## What the workflow does (per ticker)

- Health check `/health`
- Active-universe expansion → load tickers from `active.json` only when the request omits tickers
- Identifier resolution → call `/api/v2/companies` only for EDGAR runs or when metadata persistence is requested
- Metadata persistence → write per-ticker source artifacts + one manifest only for `--metadata-only` or `--metadata-mode source`
- EDGAR submissions → per ticker raw SEC submissions uploaded under `source/edgar/` when `--edgar-source/--edgar-only`
- Fundamentals path: raw → prod unless `--fundamentals-mode none` or `--edgar-only`
- Prices path: raw → prod unless `--market-mode none` or `--edgar-only`
- `period` accepts `day|week|month|quarter`; the worker resolves effective tradable dates from `as_of_date` with `pandas_market_calendars` and still fetches daily-grain rows from Marketio
- `fundamentals_mode=stage` is rejected as an invalid request
- Market daily raw retries once locally when the API returns a 200 with unusable empty-field payloads, then falls back to Temporal activity retries if the response remains empty
- Uploads fundamentals/metadata as JSON and prices as NDJSON, with execution metadata in GCS object metadata (`request_id`, `workflow_id`, `workflow_run_id`, `universe_key`, `layer`, `ticker`, `requested_period`, `effective_start_date`, `effective_end_date`).

## Activities

- `check_marketio_health`: Ensure the Marketio API is reachable.
- `load_active_universe_index`: Read `prod/models/{universe_key}/active.json` for full-universe ticker expansion.
- `resolve_company_identifiers`: Pull company metadata from `/api/v2/companies` and return compact CIK/RIC routing data.
- `persist_company_metadata`: Upload normalized per-ticker metadata artifacts and a run manifest under `source/metadata/`.
- `fetch_edgar_source`: Download raw SEC submissions from `/api/v2/edgar/raw` for tickers/CIKs and upload per ticker under `source/edgar/`.
- `fetch_fundamentals_raw` / `fetch_fundamentals_prod`: Pull fundamentals through `/api/v2/fundamentals/raw` and `/api/v2/fundamentals/production`.
- `fetch_prices_raw` / `fetch_prices_prod`: Pull market daily data from `/api/v2/market/daily/raw` and `/api/v2/market/daily/production`, resolve requested tradable windows from `period` + `as_of_date`, and write NDJSON lake artifacts under `source/prices/` and `prod/prices/`.
