# Marketio Temporal Pipeline

Temporal Python worker that orchestrates Marketio API pulls (metadata, EDGAR submissions, fundamentals, intraday) and writes artifacts to GCS in a hierarchical layout per instrument.

## Prerequisites

- Python 3.13+
- Temporal server (e.g., `temporal server start-dev`)
- Marketio API running (e.g., `uvicorn app.main:app --reload`)
- GCS bucket + credentials (service account JSON or workload identity on VM) for uploads

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
export TEMPORAL_ADDRESS=127.0.0.1:7233
export TEMPORAL_TASK_QUEUE=market-data-task-queue
export LOG_LEVEL=INFO
export HEALTHCHECK_PORT=8080                    # optional; defaults to PORT when set
# Use the public Marketio URL on a VM if needed.
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

Keep the worker running and start workflows from another terminal using the
client in `client/client.py` (or your Cloud Function).

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
INTRINIO_API_KEY=your_key
GCS_BUCKET=sbecipher-intelligence
UPLOAD_ENABLED=true
EOF

# Uses host networking so 127.0.0.1 resolves to the VM's Temporal server.
docker compose --env-file .env.worker -f docker-compose.worker.yml up -d
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
docker compose --env-file .env.worker -f docker-compose.worker.yml pull
docker compose --env-file .env.worker -f docker-compose.worker.yml up -d
```

## Run the worker via systemd (auto-start on boot)

The repo includes a systemd unit template at `systemd/marketflow-worker.service`.
Copy it to the VM, adjust the paths if needed, then enable it:

```bash
sudo mkdir -p /opt/marketflow
sudo cp -R . /opt/marketflow
sudo cp systemd/marketflow-worker.service /etc/systemd/system/marketflow-worker.service
sudo systemctl daemon-reload
sudo systemctl enable --now marketflow-worker
```

To refresh after pulling a new image:

```bash
sudo systemctl restart marketflow-worker
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
INTRINIO_API_KEY=your_key
GCS_BUCKET=sbecipher-intelligence
UPLOAD_ENABLED=true
EOF

# 4) Docker Hub auth + pull
sudo docker login
sudo docker compose --env-file /opt/marketflow/.env.worker -f /opt/marketflow/docker-compose.worker.yml pull

# 5) Install systemd unit and start
sudo cp /opt/marketflow/systemd/marketflow-worker.service /etc/systemd/system/marketflow-worker.service
sudo systemctl daemon-reload
sudo systemctl enable --now marketflow-worker
```

## Client (Cloud Function)

The workflow starter now lives in `client/` so it can be deployed separately
(for example, as a Cloud Function in another project). The client uses the
Temporal gRPC address and task queue from environment variables; all run-specific
parameters can be passed via the request payload to your function.

Minimum connection settings:

```bash
TEMPORAL_ADDRESS=temporal.sbecipher.io:7233
TEMPORAL_TASK_QUEUE=marketio-task-queue
TEMPORAL_WORKFLOW=MarketDataWorkflow
```

Inputs to pass per run (query/body → function args):

```text
tickers,start_date,end_date,intraday_frequency,fundamentals_mode,intraday_mode,edgar_source,metadata_only,edgar_only,workflow_id
```

See `client/README.md` for local CLI usage and examples.
`client/function.py` exposes the `marketflow_handler` HTTP entrypoint for Cloud
Functions.

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
