# Marketflow Client (Workflow Starter)

This client starts the `MarketDataWorkflow` on your Temporal server. It is
intended to be deployed as a Cloud Function in a separate project.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Environment

Connection defaults come from environment variables:

```bash
export TEMPORAL_ADDRESS=temporal.sbecipher.io:7233
export TEMPORAL_TASK_QUEUE=marketio-task-queue
export TEMPORAL_WORKFLOW=MarketDataWorkflow   # optional
```

## Run locally

```bash
python client.py \
  --tickers AA,NUE \
  --start-date 2020-01-01 \
  --end-date 2020-12-31 \
  --intraday-frequency daily \
  --fundamentals-mode prod \
  --intraday-mode prod \
  --edgar-source
```

Modes:

- Fundamentals: `raw` (source only), `stage` (processed), `prod` (stage → prod), `none` (skip fundamentals)
- Intraday: `raw` (source), `prod` (raw → prod), `none` (skip intraday)
- EDGAR: toggled via `--edgar-source` to fetch SEC submissions (source=True) per ticker or `--edgar-only` to fetch just EDGAR
- Metadata-only: `--metadata-only` fetches just metadata and exits early

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

## Cloud Function stub

`function.py` exposes `marketflow_handler` for HTTP-triggered Cloud Functions.
Send JSON (or query params) with the same fields as the CLI flags.

Deploy example:

```bash
gcloud functions deploy marketflow-client \
  --runtime python311 \
  --entry-point marketflow_handler \
  --trigger-http \
  --allow-unauthenticated \
  --source .
```

Request example:

```bash
curl -X POST "$FUNCTION_URL" \
  -H "Content-Type: application/json" \
  -d '{
    "tickers": ["AA", "NUE"],
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "intraday_frequency": "daily",
    "fundamentals_mode": "prod",
    "intraday_mode": "prod",
    "edgar_source": false
  }'
```
