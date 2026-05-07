# KnowledgeFlow Worker

This repository contains the Temporal worker service for the **KnowledgeFlow** extraction pipeline. The worker leverages Google Cloud services (Storage, Document AI, BigQuery) and Google GenAI (Gemini) to process raw documents (HTML, PDF) and extract structured analytical features.

## Architecture & Workflows

The main orchestrator (`client/` cloud function or equivalent) queues tasks onto the Temporal server. This worker listens on the `knowledge-ingestion-queue` and executes the `KnowledgeIngestionWorkflow`.

The workflow runs three primary activities:
1. **Ingestion**: Download the document via URL and store the raw file (PDF, HTML) in the source GCS bucket (`sbecipher-knowledge-source`).
2. **Processing**: Read the raw document from GCS, extract the text content (using Document AI or raw decoding), and generate structured analytical features using Gemini 2.5 Flash via native **Structured Outputs**. Finally, save these features as a Parquet file in the production GCS bucket (`sbecipher-knowledge-prod`).
3. **Loading**: Load the structured Parquet file from the production bucket into a BigQuery table (`knowledge.documents`) for downstream consumption.

## Requirements

The service requires Python 3.13 and uses standard libraries including `temporalio`, `google-genai`, `google-cloud-storage`, `google-cloud-documentai`, and `pydantic`.

### Codebase Standards
The codebase strictly adheres to the following industry standards:
- **Pydantic V2 & OpenAPI**: All data models (`KnowledgeDocument`, `StandardFeatures`) are rigorously defined using Pydantic V2. The `Settings` model uses `model_config` (from `pydantic-settings`), and all models have detailed `Field` descriptions for robust OpenAPI integration. End-to-end tests strongly type payloads via the Pydantic models.
- **Formatting**: The codebase is formatted completely using `black`.
- **Linting & Typing**: Fully compliant with `flake8` for linting and `mypy` for static type checking, ensuring zero unused imports and complete type safety.

### Environment Configuration

The worker configuration is strictly driven by environment variables using Pydantic `BaseSettings`. The defaults are suitable for local development.

| Variable | Description | Default |
|----------|-------------|---------|
| `PROJECT_ID` | GCP Project ID | `sbecipherio` |
| `REGION` | GCP Region | `us-central1` |
| `SOURCE_BUCKET` | Raw documents GCS bucket | `sbecipher-knowledge-source` |
| `PROD_BUCKET` | Processed Parquet GCS bucket | `sbecipher-knowledge-prod` |
| `BQ_DATASET` | Target BigQuery dataset | `knowledge` |
| `BQ_TABLE` | Target BigQuery table | `documents` |
| `TEMPORAL_ADDRESS` | Network address for Temporal server | `localhost:7233` |
| `LOG_LEVEL` | Python logging level (INFO, DEBUG) | `INFO` |
| `HEALTHCHECK_PORT` (or `PORT`) | Exposes health check HTTP server for Cloud Run | `8080` (if unset, server skips init) |

## Running Locally

1. Setup the virtual environment and install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. Start the Temporal server locally (if not already running):
   ```bash
   temporal server start-dev
   ```
3. Run the worker:
   ```bash
   python -m app.main
   ```

## Docker Container & Cloud Run

The service includes a production-ready, unprivileged Dockerfile based on `python:3.13-slim`.

To build the image:
```bash
docker build -t knowledge-worker .
```

To run the container locally:
```bash
docker run -p 8080:8080 -e TEMPORAL_ADDRESS=host.docker.internal:7233 knowledge-worker
```

The service natively spins up a threaded HTTP server on port 8080 to respond to `/health` and `/healthz` probes, making it perfectly suited for orchestration platforms like Google Cloud Run or Kubernetes.
