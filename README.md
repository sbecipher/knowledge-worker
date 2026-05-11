# KnowledgeFlow Worker

This repository contains the Temporal worker service for the **KnowledgeFlow** extraction pipeline. The worker leverages Google Cloud services (Storage, Document AI, BigQuery) and Google GenAI (Gemini) to process raw documents (HTML, PDF) and extract structured analytical features.

## Architecture & Workflows

The main orchestrator (`client/` cloud function or equivalent) queues tasks onto the Temporal server. The production worker listens on `knowledge-cloud-run-task-queue`, accepts `KnowledgeCompanyWorkflow` starts from the client function, and executes child `KnowledgeIngestionWorkflow` runs on the same queue.

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
| `PROJECT_ID` | GCP project used by the worker runtime | `data-cipher` |
| `BQ_PROJECT_ID` | GCP project containing the BigQuery target table | `sbecipherio` |
| `REGION` | GCP Region | `us-central1` |
| `SOURCE_BUCKET` | Raw documents GCS bucket | `sbecipher-intelligence` |
| `PROD_BUCKET` | Processed Parquet GCS bucket | `sbecipher-intelligence` |
| `BQ_DATASET` | Target BigQuery dataset | `knowledge` |
| `BQ_TABLE` | Target BigQuery table | `documents` |
| `TEMPORAL_ADDRESS` | Network address for Temporal server | `localhost:7233` |
| `TEMPORAL_TASK_QUEUE` | Temporal task queue polled by this worker | `knowledge-ingestion-queue` |
| `KNOWLEDGEIO_API_URL` | KnowledgeIO Cloud Run base URL | `https://knowledgeio-875978034496.us-central1.run.app` |
| `KNOWLEDGEIO_API_AUDIENCE` | Google OIDC audience used for KnowledgeIO API calls | `https://knowledgeio-875978034496.us-central1.run.app` |
| `GEMINI_MODEL` | Gemini model used for extraction and chunk aggregation | `gemini-3-flash-preview` |
| `GEMINI_PDF_MAX_BYTES` | Maximum PDF size sent directly to Gemini | `52428800` |
| `GEMINI_PDF_CHUNK_TARGET_BYTES` | Target maximum serialized bytes per split PDF chunk | `45000000` |
| `GEMINI_CHUNK_BUCKET` | GCS bucket for temporary Gemini PDF chunks | Defaults to `PROD_BUCKET` |
| `GEMINI_CHUNK_PREFIX` | GCS prefix for temporary Gemini PDF chunks | `stage/knowledge/gemini_chunks` |
| `LOG_LEVEL` | Python logging level (INFO, DEBUG) | `INFO` |
| `ACTIVITY_EXECUTOR_THREADS` | Thread pool workers for synchronous activities | `10` |
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

The worker is deployed as an always-on Cloud Run service. Cloud Scheduler invokes the separate `knowledge-client` Gen 2 function, and that client starts `KnowledgeCompanyWorkflow` executions on the task queue this service polls.

Production deploys should use an immutable Artifact Registry digest. The deploy target project and the runtime data project are separate: deploy the Cloud Run service into `data-cipher`, but keep `RUNTIME_PROJECT_ID=sbecipherio` so BigQuery loads target `sbecipherio.knowledge.documents`.

```bash
IMAGE=us-central1-docker.pkg.dev/data-cipher/knowledgeio/knowledge-worker@sha256:<digest> \
  deploy/gcp/knowledge-worker-cloud-run-service.sh
```

The deploy script configures Direct VPC egress, the production Temporal address, the Cloud Run task queue, one minimum instance, disabled CPU throttling, and private ingress. The container exposes `/health` and `/healthz` on port 8080 for Cloud Run startup checks.

Before publishing an image, verify the Docker context is clean. The repository `.dockerignore` excludes `.env`, `.git`, local virtual environments, caches, tests, and logs from the container image.
