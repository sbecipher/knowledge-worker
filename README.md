# Temporal Knowledge Scheduling App

Temporal Python application that schedules and executes workflows to fetch articles from the Companies Knowledge Data API.

## Prerequisites

- Python 3.9 or higher
- Docker (for running a local Temporal server)
- Companies Knowledge Data API running (default: http://localhost:8000; see `knowledge` app)

## Installation

```bash
# Clone the repository and navigate to the temporal_app folder
git clone <repo-url>
cd <repo-folder>/temporal_app

# (Optional) Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

## Running a Local Temporal Server

Start a local Temporal server:

```bash
temporal server start-dev
docker run --rm -d -p 7233:7233 temporalio/auto-setup:latest
```

## Configuration

Configure the Companies Knowledge API and Temporal server via environment variables:

```bash
export KNOWLEDGE_API_URL=http://localhost:8000
export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_TASK_QUEUE=knowledge-task-queue
```

## Running the Worker

```bash
python -m temporal_app.worker
```

## Starting a Workflow

```bash
# Recurring: quarterly schedule
python -m temporal_app.client --years 2021,2022 --schedule quarterly

# One-time run
python -m temporal_app.client --years 2021,2022 --schedule once
```

You can also use `current` to specify the current year:

```bash
python -m temporal_app.client --years current --schedule once

```

By default, companies `aa,amr,feam` are used. To specify different companies:

```bash
python -m temporal_app.client --companies aa,amr --years 2021 --schedule weekly
```

Supported schedules: `once`, `weekly`, `four_weeks` (approximate monthly), `quarterly`, `annually`.

## Project Structure

```
temporal_app/
├── activities.py         # Task implementations: health check, list & process articles
├── workflows.py          # Orchestrates list vs process flow in KnowledgeWorkflow
├── client.py             # CLI for starting workflows (once or cron)
├── worker.py             # Worker that polls the task queue and executes activities
├── requirements.txt      # Python dependencies
└── README.md             # This file
```

## Activities
The app defines three core activities:

- **check_api_health**
  - Pings the API `/health` endpoint to verify service availability.
  - Configured with an exponential backoff retry policy and emits a heartbeat on success.

- **list_company_articles**
  - Fetches article metadata for a given company and year from `/api/v1/companies/{company}/{year}`.
  - Returns a list of article dicts. Heartbeats with the total count.
  - Retries on transient failures.

- **process_company_article**
  - Validates a single article by performing an HTTP HEAD (fallback to GET) on the article URL.
  - Records `validated` (bool), `content_length` (if provided), and any `validation_error`.
  - Emits a heartbeat per article and retries on transient errors.

## Workflow: KnowledgeWorkflow

The `KnowledgeWorkflow` coordinates all activities:

1. **Health Check**: Runs `check_api_health` before any fetches to ensure the API is up.
2. **List Phase**: For each company/year, executes `list_company_articles` to collect metadata.
3. **Process Phase**: Fans out `process_company_article` for each metadata entry in parallel (via `asyncio.gather`).
4. **Aggregation**: Collects and returns a mapping of `"{company}_{year}"` to the list of processed article metadata.

With this split, you get:
- Fine-grained retries and backoff per activity.
- Heartbeats at both list and per-article levels for liveness.
- Cancellation support at every step.
- Parallel processing of articles for speed and throughput.

## Requirements

- temporalio>=1.0.0
- httpx