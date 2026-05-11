FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO

WORKDIR /app

COPY requirements.txt /app/worker/requirements.txt
RUN pip install --no-cache-dir -r /app/worker/requirements.txt

COPY app /app/worker/app
RUN useradd --create-home --shell /bin/bash appuser && chown -R appuser:appuser /app
USER appuser

# Starts an HTTP health server on this port when HEALTHCHECK_PORT is set.
EXPOSE 8080

WORKDIR /app/worker

CMD ["python", "-m", "app.main"]
