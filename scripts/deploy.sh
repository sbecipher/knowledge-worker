#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/marketflow}"
ENV_FILE="${ENV_FILE:-$APP_DIR/.env.worker}"
COMPOSE_FILE="${COMPOSE_FILE:-$APP_DIR/docker-compose.worker.yml}"
SERVICE_NAME="${SERVICE_NAME:-marketflow-worker}"

if [[ ! -d "$APP_DIR" ]]; then
  echo "APP_DIR not found: $APP_DIR" >&2
  exit 1
fi

cd "$APP_DIR"

echo "Pulling image..."
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" pull

if command -v systemctl >/dev/null 2>&1 && \
  systemctl list-unit-files --type=service --no-pager | grep -q "^${SERVICE_NAME}\\.service"; then
  echo "Restarting systemd service ${SERVICE_NAME}..."
  if command -v sudo >/dev/null 2>&1; then
    sudo systemctl restart "$SERVICE_NAME"
  else
    systemctl restart "$SERVICE_NAME"
  fi
else
  echo "Starting compose service..."
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d
fi
