#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/marketflow}"
ENV_FILE="${ENV_FILE:-$APP_DIR/.env.worker}"
COMPOSE_FILE="${COMPOSE_FILE:-$APP_DIR/docker-compose.worker.yml}"
SERVICE_NAME="${SERVICE_NAME:-marketflow-worker}"
IMAGE_REPO_DEFAULT="${MARKETFLOW_WORKER_IMAGE_REPO:-sbecipher/marketflow-worker}"
TAG_OVERRIDE=""

usage() {
  cat <<'EOF'
Usage: deploy.sh [--tag <tag>] [--env-file <path>] [--help]

Options:
  --tag <tag>         Update MARKETFLOW_WORKER_IMAGE tag before deploy.
  --env-file <path>   Override env file (default: /opt/marketflow/.env.worker).
  --help              Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      TAG_OVERRIDE="${2:-}"
      shift 2
      ;;
    --tag=*)
      TAG_OVERRIDE="${1#*=}"
      shift
      ;;
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --env-file=*)
      ENV_FILE="${1#*=}"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ! -d "$APP_DIR" ]]; then
  echo "APP_DIR not found: $APP_DIR" >&2
  exit 1
fi
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ENV_FILE not found: $ENV_FILE" >&2
  exit 1
fi

cd "$APP_DIR"

if [[ -n "$TAG_OVERRIDE" ]]; then
  current_image="$(grep -E '^MARKETFLOW_WORKER_IMAGE=' "$ENV_FILE" | head -n 1 | cut -d= -f2- || true)"
  repo="${current_image:-$IMAGE_REPO_DEFAULT}"
  repo="${repo%@*}"
  if [[ "$repo" == *:* && "${repo##*/}" == *:* ]]; then
    repo="${repo%:*}"
  fi
  new_image="${repo}:${TAG_OVERRIDE}"
  if grep -qE '^MARKETFLOW_WORKER_IMAGE=' "$ENV_FILE"; then
    sed -i "s|^MARKETFLOW_WORKER_IMAGE=.*|MARKETFLOW_WORKER_IMAGE=${new_image}|" "$ENV_FILE"
  else
    echo "MARKETFLOW_WORKER_IMAGE=${new_image}" >> "$ENV_FILE"
  fi
  echo "Updated MARKETFLOW_WORKER_IMAGE=${new_image}"
fi

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
