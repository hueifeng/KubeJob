#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/compose.yaml"
PROJECT_ARGS=(-f "$COMPOSE_FILE" --project-directory "$ROOT_DIR")

usage() {
  cat <<'EOF'
Usage: ./scripts/dev-stack.sh [up|down|stop|status|logs|reset|connection-string]

Commands:
  up                 Start PostgreSQL and RabbitMQ, then wait for health checks.
  down               Stop and remove containers while preserving data volumes.
  stop               Stop containers without removing them.
  status             Show service status.
  logs [service]      Follow logs for all services or one named service.
  reset --yes         Remove containers and development data volumes.
  connection-string  Print the PostgreSQL connection string for the running stack.

Set KUBEJOB_CONTAINER_ENGINE=docker or podman to force an engine.
EOF
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

select_compose() {
  local requested="${KUBEJOB_CONTAINER_ENGINE:-}"

  if [[ -z "$requested" || "$requested" == "docker" ]]; then
    if command_exists docker && docker compose version >/dev/null 2>&1; then
      COMPOSE=(docker compose)
      return
    fi
  fi

  if [[ -z "$requested" || "$requested" == "podman" ]]; then
    if command_exists podman && podman compose version >/dev/null 2>&1; then
      COMPOSE=(podman compose)
      return
    fi
    if command_exists podman-compose; then
      COMPOSE=(podman-compose)
      return
    fi
  fi

  echo "No supported Compose provider found." >&2
  echo "Install Docker Compose, 'podman compose', or podman-compose." >&2
  exit 1
}

compose() {
  "${COMPOSE[@]}" "${PROJECT_ARGS[@]}" "$@"
}

wait_for_service() {
  local service="$1"
  shift
  local attempts=60
  local delay=2

  for ((i = 1; i <= attempts; i++)); do
    if compose exec -T "$service" "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$delay"
  done

  echo "Timed out waiting for $service." >&2
  compose ps >&2 || true
  return 1
}

postgres_connection_string() {
  local user database password port
  user="$(compose exec -T postgres printenv POSTGRES_USER | tr -d '\r')"
  database="$(compose exec -T postgres printenv POSTGRES_DB | tr -d '\r')"
  password="$(compose exec -T postgres printenv POSTGRES_PASSWORD | tr -d '\r')"
  port="$(compose port postgres 5432 | tail -n 1 | awk -F: '{print $NF}' | tr -d '\r')"
  printf 'Host=localhost;Port=%s;Database=%s;Username=%s;Password=%s\n' \
    "$port" "$database" "$user" "$password"
}

print_endpoints() {
  local rabbit_port rabbit_user rabbit_password
  rabbit_port="$(compose port rabbitmq 15672 | tail -n 1 | awk -F: '{print $NF}' | tr -d '\r')"
  rabbit_user="$(compose exec -T rabbitmq printenv RABBITMQ_DEFAULT_USER | tr -d '\r')"
  rabbit_password="$(compose exec -T rabbitmq printenv RABBITMQ_DEFAULT_PASS | tr -d '\r')"

  echo
  echo "KubeJob development dependencies are ready."
  echo "PostgreSQL: $(postgres_connection_string)"
  echo "RabbitMQ UI: http://localhost:${rabbit_port}"
  echo "RabbitMQ credentials: ${rabbit_user} / ${rabbit_password}"
}

select_compose
ACTION="${1:-up}"

case "$ACTION" in
  up|start)
    compose up -d
    wait_for_service postgres sh -ec 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
    wait_for_service rabbitmq rabbitmq-diagnostics -q ping
    print_endpoints
    ;;
  down)
    compose down
    ;;
  stop)
    compose stop
    ;;
  status|ps)
    compose ps
    ;;
  logs)
    shift || true
    compose logs -f "$@"
    ;;
  reset)
    if [[ "${2:-}" != "--yes" ]]; then
      echo "reset removes all local PostgreSQL and RabbitMQ data." >&2
      echo "Re-run with: ./scripts/dev-stack.sh reset --yes" >&2
      exit 2
    fi
    compose down --volumes --remove-orphans
    ;;
  connection-string)
    postgres_connection_string
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    echo "Unknown command: $ACTION" >&2
    usage >&2
    exit 2
    ;;
esac
