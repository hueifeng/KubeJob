#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"$ROOT_DIR/scripts/dev-stack.sh" up
export ConnectionStrings__KubeJob
ConnectionStrings__KubeJob="$("$ROOT_DIR/scripts/dev-stack.sh" connection-string)"
export ConnectionStrings__RabbitMQ="${ConnectionStrings__RabbitMQ:-amqp://kubejob:kubejob-dev@localhost:5672/}"

echo
echo "Starting the unified sample with PostgreSQL persistence and RabbitMQ execution dispatch."
echo "RabbitMQ: ${ConnectionStrings__RabbitMQ}"
echo "Dashboard: http://localhost:5041/admin/jobs"
echo "After startup, seed real success/failure/retry/timeout scenarios with:"
echo "  bash scripts/seed-dashboard-demo.sh"
echo

exec dotnet run \
  --project "$ROOT_DIR/samples/KubeJob.Sample.Unified/KubeJob.Sample.Unified.csproj" \
  --launch-profile http \
  "$@"
