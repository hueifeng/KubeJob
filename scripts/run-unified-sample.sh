#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"$ROOT_DIR/scripts/dev-stack.sh" up
export ConnectionStrings__KubeJob
ConnectionStrings__KubeJob="$("$ROOT_DIR/scripts/dev-stack.sh" connection-string)"

echo
echo "Starting the unified sample with PostgreSQL persistence."
echo "Dashboard: http://localhost:5041/admin/jobs"
echo

exec dotnet run \
  --project "$ROOT_DIR/samples/KubeJob.Sample.Unified/KubeJob.Sample.Unified.csproj" \
  --launch-profile http \
  "$@"
