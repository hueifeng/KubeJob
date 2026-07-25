#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${KUBEJOB_SAMPLE_URL:-http://localhost:5041}"
ENDPOINT="${BASE_URL%/}/demo/scenarios"

echo "Submitting KubeJob Dashboard demo scenarios to $ENDPOINT"
response="$(curl --fail --silent --show-error \
  --request POST \
  --header 'Accept: application/json' \
  "$ENDPOINT")"

printf '%s\n' "$response"
echo
echo "Dashboard: ${BASE_URL%/}/admin/jobs"
echo "Failures:  ${BASE_URL%/}/admin/jobs/failures"
echo "The cancel-me job runs for up to 60 seconds; open it in the Dashboard and request cancellation."
