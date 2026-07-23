#!/usr/bin/env bash
set -euo pipefail

configuration="${CONFIGURATION:-Release}"
smoke_version="${PACKAGE_VERSION:-0.0.0-validation}"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

run dotnet restore KubeJob.sln
run dotnet build KubeJob.sln --configuration "$configuration" --no-restore
run dotnet test tests/KubeJob.Tests/KubeJob.Tests.csproj \
  --configuration "$configuration" \
  --no-build \
  --logger "console;verbosity=normal"

run dotnet restore tests/KubeJob.ApiTests/KubeJob.ApiTests.csproj
run dotnet test tests/KubeJob.ApiTests/KubeJob.ApiTests.csproj \
  --configuration "$configuration" \
  --logger "console;verbosity=normal"

run dotnet restore tests/KubeJob.GeneratorNegative/KubeJob.GeneratorNegative.csproj
set +e
negative_output=$(dotnet build tests/KubeJob.GeneratorNegative/KubeJob.GeneratorNegative.csproj \
  --configuration "$configuration" \
  --no-restore 2>&1)
negative_status=$?
set -e
printf '%s\n' "$negative_output"
if [ "$negative_status" -eq 0 ] || ! grep -q "KJGEN003" <<< "$negative_output"; then
  echo "Duplicate JobKey analyzer validation failed."
  exit 1
fi

rm -rf artifacts/packages
CONFIGURATION="$configuration" PACKAGE_VERSION="$smoke_version" bash eng/pack-v2.sh

run dotnet restore tests/KubeJob.PackageSmoke/KubeJob.PackageSmoke.csproj \
  -p:KubeJobSmokeVersion="$smoke_version" \
  --source artifacts/packages \
  --source https://api.nuget.org/v3/index.json
run dotnet build tests/KubeJob.PackageSmoke/KubeJob.PackageSmoke.csproj \
  --configuration "$configuration" \
  --no-restore \
  -p:KubeJobSmokeVersion="$smoke_version"
run dotnet run --project tests/KubeJob.PackageSmoke/KubeJob.PackageSmoke.csproj \
  --configuration "$configuration" \
  --no-build \
  -p:KubeJobSmokeVersion="$smoke_version"

if [ -n "${KUBEJOB_TEST_POSTGRES:-}" ]; then
  run dotnet test tests/KubeJob.Tests/KubeJob.Tests.csproj \
    --configuration "$configuration" \
    --no-build \
    --filter FullyQualifiedName~PostgreSqlRuntimeIntegrationTests \
    --logger "console;verbosity=detailed"
fi

printf '\nKubeJob V2 validation completed successfully.\n'
