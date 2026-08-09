#!/usr/bin/env bash
set -euo pipefail

configuration="${CONFIGURATION:-Release}"
smoke_version="${PACKAGE_VERSION:-0.0.0-complete}"

CONFIGURATION="$configuration" PACKAGE_VERSION="$smoke_version" bash eng/validate.sh

dotnet restore tests/KubeJob.EndToEndTests/KubeJob.EndToEndTests.csproj
dotnet test tests/KubeJob.EndToEndTests/KubeJob.EndToEndTests.csproj \
  --configuration "$configuration" \
  --logger "console;verbosity=normal"

dotnet restore tests/KubeJob.MetaPackageSmoke/KubeJob.MetaPackageSmoke.csproj \
  -p:KubeJobSmokeVersion="$smoke_version" \
  --source artifacts/packages \
  --source https://api.nuget.org/v3/index.json

dotnet build tests/KubeJob.MetaPackageSmoke/KubeJob.MetaPackageSmoke.csproj \
  --configuration "$configuration" \
  --no-restore \
  -p:KubeJobSmokeVersion="$smoke_version"

dotnet run --project tests/KubeJob.MetaPackageSmoke/KubeJob.MetaPackageSmoke.csproj \
  --configuration "$configuration" \
  --no-build \
  -p:KubeJobSmokeVersion="$smoke_version"

printf '\nKubeJob full validation succeeded.\n'
