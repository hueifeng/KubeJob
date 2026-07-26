#!/usr/bin/env bash
set -euo pipefail

configuration="${CONFIGURATION:-Release}"
version="${PACKAGE_VERSION:-0.0.0-local}"
output="${PACKAGE_OUTPUT:-artifacts/packages}"

mkdir -p "$output"

# KubeJob.Generators is an implementation project. Its assembly is embedded in
# KubeJob.Core under analyzers/dotnet/cs and is validated by package consumers.
projects=(
  "src/KubeJob.Core/KubeJob.Core.csproj"
  "src/KubeJob.Client/KubeJob.Client.csproj"
  "src/KubeJob.ControlPlane/KubeJob.ControlPlane.csproj"
  "src/KubeJob.Server/KubeJob.Server.csproj"
  "src/KubeJob.Worker/KubeJob.Worker.csproj"
  "src/KubeJob.Storage.PostgreSQL/KubeJob.Storage.PostgreSQL.csproj"
  "src/KubeJob.Transport.RabbitMQ/KubeJob.Transport.RabbitMQ.csproj"
  "src/KubeJob/KubeJob.csproj"
)

for project in "${projects[@]}"; do
  dotnet pack "$project" \
    --configuration "$configuration" \
    --no-build \
    -p:PackageVersion="$version" \
    --output "$output"
done
