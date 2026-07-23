# V2 validation record

The implementation is deliberately documented as at-least-once. A worker may
perform an external side effect and crash before fenced completion; callers must
deduplicate with `RunId`/business idempotency or an inbox/outbox transaction.

The intended validation commands are:

```bash
dotnet format KubeJob.sln --verify-no-changes
dotnet build KubeJob.sln -c Release
dotnet test KubeJob.sln -c Release
KUBEJOB_RUN_POSTGRES_TESTS=1 dotnet test tests/KubeJob.Tests/KubeJob.Tests.csproj -c Release
dotnet run -c Release --project benchmarks/KubeJob.Benchmarks/KubeJob.Benchmarks.csproj
```

The real PostgreSQL tests use `Testcontainers.PostgreSql` and cover concurrent
`FOR UPDATE SKIP LOCKED` claim and stale-token completion rejection. The full
18-case matrix and chaos scenarios remain the acceptance plan in
`tests/DistributedRuntimeTestMatrix.md`.

This checkout cannot execute those commands because the host has neither the
.NET SDK nor a container runtime. No build, test, chaos, or benchmark number is
claimed until a CI/staging run publishes the raw logs and BenchmarkDotNet
artifacts.
