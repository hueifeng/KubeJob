# Benchmark and load-test results

The benchmark project is executable with:

```bash
dotnet run -c Release --project benchmarks/KubeJob.Benchmarks/KubeJob.Benchmarks.csproj
```

The required PostgreSQL claim, complete/retry, idle-worker and multi-process load
measurements must be captured on a host with the .NET SDK, PostgreSQL/container
runtime, and the pinned database configuration. This workspace has no .NET SDK or
container runtime, so no numeric result is recorded here.
