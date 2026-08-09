# Benchmarking KubeJob

Benchmark numbers are workload- and environment-specific; do not compare them
across machines or use them as a production capacity guarantee.

## Reproducible method

1. Start PostgreSQL and RabbitMQ with `bash scripts/dev-stack.sh up`.
2. Build in Release mode: `dotnet build KubeJob.sln -c Release`.
3. Run the benchmark project with an explicit scenario and record the command,
   hardware, broker/storage configuration, concurrency, payload shape and
   warm-up period alongside the result.
4. Capture ingest rate, end-to-end throughput, latency percentiles, database
   connection usage and broker ready/unacknowledged counts.

## Interpret results by runtime

- **PostgresManaged** includes durable Run/Attempt/lease/completion writes. Its
  throughput is not directly comparable to a broker-only path.
- **BrokerNative** measures confirmed broker delivery and handler execution;
  it intentionally has no managed database completion write.
- Test key ordering, retries and hot-key workloads separately from parallel
  workloads. A single hot key is a serialization test, not a throughput test.

The benchmark harness and its options live in `tests/KubeJob.Benchmark/`.
Commit only results that include the full reproduction metadata above; transient
local output belongs outside the repository.
