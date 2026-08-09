# Benchmarking

The benchmark project measures the PostgresManaged pipeline. It is a console
harness for repeatable comparisons, not a promise of production capacity. The
RabbitMQ integration tests cover BrokerNative delivery separately.

## What is measured

Each scenario reports:

- submission and end-to-end throughput;
- P50, P95, and P99 completion latency;
- PostgreSQL connection peaks;
- process memory, allocations, and thread counts;
- optional container CPU and RabbitMQ queue samples.

The benchmark records both the time the server reports completion and wall-clock
time. The former helps find database batching effects; the latter is the number
an operator experiences.

## Run a comparison

Start the development stack first. With Podman:

```bash
KUBEJOB_CONTAINER_ENGINE=podman bash scripts/dev-stack.sh up
```

Then run two scenarios with the same machine, database, and worker settings:

```bash
dotnet run --project tests/KubeJob.Benchmark/KubeJob.Benchmark.csproj -c Release -- \
  --jobs 5000 \
  --warmup 200 \
  --worker-concurrency 64 \
  --scenarios Parallel,KeyOrderedHotKey \
  --out bench-results.md
```

The harness creates a temporary database for each scenario and removes it in a
`finally` block. `--out` writes a Markdown report; do not commit that report
unless it includes the complete environment and command line.

## Scenarios

- `Parallel` shows the unconstrained managed pipeline.
- `KeyOrderedUniform` spreads work across many ordering keys.
- `KeyOrderedHotKey` deliberately creates contention on four keys by default.
- `StrictFifo` serializes one logical queue and is a latency/ordering test, not
  a maximum-throughput test.

Run each scenario at least three times after the warm-up and report the median
alongside the spread. Keep payload size, handler delay, database pool,
concurrency, and lane settings fixed when comparing commits. If one of those
changes, call it a new experiment.

The complete option table and ingress-mode notes live in the benchmark
[README](../../tests/KubeJob.Benchmark/README.md). Transient result files are
ignored by the repository.
