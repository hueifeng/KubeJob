# Benchmarking

The benchmark project measures the PostgresManaged pipeline. It is a console
harness for repeatable comparisons, not a promise of production capacity.
RabbitMQ and Kafka BrokerNative delivery are covered by real-broker integration
tests separately.

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

## Kafka BrokerNative throughput

The maintained benchmark project does not yet publish Kafka capacity numbers;
do not compare its PostgresManaged results with Kafka BrokerNative throughput.
Use the Kafka integration tests to validate delivery first:

```bash
podman compose -f compose.yaml up -d kafka
KUBEJOB_KAFKA_TEST_BOOTSTRAP=localhost:9092 \
  dotnet test tests/KubeJob.KafkaIntegrationTests/KubeJob.KafkaIntegrationTests.csproj -c Release
```

A local single-broker sanity run on this development machine (100 empty jobs,
12 partitions, 32 submitters, 32 worker slots) accepted about 2,700 jobs/s but
completed about 20 jobs/s end to end. It is a diagnostic observation, not a
capacity claim: the current adapter commits offsets synchronously after each
completed delivery, which serializes its commit path. Batch/async offset commit
is required before using that number as a production sizing baseline.
