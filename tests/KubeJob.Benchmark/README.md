# KubeJob throughput benchmark

This on-demand console harness can measure both V3 runtime authorities with the
same workload knobs (`--jobs`, `--warmup`, `--work-ms`, submitter concurrency,
and worker concurrency).

## PostgresManaged baseline

```text
submit → optional transactional work-available wake outbox → PostgreSQL claim/lease
      → handler → durable completion
```

`TypedClient` submits through `IJobClient`. The optional `Ingress` mode sends
business messages through RabbitMQ's ingress adapter before they enter the
same managed runtime. Ingress is intentionally **not** labeled BrokerNative;
PostgreSQL remains execution authority.

The managed benchmark reports ingest TPS, database-recorded E2E TPS,
wall-clock E2E TPS, P50/P95/P99 latency, PostgreSQL connection peaks, process
memory/allocation, thread counts, and optional best-effort CPU samples.
RabbitMQ queue metrics are reported when ingress mode is enabled.

PostgreSQL `synchronous_commit` is **on by default**. That keeps the benchmark
aligned with normal durable production semantics. `--synchronous-commit off`
is available only for an explicitly labeled throughput experiment; those
results must not be compared with durable production results as if the
semantics were equivalent.

Managed scenarios:

- `Parallel` — no `ConcurrencyKey` and parallel queue ordering.
- `KeyOrderedUniform` — a large key space, usually one key per run.
- `KeyOrderedHotKey` — a small key space (four keys by default).
- `StrictFifo` — one logical queue ordered globally.

Each scenario uses a separate logical queue. The harness uses one explicit
managed `ExecutionLane` only as a PostgreSQL worker-eligibility label. There is
no lane-count sweep because V3 does not map `ConcurrencyKey` values onto hidden
physical broker lanes.

## BrokerNative baseline

Use `--runtime BrokerNative` (or `KUBEJOB_BENCH_RUNTIME=BrokerNative`) to run:

```text
IJobClient → RabbitMQ → BrokerNative worker → handler → ACK
```

This host does **not** configure PostgreSQL storage and asserts that no
`NpgsqlDataSource` is registered. It measures an ordinary BrokerNative Job
Queue with competing workers—no hidden partitions, `ConcurrencyKey` lanes, or
PostgresManaged claim loop.

The BrokerNative result reports enqueue TPS, handler-observed E2E TPS,
P50/P95/P99/max latency, and duplicate executions observed during the run.
KubeJob remains at-least-once; a normal no-failure benchmark is expected to see
zero duplicates, but the counter makes duplicate delivery visible rather than
silently assuming exactly-once execution.

## Running

Start the development dependencies first:

```bash
bash scripts/dev-stack.sh up
export KUBEJOB_BENCHMARK_POSTGRES='Host=localhost;Port=5432;Username=kubejob;Password=kubejob-dev;Database=postgres'
export KUBEJOB_BENCHMARK_RABBITMQ='amqp://kubejob:kubejob-dev@localhost:5672/'
```

Durable PostgresManaged baseline:

```bash
dotnet run --project tests/KubeJob.Benchmark/KubeJob.Benchmark.csproj -c Release -- \
  --jobs 5000 --warmup 200 --work-ms 0 \
  --submitters 16 --worker-concurrency 64 \
  --scenarios Parallel --out bench-managed.md
```

BrokerNative baseline with the same workload size and concurrency:

```bash
dotnet run --project tests/KubeJob.Benchmark/KubeJob.Benchmark.csproj -c Release -- \
  --runtime BrokerNative \
  --jobs 5000 --warmup 200 --work-ms 0 \
  --submitters 16 --worker-concurrency 64 \
  --out bench-broker-native.md
```

For a useful comparison matrix, repeat both commands at 10k/50k/100k jobs and
with `--work-ms 0`, `1`, and `10`. Compare throughput and tail latency; for the
managed run also compare DB connection pressure. BrokerNative intentionally has
no PostgreSQL hot-path connection metric because PostgreSQL is not configured in
that runner.

Managed scenarios provision a fresh PostgreSQL database and remove it in a
`finally` block. Ingress mode creates isolated RabbitMQ ingress topology.
BrokerNative creates unique exchange/queue names per run and removes them on a
best-effort basis after the host stops.

## Parameters

Every existing flag keeps its `KUBEJOB_BENCH_*` environment-variable form;
command-line arguments override environment values.

| Flag | Default | Meaning |
|---|---:|---|
| `--runtime` | `PostgresManaged` | `PostgresManaged` or `BrokerNative`; env: `KUBEJOB_BENCH_RUNTIME` |
| `--jobs` | 2000 | Measured jobs |
| `--warmup` | 100 | Warmup jobs |
| `--work-ms` | 0 | Simulated handler delay |
| `--submitters` | 16 | Concurrent submitter workers |
| `--mode` | `TypedClient` | Managed only: `TypedClient` or `Ingress` |
| `--worker-concurrency` | 128 | Worker capacity |
| `--outbox-concurrency` | 4 | Managed only: outbox publisher workers |
| `--outbox-batch` | 512 | Managed only: outbox claim batch size |
| `--completion-flush-ms` | 2 | Managed only: completion batcher flush window |
| `--poll-ms` | 100 | Managed only: dashboard completion poll interval |
| `--run-timeout-s` | 180 | Scenario timeout |
| `--scenarios` | all | Managed only: comma-separated scenario names |
| `--hotkey-count` | 4 | Managed KeyOrdered hot-key cardinality |
| `--uniform-keys` | 0 | Managed uniform key cardinality; zero means distinct |
| `--synchronous-commit` | `on` | Managed PostgreSQL durable commit |
| `--metrics-ms` | 1000 | Managed metrics sampling interval |
| `--cpu` | on | Managed: set `0` to disable Podman CPU sampling |
| `--container` | `kubejob-dev-postgres-1` | Managed PostgreSQL container for CPU sampling |
| `--postgres` | dev-stack value | Managed PostgreSQL admin connection string |
| `--rabbitmq` | dev-stack value | RabbitMQ AMQP URI |
| `--rabbitmq-mgmt` | `http://localhost:15672` | Managed ingress metrics API base |
| `--out` | — | Optional markdown output path |

Ingress-specific environment variables are
`KUBEJOB_BENCH_INGRESS_BATCH`, `KUBEJOB_BENCH_INGRESS_WAIT_MS`, and
`KUBEJOB_BENCH_INGRESS_PREFETCH`. PostgreSQL durability can also be set with
`KUBEJOB_BENCH_SYNCHRONOUS_COMMIT=on|off`.

The harness is intentionally separate from `dotnet test` and has no
BenchmarkDotNet dependency.
