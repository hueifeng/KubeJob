# KubeJob throughput benchmark

This on-demand console harness measures the current PostgresManaged runtime:

```text
submit → optional transactional work-available wake outbox → PostgreSQL claim/lease
      → handler → durable completion
```

`TypedClient` submits through `IJobClient`. The optional `Ingress` mode sends
business messages through RabbitMQ's ingress adapter before they enter the
same managed runtime. BrokerNative execution is exercised by the dedicated
RabbitMQ integration project; this harness intentionally reports a managed
baseline rather than pretending ingress traffic is BrokerNative execution.

## What it measures

Each scenario reports ingest TPS, database-recorded E2E TPS, wall-clock E2E TPS,
P50/P95/P99 latency, PostgreSQL connection peaks, process memory/allocation,
thread counts, and optional best-effort CPU samples. RabbitMQ queue metrics are
reported when ingress mode is enabled.

PostgreSQL `synchronous_commit` is **on by default**. That keeps the benchmark
aligned with normal durable production semantics. `--synchronous-commit off`
is available only for an explicitly labeled throughput experiment; results from
that mode must not be compared with durable production results as if the
semantics were equivalent.

## Scenarios

- `Parallel` — no `ConcurrencyKey` and parallel queue ordering.
- `KeyOrderedUniform` — a large key space, usually one key per run.
- `KeyOrderedHotKey` — a small key space (four keys by default).
- `StrictFifo` — one logical queue ordered globally.

Each scenario uses a separate logical queue. The harness uses one explicit
managed `ExecutionLane` only as a worker-eligibility label. There is no
`--lanes` sweep because V3 does not map `ConcurrencyKey` values onto a hidden
set of physical lanes or broker queues.

## Running

Start the development dependencies first:

```bash
bash scripts/dev-stack.sh up
export KUBEJOB_BENCHMARK_POSTGRES='Host=localhost;Port=5432;Username=kubejob;Password=kubejob-dev;Database=postgres'
export KUBEJOB_BENCHMARK_RABBITMQ='amqp://kubejob:kubejob-dev@localhost:5672/'
dotnet run --project tests/KubeJob.Benchmark/KubeJob.Benchmark.csproj -c Release -- \
  --jobs 5000 --warmup 200 --worker-concurrency 64 \
  --scenarios Parallel,KeyOrderedHotKey --out bench-results.md
```

Each scenario provisions a fresh PostgreSQL database and removes it in a
`finally` block. Ingress mode also creates and removes a unique RabbitMQ
exchange/queue pair.

## Parameters

Every flag has the corresponding `KUBEJOB_BENCH_*` environment variable; flags
override environment values.

| Flag | Default | Meaning |
|---|---:|---|
| `--jobs` | 2000 | Measured jobs per scenario |
| `--warmup` | 100 | Warmup jobs on a separate queue |
| `--work-ms` | 0 | Simulated handler delay |
| `--submitters` | 16 | Concurrent submitter workers |
| `--mode` | `TypedClient` | `TypedClient` or `Ingress` |
| `--worker-concurrency` | 128 | Managed worker capacity |
| `--outbox-concurrency` | 4 | Outbox publisher workers |
| `--outbox-batch` | 512 | Outbox claim batch size |
| `--completion-flush-ms` | 2 | Completion batcher flush window |
| `--poll-ms` | 100 | Dashboard completion poll interval |
| `--run-timeout-s` | 180 | Scenario timeout |
| `--scenarios` | all | Comma-separated scenario names |
| `--hotkey-count` | 4 | Hot-key cardinality |
| `--uniform-keys` | 0 | Uniform key cardinality; zero means distinct |
| `--synchronous-commit` | `on` | PostgreSQL durable commit; set `off` only for explicitly labeled throughput experiments |
| `--metrics-ms` | 1000 | Metrics sampling interval |
| `--cpu` | on | Set to `0` to disable Podman CPU sampling |
| `--container` | `kubejob-dev-postgres-1` | Container name for CPU sampling |
| `--postgres` | dev-stack value | PostgreSQL admin connection string |
| `--rabbitmq` | dev-stack value | RabbitMQ AMQP URI (ingress mode) |
| `--rabbitmq-mgmt` | `http://localhost:15672` | RabbitMQ management API base |
| `--out` | — | Optional markdown output path |

Ingress-specific environment variables are
`KUBEJOB_BENCH_INGRESS_BATCH`, `KUBEJOB_BENCH_INGRESS_WAIT_MS`, and
`KUBEJOB_BENCH_INGRESS_PREFETCH`. PostgreSQL durability can also be set with
`KUBEJOB_BENCH_SYNCHRONOUS_COMMIT=on|off`.

The harness uses one unified host and the production in-process worker
transport, so it measures managed pipeline behavior without localhost HTTP
overhead. It is intentionally separate from `dotnet test` and has no
BenchmarkDotNet dependency.
