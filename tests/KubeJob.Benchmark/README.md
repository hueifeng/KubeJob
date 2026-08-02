# KubeJob throughput benchmark

A repeatable harness that drives the full KubeJob durable pipeline — submission
→ transactional outbox → RabbitMQ execution dispatch → worker admission/lease
→ completion — and reports throughput, latency, and DB/broker telemetry for
three ordering scenarios.

The harness lives in `tests/KubeJob.Benchmark/` as a console app (kept out of
the test runner so it runs on demand, not on every `dotnet test`). It builds a
unified control-plane + worker host backed by PostgreSQL and RabbitMQ, using
the in-process worker transport for claim/lease/completion so the RabbitMQ
dispatch path is the transport under test (no localhost HTTP).

## What it measures

Per scenario:

- **Ingest TPS** — jobs submitted per second (submit-phase wall clock).
- **E2E TPS (server)** — jobs completed per second computed from the DB-recorded
  `CreatedAt`/`CompletedAt` timestamps: `N / (max(CompletedAt) - min(CreatedAt))`.
  This excludes client polling jitter.
- **E2E TPS (wall)** — `N / (submit-start → completion-detected)` wall clock.
  Excludes database provisioning, host startup, and warmup so it isolates the
  measured pipeline (the `Duration` column still reports the whole scenario).
- **Latency P50/P95/P99 (ms)** — nearest-rank percentiles of per-run
  `CompletedAt - CreatedAt` (server-side, no extra dependency).
- **DB connection count (max)** — `pg_stat_activity` rows for the bench database
  (the sampler's own connection is tagged and excluded).
- **RabbitMQ ready / unacked (max)** — via the management HTTP API on `:15672`
  (`/api/queues/%2F/<queue>`), summed over the execution queue (and the ingress
  queue when using `Ingress` mode). Requires the management plugin, which the
  dev compose (`rabbitmq:4-management-alpine`) enables.
- **PostgreSQL CPU (avg %)** — best-effort `podman stats --no-stream --format
  "{{.CPUPerc}}" <container>`. Skipped silently if podman or the container is
  unavailable; disable with `--cpu 0` or `KUBEJOB_BENCH_CPU=0`. `ps`-based CPU
  is not available for the containerized Postgres.

## Scenarios

- `Parallel` — no `ConcurrencyKey`, queue runs `ExecutionOrderingMode.Parallel`.
- `KeyOrderedUniform` — `KeyOrdered` with a distinct key per run (one large key
  space); isolates the per-key ordering-gate overhead under low contention.
- `KeyOrderedHotKey` — `KeyOrdered` with a small key space (default 4); exposes
  per-key contention that serializes execution.

Ordering mode is a deployment-level queue policy (`QueueDeliveryOptions`), so
each scenario maps to its own logical queue configured at host build time. The
per-submission `ConcurrencyKey` selects the KeyOrdered partition.

## Submission modes

- `TypedClient` (default) — `IJobClient.EnqueueAsync`, the production .NET
  client entry. Latency `CreatedAt → CompletedAt` is accurate.
- `Ingress` — publish `RabbitMqJobIngressEnvelope` JSON to the ingress
  exchange, exercising the ingress micro-batcher. Note: `CreatedAt` is set
  when the micro-batcher durably submits, so the reported latency **excludes**
  ingress micro-batch dwell time; compare `Ingest TPS` between modes to see the
  micro-batcher's effect.

## How to run

The dev stack must be up (PostgreSQL + RabbitMQ). The harness reuses the same
connection-string env vars as the integration tests:

```bash
# 1. Start dependencies (do NOT run Podman from inside the harness; do this yourself)
bash scripts/dev-stack.sh up

# 2. Set connection strings (defaults already match compose.yaml, so this is optional)
export KUBEJOB_BENCHMARK_POSTGRES="Host=localhost;Port=5432;Username=kubejob;Password=kubejob-dev;Database=postgres"
export KUBEJOB_BENCHMARK_RABBITMQ="amqp://kubejob:kubejob-dev@localhost:5672/"
export KUBEJOB_BENCHMARK_RABBITMQ_USER=kubejob
export KUBEJOB_BENCHMARK_RABBITMQ_PASSWORD=kubejob-dev

# 3. Run all three scenarios with defaults
dotnet run --project tests/KubeJob.Benchmark/KubeJob.Benchmark.csproj -c Release

# 4. Or sweep parameters from the command line (args override env, which overrides defaults)
dotnet run --project tests/KubeJob.Benchmark/KubeJob.Benchmark.csproj -c Release -- \
  --jobs 5000 --warmup 200 --worker-concurrency 64 --prefetch 64 \
  --outbox-concurrency 16 --scenarios Parallel,KeyOrderedHotKey \
  --out bench-results.md
```

Run it in a **quiet environment** — concurrent work (other agents, tests,
builds) skews the numbers. The harness does not run Podman; it only connects to
the already-running stack.

Each scenario run provisions a fresh `kubejob_bench_<guid>` database and a
unique RabbitMQ consumer group, then drops the database and deletes the broker
topology in a `finally` block, so the harness is re-runnable and leaves no
state behind.

## Parameter matrix

Every parameter has an env-var form and a `--flag value` form (flags win).

| Flag | Env var | Default | Meaning |
|---|---|---|---|
| `--jobs` | `KUBEJOB_BENCH_JOBS` | 2000 | Measured jobs per scenario |
| `--warmup` | `KUBEJOB_BENCH_WARMUP` | 100 | Warmup jobs (separate queue, discarded) |
| `--work-ms` | `KUBEJOB_BENCH_WORK_MS` | 0 | Simulated handler work per job (0 = pipeline ceiling) |
| `--submitters` | `KUBEJOB_BENCH_SUBMITTERS` | 16 | Concurrent enqueues / publishes |
| `--mode` | `KUBEJOB_BENCH_MODE` | TypedClient | `TypedClient` or `Ingress` |
| `--worker-concurrency` | `KUBEJOB_BENCH_WORKER_CONCURRENCY` | 32 | Worker `MaxConcurrentJobs` |
| `--prefetch` | `KUBEJOB_BENCH_PREFETCH` | 32 | RabbitMQ execution consumer prefetch |
| `--dispatch-concurrency` | `KUBEJOB_BENCH_DISPATCH_CONCURRENCY` | 32 | RabbitMQ consumer dispatch concurrency |
| `--outbox-concurrency` | `KUBEJOB_BENCH_OUTBOX_CONCURRENCY` | 8 | Outbox publisher concurrency |
| `--outbox-batch` | `KUBEJOB_BENCH_OUTBOX_BATCH` | 128 | Outbox batch size |
| `--publisher-concurrency` | `KUBEJOB_BENCH_PUBLISHER_CONCURRENCY` | 8 | RabbitMQ dispatcher publisher concurrency |
| `--poll-ms` | `KUBEJOB_BENCH_POLL_MS` | 200 | Completion-poll interval |
| `--status-parallelism` | `KUBEJOB_BENCH_STATUS_PARALLELISM` | 32 | (reserved) status poll parallelism |
| `--run-timeout-s` | `KUBEJOB_BENCH_RUN_TIMEOUT_S` | 180 | Per-scenario timeout |
| `--scenarios` | `KUBEJOB_BENCH_SCENARIOS` | all | Comma list, e.g. `Parallel,KeyOrderedUniform,KeyOrderedHotKey` |
| `--hotkey-count` | `KUBEJOB_BENCH_HOTKEY_COUNT` | 4 | Key space for the hot-key scenario |
| `--uniform-keys` | `KUBEJOB_BENCH_UNIFORM_KEYS` | 0 | Key space for the uniform scenario (0 = distinct per run) |
| `--metrics-ms` | `KUBEJOB_BENCH_METRICS_MS` | 1000 | Metrics sample interval |
| `--cpu` | `KUBEJOB_BENCH_CPU` | on | `0` disables Podman CPU sampling |
| `--container` | `KUBEJOB_BENCH_POSTGRES_CONTAINER` | kubejob-dev-postgres-1 | Podman container name for CPU |
| `--postgres` | `KUBEJOB_BENCHMARK_POSTGRES` | (dev stack) | PostgreSQL admin connection string |
| `--rabbitmq` | `KUBEJOB_BENCHMARK_RABBITMQ` | (dev stack) | RabbitMQ AMQP URI |
| `--rabbitmq-mgmt` | `KUBEJOB_BENCHMARK_RABBITMQ_MANAGEMENT` | http://localhost:15672 | Management API base URI |
| `--out` | — | — | Optional path to write a markdown results table |

Ingress-specific: `KUBEJOB_BENCH_INGRESS_BATCH` (100),
`KUBEJOB_BENCH_INGRESS_WAIT_MS` (5), `KUBEJOB_BENCH_INGRESS_PREFETCH` (200).

## Output

A per-scenario block is printed to the console, followed by a markdown table
(suitable for pasting into a PR/issue). With `--out`, the markdown is also
written to that file.

## Notes and limitations

- **Single unified host**: worker count is 1 process with `MaxConcurrentJobs`
  as the concurrency knob. The in-process worker transport means claim/lease/
  completion add no HTTP overhead; multi-process worker scaling is out of
  scope (a future scenario would switch to the HTTP worker transport).
- **Completion tracking** pages the dashboard store filtered by the scenario
  queue (the dashboard query returns the full filtered count, no recent cap),
  so observer cost scales with `jobs / 100` pages per poll. For very large
  `--jobs`, raise `--poll-ms` to keep observer overhead low.
- **LaneCount sweep**: `TODO(item 1: lanes)` in `PipelineBenchmark.BuildHost`.
  `ExecutionLaneCount` is being added in parallel and is not in the tree yet,
  so the harness compiles against the current API and has nothing to sweep;
  the TODO marks exactly where the lane-count parameter should be threaded
  through the routing/dispatch options once it lands.
- **No new dependencies**: no BenchmarkDotNet; percentiles are computed from a
  sorted sample array with nearest-rank. `Npgsql` and `RabbitMQ.Client` arrive
  transitively from the storage/transport project references.