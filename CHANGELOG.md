# KubeJob core/performance foundation patch

Base reviewed commit: `19886d34443eab398a61ce91638bfa61ce5b2877` (`main`).

## Correctness

- Aligns in-memory and PostgreSQL status transitions with `Pending -> Assigned -> Running -> terminal`.
- Uses `Canceled` instead of incorrectly recording canceled work as `Failed` or `Succeeded`.
- Rejects reports from the wrong worker and stale/duplicate state reports.
- Prevents retention cleanup from deleting active runs.
- Restores offline-node runs only when they are `Assigned` or `Running`.
- Fixes Dashboard trigger/toggle route attributes.
- Passes the actual shard count into `KubeJobContext`.

## Memory, throughput, and resource usage

- Bounds dispatcher and worker polling batches.
- Worker polls with its actual number of available execution slots.
- Uses a semaphore for strict local concurrency enforcement.
- Caches discovered job types instead of rescanning every loaded assembly for every execution.
- Reuses pooled HTTP handlers through `IHttpClientFactory`.
- Removes the unused per-heartbeat allocation of all running job IDs.
- Replaces `ContinueWith` cleanup with an explicit tracked execution lifecycle.
- Stops accepting jobs before graceful shutdown and waits for active jobs.
- Parses worker labels once per dispatch iteration and selects the least-loaded node without temporary LINQ lists and sorting for every run.
- Adds PostgreSQL indexes for pending dispatch, worker polling, history cleanup, retries, heartbeats, and cron scans.

## Storage

- Replaces hard-coded numeric status assumptions with enum-derived parameters.
- Migrates legacy UTC timestamps to `TIMESTAMPTZ` idempotently.
- Adds bounded `LIMIT` clauses and deterministic ordering to hot queue queries.

## Validation

- Adds in-memory repository tests for assignment, guarded transitions, cancellation, cleanup safety, and worker recovery.
- This environment does not contain the .NET SDK, so `dotnet test` still needs to run in CI or a local .NET 9 environment.
