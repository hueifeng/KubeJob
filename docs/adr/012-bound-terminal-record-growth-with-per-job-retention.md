# ADR 012: Bound terminal record growth with per-job retention

## Status

Accepted (2026-07-26) — an initial conservative reaper is implemented through
`RuntimeRetentionService`. It removes published Outbox rows and terminal Runs
without idempotency or schedule identity. Keyed terminal history remains until
idempotency tombstones are implemented. This ADR continues to define the
full per-job retention direction. Relates to [ADR 004](004-postgresql-source-of-truth-and-outbox.md)
(PostgreSQL authoritative) and [ADR 011](011-hide-physical-delivery-from-job-submitters.md)
(BrokerDispatch delivery profile).

## Context

KubeJob persists `JobRun` and `JobAttempt` in PostgreSQL as the authoritative
ledger (ADR 004). The cost of that ledger depends on how long terminal records
are kept, and that question becomes acute for the high-throughput execution
dispatch mode (ADR 011, `ExecutionDeliveryProfile.BrokerDispatch`).

Two layers of "recording" must be separated:

- **Live state** — `Pending`/`Running` Run, active `Attempt`, `Lease`, and
  `Outbox` rows. This is the state machine itself. It is already self-cleaning
  and bounded: `Outbox` rows are marked/deleted after publish, expired leases
  are reaped, and schedules are deletable. Steady-state size is roughly
  TPS × average run lifetime (1000 TPS × ~1s ≈ 1k rows; even a 30s timeout
  caps it near 30k).
- **Terminal history** — `Succeeded`/`Dead` Run and completed `Attempt`s kept
  for audit and the Dashboard. This is the layer that grows at TPS: ~86M
  runs/day at 1000 TPS. The run payload (stored on the Run, fetched at
  admission in BrokerDispatch mode) dominates storage — potentially hundreds
  of GB/day — far more than the metadata rows.

Published Outbox rows and unkeyed terminal Runs now have a conservative
retention path. Keyed terminal history is still retained because deleting it
without idempotency tombstones would allow a late duplicate message to create a
new Run. The remaining data-volume concern for keyed high-TPS workloads is
therefore intentionally unresolved until tombstones are implemented.

The question arose whether to stop recording code-driven (high-TPS) submissions
and record only scheduled executions.

## Decision

Bound only terminal history, never live state. Recording every active Run —
code-driven or scheduled — is mandatory and stays.

### 1. Live state is mandatory and already bounded

The persisted `JobRun` is the correctness anchor for:

- **Idempotency** — a redelivered submission resolves to an existing Run by
  idempotency key, so it does not execute twice.
- **Targeted admission** — the BrokerDispatch envelope carries `RunId` only;
  the worker admits by `RunId` against the authoritative Run.
- **Retry** — after a crashed or failed attempt the Run returns to `Pending`
  and is redispatched.
- **Fencing** — a stale worker's completion is rejected by lease-token match.

Not recording a code-driven Run removes all four guarantees. The §7
high-throughput mode is itself a code/ingress-driven high-TPS path whose entire
correctness depends on the persisted Run. If a job genuinely needs none of
these guarantees (downstream is idempotent on its own key, broker redelivery +
DLQ is sufficient, no observability required), it should bypass KubeJob and
publish straight to the broker. There is no safe middle ground that keeps
KubeJob's guarantees without the record.

### 2. Terminal history is optional and bounded by per-job retention

Add a `TerminalRecordReaper` hosted service with the same shape as
`LeaseReaper` and `OutboxPublisher`: periodically call
`IJobRuntimeStores.DeleteTerminalAsync(olderThan, batchSize)` to delete
terminal Run + cascaded Attempts older than a per-job retention window.
Retention is a global default with per-`JobKey` overrides:

- **Scheduled jobs** — low volume, high audit value ("did the 09:00 reconciliation
  run?") → **tiered** retention (hot detailed, cold aggregated); see §4 below.
  Compliance-critical schedules override to keep-all.
- **High-TPS code/ingress jobs** — high volume, low per-run audit value → short
  retention (e.g., 1–24h after terminal) or delete-on-terminal.
- **Failure bias** — `Dead`/retried records are retained longer than `Succeeded`
  (post-mortem and SLA), so retention may be shorter for successes than for
  failures.

### 3. Store contract and payload

- Add `IJobRuntimeStores.DeleteTerminalAsync(olderThan, batchSize)` and
  implement it on every adapter (PostgreSQL, In-memory). The PostgreSQL impl
  deletes Attempts whose Run is terminal and older than the cutoff, then the
  Runs, indexed on `(Status, TerminalAt)`; the reaper is batched, rate-limited,
  and idempotent.
- The run payload is the dominant storage cost. Short retention removes it
  quickly. Optionally, terminal rows may drop or externalize payload and keep
  a slim tombstone for the retention window, or split hot (active) / cold
  (terminal) tables. This is an implementation choice, not a decision-level
  requirement.

### 4. Scheduled jobs use tiered retention, not flat delete

Scheduled occurrences are low volume (thousands/day, not thousands/sec), so
retention is comparatively easy — but "most appropriate" is still bounded, not
"keep everything forever." Scheduled retention is tiered:

- **Schedule definitions** (cron spec, last/next fire, enabled state) are kept
  forever; they are tiny metadata.
- **Occurrence Runs** are retention-bound and tiered:
  - **Hot window** (default 30 days, per-schedule override): keep per-occurrence
    Run + Attempts + payload — full Dashboard detail ("did the 09:00 report run,
    what was the output, how many attempts, latency").
  - **Cold window** (default 1 year, per-schedule override): roll
    hot-window-expired **successes** up to a daily aggregate per schedule
    (count, succeeded, failed, max latency, last failure summary) — about
    365 rows/schedule/year.
  - **Failure bias**: `Dead`/retried occurrences are kept detailed into the cold
    window (not rolled up), for post-mortem and SLA.
- Compliance-critical schedules (e.g., financial reconciliation) override to
  `KeepAll` (no roll-up) or a long hot window.
- A scheduled-specific reaper `RollupScheduleAsync(scheduleId, olderThanDetailed)`
  writes the daily aggregate and deletes the detailed Run + Attempts for
  successes past the hot window; it is separate from the generic terminal reaper
  because roll-up (write-then-delete) differs from flat delete.

At thousands/day, full detail for 30 days is ~30k rows (negligible); cold
aggregates for 1 year are ~365 rows/schedule (negligible). Audit value is
preserved (a year-long "did it run" view) without unbounded growth.

## Consequences

### Positive

- Storage is bounded regardless of TPS, while the state machine and its
  guarantees are preserved unchanged.
- Scheduled and high-throughput jobs get different retention without different
  correctness semantics — the axis is retention, not whether to record.
- Dashboard history for high-TPS jobs no longer depends on retaining every row
  (see Observability below).

### Costs

- A new reaper hosted service and a new store method on every adapter.
- A `(Status, TerminalAt)` index and batched deletes add write load; the reaper
  must be rate-limited and idempotent to avoid contention with live traffic.
- A per-job retention config surface (global default + `JobKey` overrides).
- Deleting terminal records removes per-run Dashboard history for those jobs;
  an aggregate metrics projection is required to preserve observability.

## Observability under retention

Deleting terminal runs deletes their per-row Dashboard history. For high-TPS
jobs, keep a separate aggregate projection (`Kj2_JobMetrics`:
`JobKey × hour × count / succeeded / dead / p99 latency / last-N failures`)
written at completion; this table survives the reaper. The Dashboard renders
high-TPS jobs from the aggregate plus a recent-failures list, and renders
low-frequency jobs from per-run rows. This is the correct form of "record a
summary, not every row" — applied to **terminal history**, never to live state.

## Rejected alternatives

### Do not record code-driven submissions; record only scheduled executions

Rejected because the persisted `JobRun` is the correctness anchor for
idempotency, admission, retry, and fencing. The §7 high-throughput mode is
itself a code/ingress-driven high-TPS path whose entire correctness depends on
the persisted Run. Not recording removes the guarantees; if a job genuinely
needs no durability, it should bypass KubeJob and publish directly to the
broker. There is no safe middle ground that keeps KubeJob's guarantees without
the record.

### One global retention for every job

Rejected because ordinary/scheduled jobs (low volume, audit-worthy) and
high-throughput jobs (high volume, low per-run value) have different retention
needs. A single value either over-retains high-TPS data or under-retains audit
history.

### Keep all records forever and absorb storage growth

Rejected because terminal growth is unbounded at high TPS; operational cost and
query degradation make it unsustainable for the workloads this ADR exists to
support.