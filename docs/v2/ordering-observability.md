# Ordering state observability

KubeJob enforces per-key FIFO ordering in the control plane (the durable
`KeyOrdered` claim gate + `OrderingSequence`), not at the broker. The broker
only wakes workers and, with execution lanes, co-locates same-key runs. This
page describes the metrics that make the ordering backlog and wait observable.

## Metrics

All metrics are emitted by the `KubeJob.ControlPlane` meter and tagged with
`kubejob.queue`.

| Metric | Kind | Unit | Meaning |
|---|---|---|---|
| `kubejob.control_plane.ordering.wait_duration` | histogram | `s` | Wall-clock time a KeyOrdered Run waited before admission (claim time minus `AvailableAt`), including time blocked behind a non-terminal same-key predecessor. Recorded only for KeyOrdered runs at `WorkerControlPlane.AdmitAsync`. Parallel runs are not recorded. |
| `kubejob.control_plane.ordering.blocked_runs` | observable gauge | `{run}` | KeyOrdered Pending Runs currently blocked behind a non-terminal same-key predecessor, per queue. |
| `kubejob.control_plane.ordering.oldest_blocked_age` | observable gauge | `s` | Age of the oldest blocked KeyOrdered Run per queue. |
| `kubejob.control_plane.ordering.active_keys` | observable gauge | `{key}` | Distinct `ConcurrencyKey`s with at least one non-terminal KeyOrdered Run per queue (the in-flight "hot" keys). |

### No full-table-scan on scrape

The three gauges are **observable** instruments backed by a cached snapshot.
A background `OrderingMetricsRefreshService` (hosted) refreshes the cache every
`JobRuntimeOptions.OrderingBacklogRefreshInterval` (default 5s) by calling
`IJobRuntimeDashboardStore.GetOrderingBacklogAsync`. A metrics scrape returns
the cached value and never runs a database query.

* **In-memory store**: scans `_runs` (bounded; dev/test only).
* **PostgreSQL**: a single window-function query over the partial index
  `IX_Kj2_JobRuns_KeyOrderedHead ON (Queue, ConcurrencyKey, OrderingSequence)
  WHERE OrderingMode = 1 AND Phase IN (0, 1)` — only KeyOrdered non-terminal
  rows, never a full table scan. The head (lowest `OrderingSequence`) of each
  `(Queue, ConcurrencyKey)` group is the claimable run; every successor
  (`rn > 1`) is counted as blocked, and its age is `clock_timestamp() -
  AvailableAt` clamped at 0.

### Interpreting the backlog

* `blocked_runs` rising per queue ⇒ the KeyOrdered gate is doing real work; a
  same-key predecessor is still non-terminal while successors wait.
* `oldest_blocked_age` is the SLO-impacting signal: how long the next-in-line
  has been stuck behind an uncompleted predecessor.
* `active_keys` is the hot-key cardinality. Combined with `blocked_runs`, a
  few active keys with many blocked runs ⇒ a single slow key is the bottleneck
  (consider splitting the key or raising predecessor throughput); many active
  keys with few blocked runs ⇒ healthy fan-out.

## Dashboard

The run detail page surfaces `ConcurrencyKey`. `OrderingMode` /
`OrderingSequence` columns on the dashboard list/detail views are a follow-up
TODO; the data lives on `JobRunRecord`, and `DashboardRunDetails` already
exposes `ConcurrencyKey`.

## Per-lane backlog

Per-lane queue depth is a transport-level metric (RabbitMQ ready/unacked per
lane queue) and is tracked with the execution-lane feature (see
`docs/v2/message-ordering-research.zh-CN.md`). It is intentionally separate
from the control-plane ordering gauges above, which report per logical queue.