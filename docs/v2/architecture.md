# KubeJob Runtime Architecture

> Historical V2 architecture. For supported V3 behavior, see the
> [V3 runtime model](../v3/runtime-model.md).

## Product boundary

KubeJob is a typed, PostgreSQL-first distributed background-job runtime for
.NET. It is not a Kubernetes replacement, workflow-history engine, message
broker, actor runtime, or CI/CD system.

Optional packages may integrate those systems without moving their complexity
into the core handler API.

The implementation is layered as `KubeJob.Core` contracts, the independent
`KubeJob.ControlPlane` runtime, ASP.NET transport/Dashboard adapters in
`KubeJob.Server`, storage adapters such as PostgreSQL, and optional transport
packages such as RabbitMQ. `KubeJob.Server` assembles these modules but does not
own the durable state machine implementation.

The project dependency direction follows the same boundary:

```text
KubeJob.Core
    ↑
KubeJob.ControlPlane ← KubeJob.Storage.PostgreSQL
    ↑                         ↑
KubeJob.Server ───────────────┘
```

`KubeJob.Storage.PostgreSQL` exposes a service-collection registration seam;
`KubeJob.Server` owns the `KubeJobServerOptions` composition wrapper. Storage
must not depend on ASP.NET Server types or the Server project.

For the end-to-end topology and request sequences, see
[Logical Architecture and Sequences](./logical-architecture.md).

## Public model

```text
JobKey<TPayload>    stable business identity
IKubeJob<TPayload>  constructor-injected handler
IJobClient          submission/query/cancellation
IJobScheduleClient  independent cron schedule management
```

A handler is not a scheduler, transport consumer, database repository, or worker
registration object.

## Control-plane module seam

Transport adapters do not orchestrate stores directly. They converge on three
control-plane modules:

```text
typed IJobClient ─┐
HTTP jobs API ────┼──> JobControlPlane ─────> submission/query stores
message ingress ──┤
Dashboard actions ┘

HTTP runtime API ─┐
in-process worker ┼──> WorkerControlPlane ───> session/claim/completion stores

typed schedules ──┐
HTTP schedules API┼──> ScheduleControlPlane ─> schedule store
Dashboard actions ┘
```

These modules own validation, conversion to durable commands, runtime limits,
cron calculation, and public snapshots. HTTP status codes, JSON serialization,
broker acknowledgements, and worker transport remain adapter concerns.

The modules are concrete classes rather than a second set of nearly identical
interfaces. Storage behavior is the real varying seam and already has in-memory
and PostgreSQL adapters. This keeps one implementation behind multiple callers
without introducing pass-through abstractions.

## Submission batch boundary

`IJobClient.EnqueueBatchAsync` is a bounded admission operation for independent
Runs. The control plane validates every request before calling
`IJobSubmissionStore.SubmitBatchAsync`; the store then persists the new Runs and
their Outbox rows in one transaction, preserving input order and per-item
idempotency. `JobRuntimeOptions.MaxSubmissionBatchSize` defaults to 256 and
protects adapter allocation, validation cost, transaction duration, and rollback
size. Typed and broker-ingress adapters apply the same boundary before creating
translated command arrays; the control plane repeats it as the authoritative
check.

The in-process client and HTTP client use the same contract. HTTP callers use
`POST /api/kubejob/jobs/batch`, so the remote adapter does not silently degrade
the operation into unbounded concurrent single-row requests. This is still not
a durable `JobBatch` aggregate: there is no batch lifecycle, group status,
`MaxParallelism`, sharding, or broadcast policy. Those remain the separate
roadmap feature described in [roadmap.md](./roadmap.md).

## Durable model

```text
JobSchedule ──creates──> JobRun ──contains──> JobAttempt
                              │
                              └── current Attempt lease/fencing pointer

Worker ──starts──> WorkerSession ──owns temporarily──> JobAttempt
```

- **JobRun** is the logical request. It survives retries and worker changes.
- **JobAttempt** is one physical execution.
- **WorkerId** is stable deployment identity.
- **SessionId/Epoch** identify one process lifetime.
- **LeaseToken** fences a specific Attempt ownership period.
- **JobSchedule** is an independent template that creates Runs.

## State source and transport

PostgreSQL is the authoritative state source. A submission transaction inserts:

```text
Kj2_JobRuns
Kj2_Outbox   (BrokerDispatch-profile queues only)
```

The outbox row is written only for `BrokerDispatch`-profile queues; `Pull`
submissions skip it because workers discover claimable runs directly via the
control plane. The Outbox publisher converts the durable row to an execution
envelope through the selected transport adapter.

The Outbox publisher invokes `IWorkAvailableNotifier`. The default implementation
is a no-op because workers periodically pull. MQ adapters may wake workers sooner,
but notification delivery is never the source of execution state.

## Pull scheduling

A worker reports its queues and handler capabilities, but the control plane
recomputes actual free capacity from `MaxConcurrency - active Attempts`.

The PostgreSQL claim transaction:

1. locks the Worker Session row;
2. verifies SessionId/Epoch and `Ready` state;
3. computes server-side capacity;
4. selects eligible Runs with `FOR UPDATE SKIP LOCKED`;
5. serializes identical concurrency keys with transaction advisory locks;
6. inserts Attempts and leases;
7. updates each Run's current Attempt pointer;
8. commits once.

No central in-memory scheduler is required, and multiple control-plane replicas
may claim concurrently.

## Key-ordered queues

Queues default to `Parallel`. A deployment may set a queue's
`QueueDeliveryOptions.Queues[queue].OrderingMode` to `KeyOrdered`; every submission to that queue
must then provide `ConcurrencyKey`. PostgreSQL assigns each Run an immutable
`OrderingSequence` when it is persisted. A claim may execute a key-ordered Run
only when no non-terminal earlier Run with the same queue, execution lane, and
key exists.

This is a durable ordering gate over committed-visible Runs: broker prefetch,
redelivery, retry delay, and worker failover cannot allow a later committed Run
to overtake a visible predecessor. Concurrent submissions that have not yet
committed are outside the database visibility contract, so a commit gap can
still produce an inverted sequence. Different keys and execution lanes remain
parallel. A retry therefore blocks only its own key within its lane, rather than
the whole logical queue.

Schedules use the same Run policy boundary. `CronScheduleOptions.ConcurrencyKey`
is copied to every occurrence Run; a Schedule targeting a `KeyOrdered` queue
must provide a non-empty key or the control plane rejects the Schedule. The
Schedule also persists its per-run `RetryPolicy`, `Continuation`, and
`Compensation`, so an occurrence does not silently fall back to a different
submission contract.

## Strict-FIFO queues

A queue may instead set `OrderingMode` to `StrictFifo`: the entire queue (or
lane) is processed one Run at a time — a Run is never claimed while a
non-terminal predecessor on the same queue and lane exists, equivalent to
prefetch=1 on every consumer. The claim gate verifies OrderingSequence
monotonicity like `KeyOrdered`, but it is keyed on the whole lane rather than
per `ConcurrencyKey`, so no key is required. RabbitMQ deployments must satisfy
`ValidateStrictFifoPolicy`: `UseSingleActiveConsumer` enabled
(x-single-active-consumer), `PrefetchCount` = 1, and `ExecutionLaneCount` = 1
for global FIFO; the transport fails startup with a clear error otherwise.

## Completion fencing

A completion is accepted only when all durable identity and ownership values
still match and the lease has not expired. Worker registration and completion
use the same WorkerId advisory-lock key, so a newly registered Session prevents
the old Session from committing afterward.

Database state is written before transport acknowledgement. Duplicate delivery
therefore becomes a harmless lookup of an already terminal Run.

A Worker whose heartbeat is rejected stops claiming, cancels its local Attempts,
and fails its hosted service so the process supervisor can restart it with a new
SessionId. It does not continue polling with a fenced identity. If a handler
ignores cancellation, the worker still fails its hosted service after the
configured drain timeout so the supervisor can interrupt it.

Handlers must honor the `CancellationToken` in their `JobExecutionContext`.
The token links the attempt timeout, the session fence, and worker drain.
Delivery is at-least-once: an attempt whose handler ignores cancellation may
keep running after its lease expires and the control plane has requeued the
Run, so a later attempt can execute concurrently until the old handler returns
or its process is restarted.

## Retry and lease recovery

Retryable failure closes the current Attempt and returns the same Run to
`Pending` with a future `AvailableAt`. A new Attempt is created on the next
claim.

The lease reconciler closes expired Attempts as `LeaseLost`, then either:

- cancels a cancel-requested Run;
- requeues the Run and writes another Outbox event;
- or marks it `Dead` after the configured attempt limit.

When a Run reaches a terminal state, configured Continuation and Compensation
actions are created in the same durable transaction. A terminal Run reached by
retry exhaustion is eligible for `OnAnyTerminal` continuation and compensation;
a cancel does not fire either action. The current contract rejects cross-Queue
terminal actions until their separate Queue delivery target can be resolved and
persisted. Each child Run records `ParentRunId` and `RelationKind`; lineage is
therefore a storage contract shared by the in-memory reference adapter and
PostgreSQL, not an adapter-specific metadata convention.

## Schedule reconciliation

Schedules have recoverable claims and optimistic versions. A reconciler computes
a fire plan using cron plus the configured time zone.

The PostgreSQL fire transaction:

1. verifies ScheduleId, claim token, and expected version;
2. evaluates `SkipIfRunning` inside the transaction;
3. inserts a deterministically identified occurrence Run if required;
4. inserts the Outbox event;
5. advances `NextFireAt` and clears the claim;
6. commits once.

`FireOnce` creates one compensating Run after multiple missed occurrences,
but only while the miss is still within the configured
`ScheduleMisfireThreshold` (default 1 hour); a stale miss — e.g. a schedule
that was disabled for a long time and re-enabled, whose `NextFireAt` still
points at the old due time — is skipped like `SkipMissed` instead of
backfilling an outdated occurrence. `SkipMissed` advances directly to the next
future occurrence. Failed claim processing backs off with
`ScheduleFailureDelay` jittered to `[0.5, 1.5] x` so a recovering database
blip does not re-synchronize every schedule and control-plane instance onto
the same retry instant.

## Dashboard query boundary

The embedded Dashboard reads through `IJobRuntimeDashboardStore` and
`IJobQueryStore`; it does not reach into storage repositories or worker protocol
credentials.

It exposes:

```text
Overview and Queue backlog
Run list and Run detail
Attempt timeline
Worker Session state and capacity
Schedule state and policies
```

List pages use payload-free Run projections; full Payload JSON is fetched only
when an operator explicitly opens one Run detail page and the host has enabled
payload display. Runs are paginated. Worker Session and Schedule views have
configurable hard limits. PostgreSQL supplies indexes for the list filters and
sort order.

The Dashboard has no public CDN dependency, is read-only by default, and hides
Payload JSON by default. A host may bind it to a named ASP.NET Core authorization
policy and explicitly enable payload or mutation capabilities. LeaseToken and
fencing credentials are never rendered.

## Bounded process memory

Worker process memory is bounded by:

```text
MaxConcurrentJobs
+ bounded execution channel
+ owned Attempt dictionary
+ claim/renewal batches
+ handler registry
+ bounded persisted failure details
```

Production history and Payloads remain in PostgreSQL and are not accumulated by
Worker processes. The in-memory provider intentionally retains process-local
state and is intended for development, tests, and small ephemeral deployments;
it is not a durable production history store.

## Deployment modes

### Unified

```text
ASP.NET process
├── control plane services
├── in-process worker protocol
├── worker execution engine
├── optional Dashboard
└── shared PostgreSQL state store
```

The transport is an interface call, not localhost HTTP.

### Distributed

```text
API / client ──HTTP──> control plane replicas ──PostgreSQL
worker replicas ──HTTP pull/renew/complete──> control plane replicas
operators ──HTTP──> protected Dashboard route
```

A worker may contact any healthy control-plane replica because coordination is
transactional in the state store.

Business-message ingress is a separate adapter role. A generic RabbitMQ or Kafka
ingress adapter submits an `EnqueueJobRequest` through `JobControlPlane`, uses
the broker message identity as the idempotency key, and acknowledges or commits
only after durable acceptance. It never grants execution ownership.

## Version boundary

The runtime is V2-only. The previous non-generic handler API, push dispatcher,
JobSpec/WorkerNode model, legacy tables, and legacy Dashboard are not registered
or supported as a compatibility path.
