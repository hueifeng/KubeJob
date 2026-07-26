# KubeJob Runtime Architecture

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
Kj2_Outbox
```

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

## Completion fencing

A completion is accepted only when all durable identity and ownership values
still match and the lease has not expired. Worker registration and completion
use the same WorkerId advisory-lock key, so a newly registered Session prevents
the old Session from committing afterward.

Database state is written before transport acknowledgement. Duplicate delivery
therefore becomes a harmless lookup of an already terminal Run.

A Worker whose heartbeat is rejected stops claiming, cancels its local Attempts,
and fails its hosted service so the process supervisor can restart it with a new
SessionId. It does not continue polling with a fenced identity.

## Retry and lease recovery

Retryable failure closes the current Attempt and returns the same Run to
`Pending` with a future `AvailableAt`. A new Attempt is created on the next
claim.

The lease reconciler closes expired Attempts as `LeaseLost`, then either:

- cancels a cancel-requested Run;
- requeues the Run and writes another Outbox event;
- or marks it `Dead` after the configured attempt limit.

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

`FireOnce` creates one compensating Run after multiple missed occurrences.
`SkipMissed` advances directly to the next future occurrence.

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
