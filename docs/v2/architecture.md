# KubeJob V2 Runtime Architecture

## Product boundary

KubeJob V2 is a typed, PostgreSQL-first distributed background-job runtime for
.NET. It is not a Kubernetes replacement, workflow-history engine, message
broker, actor runtime, or CI/CD system.

Optional packages may integrate those systems without moving their complexity
into the core handler API.

## Public model

```text
JobKey<TPayload>    stable business identity
IKubeJob<TPayload>  constructor-injected handler
IJobClient          submission/query/cancellation
IJobScheduleClient  independent cron schedule management
```

A handler is not a scheduler, transport consumer, database repository, or worker
registration object.

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

## Retry and lease recovery

Retryable failure closes the current Attempt and returns the same Run to
`Pending` with a future `AvailableAt`. A new Attempt is created on the next
claim.

The lease reconciler closes expired Attempts as `LeaseLost`, then either:

- cancels a cancel-requested Run;
- requeues the Run and writes another Outbox event;
- or marks it `Dead` after the configured attempt limit.

## Schedule reconciliation

Schedules have their own recoverable claims and optimistic version. A reconciler
computes a fire plan using cron plus the configured time zone.

The PostgreSQL fire transaction:

1. verifies ScheduleId, claim token, and expected version;
2. evaluates `SkipIfRunning` inside the transaction;
3. inserts a deterministically identified occurrence Run if required;
4. inserts the Outbox event;
5. advances `NextFireAt` and clears the claim;
6. commits once.

`FireOnce` creates one compensating Run after multiple missed occurrences.
`SkipMissed` advances directly to the next future occurrence.

## Bounded memory

Worker memory is bounded by:

```text
MaxConcurrentJobs
+ bounded execution channel
+ owned Attempt dictionary
+ claim/renewal batches
+ handler registry
```

No in-memory collection grows with historical jobs. History and payloads remain
in durable storage.

## Deployment modes

### Unified

```text
ASP.NET process
├── control plane services
├── in-process worker protocol
├── worker execution engine
└── shared PostgreSQL state store
```

The transport is an interface call, not localhost HTTP.

### Distributed

```text
API / client ──HTTP──> control plane replicas ──PostgreSQL
worker replicas ──HTTP pull/renew/complete──> control plane replicas
```

A worker may contact any healthy control-plane replica because coordination is
transactional in the state store.

## Compatibility window

Legacy tables, services, and non-generic handlers remain available while V2 is
adopted. V2 uses `Kj2_*` tables and explicit registration methods so queues can
be migrated incrementally.
