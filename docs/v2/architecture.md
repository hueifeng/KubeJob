# KubeJob Runtime Architecture

> The `docs/v2` directory name is retained for link compatibility. The runtime described here is the current V3 Single Authority architecture.

## Product boundary

KubeJob is a typed, embeddable, distributed background-job runtime for .NET. It is not a Kubernetes replacement, workflow-history engine, message broker, actor runtime, or CI/CD system.

The implementation is layered as:

```text
KubeJob.Core
    ↑
KubeJob.ControlPlane ← KubeJob.Storage.PostgreSQL
    ↑                         ↑
KubeJob.Server ───────────────┘
    ↑
KubeJob.Transport.*
```

Transport-specific concepts such as RabbitMQ exchanges, delivery tags, publisher confirms, retry queues, DLX and DLQ stay inside transport adapters.

## Single Authority rule

Every logical Job Queue selects exactly one execution authority:

```text
QueueRuntimeMode.PostgresManaged
QueueRuntimeMode.BrokerNative
```

A Queue never uses RabbitMQ to deliver a task and then asks PostgreSQL to approve that same delivery. That legacy dual-authority BrokerDispatch path has been removed.

### PostgresManaged

PostgreSQL owns execution state and eligibility:

```text
IJobClient
   ↓
JobControlPlane
   ↓
PostgreSQL JobRun
   ↓
Claim / Attempt / Lease
   ↓
WorkerExecutionEngine
   ↓
Handler
   ↓
Durable Completion
```

PostgresManaged provides:

- durable `JobRun` / `JobAttempt` state;
- worker sessions, epochs and fencing;
- claim and lease renewal;
- durable cancellation;
- strong per-Run status and attempt history;
- managed retry policy;
- continuation and compensation;
- database-owned `KeyOrdered` and `StrictFifo` ordering.

Workers request work only when they have free execution slots. PostgreSQL claims eligible Runs with transactional locking and `FOR UPDATE SKIP LOCKED`.

### BrokerNative

The selected message transport owns delivery, redelivery and retry:

```text
IJobClient
   ↓
IMessageTransportPublisher
   ↓
Transport
   ↓
Transport Consumer
   ↓
WorkerExecutionEngine
   ↓
Handler
   ↓
ACK / Retry / DLQ
```

A normal BrokerNative execution does **not**:

- create a PostgreSQL `JobRun`;
- call control-plane admission;
- create a managed `JobAttempt`;
- acquire or renew a KubeJob database lease;
- synchronously persist completion before broker ACK.

RabbitMQ is the first implemented BrokerNative transport. Kafka, SQS, Redis Streams, Pulsar and other adapters are extension targets, not current built-in features.

BrokerNative is at-least-once. External side effects must therefore tolerate duplicate execution. KubeJob currently has no BrokerNative Inbox/deduplication store, so `JobEnqueueOptions.IdempotencyKey` is rejected for BrokerNative rather than pretending that carrying a key in the message provides duplicate suppression.

## Shared execution engine

Both runtime modes converge on the same transport-neutral execution pipeline:

```text
WorkerExecutionEngine
├── DI scope
├── payload deserialization
├── middleware
├── timeout / cancellation
├── handler invocation
├── telemetry
└── normalized outcome classification
```

The execution engine does not know about PostgreSQL leases, RabbitMQ ACKs, broker delivery tags, or storage completion. Runtime coordinators translate an execution result into their own authority-specific completion action.

## Job and Event semantics

KubeJob separates command/job semantics from publish/subscribe semantics.

### Job Queue

A logical Queue is a competing-consumer pool:

```text
logical queue
     │
 ┌───┼───┐
 ▼   ▼   ▼
W1  W2  W3
```

Worker replicas do not receive private queues. One Job delivery is processed by one worker replica.

### Event Topic

An Event Topic fans out to independent Subscriptions:

```text
Topic
 ├─ Subscription A → queue → workers A1..An
 ├─ Subscription B → queue → workers B1..Bn
 └─ Subscription C → queue → workers C1..Cn
```

Retries are Subscription-scoped. A failure in Subscription A must never republish the event to the Topic and replay already-successful Subscriptions B and C.

## Public submission model

```text
JobKey<TPayload>    stable handler identity
IKubeJob<TPayload>  constructor-injected handler
IJobClient          enqueue plus managed observation/cancellation
IEventBus           publish/subscribe event surface
IJobScheduleClient  durable schedule definitions
```

`JobHandle` identifies the result of a submission:

- PostgresManaged: `JobId` is a durable Run id.
- BrokerNative: `JobId` is a transport message id.

`JobHandle.RuntimeMode`, `TransportId`, `SupportsStrongStatus` and `SupportsStrongCancellation` let callers distinguish these capabilities without changing the existing positional constructor.

Strong `IJobClient.GetStatusAsync` and `CancelAsync` semantics belong to PostgresManaged. BrokerNative does not fabricate a synchronous PostgreSQL Run projection.

## Batch submission

`JobRuntimeOptions.MaxSubmissionBatchSize` bounds both runtime modes.

For PostgresManaged, `EnqueueBatchAsync` validates the batch then persists the independent Runs atomically in one state-store transaction.

For BrokerNative, a batch is **not atomic**. All items are validated and serialized before the first publish. If a transport implements `IMessageTransportBatchPublisher`, KubeJob may amortize durable broker acknowledgements across the batch. A broker/network failure can still leave a confirmed prefix published.

This API is not a durable `JobBatch` aggregate: it does not provide batch lifecycle, group status, `MaxParallelism`, sharding, broadcast or fan-in semantics.

## Managed durable model

```text
JobSchedule ──creates──> JobRun ──contains──> JobAttempt
                              │
                              └── current Attempt lease/fencing pointer

Worker ──starts──> WorkerSession ──temporarily owns──> JobAttempt
```

- **JobRun** is the logical managed request and survives retries or worker changes.
- **JobAttempt** is one physical managed execution.
- **WorkerId** is stable deployment identity.
- **SessionId/Epoch** identify one worker process lifetime.
- **LeaseToken** fences a specific Attempt ownership period.
- **JobSchedule** is an independent durable definition.

## Managed ordering

PostgresManaged queues default to `Parallel`.

### KeyOrdered

`KeyOrdered` requires a non-empty `ConcurrencyKey`. PostgreSQL prevents a later committed Run with the same Queue/key ordering domain from claiming before its earlier non-terminal predecessor.

Different keys remain parallel.

### StrictFifo

`StrictFifo` serializes the managed Queue/lane through PostgreSQL's ordering gate. It is not implemented by reviving the removed RabbitMQ lane/admission model.

BrokerNative ordering must be provided by transport-native partition or single-consumer semantics. The current V3 RabbitMQ Job runtime does not expose a managed `ConcurrencyKey` ordering guarantee, so the client rejects that option.

## Managed completion and fencing

A PostgresManaged completion is accepted only when the active Run/Attempt/session/epoch/lease identity still matches. A stale worker cannot overwrite a newer session.

Handlers must honor `JobExecutionContext.CancellationToken`. At-least-once still applies: a handler that ignores cancellation can continue external side effects after its lease is lost and a replacement attempt starts.

## Retry and lease recovery

For PostgresManaged, retryable failure closes the current Attempt and returns the Run to `Pending` with a future `AvailableAt`. A later claim creates a new Attempt. Lease reconciliation can cancel, requeue or dead-letter the managed Run according to durable state and attempt budget.

For BrokerNative, the transport adapter owns the retry handoff. The RabbitMQ adapter publishes the retry copy, waits for publisher confirmation, and only then ACKs the original delivery. Worker/process loss leaves the original delivery unacked so RabbitMQ can redeliver it.

## Work-available wake hints

PostgreSQL remains the only execution authority for PostgresManaged. The current implementation writes `WorkAvailable` rows to the internal outbox and publishes them through `IWorkAvailableNotifier` as optional wake hints.

These notifications do **not** grant ownership. Worker polling remains the correctness path if a wake notification is lost.

Because the wake hint is non-authoritative, this durable outbox is an optimization boundary rather than part of the Single Authority requirement. It may be replaced by a cheaper best-effort or database-native wake mechanism without changing correctness.

## Scheduling

Schedule definitions remain durable in PostgreSQL and are claimed with recoverable ownership plus optimistic versions.

At fire time:

- **PostgresManaged** creates a durable occurrence Run and advances the schedule cursor transactionally.
- **BrokerNative** creates a deterministic occurrence/message id, publishes the self-contained message through the selected transport, waits for publisher confirmation, then advances the schedule cursor.

A crash after BrokerNative publish confirmation but before cursor commit may redeliver the same deterministic occurrence id. This is an intentional at-least-once trade-off and is safer than advancing the cursor before publish succeeds.

Policies requiring strong Run state, such as `SkipIfRunning`, remain PostgresManaged-only.

## Dashboard boundary

The Dashboard reads managed state through runtime query interfaces. It does not own execution state and it does not expose RabbitMQ physical queue/exchange topology as the product model.

Its logical Queue view should focus on:

```text
Queue
Runtime authority
Transport (BrokerNative only)
Managed ordering policy
Worker/session health
```

BrokerNative does not currently provide a strong per-message lifecycle in the managed Dashboard unless a separate asynchronous projection is added in the future.

## Deployment modes

### Unified managed host

```text
ASP.NET process
├── control plane
├── PostgresManaged worker
├── shared WorkerExecutionEngine
├── optional Dashboard
└── PostgreSQL
```

### Distributed managed host

```text
API/client ──HTTP──> control-plane replicas ──PostgreSQL
workers ──HTTP claim/renew/complete──────────> control-plane replicas
```

### BrokerNative worker pool

```text
producer ──IMessageTransportPublisher──> broker
worker replicas ──consume───────────────> broker
worker replicas ──WorkerExecutionEngine→ handlers
```

The BrokerNative data plane does not require an `IWorkerRuntimeClient` or PostgreSQL connection for normal execution.

## Version boundary

The active runtime is V3 Single Authority. Historical V2/BrokerDispatch ADRs remain in the repository only as decision history. ADR 015 is the current architecture decision.
