# ADR 015: V3 Single Authority Runtime Model

## Status

Accepted (2026-08-08). Supersedes [ADR 013](013-defer-broker-authoritative-directpublish-mode.md) and [ADR 014](014-promote-brokerdispatch-to-default-delivery-profile.md).

## Context

KubeJob previously combined a message broker with PostgreSQL on the same execution hot path. `BrokerDispatch` published an `ExecutionEnvelope` to RabbitMQ, but the Worker still called PostgreSQL-backed admission/claim APIs before executing, renewed database leases while running, persisted completion, and only then acknowledged the broker delivery.

That model preserved strong PostgreSQL semantics, but it paid for two queueing systems per execution without making the broker the actual execution authority. The database cost was not only storage: worker-session locking, candidate scans, ordering gates, attempt creation, Run updates, lease renewal, completion persistence, and outbox state all remained in the hot path.

KubeJob also needs two distinct messaging semantics:

1. a **Job Queue**, where replicas compete and one delivery is processed by one Worker; and
2. an **Event Topic**, where every Subscription receives an independent delivery while replicas inside one Subscription compete.

Finally, RabbitMQ is only one transport implementation. Runtime semantics must not be defined in terms of RabbitMQ exchanges, queues, DLX, delivery tags, or publisher-confirm APIs.

## Decision

### One logical Queue has one execution authority

KubeJob exposes two explicit runtime modes.

#### PostgresManaged

PostgreSQL is the queue and execution authority.

```text
IJobClient
   ↓
Durable JobRun
   ↓
Claim / Attempt / Lease
   ↓
WorkerExecutionEngine
   ↓
Handler
   ↓
Durable completion
```

PostgresManaged owns:

- `JobRun` and `JobAttempt` state;
- worker sessions and epochs;
- Claim and Lease renewal;
- fencing;
- durable cancellation;
- strong per-Run status;
- managed retry policy;
- continuation and compensation;
- database-owned `KeyOrdered` and `StrictFifo` ordering.

Work-available notification is deliberately non-authoritative. For immediately runnable Runs, the control plane first commits the Run and then sends a best-effort process-local wake through a Queue-coalescing dispatcher. Losing that wake cannot lose the Job because workers continue polling PostgreSQL. Future-dated and explicit recovery/requeue scenarios may still use the compatibility durable WorkAvailable outbox until those lower-frequency paths are migrated separately.

#### BrokerNative

The configured message transport is the delivery and execution authority.

```text
IJobClient
   ↓
IMessageTransportPublisher
   ↓
Transport
   ↓
Transport consumer
   ↓
WorkerExecutionEngine
   ↓
Handler
   ↓
ACK / Retry / DLQ
```

Normal BrokerNative execution does not:

- create a PostgreSQL `JobRun`;
- call database admission;
- create a managed `JobAttempt`;
- acquire or renew a KubeJob database lease; or
- synchronously persist completion before broker acknowledgement.

BrokerNative is at-least-once. External side effects must tolerate duplicate execution. KubeJob does not currently provide a BrokerNative Inbox/deduplication store, so `IdempotencyKey` is rejected rather than implying duplicate suppression that does not exist.

### Runtime mode and transport are separate

`QueueRuntimeMode` describes execution semantics:

- `PostgresManaged`
- `BrokerNative`

Transport selection is independent and goes through transport-neutral contracts. RabbitMQ is the first implemented BrokerNative adapter. Kafka, SQS, Redis Streams, Pulsar, and similar systems are extension targets rather than implied built-in features.

### Shared execution engine

Both runtime modes converge on the same `WorkerExecutionEngine` for DI scope creation, deserialization, middleware, timeout/cancellation, handler invocation, telemetry, and normalized execution outcomes.

Storage/transport coordinators wrap that engine with authority-specific mechanics instead of duplicating handler execution logic.

### Job and Event are separate semantics

A Job Queue is a competing-consumer pool:

```text
logical Queue
    ↓
Worker1 / Worker2 / Worker3
```

An Event Topic fans out to independent Subscriptions:

```text
Topic
 ├─ Subscription A → queue → workers A1..An
 ├─ Subscription B → queue → workers B1..Bn
 └─ Subscription C → queue → workers C1..Cn
```

Retries are Subscription-scoped. A failure in Subscription A must not republish the Topic and replay already-successful Subscriptions B and C.

### Broker topology is an adapter detail

Logical KubeJob concepts are Queue, Topic, Subscription, runtime mode, retry semantics, and transport capabilities. RabbitMQ exchange/binding/retry/DLX/DLQ names remain inside `KubeJob.Transport.RabbitMQ` and must not become the general product model.

### Ordering belongs to the authority

PostgresManaged ordering remains database-owned. BrokerNative ordering must use transport-native partition/single-consumer semantics. KubeJob must not reintroduce PostgreSQL admission merely to provide BrokerNative ordering.

### Scheduling remains control-plane durable

Schedule definitions remain durable in PostgreSQL.

At fire time:

- PostgresManaged creates a durable occurrence Run.
- BrokerNative publishes a deterministic occurrence/message id and advances the schedule cursor only after publisher confirmation.

A crash after publish confirmation but before cursor commit can redeliver the same occurrence id; this is an intentional at-least-once trade-off. Policies requiring strong Run state remain PostgresManaged capabilities.

## Removed architecture

V3 removes the active dual-authority BrokerDispatch data plane, including:

- `BrokerDispatch` execution profile;
- `ExecutionEnvelope`;
- execution admission/batch admission;
- RabbitMQ execution dispatcher and consumer;
- RabbitMQ execution lane router;
- broker cancellation queue/publisher; and
- BrokerDispatch-specific runtime registration/options/tests.

Historical schema columns may remain temporarily for migration compatibility, but active runtime code does not use them to resurrect BrokerDispatch execution.

## Consequences

### Positive

- BrokerNative throughput is decoupled from the PostgreSQL claim state machine.
- Immediate PostgresManaged submission no longer requires one durable wake-outbox row per Run.
- PostgresManaged retains strong database semantics for workloads that need them.
- RabbitMQ becomes an adapter rather than an architecture dependency.
- Job and Event delivery semantics are explicit.
- Transport implementations can advertise capabilities honestly instead of pretending all brokers behave like RabbitMQ.

### Trade-offs

- BrokerNative does not automatically have a strongly consistent `JobRun` lifecycle.
- BrokerNative cancellation is not the same durable contract as PostgresManaged cancellation.
- At-least-once delivery requires idempotent external effects or a future explicit Inbox/deduplication feature.
- Best-effort immediate managed wake can be lost, which may add up to the normal polling delay but cannot lose a durable Run.
- Switching a Queue between runtime modes changes execution semantics and is therefore an explicit deployment decision, not an automatic failover mechanism.

## Validation requirements

The architecture remains healthy only if tests continue to enforce that:

- BrokerNative publish/consume runs without PostgreSQL admission;
- PostgresManaged Claim/Lease/Fencing/Cancel/Ordering remain covered by PostgreSQL integration tests;
- immediate PostgresManaged Submit/Batch does not create one durable wake row per Run;
- delayed/recovery WorkAvailable rows remain recoverable while that compatibility path exists;
- Event retry does not replay sibling Subscriptions;
- transport-specific dependencies do not leak into Core/Worker/ControlPlane runtime contracts; and
- active code contains no BrokerDispatch/ExecutionEnvelope/admission data plane.
