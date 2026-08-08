# KubeJob V3 Runtime Architecture

KubeJob V3 is built around one rule: **one logical Queue has one execution authority**.

## Runtime modes

| Runtime | Authority | Durable Run/Attempt | Strong status/cancel | Normal execution DB dependency |
| --- | --- | --- | --- | --- |
| `PostgresManaged` | PostgreSQL | Yes | Yes | Yes |
| `BrokerNative` | Message transport | No | No | No |

Runtime mode and transport are separate concepts. `BrokerNative` currently has a RabbitMQ adapter; Kafka, SQS, Redis Streams and other transports are extension targets, not implemented features.

## PostgresManaged

PostgreSQL owns queue state and execution ownership:

```text
IJobClient
  -> JobRun
  -> worker Claim
  -> Attempt + Lease + Fence
  -> WorkerExecutionEngine
  -> handler
  -> durable completion
```

This mode is intended for workloads that need durable lifecycle state, cancellation, retries, worker fencing, `KeyOrdered`/`StrictFifo`, continuations or compensation.

Workers periodically poll PostgreSQL. A deployment may configure a work-available notifier to reduce claim latency. The notifier is only a hint: losing it cannot lose the Job. When no notifier is configured, V3 does not need a `WorkAvailable` outbox row for every submission.

## BrokerNative

The transport owns delivery and retry:

```text
IJobClient
  -> IMessageTransportPublisher
  -> broker
  -> transport consumer
  -> WorkerExecutionEngine
  -> handler
  -> ACK / retry / DLQ
```

The normal path does not create or claim a PostgreSQL `JobRun`, create an Attempt, acquire a database lease, call admission, or write synchronous completion state.

BrokerNative is **at-least-once**. A publish failure, connection loss, worker crash or retry handoff can produce duplicate delivery. Business side effects must therefore be idempotent. `IdempotencyKey` is carried in the BrokerNative message as metadata; KubeJob V3 does not currently provide a BrokerNative deduplication store.

`JobHandle.JobId` is the durable Run id in PostgresManaged and the transport MessageId in BrokerNative. `IJobClient.GetStatusAsync` and `CancelAsync` currently provide the strong PostgresManaged contract only; V3 does not yet include a BrokerNative history or queued-cancel projection.

## Shared execution engine

Both runtimes converge on `WorkerExecutionEngine` for:

- DI scope creation
- payload deserialization
- execution middleware
- handler invocation
- timeout and cancellation tokens
- telemetry
- exception classification

Storage/broker coordination remains outside the engine.

## Job Queue semantics

A Job Queue is a competing-consumer pool:

```text
logical queue
    |
    +-- worker replica A
    +-- worker replica B
    +-- worker replica C
```

One Job delivery should be completed by one worker replica. KubeJob does not create a private queue per worker.

The default RabbitMQ BrokerNative queue is parallel. The adapter therefore does **not** advertise ordered execution merely because RabbitMQ preserves queue order at the broker. Concurrent consumers and concurrent handlers can complete out of order. BrokerNative ordering must be an explicit transport-native capability/policy.

## Event semantics

Events use Topic + Subscription semantics rather than Job Queue semantics:

```text
Topic: order.events
RoutingKey: order.created
       |
       +-- Subscription: business -> queue -> worker replicas
       +-- Subscription: audit    -> queue -> worker replicas
       +-- Subscription: cleanup  -> queue -> worker replicas
```

Each Subscription receives its own copy. Replicas inside one Subscription compete for that copy.

Retry and DLQ are Subscription-scoped. A failure in `business` returns only to the `business` delivery path; it must not republish to the Topic and replay already-successful subscriptions.

## RabbitMQ retry handoff

For a retryable BrokerNative Job failure, the RabbitMQ adapter:

1. publishes the retry copy;
2. waits for publisher confirmation;
3. ACKs the original delivery only after the retry copy is durably accepted.

If transport infrastructure fails before that handoff completes, the original delivery is left unacked/requeued, preserving at-least-once delivery.

## Producer batching

`IJobClient.EnqueueBatchAsync` has runtime-specific guarantees:

- PostgresManaged uses one bounded database transaction.
- BrokerNative is not atomic. A transport may batch several publishes behind one durability confirmation for throughput, but an error can be observed after a subset or all messages were accepted.

Callers must treat a BrokerNative batch retry as at-least-once.

## Scheduling

Schedule definitions remain durable control-plane resources.

- PostgresManaged fire: create a durable Run.
- BrokerNative fire: publish a self-contained transport message and advance the schedule cursor only after publish confirmation.

Policies that require strong Run state remain PostgresManaged capabilities.

## Compatibility boundary

The current PostgreSQL schema still contains compatibility fields such as `DeliveryProfile` and `TransportId`. New managed writes normalize them to Pull/null. They are not an active broker-routing mechanism and can be removed in a later explicit schema migration.

Historical `docs/v2` material is retained for design history. New runtime behavior is documented under `docs/v3` and ADR 015.
