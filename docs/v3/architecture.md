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

Worker shutdown/drain is not persisted as an artificial Job cancellation. It is propagated back to the runtime coordinator so PostgresManaged lease recovery or broker redelivery can recover ownership. A handler-thrown `OperationCanceledException` is only classified as a KubeJob timeout when the runtime timeout token actually fired.

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

### Durable subscription provisioning

RabbitMQ exchanges do not retain events by themselves. A durable Subscription queue must exist **before** an event is published if the deployment expects the event to accumulate while handler workers are offline.

Normal Event workers register `AddKubeJobEventHandler(...)` and `AddRabbitMqKubeJobEventConsumer(...)`; the consumer registration provisions all durable Subscription/Retry/DLQ topology before starting consumption.

For deployment-time provisioning without a handler worker, register topology-only subscriptions and the RabbitMQ provisioner:

```csharp
services.AddKubeJobEventSubscription(
    EventKey<OrderCreated>.Create("order.events", "order.created"),
    "audit");

services.AddRabbitMqKubeJobEventTopologyProvisioner(options =>
{
    options.ConnectionString = rabbitMqConnectionString;
});
```

This is the supported way to create durable Subscription queues before publishers are enabled. Publishing to a Topic that has never had its intended Subscription queue provisioned follows normal broker pub/sub semantics: there is no queue in which that subscriber's copy can be retained.

### RabbitMQ physical namespace isolation

Job and Event physical topology must never alias. Job Queue names remain backward-compatible (`kubejob.<logical-queue>` by default). Event exchanges/queues use a reserved `~` structural boundary, for example:

```text
kubejob.eventx~order.events
kubejob.eventsub~order.events~audit
kubejob.eventretryq~order.events~audit
kubejob.eventdlq~order.events~audit
```

Logical KubeJob names do not permit `~`, and RabbitMQ `QueuePrefix`/`ExchangeName` also reject it. This prevents a Job Queue such as `order.audit` from colliding with Event Topic `order` / Subscription `audit`, and prevents a Topic such as `jobs` from colliding with the Job exchange.

### Event topology migration note

The post-merge V3 hardening release changes **Event physical names only** to enforce the namespace isolation above. Existing BrokerNative Job Queue names are unchanged.

If an environment already created the earlier Event queues, upgrade deliberately:

1. stop or quiesce Event publishers for the affected Topics;
2. provision the new Event topology with the target Subscription definitions;
3. inspect old Subscription/Retry/DLQ queues for pending messages;
4. drain, replay, or explicitly discard those messages according to business semantics;
5. start consumers on the new topology;
6. resume publishers;
7. delete old Event topology only after confirming it is no longer needed.

KubeJob does not automatically move messages between old and new RabbitMQ queues because doing so would silently choose replay/duplication semantics on behalf of the application.

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

The same configured `MaxSubmissionBatchSize` bounds both runtimes. For BrokerNative this bounds serialization memory, publisher-lock hold time, and one publisher-confirm window as well as protecting callers from accidentally creating extremely large application batches.

Callers must treat a BrokerNative batch retry as at-least-once.

RabbitMQ caches successful Job Queue/Event Topic declarations for the current publisher channel lifetime so normal publish does not pay a synchronous topology-declare RPC for every message. The cache is discarded when the channel/connection is rebuilt or an unroutable mandatory Job publish is observed.

## Scheduling

Schedule definitions remain durable control-plane resources.

- PostgresManaged fire: create a durable Run.
- BrokerNative fire: publish a self-contained transport message and advance the schedule cursor only after publish confirmation.

BrokerNative occurrence MessageIds are deterministic for `(ScheduleId, ScheduledFor)`. A crash after broker confirmation but before cursor commit can therefore replay the same occurrence with the same MessageId rather than losing the occurrence by advancing the cursor first.

Policies that require strong Run state remain PostgresManaged capabilities.

## Compatibility boundary

The current PostgreSQL schema still contains compatibility fields such as `DeliveryProfile` and `TransportId`. New managed writes normalize them to Pull/null. They are not an active broker-routing mechanism and can be removed in a later explicit schema migration.

Historical `docs/v2` material is retained for design history. New runtime behavior is documented under `docs/v3` and ADR 015.
