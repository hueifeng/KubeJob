# ADR 015: Use explicit single-authority runtime modes

## Status

Accepted (2026-08-08). Supersedes [ADR 013](013-defer-broker-authoritative-directpublish-mode.md)
and [ADR 014](014-promote-brokerdispatch-to-default-delivery-profile.md).

## Context

KubeJob originally evolved around a single PostgreSQL-owned execution model. The
later `BrokerDispatch` profile improved discovery by publishing an
`ExecutionEnvelope` through RabbitMQ, but a delivered message still had to call
back into PostgreSQL for admission/claim before the handler could run and again
for durable completion. That preserved one state model, but it also put
PostgreSQL transactions, connection pressure, locking and admission latency in
the broker consumer hot path.

That compromise is a poor fit for workloads that deliberately choose a broker
for high-throughput competing-consumer delivery. It also makes the broker look
like the queue authority while PostgreSQL remains the real execution authority,
which creates two coordination systems on the same hot path.

At the same time, removing PostgreSQL semantics globally would be wrong for
workloads that depend on strong Run/Attempt/Lease/Fence state, durable status,
cancellation, concurrency control and database-owned retries.

The two workload classes therefore need different execution authorities. The
choice must be explicit because their guarantees are intentionally different.
It must not be hidden behind an optimization flag or changed dynamically at
runtime.

## Decision

KubeJob exposes two explicit queue runtime modes and applies a **Single Authority
Principle**: exactly one subsystem owns execution authority for a logical queue.

### `PostgresManaged`

PostgreSQL is the queue and execution authority.

- submission creates durable `JobRun` state;
- workers claim from PostgreSQL;
- `JobAttempt`, lease renewal and fencing protect ownership;
- PostgreSQL owns retry budgets, strong status, cancellation and concurrency
  policy;
- broker notifications, when configured, are wake-up hints only and never grant
  execution authority;
- a deployment that uses the default no-op notifier does not write
  `WorkAvailable` outbox rows merely to wake a PostgreSQL pull worker.

This mode is the default because it provides the strongest framework-managed
semantics.

### `BrokerNative`

The configured message broker is the delivery and execution authority.

- submission publishes a self-contained executable message directly through the
  selected transport;
- the normal consumer hot path performs no PostgreSQL admission/claim;
- workers bound concurrency locally using broker prefetch/dispatch limits and
  worker capacity;
- success is acknowledged to the broker;
- retry handoff is `publish retry -> publisher confirm -> ACK original`;
- worker crashes rely on broker redelivery;
- exhausted retries go to a DLQ;
- delivery is at-least-once, so handlers must be idempotent;
- `JobHandle` identifies the broker message, not a PostgreSQL `JobRun`;
- strong framework-managed status and cancellation are not promised;
- `IdempotencyKey` is metadata unless an application or future optional
  component implements deduplication.

The BrokerNative hot path must remain usable when PostgreSQL/control-plane
services are unavailable.

### Runtime mode and transport are separate choices

Runtime semantics are not encoded in a broker name. A queue resolves both a
runtime mode and, for BrokerNative, a transport identifier such as `rabbitmq`,
`kafka` or `sqs`.

Conceptually:

```text
QueueRuntimeMode.PostgresManaged
QueueRuntimeMode.BrokerNative

TransportId = rabbitmq | kafka | sqs | ...
```

A queue does not automatically fail over from one runtime mode to the other.
Doing so would silently change status, cancellation, retry, deduplication and
ordering guarantees.

### Shared execution engine

Both coordinators translate their ownership model into the same
transport-neutral handler execution contract:

```text
PostgresManaged coordinator ----\
                                 -> IWorkerExecutionEngine -> handler pipeline
BrokerNative coordinator -------/
```

The execution engine owns handler resolution, DI scope, middleware, timeout and
execution telemetry. It does not own PostgreSQL leases or broker ACK/retry
semantics.

### Jobs and events are distinct contracts

`IJobClient.EnqueueAsync` represents competing-consumer background work.
`IEventBus.PublishAsync` represents fan-out to independent subscriptions.

For events, retry is subscription-scoped. A failed subscription must never
republish the event to the topic because that would repeat delivery to
subscriptions that already succeeded.

RabbitMQ physical Event topology is namespaced separately from Job topology so
a Job queue and an Event subscription cannot accidentally resolve to the same
queue/exchange name.

## Consequences

### Positive

- BrokerNative removes PostgreSQL connection, transaction and admission work
  from the steady-state broker consumer hot path.
- PostgresManaged keeps the strong state machine for workloads that actually
  need it.
- authority is visible in configuration and documentation rather than being an
  emergent property of the transport path;
- additional brokers can implement the transport contract without importing
  PostgreSQL admission semantics;
- the handler pipeline remains reusable across runtime modes.

### Trade-offs

- there are intentionally two operational/consistency contracts;
- moving a queue between modes is a semantic migration, not a transparent
  tuning change;
- BrokerNative applications must design handlers for at-least-once execution
  and cannot assume strong KubeJob status/cancel/deduplication;
- PostgresManaged still pays the database coordination cost required by its
  stronger guarantees.

## Migration from `BrokerDispatch`

`BrokerDispatch` is no longer an active execution model. Deployments should
choose explicitly:

1. use `PostgresManaged` when Run/Attempt/Lease/Fence, strong status/cancel,
   concurrency policy or database-owned retry semantics are required; or
2. use `BrokerNative` when broker-native delivery throughput and independence
   from PostgreSQL on the consume path are the priority.

Do not translate `BrokerDispatch` to BrokerNative automatically. The latter has
a different authority and guarantee set.

Legacy database columns/enums may remain temporarily for schema migration or
historical reads, but they must not reintroduce a broker-delivery-then-database-
admission runtime path.

## Rejected alternatives

### Keep BrokerDispatch as the default hybrid path

Rejected. It makes RabbitMQ pay delivery cost while PostgreSQL still grants each
execution, retaining the database bottleneck and two-system coordination on the
hot path.

### Make every queue BrokerNative

Rejected. It would remove strong status, cancellation, lease fencing and
framework-managed concurrency semantics from workloads that rely on them.

### Dynamically switch authority when PostgreSQL or the broker is unhealthy

Rejected. Runtime modes have different correctness contracts. Automatic
failover would silently change semantics and can create duplicate authorities.

### Hide BrokerNative behind a performance flag

Rejected. A flag that changes execution authority also changes guarantees; the
choice must be explicit at the runtime-mode level.
