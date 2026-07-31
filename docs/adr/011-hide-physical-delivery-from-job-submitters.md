# ADR 011: Hide Physical Delivery from Job Submitters

## Status

Accepted — Direct Dispatch implemented via `ExecutionDeliveryProfile.BrokerDispatch`
(see [`docs/v2/logical-architecture.md` Section 7](../v2/logical-architecture.md)
and [`docs/v2/message-transport.md` Direct Dispatch section](../v2/message-transport.md)).
Promoted to the default delivery profile by [ADR 014](014-promote-brokerdispatch-to-default-delivery-profile.md).

## Context

Pull workers, RabbitMQ, Kafka, Consumer Groups, partitions, leases, and
delivery profiles are infrastructure mechanisms. Exposing them in a job
submission request would make every business application depend on deployment
topology and would make transport migration an application change.

The public task model needs a stable logical routing concept, but it does not
need to reveal how a logical queue is physically served.

## Decision

The public job contract exposes logical execution semantics only:

```text
JobKey, Payload, Queue, Priority, NotBefore,
IdempotencyKey, ConcurrencyKey, Retry, Timeout
```

It does not expose:

```text
ExecutionMode, Broker, Topic, ConsumerGroup,
Partition, WorkerId, Lease, DeliveryProfile
```

`Queue` is a stable logical name. An internal `QueueRouter` maps it to a
platform-owned Delivery Profile. The router can use deployment topology,
backlog, Worker capacity, broker health, and latency objectives. A deployment
may pin a profile for operational reasons, but a business caller cannot choose
or override it for an individual Run.

## Consequences

### Positive

- Business code remains independent of RabbitMQ, Kafka, and Worker placement.
- The platform can migrate a Queue from Pull to MQ dispatch without changing
  job producers or handlers.
- Dashboard and status APIs show one logical Run model regardless of delivery.
- Infrastructure policy and application semantics stay at separate seams.

### Costs

- The platform needs Queue Router telemetry and policy management.
- Automatic switching must preserve one Run/Attempt/Lease state machine and
  cannot simply duplicate a task into two transports.
- Operators still need deployment-level controls for capacity and incident
  isolation, even though application users do not see them.

## Rejected alternatives

### Per-job execution mode

Rejected because it leaks infrastructure into application code, makes retries
and observability transport-specific, and complicates migration.

### One global transport for every Queue

Rejected because ordinary tasks and high-throughput tasks have different
latency, throughput, and operational requirements.
