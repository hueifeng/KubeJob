# ADR 010: Dual Execution Adapters, Single State Machine

## Status

Accepted

## Context

KubeJob must support both ordinary background work and high-throughput work
arriving through RabbitMQ, Kafka, or another message broker. A database Pull
worker is easy to reason about and gives the control plane direct ownership of
capacity, leases, retries, and fencing. A broker Consumer Group removes idle
polling and broad queue scans, and is a better delivery mechanism for very
high message rates.

Using two independent task state machines would make Run status, retry
semantics, cancellation, fencing, and Dashboard behavior diverge. Treating a
broker delivery as execution authority would also allow a stale redelivery to
produce an external side effect after a lease takeover.

## Decision

KubeJob will have one authoritative control-plane state machine and two
execution adapters:

1. **Pull execution adapter** — workers claim eligible Runs from the control
   plane using database transactions and leases. (This was the default at the
   time of this ADR; [ADR 014](014-promote-brokerdispatch-to-default-delivery-profile.md)
   promoted BrokerDispatch to the default, making Pull the opt-in per-queue
   profile.)
2. **MQ dispatch execution adapter** — the default since ADR 014. A
   dispatcher publishes an Execution Envelope to a broker Consumer Group.
   The Worker must still pass the control-plane admission/fencing check before
   executing and must persist completion before acknowledging the broker
   message.

Both adapters use the same JobRun, JobAttempt, Worker Session, Execution lease,
Retry, Cancellation, Idempotency, and Dashboard contracts.

The public submission contract contains only logical job semantics. It does not
contain an execution mode, broker name, consumer group, partition, delivery
profile, or transport-specific option. `Queue` is a logical routing name. An
internal Queue Router selects the Delivery Profile from deployment topology,
backlog, capacity, and health. A platform operator may tune that policy at
deployment scope, but an individual application or Run cannot override it.

The existing WorkAvailable notification remains a third, smaller concern: it
is only a wake-up hint for Pull workers and is not an Execution Envelope.

## Consequences

### Positive

- Ordinary and high-throughput jobs have one operator-facing model.
- RabbitMQ, Kafka, and other brokers can be added as adapters without moving
  domain rules into consumers.
- MQ can absorb bursts and remove empty Pull scans for selected queues.
- Stale messages remain harmless because admission and completion are fenced.

### Costs

- MQ dispatch requires an additional outbox/publisher path and admission
  handshake.
- PostgreSQL remains involved in durable Run/Attempt state; MQ does not make
  database writes disappear.
- Exactly-once external side effects remain outside KubeJob and require a
  business idempotency key or application Outbox.
- Retry ownership must be explicit so broker redelivery and KubeJob Retry do
  not multiply each other.

## Rejected alternatives

### Separate broker-native state machine

Rejected because it duplicates Run, Attempt, cancellation, fencing, and
Dashboard semantics and makes the two execution modes behave differently.

### Broker as the only source of truth

Rejected as the default because unconsumed work becomes difficult to query and
operate consistently, and broker-specific delivery semantics leak into the
public job model.

### MQ only as a wake-up signal for every workload

Rejected because it adds operational cost without reducing the authoritative
database Claim work for ordinary queues.
