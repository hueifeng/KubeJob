# ADR 007: MQ notifications do not own jobs

- Status: Accepted
- Date: 2026-07-23

## Context

RabbitMQ and similar brokers can reduce empty polling latency, but treating a
broker delivery as job ownership reintroduces dual-write, duplicate delivery,
acknowledgement, cancellation, retry, and stale-worker consistency problems.

## Decision

The first MQ integration is notification-assisted pull.

- PostgreSQL stores accepted Runs, Attempts, leases, and the transactional
  Outbox.
- The Outbox publishes queue-specific `work-available` hints.
- Workers in the same configured `ConsumerGroup` compete on one queue per
  logical KubeJob queue. Separate groups receive independent copies.
- A hint only releases a bounded wake signal and triggers the normal Claim.
- Consumers acknowledge a hint after the local wake signal is accepted.
- Publishers confirm broker acceptance before marking an Outbox row published.
- RabbitMQ delivery tags are never KubeJob fencing credentials.

## Consequences

- Duplicate notifications produce extra Claim calls, not duplicate ownership.
- Competing consumers avoid waking every Worker for one available Run.
- Missing notifications fall back to periodic pull.
- Broker outage affects latency rather than durability.
- Unified mode normally does not need a broker.
- A future full broker transport must be a separate design and cannot silently
  replace this state model.
