# ADR 004: Keep PostgreSQL authoritative and treat MQ as notification

- Status: Accepted
- Date: 2026-07-23

## Context

Creating a durable job and publishing a broker message is a dual-write problem.
Using broker acknowledgement as job state also makes cancellation, retries,
leases, dashboard queries, and stale completion dependent on transport details.

## Decision

PostgreSQL is the source of truth for Runs, Attempts, Worker Sessions, Schedules,
and current state. Submission and retry transactions write an Outbox record with
the durable state change.

`IWorkAvailableNotifier` publishes an asynchronous wake-up hint. Missing or
duplicate notifications do not authorize execution; workers always claim from
the authoritative store.

Outbox publication itself uses a recoverable claim lease so a publisher crash
cannot leave a message permanently stuck in `Publishing`.

## Consequences

- Broker outages delay wake-up but do not lose accepted jobs.
- MQ adapters can be added without changing handlers or `IJobClient`.
- Duplicate publication is expected and harmless.
- PostgreSQL must be sized and indexed for active coordination traffic.
- Full broker-as-queue mode is a separate future transport with stricter
  consistency requirements, not the default implementation.
