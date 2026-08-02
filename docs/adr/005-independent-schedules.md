# ADR 005: Model schedules independently from handlers

- Status: Accepted
- Date: 2026-07-23

## Context

A handler may have multiple schedules, no schedule, or schedules managed at
runtime. Storing cron, time zone, retry, and concurrency behavior in the handler
attribute couples business code to deployment policy.

## Decision

`JobSchedule` is an independent durable resource that references a stable
`JobKey` and a payload snapshot. It owns cron, time zone, queue, misfire policy,
concurrency policy, retries, timeout, enabled state, and next-fire cursor.

A recoverable schedule claim and optimistic version coordinate multiple control
planes. Advancing the cursor, creating an occurrence Run, and writing its Outbox
message are one transaction.

## Consequences

- One handler can have many schedules.
- Operators can enable, disable, or change schedules without changing handler
  code.
- Every occurrence is traceable by ScheduleId and ScheduledFor.
- Misfire and overlap semantics are explicit.
- The legacy attribute-based cron no longer exists: `KubeJobAttribute`
  carries only the stable `Key`.
