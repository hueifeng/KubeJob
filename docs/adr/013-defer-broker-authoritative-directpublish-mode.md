# ADR 013: Defer broker-authoritative DirectPublish execution mode

## Status

Superseded by [ADR 015: V3 Single Authority Runtime Model](015-v3-single-authority-runtime-model.md).

Original decision date: 2026-07-26.

## Historical decision

This ADR originally decided **not** to build a Run-less, broker-authoritative execution path. At the time, KubeJob treated PostgreSQL as the universal durable execution authority and expected high-throughput workloads to use `BrokerDispatch` plus bounded retention.

The deferred DirectPublish concept would have let a broker own delivery/retry while a KubeJob Worker invoked the typed handler without `JobRun`, `JobAttempt`, Claim, or Lease state.

## Why the decision changed

V3 implementation work showed that the relevant cost was not only terminal-history storage. The previous `BrokerDispatch` path put both RabbitMQ and the PostgreSQL state machine on every execution hot path:

```text
broker delivery
   ↓
PostgreSQL admission / session checks / claim
   ↓
Attempt + Run updates
   ↓
Handler
   ↓
PostgreSQL completion
   ↓
broker ACK
```

That path paid for worker-session locking, candidate/ordering checks, attempt creation, lease state, completion persistence, and broker delivery for the same execution.

V3 therefore adopted two explicit authorities instead of one universal ledger:

- `PostgresManaged` for durable Run/Attempt/Lease/Fencing/Cancel semantics.
- `BrokerNative` for transport-authoritative at-least-once execution without synchronous PostgreSQL admission.

This is broader and more explicit than the original DirectPublish proposal: BrokerNative remains part of KubeJob's typed handler, middleware, telemetry, scheduling, and transport abstraction while deliberately exposing different guarantees.

## Historical value

The concerns recorded by this ADR still matter. BrokerNative must not silently pretend to provide managed idempotency, strong cancellation, or strong per-Run status. Those capabilities must either be implemented explicitly for BrokerNative or rejected/documented as unavailable.

For the current decision, see ADR 015.
