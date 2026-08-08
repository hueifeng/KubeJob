# ADR 014: Promote BrokerDispatch to the default execution delivery profile

## Status

Superseded by [ADR 015: V3 Single Authority Runtime Model](015-v3-single-authority-runtime-model.md).

Original decision date: 2026-07-31.

## Historical decision

This ADR promoted `BrokerDispatch` as the default discovery/delivery profile while PostgreSQL remained the sole execution authority.

The intended path was:

```text
PostgreSQL durable Run
   ↓
Outbox / ExecutionEnvelope
   ↓
RabbitMQ
   ↓
Worker
   ↓
PostgreSQL admission / Claim / Lease
   ↓
Handler
   ↓
PostgreSQL completion
   ↓
RabbitMQ ACK
```

The goal was to reduce unscoped polling by using RabbitMQ to target claimable Runs while keeping the existing Run/Attempt/Lease/Fencing model unchanged.

## Why the decision changed

Although BrokerDispatch improved discovery, it still required both RabbitMQ and PostgreSQL for every execution. PostgreSQL remained responsible for admission, worker-session checks, ordering, attempt creation, lease renewal, retry state, cancellation, and completion. The broker therefore added an additional delivery system without removing the database queue/state-machine cost from the hot path.

V3 replaces BrokerDispatch with two explicit Single Authority modes:

- `PostgresManaged`: PostgreSQL is the queue and execution authority. Optional wake notifications are hints only.
- `BrokerNative`: the transport is the delivery/execution authority. Normal execution does not perform PostgreSQL admission, Run/Attempt creation, Lease renewal, or synchronous completion persistence.

The legacy RabbitMQ execution dispatcher/consumer, `ExecutionEnvelope`, admission APIs, execution lane router, broker cancellation queue, and BrokerDispatch configuration surface were removed.

## Historical value

This ADR remains useful as the record of why targeted broker discovery was tried. Its key lesson is that reducing empty polling is not enough if the complete database execution state machine is still required after every broker delivery.

For the current architecture, see ADR 015.
