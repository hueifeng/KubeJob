# KubeJob Queue Routing Completion Implementation Plan

> **For Hermes:** Execute with source-first, test-first vertical slices. Preserve unrelated existing working-tree changes.

**Goal:** Complete logical Queue routing across ordinary submission, Cron, persistence, Group/Lane/Transport selection, Worker capability admission, and StrictFifo configuration.

**Architecture:** Keep Queue as the business-visible logical identity. Resolve a canonical QueueDefinition at control-plane submission/schedule creation, persist the resolved delivery target with the Run and Outbox, keep ConsumerGroup transport-owned but explicitly selected by a persisted target, and keep ExecutionLane as a separate logical eligibility/isolation field. RabbitMQ maps the persisted target to its physical topology. Worker registration/admission validates the same profile rather than relying on deployment convention.

**Verification:** Add focused regression tests first, run them RED, implement minimally, then run the affected test projects and a solution build. Do not claim integration coverage unless PostgreSQL/RabbitMQ dependencies are actually available.

## Vertical slices

1. Canonical Queue contract and policy-key validation.
2. Cron schedule target capture and Run/Outbox propagation in InMemory and PostgreSQL.
3. Explicit Group/Lane target model and RabbitMQ route selection.
4. Worker capability/profile validation and admission behavior.
5. StrictFifo configuration validation and unused-strategy cleanup/boundary.
6. Full test/build verification and documentation alignment.
