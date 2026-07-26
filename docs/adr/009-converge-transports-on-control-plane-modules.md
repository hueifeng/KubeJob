# ADR 009: Converge transports on control-plane modules

- Status: Accepted
- Date: 2026-07-26

## Context

Job submission, Worker runtime, and Schedule lifecycle behavior had two callers
each:

- HTTP controllers and in-process typed clients;
- the HTTP Worker protocol and the unified in-process Worker transport;
- the HTTP Schedule protocol and the in-process typed Schedule client.

Those callers independently performed validation, calculated policy values,
invoked stores, and mapped durable records. The duplicate paths could drift.
Adding RabbitMQ or Kafka ingress directly to the stores would create a third
copy and expose persistence commands as the integration contract.

## Decision

KubeJob introduces three deep control-plane Modules:

- `JobControlPlane` owns raw submission validation, durable submission,
  cancellation, Run snapshots, and Attempt snapshots.
- `WorkerControlPlane` owns Worker registration, Claim limits, lease renewal,
  completion policy, and the configured runtime durations.
- `ScheduleControlPlane` owns Schedule validation, cron/time-zone calculation,
  lifecycle changes, and snapshots.

HTTP controllers, Dashboard mutations, in-process clients, and future
message-ingress Adapters are transport Adapters around these Modules. They own
serialization, authentication, status codes, form feedback, broker
acknowledgement, and offset commits, but not runtime state transitions.

The Modules are concrete classes. A parallel set of C# interfaces would have one
implementation and repeat nearly the same surface. Storage is the real varying
seam and keeps its in-memory and PostgreSQL Adapters.

`ControlPlaneValidationException` marks permanent invalid input with a stable
code. Infrastructure and storage exceptions are not converted to validation
failures.

## Consequences

- Unified and distributed deployments execute the same orchestration.
- RabbitMQ and Kafka ingress can share a durable submission seam without
  depending on HTTP or on storage commands.
- Validation behavior cannot drift between typed, HTTP, and broker callers.
- Tests exercise behavior through the control-plane interface rather than
  duplicating controller internals.
- The runtime implementation and storage contracts now compile into the
  `KubeJob.ControlPlane` assembly. Source files retain the historical
  `KubeJob.Server.Runtime` namespace during this incremental extraction so
  existing using directives remain source-compatible. ASP.NET Server contains
  only transport, Dashboard, and composition code.
