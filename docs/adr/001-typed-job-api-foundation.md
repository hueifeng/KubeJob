# ADR 001: Introduce a typed job API without replacing the legacy runtime

- Status: Accepted
- Date: 2026-07-23

## Context

The current public contract uses an untyped `IKubeJob` and a mutable `KubeJobContext` that exposes `IServiceProvider`. Scheduling policy, payload access, execution identity, and dependency resolution are therefore difficult to evolve independently.

A full runtime rewrite would be too risky as a first change. Existing users need a migration path while the control plane, worker protocol, persistence model, and dashboard are redesigned incrementally.

## Decision

Add an additive typed API foundation:

- `JobKey<TPayload>` provides a stable job identity tied to a payload contract.
- `IKubeJob<TPayload>` receives the payload explicitly.
- `JobExecutionContext` exposes a scoped `IServiceProvider` for middleware
  and handler dependency resolution, but never a storage connection,
  repository, lease token, or fencing token.
- `WorkerExecutionInfo` identifies the exact worker session and build handling an attempt.
- `IJobClient` defines typed enqueue, status, and cancellation operations without exposing storage or transport.
- `JobHandle` identifies the submitted logical run.
- `JobStatusSnapshot` represents latest-known user-facing state.

The legacy untyped `IKubeJob` and `KubeJobContext` are not retained: the
V2 runtime is the only runtime (see `docs/v2/README.md`).

## Consequences

### Positive

- Business handlers can use constructor injection and strongly typed payloads.
- Durable job identity is no longer coupled to CLR type names.
- Transport implementations such as PostgreSQL pull, RabbitMQ notifications, or NATS can share one client contract.
- Attempt and worker-session information can be added without exposing lease or fencing tokens to handlers.
- Runtime migration can proceed behind adapters instead of requiring a flag-day rewrite.

### Negative

- Two handler contracts temporarily coexist.
- The new interfaces are contracts only until adapters and runtime implementations are added.
- Source generation for strongly typed `Jobs.*` keys is shipped in the
  `KubeJob.Generators` analyzer (KJGEN diagnostics; see `eng/pack-v2.sh`).

## Follow-up

1. Add a legacy-handler adapter and typed handler registry.
2. Implement `IJobClient` over the current server API.
3. Separate job definitions from schedules.
4. Introduce logical runs and physical attempts with lease fencing.
5. Add PostgreSQL-backed atomic claim and transactional outbox support.
