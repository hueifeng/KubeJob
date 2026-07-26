# ADR 006: Share one worker engine across in-process and remote transports

- Status: Accepted
- Date: 2026-07-23

## Context

The legacy unified sample sends worker traffic through a localhost HTTP endpoint.
That adds serialization, sockets, configuration, and failure modes even though
control plane and worker share one dependency-injection container.

A separate implementation for unified mode would be faster but risks diverging
from distributed Attempt, lease, cancellation, and fencing semantics.

## Decision

The worker execution engine depends on `IWorkerRuntimeClient`.

- Remote workers use `HttpWorkerRuntimeClient`.
- Unified hosts replace it with `InProcessWorkerRuntimeClient`.
- Both implementations expose the same register, heartbeat, claim, renew,
  complete, and close protocol.
- The HTTP controller and `InProcessWorkerRuntimeClient` both invoke
  `WorkerControlPlane`, so lease duration, batch limits, session fencing, and
  completion behavior have one implementation.

## Consequences

- Unified mode avoids localhost HTTP.
- Distributed and unified deployments share the same state machine.
- Handler code is independent of transport.
- Optional broker listeners pulse `IWorkerClaimTrigger`; they do not decorate or
  replace the worker runtime client.
