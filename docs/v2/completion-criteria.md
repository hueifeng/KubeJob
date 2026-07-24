# Runtime Completion Criteria

The V2-only runtime and operator surface are implementation-complete when all of
the following pass on the final PR Head:

- Typed handlers and generated JobKeys compile.
- A class marked with `[KubeJob]` must implement `IKubeJob<TPayload>`.
- Unified hosting resolves the in-process worker transport.
- Remote workers resolve the HTTP worker transport.
- KubeJob controllers are discovered when the runtime is consumed as a library.
- Concurrent workers cannot claim the same current Attempt.
- A replaced Worker Session cannot renew or complete current state.
- Lease expiry requeues or terminates according to attempt policy.
- Idempotency conflicts are explicit and race-safe.
- Schedule fire advances the cursor and inserts Run + Outbox atomically.
- Outbox publishing claims recover after publisher crashes.
- Attempt query DTOs do not expose lease or fencing credentials.
- RabbitMQ notifications remain non-authoritative and polling remains a fallback.
- Dashboard Overview, Queue backlog, Runs, Attempt timeline, Worker Sessions, and
  Schedules render from V2 stores.
- Dashboard authorization can be scoped to a named ASP.NET Core policy.
- Dashboard is read-only and hides payloads by default.
- Unit/API tests, real PostgreSQL integration tests, unified E2E tests, and package
  consumer validation pass.
- NuGet output includes the typed-key analyzer and all declared packages.

Versioned placement, durable batches, broadcast, sharding, archive/performance
gates, and optional workflows remain independent follow-up features. They must
not change the accepted Run/Attempt/lease semantics.
